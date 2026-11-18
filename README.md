# 本机多进程 Shuffle 交换层

离线数据加工场景：大表按目标键分给多个下游进程；慢分片可以启动第二个工人
**重做整个 split**，但下游只能使用其中一次完整 attempt——不能从甲拿一个分区、
从乙拿另一个。本项目用 Python 本机多进程实现该交换层：

* **SQLite（WAL 模式）** 保存全部协调元数据：作业绑定、attempt、段登记、
  winner、reader、事件；
* **普通文件** 保存实际分区段（自定义二进制格式，含行/段 SHA-256 摘要）；
* 工人、协调器、读取器都是独立 OS 进程，只通过数据库与文件交换数据，
  不存在只改状态表的假调度。

## 目录

```
shuffle/
  common.py       公开分区规则、摘要、段格式、SQLite schema、路径布局
  segment.py      受限内存溢写写入器 + 流式校验读取器
  coordinator.py  create/launch/claim winner/inspect/recover/cleanup/cancel
  worker.py       真实工人进程（python -m shuffle.worker）
  reader.py       固定获胜清单的分区流式读取器
  cli.py          命令行接口（python -m shuffle.cli ...）
demo/
  main_demo.py    主例 7 关卡（A1 暂停/A2 获胜/晚报不换、B 独立、空分区、
                  段写一半被杀、协调器重启、确认幂等、reader 打开时清理）
  fault_demo.py   并发申请 winner、段损坏、源身份改变、取消、提交前退出
  scale_demo.py   30 万行规模数据：5 个真实工人、溢写段、RSS/磁盘报告
  cli_smoke.py    仅用六个 CLI 接口的端到端冒烟
```

运行期数据全部落在 `.run/` 下，**仓库不包含大型数据**，重新运行即重新生成。

## 公开确定性分区规则（无进程随机 hash）

1. 分区键为 text 或 null：text 取 UTF-8 字节；null 取固定哨兵 `0x00 || "NULL"`；
2. 对 `b"shuffle-pv" || u8(partition_version) || 键字节` 计算 SHA-256；
3. 取摘要前 8 字节按**大端**解释为无符号整数，对目标分区数取模。

读取器与演示的独立校验都用同一规则复算，不依赖协调器状态。
主例选择 `partition_version=13`、4 个目标分区，使 `a1→p0、a2→p1`，
另有 `b1→p0、b2/b3→p2`，于是 `p3` 全局为空，用于验证空分区完成证据。

## 行身份与段格式

每行规范化为排序键 JSON：

```json
{"split_id":"A","row_number":1,"key":"a1","value":"alpha-one"}
```

行身份就是 `(split_id, row_number)`；内容相同但身份不同的行**不合并**。

段文件（`.seg`，小端）：

```
MAGIC(8) | header_len(4) | header_json
重复: row_len(4) | 行SHA-256原始摘要(32) | 规范行字节
FOOTER_MAGIC(8) | footer_len(4) | footer_json(row_count, seg_digest)
```

* 段摘要是段内全部规范行字节的流式 SHA-256；
* 写入先落 `.tmp`，`fsync` 后原子 `os.replace` 改名为 `.seg`，再两阶段登记
  `tmp → done`；写一半被杀只会留下 `.tmp`，对读取永远不可见；
* **空分区也写一个 0 行段**，作为“该 attempt 覆盖了此分区”的明确证据。

## 核心协议保证

| 主题 | 保证 |
| --- | --- |
| 作业绑定 | 建作业固定 split 清单（含次序）、每份源摘要、分区数、分区函数版本，之后不可改 |
| 工人 attempt | 绑定唯一 `(split, attempt_no)`；开工先复算源摘要，身份改变直接失败 |
| 受限内存 | 每分区独立缓冲，达到上限即封尾溢写新段，输入/输出全程逐行流式 |
| winner 确认 | 单 `BEGIN IMMEDIATE` 事务内流式核对**全部目标分区**（含空分区）：文件长度、行摘要、段摘要、行数、row_number 严格递增、键复算分区、总行数=绑定行数 |
| winner 固定 | 先提交者赢；晚到 attempt 持久化为 `lost`。winner 一经可读不可替换；现任 winner 重复确认幂等返回 |
| 读取 | reader 打开时固定获胜清单快照；只读 winner 段；split 按固定清单次序、split 内按 row_number；未就绪可等待但不漏过，超时显式失败，绝不返回缺行成功流 |
| 重启恢复 | winner 重新核对保持可见；进程已死的未完成 attempt 标 `failed` 且不可见；确认原 attempt 不再执行后才允许重新派工（本机用 PID 信号，不做跨主机租约） |
| 取消 | 拒绝新 reader；已开始者按原清单读完或显式失败 |
| 回收 | 只删作业目录内的受管 `.tmp/.seg`；有打开 reader 时 winner 段不删；`--destroy` 在有 open reader 时直接拒绝 |
| 损坏 | 截断、魔数/header/footer/行 JSON 损坏、行/段摘要不符、长度不符全部显式抛 `SegmentCorrupt/DigestMismatch`，不静默丢行 |

## 三个被杀位置的数据可读性

1. **段写一半**：磁盘留 `.tmp`，段表无 `done` 行，attempt 保持/恢复为
   `failed`，无 winner、reader 显式 NotReady；
2. **段齐备但未确认**：文件完整可读但 winners 表无行，协调器重启后
   `recover` 把进程已死的 attempt 标 `failed`，数据仍不可见，可重新派工；
3. **winner 已确认但工人没收到响应**：winners 行已持久，重复确认幂等返回
   同一 winner，恢复后重新核对即可读，winner 不会被替换。

## 使用方式

```bash
# 建作业（split 顺序即读取时跨 split 的交付顺序）
python -m shuffle.cli create --root .run/x --job j \
    --split A=inputs/A.jsonl --split B=inputs/B.jsonl \
    --partitions 4 --version 13

# 派工并拉起真实工人进程，完成后确认 winner
python -m shuffle.cli launch-attempt --root .run/x --job j --split A \
    --mem-bytes 65536 --spawn --wait --claim

python -m shuffle.cli inspect        --root .run/x --job j
python -m shuffle.cli read-partition --root .run/x --job j --partition 0
python -m shuffle.cli recover        --root .run/x --job j
python -m shuffle.cli cleanup        --root .run/x --job j            # 回收非 winner/临时文件
python -m shuffle.cli cleanup        --root .run/x --job j --destroy  # 无打开 reader 时销毁
```

## 演示与自测

```bash
python demo/main_demo.py    # 主例 + 7 个同步关卡
python demo/fault_demo.py   # 5 个故障/并发关卡（真实多进程竞争）
python demo/scale_demo.py   # 30 万行规模运行，输出进程/溢写/RSS/磁盘报告
python demo/cli_smoke.py    # 六个 CLI 接口端到端
```

规模关卡的典型实测（4×75,000 行、8 分区、每分区 64KiB 溢写阈值、5 个工人进程）：
875 个溢写段（清理后 winner 段 700 个）、工人峰值 RSS ≈ 20 MiB、
读取端流式峰值约 60 KiB（对照全量 `list` 收集同分区约 35 MiB）。

---

# 双输入关联算子（shuffle.join）

在交换层之上新增的可落盘双输入关联：**直接消费两个既有作业的 winner 段**
（可不同 root、不同分区数、不同分区版本；同一作业同时作为左右输入也合法，
左右角色各自登记独立保护），产出自己的关联结果文件。不读原始 split 文件，
不把交换层另写一份，也不用 SQL JOIN 或任何现成查询引擎代算——键对齐、
分组关联、热点乘积枚举、结果次序恢复全部在 `shuffle/join.py` 内以
流式磁盘算法完成；SQLite 只保存协调状态、磁盘索引与检查点。

```
shuffle/
  join_common.py  框式落盘流（逐记录摘要）、文件句柄账本、预算核算、同步关卡
  join.py         关联算子：create/run/status/read/delete 与六个执行阶段
  join_cli.py     命令行接口（python -m shuffle.join_cli ...）
demo/
  join_ref.py        朴素参考实现（内存字典，不调用产品关联逻辑）
  join_demo.py       功能关卡 J1–J13（真实上游工人建输入）
  join_scale_demo.py 规模关卡：双侧输入超内存 / 热键组超内存 / 指标报告
```

## 使用方式

```bash
# 建关联：固定两侧 winner 清单并登记持久段保护（任一侧未就绪/已取消/
# 绑定非法都显式失败，且不在另一侧留下永不释放的引用）
python -m shuffle.join_cli create --workdir .run/work --name j1 \
    --left-root .run/rootL --left-job jobL \
    --right-root .run/rootR --right-job jobR \
    --mode full --mem-bytes 268435456 --max-open-files 32 \
    --result-kind full          # full=完整原行（含 value）；identity=只存身份字段

python -m shuffle.join_cli run    --workdir .run/work --name j1   # 中断后再跑即恢复
python -m shuffle.join_cli status --workdir .run/work --name j1   # 阶段进度/接收范围/源段读取计数
python -m shuffle.join_cli read   --workdir .run/work --name j1 --start 0 --count 100
python -m shuffle.join_cli delete --workdir .run/work --name j1   # 释放自己的保护并移除目录
```

库接口：`shuffle.join.create_join / run_join / join_status / JoinResult / delete_join`；
`JoinResult.read(start, count)` 分页与 `JoinResult.iter(start)` 迭代器拼接一致。

## 执行链与检查点持久边界

```
bind      校验两侧绑定，固定 winner 清单快照，在源作业 readers 表登记持久保护
receive   逐源段校验（长度/身份/行摘要/段摘要）→ 追加接收流 → fsync → 落段级检查点
normalize 按 (split 清单序, row_number) k 路归并出两侧源序流（扇入受文件预算限制）
keysort   右侧按键字节外排 + 稀疏索引（null 键不进键流：null 永不匹配）
emit      左流按源序探测右键流，同键组流式枚举 m×n；left/full 补未匹配左行
emit_tail 仅 full：按右源序补未匹配右行（磁盘位图标记已匹配）
finalize  结果整流核验（逐条摘要/序号连续/计数/流摘要）→ 写 footer 后才可读
```

持久边界（全部先 fsync 文件、再提交 SQLite 检查点）：

* **receive**：每个源段一个检查点。恢复时已接收段**绝不重读**（状态可查
  `receive.{L,R}` 与计数器 `src_segments_read`）；接收一半的段截掉重收；
* **normalize/keysort**：阶段级检查点，产物为一次性文件（tmp+原子改名），
  恢复复用前整流校验摘要，未完成的中间 run 留在 `tmp/` 随阶段重启清理；
* **emit/emit_tail**：每 `checkpoint_rows` 结果行一个检查点（左/右源序位置、
  输出字节、索引字节、位图同步）。恢复截断到检查点重做未落定部分；
* **finalize**：整流核验通过才写 footer 并置 `done`。**只有 `done` 才可读**——
  被杀在终验前，已写出的前缀不被当作完成；结果落盘后的长度/顺序/数量/
  内容损坏，读取都显式报错。

同步关卡（`SHUFFLE_JOIN_HOOK` 或 `run --hook`，供测试在指定位置杀进程）：
`kill-after-receive-segment=k`（接收第 k 段后）、`kill-after-receive`、
`kill-after-normalize`、`kill-after-keysort`、`kill-at-emit=n`（第 n 条结果）、
`kill-before-finalize`（结果可读前）。受控终止退出码 42。

## 语义要点

* **键**：精确 text 相等（不去空白、不折叠大小写、按 UTF-8 字节）；null 永不
  相等（含 null 对 null）；空串/Unicode/含 NUL 的合法 text 各按自身值；
  分区桶相同不等于键相等（null 与文本 `"\u0000NULL"` 同桶但不误配）；
* **重数**：同键左 m 行 × 右 n 行 = m×n 对，重复内容行各自参与；
* **次序**：左 split 清单序 + split 内 row_number；匹配右行按右源序；
  full 末尾按右源序追加未匹配右行；结果序号从 0 连续；
* **结果行**：`{seq, left, right}`，左右为完整原行（split_id/row_number/key/
  value），未匹配侧整个字段为 null——与合法的 null value 严格区分；
* **预算**：总工作内存（单行上限=预算/16，超限显式报错）与同时打开文件数
  （句柄账本强制，峰值落库）；不把整边/整分区/同键大组/全部输出装进内存；
* **段保护**：加工期间源作业 destroy 被拒（进程被杀后保护仍有效）；源作业
  之后取消不换输入、不吞行；结果交付后释放保护，源作业可销毁而结果仍可读；
  删除关联只释放它自己的保护，不影响别的 reader 或关联。

## 关联演示

```bash
python demo/join_demo.py        # 功能关卡 J1–J13（规定小例/键语义/随机交叉核对/
                                # 分页/空侧/自关联/失败场景/保护生命周期/杀进程恢复/
                                # 结果损坏/预算/分区规则无关性/CLI 端到端）
python demo/join_scale_demo.py  # 规模关卡：双侧输入超内存、热键组超内存、
                                # 峰值 RSS/临时磁盘/打开文件/源段读取计数报告
```

---

# 双时间历史表（shuffle.history）

上游每天送来的归属记录会**追溯修改**过去的生效区间、也会**撤回**误报。历史表
在交换层之上持续维护两条时间轴，回答"**在当时已知的信息下，这个客户那一天
归谁**"：业务人员既能查今天认可的历史（head），也能重现上周报表使用的历史
（任意旧知识版本，永不改写）。

* **知识版本 K**（1,2,3,…）：一个已提交批次占一个版本，表示"何时获知这批
  事实"（不是业务生效时间）。空批次也占一个版本。版本只增不改。
* **业务时间 T**：有符号 64 位整数；断言区间左闭右开 `[valid_from, valid_to)`，
  `valid_to` 为 null 表示正无穷。

```
shuffle/
  history.py    历史表：建表/建批/接收/暂存/提交/放弃 + 点查/时间线/差异/导出
  hist_cli.py   命令行接口（python -m shuffle.hist_cli ...）
demo/
  history_ref.py        独立小规模参考实现（不调用产品区间逻辑）
  history_demo.py       功能关卡 H1–H8（真实工人建输入，逐项对照参考）
  history_scale_demo.py 规模关卡 S1–S3（实体总数/单实体历史超内存）
  hist_child.py         演示子进程（并发提交裁决、受计量入仓/导出）
```

## 事实模型与裁定规则

源行 `key` 是实体的精确 text 标识（null 非法；空串、NUL、Unicode 合法，不
修剪、不折叠大小写）。`value` 是一条断言或撤回：

* 断言 `{"event_id","op":"assert","valid_from","valid_to","payload"}`；
  payload 可为任意标准 JSON 值（含 null）。结果中独立的 `present` 存在标记
  区分"无事实"（空洞，`present=false`）与"事实值为 null"（`present=true`）。
* 撤回 `{"event_id","op":"retract","target_event_id"}`；目标必须是同一实体
  在**更早知识版本**已提交、尚未撤回的断言。撤回从它所属版本起生效，不改写
  旧版本的认知；被覆盖的旧断言保留，上层被撤回后下层重新露出（可能只露出
  原区间的一部分）。撤回本身不接受被撤回。
* `event_id` 全表全局唯一（断言与撤回同一命名空间）；同内容不同 event_id
  仍是两条事实。重复 ID、错实体撤回、前向引用、再次撤回都使**整批**被拒绝，
  不静默去重。

在知识版本 K、业务时点 T 的裁定：只考虑版本 ≤ K 的断言与 ≤ K 已发生的撤回；
仍有效且覆盖 T 的断言中，按 `(kversion, split 清单序, row_number)` 取**后者**
优先（知识版本越大越优先；同批内 split 清单次序靠后者优先，再同则 row_number
大者优先）。不以桶号、工人完成顺序或文件遍历顺序决胜。

## 使用方式

```bash
# 建表（独立目录 + 不可混淆的持久身份 table_token）
python -m shuffle.hist_cli create --workdir .run/work --name cust \
    --mem-bytes 268435456 --max-open-files 32

# 登记批次：固定完整 winner 清单与批次标识，目标版本 = 当前 head+1
python -m shuffle.hist_cli submit --workdir .run/work --name cust \
    --root .run/root --job day1 --batch-id 2026-11-18

# 继续完成入仓：接收→暂存→提交（中断后再跑即从检查点恢复）
python -m shuffle.hist_cli run    --workdir .run/work --name cust --batch-id 2026-11-18

python -m shuffle.hist_cli status --workdir .run/work --name cust   # head/批次进度/计数器
python -m shuffle.hist_cli point --workdir .run/work --name cust --entity c1 --time 5 --kversion 3
python -m shuffle.hist_cli timeline --workdir .run/work --name cust --entity c1 --t0 0 --t1 100 --kversion 3 --limit 50
python -m shuffle.hist_cli diff --workdir .run/work --name cust --entity c1 --t0 0 --t1 100 --kold 2 --knew 3
python -m shuffle.hist_cli export --workdir .run/work --name cust --kversion 3 --limit 1000 [--cursor ...]
python -m shuffle.hist_cli abandon --workdir .run/work --name cust --batch-id 2026-11-18
python -m shuffle.hist_cli drop --workdir .run/work --name cust
```

库接口：`shuffle.history.create_table / submit_batch / run_batch（及
receive_batch / stage_batch / commit_batch 分相）/ abandon_batch /
table_status / drop_table`；查询句柄 `HistoryTable` 提供
`point / timeline / diff / export`（均支持 `limit` 分页与 `cursor` 续读）。

## 入仓管线与持久边界

```
submit   固定完整 winner 清单（段身份/数量/长度/摘要）+ 批次标识，登记源段保护
receive  逐源段对照批次清单校验 → 追加接收流 → fsync → 段落检查点（不重读已接收）
stage    流式重读接收流，整批语义校验（单事务）→ 暂存表；非法行使整批不可见
commit   单 IMMEDIATE 事务：暂存落入 events/buckets、推进 head、标记批次
```

* **批次幂等**：相同标识 + 完全相同绑定的重送返回原结果（已提交/暂存/失败/
  放弃均原样返回，不再占版本）；换源、换清单显式冲突；未提交批次在版本推进
  后重送按"换版本"显式冲突。
* **提交裁决**：两个本机写进程同时提交同一版本时，`BEGIN IMMEDIATE` 串行化，
  只有一个可见提交；失败方 `CommitConflict`，不能覆盖或拼接，批次保持
  staged 可检查后放弃。普通查询看不到半批事实（暂存表不参与查询）。
* **崩溃恢复**：同步点 `kill-after-receive-segment=k / kill-after-receive /
  kill-after-stage / kill-after-commit`（退出码 42）。重开后每个版本要么整体
  存在要么整体不存在；已持久接收的源范围绝不重读；提交已持久但调用者未收到
  成功时重送幂等。坏段、非法末行或语义冲突使整批保持不可见，旧 head 与旧
  查询不变，可检查原因并放弃（只释放自己的引用与暂存）。不得用修改源绑定、
  回写旧知识版本或删除旧事实来"修好"恢复。
* **段保护**：批次处理期间在源作业 readers 表登记持久保护（destroy 被拒）；
  完成自有接收后释放引用，之后删除源作业不妨碍继续提交或查询已入仓历史。
* **可观测**：接收进度、校验好的范围、源文件实际打开次数、落入的断言/撤回
  数与受影响实体数均可查（`status` 的 batches/counters）。

## 查询语义

* **点查** `point(entity, t, kversion)`：获胜断言或空洞（present=false）。
* **时间线** `timeline(entity, t0, t1, kversion)`：把有限区间完整切成连续、
  不重叠、无遗漏的片段；空洞 `present=false`；有事实时返回 payload、获胜
  event_id 与原上游行身份（job/split/row_number）。只有获胜 event_id 相同的
  相邻片段才合并，不因 payload 恰好相等而抹掉来源差别。
* **差异** `diff(entity, t0, t1, kold, knew)`：按两边共同边界切分，只返回
  存在性/payload/获胜 event_id 改变的片段，并保留两边完整判定。
* **导出** `export(kversion, [t0,t1])`：流式全实体导出，按实体 UTF-8 字节序
  （BLOB memcmp，不依赖 SQLite 文本排序的偶然结果）、再按业务时间排列。
* **版本固定**：查询必须显式选知识版本或在开始时固定当前 head；查询不存在
  的未来版本显式拒绝（不自动改成最新版本）；未提交批次版本与合法空结果、
  损坏错误三者严格区分。
* **游标**：时间线/差异/导出支持受限页长与继续游标。游标绑定表的持久身份、
  固定知识版本、查询范围及当前位置（含完整性签名）；开始翻页后又提交新批次，
  后续页仍与原查询逐条一致；进程退出再打开可接着读。把游标用于另一张表、
  另一组参数、另一种接口，或破坏游标内容，都显式拒绝。

## 正确性依据、增长代价与恢复边界

* **存储**：事件只追加落 `events` 表（含逐行 SHA-256 内容摘要，读取期发现
  自身存储损坏即显式 `HistoryCorrupt`，不用空列表吞掉异常）。提交事务内同时
  写入**二分区间桶索引** `buckets`：把每条断言区间分解为 O(log span) 个规范
  二分桶（时间轴平移到无符号 [0,2^64)，正无穷为 2^64）。点查 = 65 次精确
  查找；区间查 = 每层一次范围扫描，命中的桶必属与区间相交的断言（真阳性），
  因此短区间查询只读"覆盖该区间"的事件，不重放全部已提交批次。
* **裁定**：候选按 valid_from 排序（SQLite 外部排序落盘），开放集合为 Python
  堆的扫描线算法在本模块内完成——不调用现成时态查询引擎。SQLite 只做持久
  事务、索引与排序。被 ≤K 撤回命中的断言不产生扫描位置（否则会在死断言端点
  处错误切开空洞）。
* **增长代价**：每条断言约 2·log2(span)+2 行桶索引（有限区间通常 < 50，全轴
  区间 ≤ 130）；事件与桶只增不删，存储随事实数线性增长。
* **恢复边界**：接收按段落检查点（先 fsync 接收流、再提交 SQLite 检查点）；
  语义改写先落暂存表；提交为单个 IMMEDIATE 事务。崩溃后：已持久接收的源
  范围绝不重读；暂存可重做或继续；提交原子生效。
* **预算**：`mem_bytes` 工作内存（单行上限 = mem_bytes/16，SQLite 页缓存 ≤
  mem_bytes/4，扫描线开放堆条目 ≤ mem_bytes/2/96）；`max_open_files` 自管数据
  文件句柄上限（SQLite 连接为固定常数个，不占额度）。解释器固定开销不计入
  预算，由规模演示单独报告。不把整张表、单实体全部事件、某版本全量视图或
  全部差异收进 Python 容器：导出逐实体流式，时间线/差异/导出均分页。

## 演示与自测

```bash
python demo/history_demo.py        # 功能关卡 H1–H8
python demo/history_scale_demo.py  # 规模关卡 S1–S3（指标报告）
```

* **H1 手算演示**：v1 断言 A[0,20)"old"；v2 断言 B[5,15)"new"；v3 断言
  C[8,12)null；v4 撤回 B；v5 撤回 C。版本 2 在 [0,20) 的来源依次 A/B/A，
  版本 3 为 A/B/C/B/A，版本 4 为 A/C/A，版本 5 重新只有 A；任何后续操作
  （含空批次 v6）都不改变版本 2 的查询。v3→v4 在 [0,20) 的差异只有 [5,8)
  与 [12,15) 两段 B→A，C 的 null 不当成空洞。
* **H2–H8**：同批重叠、不同分区规则、同内容不同 ID、相同 ID 跨实体冲突、
  错误撤回、批尾错误（逐项对照独立参考）；并发提交裁决与游标快照（独立
  进程交错写入与翻页）；同步点杀进程恢复；键/值语义；分页与游标；自身
  存储损坏显式报错；CLI 端到端。
* **S1–S3**：8,000 实体超内存入仓 + 全量导出；单实体 3 万事件超内存 +
  短区间查询指标（实际访问历史记录 ≪ 总事件、源打开次数 = 0）+ 独立端点
  枚举与抽样验证；大批次杀进程恢复与幂等重送。报告真实入仓/输出数量、
  RSS、临时磁盘与文件峰值。规模数据全部运行时生成，落在 `.run/` 下不进 Git。

原交换层（create/launch-attempt/inspect/read-partition/recover/cleanup）与
关联算子（join）接口保持不变，可继续独立使用。

---

# 时态引用完整性审计（shuffle.audit）

历史表回答"某实体在某段业务时间、某个知识版本里是什么"；审计回答其上的
**跨实体引用完整性**：源实体每条获胜事实在其**整个生效区间**内，payload
引用字段指向的目标实体是否始终存在。只处理已提交历史——审计不修改事件、
知识版本或 head；head 之后推进不改变已开始审计的结果。

```
shuffle/
  audit.py      审计算子：create/run/status/read/delete + 扫描/物化/终验
  audit_cli.py  命令行接口（python -m shuffle.audit_cli ...）
demo/
  audit_ref.py        独立参考（只读原始断言、枚举端点，不调产品逻辑）
  audit_demo.py       功能关卡 A1–A10（真实工人建输入，逐项对照参考）
  audit_scale_demo.py 规模关卡 AS1（7.5k 实体、3.4 万事件、超内存审计）
  audit_child.py      演示子进程（流式整流核验）
```

## 审计规格与规则

建审计时固定：历史表（绑定其持久身份 table_token）、知识版本 K（显式给定
或固定当前 head）、业务区间 `[t0, t1)`（t1 缺省为正无穷）、规则集合。
每条规则：

```json
{"rule_id": "owner-ref",
 "source": {"kind": "prefix", "prefix": "cust:"},
 "path": ["owner", "id"]}
```

* `rule_id` 全局唯一；`source.kind ∈ all / prefix / entities`（源实体范围）；
* `path` 是 payload 中的 JSON 字段路径（1–8 个非空字符串）；字段值必须是
  字符串，即目标实体键。非法字段路径、非法源范围、重复 rule_id 在建审计
  时明确报错。

## 判定语义

* **源引用区间** = 源实体在 [t0,t1)、版本 K 下按历史表裁定规则的获胜片段
  （相邻同 event_id 已合并）；空洞（present=false）没有引用，不产生结果。
* 对每段源引用区间提取目标键，用**两侧时间线的区间交集**判定整个区间
  （不是只查区间起点）：目标时间线空洞 ∩ 源区间 = 缺失子区间，逐段输出。
  目标 payload 为 JSON null 仍算存在；没有事实才是缺失。源引用值变化、
  源事实被覆盖或撤回、目标实体出现或消失，都在相应边界切开结果。
* **缺失原因**（严格区分，不混为一类）：`missing_field`（payload 缺字段，
  目标键=null）、`null_reference`（字段值为 JSON null，目标键=null）、
  `empty_target`（字段值为空串，目标键=""）、`target_absent`（目标实体
  在子区间内无事实）。
* **明确报错**：非字符串目标键、payload 不是对象等不支持的 JSON 形态，
  运行期遇到即把审计置为 `failed` 并记录规则/源实体/事件——不静默跳过
  源事实；失败审计拒绝读取。
* **结果次序**（固定）：源实体 UTF-8 字节序 → rule_id → 目标键 UTF-8
  字节序（null 目标最前）→ 区间起点 → 源 event_id。相邻区间只有（源
  实体、rule_id、目标键、源 event_id、缺失原因）完全相同且首尾相接才
  合并。每条结果记录：`{seq, entity, rule_id, target, start, end,
  kversion, event_id, reason}`（end=null 表示正无穷）。

## 使用方式

```bash
# 建审计（规则集合用 JSON 文件或内联 JSON 给出）
python -m shuffle.audit_cli create --root .run/audits --name a1 \
    --table-workdir .run/work --table cust \
    --t0 0 --t1 100000 --kversion 3 \
    --rules-file rules.json --mem-bytes 1048576 --max-open-files 24

python -m shuffle.audit_cli run    --root .run/audits --name a1   # 中断后再跑即恢复
python -m shuffle.audit_cli status --root .run/audits --name a1   # 状态/进度/真实读取量
python -m shuffle.audit_cli read   --root .run/audits --name a1 --limit 100 [--cursor ...]
python -m shuffle.audit_cli delete --root .run/audits --name a1   # 只移除审计自己的目录
```

库接口：`shuffle.audit.create_audit / run_audit / audit_status / read_audit /
delete_audit`。`read_audit(limit, cursor)` 分页拼接与整次读取一致。

## 实现路线与代价（路线 a：逐源区间读目标时间线 + 流式求交）

候选路线：(a) 逐个源引用区间用历史表持久桶索引读取对应目标实体的时间线
并流式求交；(b) 把双方相关区间有界外排后按目标键归并扫描。本实现采用
**(a) 扫描 + 有界外排定序**，两条路线在规格上必须得到完全相同的缺失区间
与固定结果次序——正确性由独立端点枚举参考（`demo/audit_ref.py`，不调用
产品 timeline/审计逻辑）逐条对照保证。

* **读取代价**：O(源获胜片段数 × (65 次桶探测 + 相交目标事件数))。短业务
  区间下桶索引只取与源片段相交的目标事件（真阳性），不重放目标全部历史；
  源/目标各一条只读连接（各自独立的临时候选表，互不干扰）。真实读取量
  （源/目标候选数、检查事件数、目标查询次数、源段打开次数=0）全部落
  counters 可查。
* **暂存空间**：缺失发现先落审计自有 SQLite 的 findings 表；物化时受限
  内存分块排序（块 = mem_bytes/4）溢写 run 文件（审计 `tmp/` 目录），
  受限扇入归并后相邻合并，写框式结果流 + 稀疏索引（tmp + 原子改名）。
  全部缺失区间不进入 Python 容器。
* **恢复边界**：扫描按 (源实体, 规则) 对一个事务（findings + 进度检查点
  同事务提交），已提交的对绝不重算；物化检查点（记录数/摘要/字节数）
  落库后结果文件才生效；终验整流重读（逐条摘要、序号连续、次序严格
  递增、无应合并而未合并的相邻记录、总数与摘要等于检查点）通过才置
  `done`。同步关卡（`SHUFFLE_AUDIT_HOOK` 或 `--hook`）：
  `kill-after-audit-pair=N` / `kill-after-materialize` / `kill-before-done`，
  受控终止退出码 42。
* **读取**：只有 `done` 才可读——结果未完成或已损坏时不返回成功前缀。
  每次打开复核 header/footer 与元数据的记录数、摘要；读到的每条记录
  再验摘要与序号。游标绑定审计身份、表持久身份、知识版本、业务区间、
  规则集合与当前位置（含完整性签名）：换审计、换参数、换表或破坏游标
  都显式拒绝；进程退出再打开可接着读。
* **预算**：`mem_bytes`（单行上限 = mem_bytes/16，排序块 = mem_bytes/4，
  SQLite 页缓存 ≤ mem_bytes/4）与 `max_open_files`（审计自管数据文件
  句柄上限，句柄账本强制，峰值落库）；历史表查询侧上限由历史表自身
  预算约束。全表实体、单个实体的全部历史、全部缺失区间都不放入
  Python 容器。所有审计文件限于自身受管目录；删除或失败不影响历史表、
  其他审计或正在读取的结果。

## 审计演示

```bash
python demo/audit_demo.py        # 功能关卡 A1–A10
python demo/audit_scale_demo.py  # 规模关卡 AS1（指标报告）
```

* **A1–A10**：规格手算（目标 [0,10) 存在/[10,20) 缺失 × 源 [5,15) 只报
  [10,15)；null payload 的 [15,20) 仍存在；v2 撤回目标后 v1 审计与游标
  结果不变）、源引用替换、局部覆盖后撤回、缺字段/空串/null/非法类型、
  目标与源边界重合、负时间/int64 极值/无穷端点、空结果、分页跨页、
  翻页途中提交新知识版本、三同步点杀进程恢复（已提交对绝不重算）、
  审计文件损坏（记录/索引/截断）、两个审计并行清理、CLI 端到端。
* **AS1**：5,000 源实体 + 2,500 目标实体 + 热点实体（3,000 事件）、
  6 批交错长历史（33,992 事件），审计预算 1 MiB、短业务区间
  [-30000, 30000)。实测：扫描源区间 12,156 段、目标查询 12,156 次、
  源段打开 0 次、实际检查目标事件 119 条（≪ 目标事件总数 5,990，持久
  索引收窄）、缺失输出 12,143 条、审计进程峰值 RSS ≈ 23 MiB、临时磁盘
  峰值 ≈ 8.9 MiB（超 1 MiB 预算，外排生效）、句柄峰值 9；25 个源实体
  抽样与独立参考逐条一致。

原交换层、关联算子（join）与历史表（history）既有接口保持不变；本次新增
的是跨实体引用完整性审计，不重做已有的单实体时间线、差异查询或双输入
关联。
