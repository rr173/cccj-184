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
