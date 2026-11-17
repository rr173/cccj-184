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
