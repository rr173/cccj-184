"""双时间历史表：在 shuffle 交换层之上持续维护"何时获知 × 何时生效"的历史维度。

两条时间轴：
  * **知识版本 K**（1,2,3,...）：一个已提交批次占一个版本，表示"我们何时获知
    这批事实"。空批次也占版本。版本只增不改，旧版本永不重写。
  * **业务时间 T**：有符号 64 位整数；断言区间左闭右开 ``[valid_from, valid_to)``，
    ``valid_to`` 为 null 表示正无穷。

事实模型（源行 value）：
  * 断言 ``{"event_id","op":"assert","valid_from","valid_to","payload"}``；
    payload 可为任意标准 JSON 值（含 null）。"无事实"与"事实值为 null"由结果中
    独立的 ``present`` 存在标记区分。
  * 撤回 ``{"event_id","op":"retract","target_event_id"}``；目标必须是同一实体
    在更早知识版本已提交、尚未撤回的断言。撤回从它所属版本起生效，不改写旧版本
    的认知；被覆盖的旧断言保留，上层被撤回后下层重新露出（可能只露出一部分）。
  * ``event_id`` 全表全局唯一（断言与撤回同一命名空间）；重复 ID、错实体撤回、
    前向引用、再次撤回都使**整批**被拒绝，不静默去重。

裁定规则（在知识版本 K、业务时点 T）：
  只考虑版本 ≤ K 的断言与 ≤ K 已发生的撤回；仍有效且覆盖 T 的断言中，
  按 ``(kversion, split 清单序, row_number)`` 取**后者**优先（知识版本越大越优先，
  同批内 split 清单次序靠后者优先，再同则 row_number 大者优先）。不以桶号、
  工人完成顺序或文件遍历顺序决胜。

存储与正确性依据
----------------
* 事件只追加落 ``events`` 表（含逐行 SHA-256 内容摘要 ``edigest``），提交事务
  内同时写入**二分区间桶索引** ``buckets``：把每条断言区间分解为 O(log span)
  个规范二分桶（时间轴平移到无符号 [0, 2^64)，正无穷为 2^64）。点查 = 65 次精确
  查找；区间查 = 每层的范围扫描，命中的桶必属与区间相交的断言（真阳性），
  因此短区间查询只读"覆盖该区间"的事件，不重放全部已提交批次。
* 区间裁定（可见分层）由本模块的扫描线算法完成：候选断言按 valid_from 排序
  （SQLite 外部排序落盘），开放集合为 Python 堆，逐端点输出获胜者——不调用
  任何现成时态查询引擎。SQLite 只做持久事务、索引与排序。
* 增长代价：每条断言约 2·log2(span)+2 行桶索引（有限区间通常 < 50，全轴区间
  ≤ 130）；事件与桶只增不删，存储随事实数线性增长。
* 恢复边界：接收按段落检查点（先 fsync 接收流、再提交 SQLite 检查点）；
  语义改写先落暂存表（staging），提交为单个 IMMEDIATE 事务（插 events/buckets、
  推进 head、标记批次）——每个版本要么整体存在要么整体不存在。崩溃后：
  已持久接收的源范围绝不重读；暂存可重做或继续；提交已持久但调用者未收到
  成功时，重送同标识同绑定幂等返回原结果，不再占版本。

预算：``mem_bytes`` 工作内存（单行上限 = mem_bytes/16，SQLite 页缓存 ≤
mem_bytes/4，扫描线开放堆条目 ≤ mem_bytes/2/96）；``max_open_files`` 自管数据
文件句柄上限（SQLite 连接为固定常数个，不占额度）。解释器固定开销不计入预算，
由规模演示单独报告。
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import struct
import time
import uuid
from dataclasses import dataclass
from typing import Any, Iterator, Optional

from . import coordinator as coord
from .common import (
    JOB_CANCELLED,
    JOB_DESTROYED,
    Layout,
    ShuffleError,
    connect,
    json_dumps,
    json_loads,
)
from .join_common import (
    BudgetExceeded,
    FileLedger,
    FramedWriter,
    hook_fire,
    parse_hook,
    read_header,
)
from .segment import iter_segment_rows

# --------------------------------------------------------------------------- #
# 常量
# --------------------------------------------------------------------------- #
INT64_MIN = -(1 << 63)
INT64_MAX = (1 << 63) - 1
U_INF = 1 << 64           # 平移轴上的正无穷（valid_to=NULL / 查询上界缺省）
_BIAS = 1 << 63           # 无符号桶号 → SQLite 有符号 INTEGER 的平移量
LEVELS = 65               # 二分桶层数（[0,2^64) 轴）
HOOK_EXIT_CODE = 42

B_RECEIVING = "receiving"   # 已建批次，接收中
B_STAGING = "staging"       # 接收完成，语义校验/暂存中
B_STAGED = "staged"         # 暂存完成，待提交
B_COMMITTED = "committed"
B_FAILED = "failed"
B_ABANDONED = "abandoned"

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;

CREATE TABLE IF NOT EXISTS meta(
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batches(
    batch_id         TEXT PRIMARY KEY,
    kversion         INTEGER NOT NULL,        -- 建批时固定的目标知识版本（head+1）
    state            TEXT NOT NULL,
    src_root         TEXT NOT NULL,
    src_job          TEXT NOT NULL,
    manifest_json    TEXT NOT NULL,           -- 建批固定的完整 winner 清单
    manifest_digest  TEXT NOT NULL,
    reader_id        TEXT NOT NULL,           -- 源作业 readers 表中的保护标识
    protection_held  INTEGER NOT NULL DEFAULT 1,
    recv_path        TEXT,
    recv_segments    INTEGER NOT NULL DEFAULT 0,   -- 已持久接收的段数
    recv_rows        INTEGER NOT NULL DEFAULT 0,
    recv_bytes       INTEGER NOT NULL DEFAULT 0,
    asserts          INTEGER,
    retracts         INTEGER,
    entities         INTEGER,
    error            TEXT,
    created_at       REAL NOT NULL,
    committed_at     REAL
);
CREATE TABLE IF NOT EXISTS batch_segments(
    batch_id    TEXT NOT NULL REFERENCES batches(batch_id),
    split_ord   INTEGER NOT NULL,
    partition   INTEGER NOT NULL,
    seq         INTEGER NOT NULL,
    relpath     TEXT NOT NULL,
    row_count   INTEGER NOT NULL,
    byte_length INTEGER NOT NULL,
    seg_digest  TEXT NOT NULL,
    state       TEXT NOT NULL DEFAULT 'pending',   -- pending/received
    recv_offset INTEGER,
    recv_bytes  INTEGER,
    recv_rows   INTEGER,
    PRIMARY KEY (batch_id, split_ord, partition, seq)
);
CREATE TABLE IF NOT EXISTS batch_events(
    batch_id     TEXT NOT NULL REFERENCES batches(batch_id),
    seq          INTEGER NOT NULL,            -- 接收次序（从 0）
    entity_key   TEXT NOT NULL,
    entity_bytes BLOB NOT NULL,
    event_id     TEXT NOT NULL,
    op           TEXT NOT NULL,
    valid_from   INTEGER,
    valid_to     INTEGER,                     -- NULL = 正无穷
    payload_json TEXT,                        -- 规范化 JSON 文本（可为 'null'）
    target_event_id TEXT,
    src_ord      INTEGER NOT NULL,
    src_row      INTEGER NOT NULL,
    src_split    TEXT NOT NULL,
    PRIMARY KEY (batch_id, seq),
    UNIQUE (batch_id, event_id)               -- 批内 event_id 唯一（含跨实体）
);
CREATE TABLE IF NOT EXISTS events(
    entity_bytes BLOB NOT NULL,
    entity_key   TEXT NOT NULL,
    kversion     INTEGER NOT NULL,
    src_ord      INTEGER NOT NULL,
    src_row      INTEGER NOT NULL,
    event_id     TEXT NOT NULL,
    op           TEXT NOT NULL,               -- assert/retract
    valid_from   INTEGER,
    valid_to     INTEGER,                     -- NULL = 正无穷
    payload_json TEXT,
    target_event_id TEXT,
    src_job      TEXT NOT NULL,
    src_split    TEXT NOT NULL,
    edigest      TEXT NOT NULL                -- 逐行内容摘要（读取期损坏检测）
);
CREATE UNIQUE INDEX IF NOT EXISTS events_event_id ON events(event_id);
CREATE INDEX IF NOT EXISTS events_entity ON events(entity_bytes, kversion, src_ord, src_row);
CREATE INDEX IF NOT EXISTS events_target ON events(target_event_id);
CREATE TABLE IF NOT EXISTS buckets(
    entity_bytes BLOB NOT NULL,
    lvl          INTEGER NOT NULL,
    bidx         INTEGER NOT NULL,            -- 无符号桶号减 2^63（保序）
    rid          INTEGER NOT NULL             -- events.rowid
);
CREATE INDEX IF NOT EXISTS buckets_idx ON buckets(entity_bytes, lvl, bidx, rid);
CREATE TABLE IF NOT EXISTS counters(
    name  TEXT PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS hist_events(
    ts     REAL NOT NULL,
    kind   TEXT NOT NULL,
    detail TEXT NOT NULL
);
"""


# --------------------------------------------------------------------------- #
# 错误类型
# --------------------------------------------------------------------------- #
class HistoryError(ShuffleError):
    """历史表所有可预期失败的基类。"""


class HistoryNotReady(HistoryError):
    """源作业未就绪/已取消/已销毁，或表不存在。"""


class BatchConflict(HistoryError):
    """同批次标识但绑定（源/清单/版本）不一致。"""


class CommitConflict(HistoryError):
    """提交裁决失败：下一版本已被其他批次占用，本批次不得覆盖或拼接。"""


class BatchFailed(HistoryError):
    """批次校验失败（坏段/非法行/语义冲突），整批不可见，可检查后放弃。"""


class VersionNotFound(HistoryError):
    """查询的知识版本不存在（未来版本不得自动改成最新版本）。"""


class UncommittedVersion(HistoryError):
    """查询的版本对应一个尚未提交的批次。"""


class CursorError(HistoryError):
    """游标被用于另一张表/另一组参数/另一种接口，或内容被破坏。"""


class HistoryCorrupt(HistoryError):
    """读取期发现自身存储损坏（内容摘要/结构不一致）。"""


# --------------------------------------------------------------------------- #
# 布局与预算
# --------------------------------------------------------------------------- #
class HistLayout:
    def __init__(self, workdir: str, name: str) -> None:
        if not name or any(c in name for c in "/\\\x00") or name in (".", ".."):
            raise HistoryError(f"非法表名: {name!r}")
        self.workdir = os.path.abspath(workdir)
        self.name = name
        self.dir = os.path.join(self.workdir, name)

    @property
    def db_path(self) -> str:
        return os.path.join(self.dir, "history.sqlite3")

    @property
    def recv_dir(self) -> str:
        return os.path.join(self.dir, "recv")

    def recv_path(self, batch_id: str) -> str:
        # batch_id 可含任意字符：文件名取摘要，身份仍在库内
        return os.path.join(self.recv_dir, f"batch-{hashlib.sha256(batch_id.encode('utf-8')).hexdigest()[:24]}.recv")


@dataclass(frozen=True)
class HistBudget:
    mem_bytes: int
    max_open_files: int

    def __post_init__(self) -> None:
        if self.mem_bytes < 8192:
            raise HistoryError("工作内存预算至少 8192 字节")
        if self.max_open_files < 12:
            raise HistoryError("同时打开文件预算至少 12")

    @property
    def single_row_limit(self) -> int:
        return self.mem_bytes // 16

    @property
    def recv_buffer(self) -> int:
        return self.mem_bytes // 8

    @property
    def sqlite_cache_kb(self) -> int:
        return max(16, self.mem_bytes // 4 // 1024)

    @property
    def sweep_open_limit(self) -> int:
        """扫描线开放堆条目上限（每条按 ~96 字节估算，占预算一半）。"""
        return max(1024, self.mem_bytes // 2 // 96)


# --------------------------------------------------------------------------- #
# 小工具
# --------------------------------------------------------------------------- #
def _connect_hist(hl: HistLayout, cache_kb: int = 256) -> sqlite3.Connection:
    conn = connect(hl.db_path)
    conn.executescript(SCHEMA)
    conn.execute(f"PRAGMA cache_size={-cache_kb}")   # 页缓存上界（KiB）
    conn.execute("PRAGMA temp_store=1")              # 临时表落盘，不占程序内存
    conn.commit()
    return conn


def _meta_get(conn: sqlite3.Connection, key: str, default=None):
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return json_loads(row["value"].encode("utf-8")) if row else default


def _meta_set(conn: sqlite3.Connection, key: str, value) -> None:
    conn.execute(
        "INSERT INTO meta(key,value) VALUES (?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, json_dumps(value).decode("utf-8")),
    )


def _counter_add(conn: sqlite3.Connection, name: str, delta: int) -> None:
    conn.execute(
        "INSERT INTO counters(name,value) VALUES (?,?) "
        "ON CONFLICT(name) DO UPDATE SET value=value+excluded.value",
        (name, delta),
    )


def _counter_get(conn: sqlite3.Connection, name: str) -> int:
    row = conn.execute("SELECT value FROM counters WHERE name=?", (name,)).fetchone()
    return int(row["value"]) if row else 0


def _hevent(conn: sqlite3.Connection, kind: str, detail: str = "") -> None:
    conn.execute("INSERT INTO hist_events(ts,kind,detail) VALUES (?,?,?)",
                 (time.time(), kind, detail))


def _head_of(conn: sqlite3.Connection) -> int:
    return int(_meta_get(conn, "head", 0))


def _is_int64(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool) and INT64_MIN <= v <= INT64_MAX


def _shift(t: int) -> int:
    """业务时间 → 平移轴 [0, 2^64)；None（正无穷端点）→ 2^64。"""
    if t is None:
        return U_INF
    return t + _BIAS


def _unshift(u: int) -> Optional[int]:
    if u >= U_INF:
        return None
    return u - _BIAS


def _canon_payload(payload: Any) -> str:
    """payload 的规范化 JSON 文本；非标准 JSON（NaN/Infinity）显式拒绝。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def _cover(u0: int, u1: int) -> Iterator[tuple[int, int]]:
    """把 [u0,u1) 分解为规范二分桶（每层至多 2 个，总计 O(log span)）。"""
    while u0 < u1:
        align = (u0 & -u0).bit_length() - 1 if u0 else 64
        lvl = min(align, (u1 - u0).bit_length() - 1)
        yield lvl, u0 >> lvl
        u0 += 1 << lvl


def _event_digest(entity_bytes: bytes, kversion: int, src_ord: int, src_row: int,
                  event_id: str, op: str, valid_from: Optional[int],
                  valid_to: Optional[int], payload_json: Optional[str],
                  target_event_id: Optional[str], src_job: str, src_split: str) -> str:
    """事件行内容摘要：读取期据此发现自身存储损坏（SQLite 页无校验和）。"""
    canon = json_dumps({
        "e": entity_bytes.hex(), "kv": kversion, "so": src_ord, "sr": src_row,
        "id": event_id, "op": op, "vf": valid_from, "vt": valid_to,
        "p": payload_json, "t": target_event_id, "j": src_job, "s": src_split,
    })
    return hashlib.sha256(canon).hexdigest()


def _check_int64_param(v: Any, what: str) -> int:
    if not _is_int64(v):
        raise HistoryError(f"{what} 必须是有符号 64 位整数，得到 {v!r}")
    return int(v)


# --------------------------------------------------------------------------- #
# 建表 / 状态
# --------------------------------------------------------------------------- #
def create_table(workdir: str, name: str, *, mem_bytes: int,
                 max_open_files: int) -> dict:
    """创建历史表：独立目录 + 不可混淆的持久身份（table_token）。"""
    budget = HistBudget(mem_bytes, max_open_files)
    hl = HistLayout(workdir, name)
    if os.path.exists(hl.dir):
        raise HistoryError(f"历史表 {name!r} 已存在: {hl.dir}")
    try:
        os.makedirs(hl.dir, exist_ok=False)
    except FileExistsError as exc:
        raise HistoryError(f"历史表 {name!r} 已存在（并发创建）: {hl.dir}") from exc
    os.makedirs(hl.recv_dir)
    token = uuid.uuid4().hex
    conn = _connect_hist(hl, budget.sqlite_cache_kb)
    try:
        _meta_set(conn, "name", name)
        _meta_set(conn, "table_token", token)
        _meta_set(conn, "head", 0)
        _meta_set(conn, "mem_bytes", mem_bytes)
        _meta_set(conn, "max_open_files", max_open_files)
        _meta_set(conn, "created_at", time.time())
        _hevent(conn, "table_created", f"mem={mem_bytes} files={max_open_files}")
        conn.commit()
    finally:
        conn.close()
    return {"name": name, "dir": hl.dir, "table_token": token, "head": 0,
            "budgets": {"mem_bytes": mem_bytes, "max_open_files": max_open_files}}


def _load_table(conn: sqlite3.Connection) -> dict:
    meta = {k: _meta_get(conn, k) for k in
            ("name", "table_token", "head", "mem_bytes", "max_open_files", "created_at")}
    if meta["table_token"] is None:
        raise HistoryCorrupt("历史表元数据缺失 table_token（目录损坏或不是历史表）")
    return meta


def _open_table_conn(workdir: str, name: str) -> tuple[HistLayout, sqlite3.Connection, dict]:
    hl = HistLayout(workdir, name)
    if not os.path.exists(hl.db_path):
        raise HistoryNotReady(f"历史表不存在: {hl.dir}")
    conn = _connect_hist(hl)
    meta = _load_table(conn)
    budget = HistBudget(int(meta["mem_bytes"]), int(meta["max_open_files"]))
    conn.execute(f"PRAGMA cache_size={-budget.sqlite_cache_kb}")
    return hl, conn, meta


def table_status(workdir: str, name: str) -> dict:
    """表状态：head、待处理批次进度、校验好的范围、源文件打开次数、
    落入的断言/撤回数与受影响实体数、预算与句柄峰值。"""
    hl, conn, meta = _open_table_conn(workdir, name)
    try:
        batches = []
        for b in conn.execute(
                "SELECT * FROM batches ORDER BY created_at, batch_id").fetchall():
            seg_tot = conn.execute(
                "SELECT COUNT(*) AS n FROM batch_segments WHERE batch_id=?",
                (b["batch_id"],)).fetchone()["n"]
            seg_done = conn.execute(
                "SELECT COUNT(*) AS n FROM batch_segments WHERE batch_id=?"
                " AND state='received'", (b["batch_id"],)).fetchone()["n"]
            seg_ranges = [dict(r) for r in conn.execute(
                "SELECT split_ord, partition, seq, state, recv_offset, recv_bytes,"
                " recv_rows FROM batch_segments WHERE batch_id=?"
                " ORDER BY split_ord, partition, seq", (b["batch_id"],)).fetchall()]
            batches.append({
                "batch_id": b["batch_id"], "kversion": b["kversion"],
                "state": b["state"],
                "source": {"root": b["src_root"], "job": b["src_job"]},
                "manifest_digest": b["manifest_digest"],
                "receive": {"segments_done": seg_done, "segments_total": seg_tot,
                            "rows": b["recv_rows"], "bytes": b["recv_bytes"],
                            "ranges": seg_ranges},
                "asserts": b["asserts"], "retracts": b["retracts"],
                "entities": b["entities"], "error": b["error"],
                "protection_held": bool(b["protection_held"]),
                "created_at": b["created_at"], "committed_at": b["committed_at"],
            })
        counters = {r["name"]: int(r["value"]) for r in
                    conn.execute("SELECT name, value FROM counters").fetchall()}
        return {
            "name": name, "dir": hl.dir, "table_token": meta["table_token"],
            "head": int(meta["head"]),
            "budgets": {
                "mem_bytes": meta["mem_bytes"],
                "max_open_files": meta["max_open_files"],
                "single_row_limit": meta["mem_bytes"] // 16,
                "sqlite_cache_kb": max(16, meta["mem_bytes"] // 4 // 1024),
            },
            "batches": batches,
            "counters": counters,
            "events_total": counters.get("asserts_landed", 0) + counters.get("retracts_landed", 0),
            "buckets_total": counters.get("buckets_landed", 0),
        }
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 源快照与段保护（复用交换层 readers 表保护协议）
# --------------------------------------------------------------------------- #
def _snapshot_source(root: str, job_id: str, reader_id: str) -> dict:
    """登记持久段保护后固定完整 winner 清单。失败抛 HistoryNotReady 且不留引用。"""
    layout = Layout(root)
    if not os.path.exists(layout.db_path):
        raise HistoryNotReady(f"源 root 不存在协调库: {layout.db_path}")
    conn = connect(layout.db_path)
    try:
        coord.init_db(conn)
        try:
            job = coord.get_job(conn, job_id)
        except ShuffleError as exc:
            raise HistoryNotReady(f"源作业绑定非法: {exc}") from exc
        if job["state"] == JOB_CANCELLED:
            raise HistoryNotReady(f"源作业 {job_id} 已取消，拒绝建批")
        if job["state"] == JOB_DESTROYED:
            raise HistoryNotReady(f"源作业 {job_id} 已销毁，拒绝建批")
        # 先登记保护，再固定清单：此后源 destroy 必被拒绝
        conn.execute(
            "INSERT INTO readers(reader_id,job_id,opened_at,state) VALUES (?,?,?,'open')",
            (reader_id, job_id, time.time()),
        )
        splits = conn.execute(
            "SELECT * FROM job_splits WHERE job_id=? ORDER BY ord", (job_id,)
        ).fetchall()
        if not splits:
            raise HistoryNotReady(f"源作业 {job_id} 没有绑定 split")
        num_p = int(job["num_partitions"])
        manifest = []
        for sp in splits:
            w = conn.execute(
                "SELECT attempt_no FROM winners WHERE job_id=? AND split_id=?",
                (job_id, sp["split_id"]),
            ).fetchone()
            if w is None:
                raise HistoryNotReady(
                    f"源 split {sp['split_id']} 尚无 winner，历史表不读取未就绪输入"
                )
            attempt_no = int(w["attempt_no"])
            segs = conn.execute(
                "SELECT * FROM segments WHERE job_id=? AND split_id=? AND attempt_no=? "
                "AND state='done' ORDER BY partition_id, seq",
                (job_id, sp["split_id"], attempt_no),
            ).fetchall()
            covered = {int(s["partition_id"]) for s in segs}
            if covered != set(range(num_p)):
                raise HistoryNotReady(
                    f"源 winner {sp['split_id']}#{attempt_no} 段登记不完整"
                    f"（覆盖 {sorted(covered)}，应为 0..{num_p - 1}）"
                )
            manifest.append({
                "split_id": sp["split_id"], "ord": int(sp["ord"]),
                "attempt_no": attempt_no, "row_count": int(sp["row_count"]),
                "input_digest": sp["input_digest"],
                "segments": [{
                    "partition": int(s["partition_id"]), "seq": int(s["seq"]),
                    "relpath": s["relpath"], "row_count": int(s["row_count"]),
                    "byte_length": int(s["byte_length"]), "seg_digest": s["seg_digest"],
                } for s in segs],
            })
        conn.commit()
        snap = {
            "root": os.path.abspath(root), "job_id": job_id,
            "num_partitions": num_p,
            "partition_version": int(job["partition_version"]),
            "reader_id": reader_id,
            "manifest": manifest,
        }
        snap["manifest_digest"] = hashlib.sha256(json_dumps(
            {k: v for k, v in snap.items() if k != "reader_id"})).hexdigest()
        return snap
    except Exception:
        try:
            conn.execute(
                "UPDATE readers SET state='closed', closed_at=?"
                " WHERE reader_id=? AND state='open'",
                (time.time(), reader_id),
            )
            conn.commit()
        finally:
            conn.close()
        raise
    finally:
        try:
            conn.close()
        except sqlite3.ProgrammingError:
            pass


def _release_protection(src_root: str, reader_id: str) -> None:
    layout = Layout(src_root)
    if not os.path.exists(layout.db_path):
        return
    conn = connect(layout.db_path)
    try:
        conn.execute(
            "UPDATE readers SET state='closed', closed_at=? WHERE reader_id=? AND state='open'",
            (time.time(), reader_id),
        )
        conn.commit()
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 建批（绑定完整 winner 清单 + 批次标识；重送幂等；换源/换版本/换清单冲突）
# --------------------------------------------------------------------------- #
def _batch_record(conn: sqlite3.Connection, b: sqlite3.Row) -> dict:
    return {
        "batch_id": b["batch_id"], "kversion": b["kversion"], "state": b["state"],
        "source": {"root": b["src_root"], "job": b["src_job"]},
        "manifest_digest": b["manifest_digest"],
        "receive": {"segments_done": b["recv_segments"], "rows": b["recv_rows"],
                    "bytes": b["recv_bytes"]},
        "asserts": b["asserts"], "retracts": b["retracts"],
        "entities": b["entities"], "error": b["error"],
        "committed_at": b["committed_at"],
    }


def submit_batch(workdir: str, name: str, *, src_root: str, src_job: str,
                 batch_id: str) -> dict:
    """登记一个批次：固定完整 winner 清单与批次标识，目标版本 = 当前 head+1。

    相同标识 + 完全相同绑定的重送返回原结果（已提交/暂存/失败/放弃均原样返回，
    不再占版本）；换源、换清单显式冲突；批次仍处于未提交状态而版本已推进
    （head+1 改变）时重送按"换版本"显式冲突。
    """
    if not batch_id or not isinstance(batch_id, str):
        raise HistoryError("batch_id 必须是非空 text")
    hl, conn, meta = _open_table_conn(workdir, name)
    try:
        head = int(meta["head"])

        def _resend_check(existing: sqlite3.Row) -> dict:
            # 绑定核对：源作业存活则重算清单摘要；源已销毁/取消/丢失则核对 (root, job)
            same = None
            if os.path.exists(Layout(existing["src_root"]).db_path):
                try:
                    probe = _snapshot_source(
                        existing["src_root"], existing["src_job"],
                        reader_id=f"hist-probe:{uuid.uuid4().hex[:12]}")
                except HistoryNotReady:
                    probe = None
                if probe is not None:
                    try:
                        same = (probe["manifest_digest"] == existing["manifest_digest"]
                                and os.path.abspath(src_root) == existing["src_root"]
                                and src_job == existing["src_job"])
                    finally:
                        _release_protection(existing["src_root"], probe["reader_id"])
            if same is None:
                same = (os.path.abspath(src_root) == existing["src_root"]
                        and src_job == existing["src_job"])
            if not same:
                raise BatchConflict(
                    f"批次标识 {batch_id!r} 已绑定其他源/清单（digest "
                    f"{existing['manifest_digest'][:12]}…），换源或换清单必须冲突"
                )
            if existing["state"] in (B_RECEIVING, B_STAGING, B_STAGED) and \
                    int(existing["kversion"]) != head + 1:
                raise BatchConflict(
                    f"批次 {batch_id!r} 绑定知识版本 {existing['kversion']}，"
                    f"但当前下一版本为 {head + 1}：换版本必须冲突；"
                    "请先放弃旧批次再以新标识重送"
                )
            return _batch_record(conn, existing)

        existing = conn.execute(
            "SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        if existing is not None:
            return _resend_check(existing)

        reader_id = f"hist:{name}:{batch_id}:{meta['table_token'][:8]}:{uuid.uuid4().hex[:8]}"
        snap = _snapshot_source(src_root, src_job, reader_id)
        kversion = head + 1
        recv_path = hl.recv_path(batch_id)
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO batches(batch_id,kversion,state,src_root,src_job,"
                "manifest_json,manifest_digest,reader_id,protection_held,recv_path,"
                "created_at) VALUES (?,?,?,?,?,?,?,?,1,?,?)",
                (batch_id, kversion, B_RECEIVING, snap["root"], src_job,
                 json_dumps(snap["manifest"]).decode("utf-8"),
                 snap["manifest_digest"], reader_id, recv_path, time.time()),
            )
            for sp in snap["manifest"]:
                for sg in sp["segments"]:
                    conn.execute(
                        "INSERT INTO batch_segments(batch_id,split_ord,partition,seq,"
                        "relpath,row_count,byte_length,seg_digest,state) "
                        "VALUES (?,?,?,?,?,?,?,?,'pending')",
                        (batch_id, sp["ord"], sg["partition"], sg["seq"], sg["relpath"],
                         sg["row_count"], sg["byte_length"], sg["seg_digest"]),
                    )
            _hevent(conn, "batch_submitted",
                    f"batch={batch_id} kversion={kversion} job={src_job} "
                    f"segments={sum(len(sp['segments']) for sp in snap['manifest'])}")
            conn.commit()
        except sqlite3.IntegrityError:
            # 另一写进程同时提交了同标识批次：回滚后按重送做幂等核对
            conn.rollback()
            _release_protection(snap["root"], reader_id)
            existing = conn.execute(
                "SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if existing is None:
                raise HistoryCorrupt("批次并发登记后读取不到（存储损坏）")
            return _resend_check(existing)
        except Exception:
            conn.rollback()
            _release_protection(snap["root"], reader_id)
            raise
        b = conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        return _batch_record(conn, b)
    finally:
        conn.close()


def _release_if_held(conn: sqlite3.Connection, b: sqlite3.Row) -> None:
    """幂等释放本批次在源作业的段保护（接收完成后各阶段补齐，防崩溃泄漏）。"""
    if not b["protection_held"]:
        return
    try:
        _release_protection(b["src_root"], b["reader_id"])
    except Exception as exc:  # noqa: BLE001 - 源库缺失不阻断
        _hevent(conn, "release_warning", str(exc))
    conn.execute("UPDATE batches SET protection_held=0 WHERE batch_id=?", (b["batch_id"],))
    conn.commit()


def _get_batch(conn: sqlite3.Connection, batch_id: str) -> sqlite3.Row:
    b = conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
    if b is None:
        raise HistoryError(f"批次不存在: {batch_id!r}")
    return b


def _check_next_version(conn: sqlite3.Connection, b: sqlite3.Row) -> None:
    head = _head_of(conn)
    if int(b["kversion"]) != head + 1:
        raise CommitConflict(
            f"批次 {b['batch_id']!r} 绑定知识版本 {b['kversion']}，当前下一版本为 "
            f"{head + 1}：该批次不再可提交（不得覆盖或拼接），请检查后放弃"
        )


# --------------------------------------------------------------------------- #
# 接收：逐源段对照批次固定清单校验（身份/数量/长度/摘要），持久化到接收流
# --------------------------------------------------------------------------- #
def _recv_recorded_end(conn: sqlite3.Connection, batch_id: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(recv_offset + recv_bytes),0) AS e FROM batch_segments"
        " WHERE batch_id=? AND state='received'", (batch_id,)).fetchone()
    return int(row["e"])


def _iter_recv_frames(path: str, ledger: FileLedger) -> Iterator[bytes]:
    """读取接收流（增量文件，无 footer）：逐条摘要文件头 + 帧摘要校验到干净 EOF。"""
    with ledger.open(path, "rb") as fh:
        read_header(fh, path)
        while True:
            peek = fh.read(4)
            if not peek:
                return
            if len(peek) != 4:
                raise HistoryCorrupt(f"{path}: 接收流帧头截断")
            (plen,) = struct.unpack("<I", peek)
            if plen <= 0 or plen > 256 * 1024 * 1024:
                raise HistoryCorrupt(f"{path}: 非法接收记录长度 {plen}")
            payload = fh.read(plen)
            digest = fh.read(32)
            if len(payload) != plen or len(digest) != 32:
                raise HistoryCorrupt(f"{path}: 接收记录截断")
            if hashlib.sha256(payload).digest() != digest:
                raise HistoryCorrupt(f"{path}: 接收记录摘要不符（内容损坏）")
            yield payload


def receive_batch(workdir: str, name: str, batch_id: str, *,
                  hook: Optional[tuple[str, int]] = None) -> dict:
    """接收阶段（可恢复）：逐段校验并落入接收流，每段一个持久检查点。

    已持久接收的源范围绝不重读；接收一半的段截掉重收。坏段/非法末行使整批
    保持不可见并落为 failed（可检查原因后放弃）。
    """
    hl, conn, meta = _open_table_conn(workdir, name)
    budget = HistBudget(int(meta["mem_bytes"]), int(meta["max_open_files"]))
    ledger = FileLedger(budget.max_open_files)
    try:
        b = _get_batch(conn, batch_id)
        if b["state"] == B_COMMITTED:
            return _batch_record(conn, b)
        if b["state"] == B_FAILED:
            raise BatchFailed(f"批次 {batch_id!r} 已失败：{b['error']}")
        if b["state"] == B_ABANDONED:
            raise HistoryError(f"批次 {batch_id!r} 已放弃")
        if b["state"] != B_RECEIVING:
            return _batch_record(conn, b)   # 接收已完成，幂等返回
        _check_next_version(conn, b)
        manifest = json_loads(b["manifest_json"].encode("utf-8"))
        src_layout = Layout(b["src_root"])
        recorded = _recv_recorded_end(conn, batch_id)
        if recorded == 0:
            writer = FramedWriter(ledger, b["recv_path"],
                                  {"type": "hist-recv", "table": name,
                                   "batch_id": batch_id})
            writer.flush_fsync()
        else:
            writer = FramedWriter(ledger, b["recv_path"], None, resume_bytes=recorded)
        received_total = int(b["recv_segments"])
        try:
            pending = conn.execute(
                "SELECT * FROM batch_segments WHERE batch_id=? AND state='pending'"
                " ORDER BY split_ord, partition, seq", (batch_id,)).fetchall()
            split_rows = {int(sp["ord"]): sp for sp in manifest}
            for seg in pending:
                seg_offset = writer.tell()
                seg_rows = 0
                path = coord._abs_seg_path(src_layout, b["src_job"], seg["relpath"])
                if not os.path.exists(path):
                    raise BatchFailed(f"源段丢失: {path}")
                if os.path.getsize(path) != int(seg["byte_length"]):
                    raise BatchFailed(f"源段长度与批次固定清单不符: {path}")
                sp = split_rows[int(seg["split_ord"])]
                expected = {"job_id": b["src_job"], "split_id": sp["split_id"],
                            "attempt_no": int(sp["attempt_no"]),
                            "partition_id": int(seg["partition"]), "seq": int(seg["seq"])}
                _counter_add(conn, "src_file_opens", 1)   # 源文件实际打开计数
                conn.commit()
                for payload, _digest in iter_segment_rows(path, expected):
                    if len(payload) > budget.single_row_limit:
                        raise BudgetExceeded(
                            f"单行 {len(payload)} 字节超过单行预算 "
                            f"{budget.single_row_limit}（工作内存 {budget.mem_bytes}）"
                        )
                    try:
                        row = json_loads(payload)
                    except (UnicodeDecodeError, ValueError) as exc:
                        raise BatchFailed(f"{path}: 行 JSON 损坏: {exc}") from exc
                    if row.get("split_id") != sp["split_id"]:
                        raise BatchFailed(f"{path}: 行身份与批次清单不符")
                    rn = row.get("row_number")
                    if not isinstance(rn, int) or not (1 <= rn <= int(sp["row_count"])):
                        raise BatchFailed(f"{path}: row_number 越界 {rn!r}")
                    writer.write(struct.pack("<I", int(seg["split_ord"])) + payload)
                    seg_rows += 1
                if seg_rows != int(seg["row_count"]):
                    raise BatchFailed(
                        f"源段登记行数 {seg['row_count']} 与实读 {seg_rows} 不符: {path}"
                    )
                writer.flush_fsync()   # 先 fsync 接收流，再落段级检查点
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE batch_segments SET state='received', recv_offset=?,"
                    " recv_bytes=?, recv_rows=? WHERE batch_id=? AND split_ord=?"
                    " AND partition=? AND seq=?",
                    (seg_offset, writer.tell() - seg_offset, seg_rows, batch_id,
                     int(seg["split_ord"]), int(seg["partition"]), int(seg["seq"])),
                )
                conn.execute(
                    "UPDATE batches SET recv_segments=recv_segments+1,"
                    " recv_rows=recv_rows+?, recv_bytes=recv_bytes+? WHERE batch_id=?",
                    (seg_rows, int(seg["byte_length"]), batch_id),
                )
                _counter_add(conn, "src_segments_read", 1)
                _counter_add(conn, "rows_received", seg_rows)
                if ledger.peak > _counter_get(conn, "peak_open_files"):
                    _counter_add(conn, "peak_open_files",
                                 ledger.peak - _counter_get(conn, "peak_open_files"))
                conn.commit()
                received_total += 1
                hook_fire(hook, "kill-after-receive-segment", received_total)
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("UPDATE batches SET state=? WHERE batch_id=?",
                         (B_STAGING, batch_id))
            _hevent(conn, "receive_done",
                    f"batch={batch_id} rows={_counter_get(conn, 'rows_received')}")
            conn.commit()
        finally:
            writer.abort()
        # 接收完成：释放本批次在源作业的段保护（不影响其他 reader/表）
        _release_if_held(conn, b)
        hook_fire(hook, "kill-after-receive")
        return _batch_record(conn, _get_batch(conn, batch_id))
    except Exception as exc:
        if isinstance(exc, (CommitConflict, HistoryNotReady, HistoryCorrupt)):
            raise   # 版本裁决/源状态/自身存储损坏不改变批次状态
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        # 坏段/非法末行/超预算/行非法：整批保持不可见并落为 failed（可检查后放弃）
        reason = f"{type(exc).__name__}: {exc}"
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("UPDATE batches SET state=?, error=? WHERE batch_id=?"
                         " AND state NOT IN (?,?)",
                         (B_FAILED, reason, batch_id, B_COMMITTED, B_ABANDONED))
            _hevent(conn, "batch_failed", f"batch={batch_id} {reason}")
            conn.commit()
        except sqlite3.Error:
            pass
        if isinstance(exc, BatchFailed):
            raise
        raise BatchFailed(f"批次 {batch_id!r} 接收失败（整批不可见）: {reason}") from exc
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 语义校验与暂存：整批原子——任一非法行使整批不可见
# --------------------------------------------------------------------------- #
class _RowInvalid(Exception):
    """单行语义非法（整批拒绝）。"""


def _validate_event_row(row: dict, src_ord: int) -> dict:
    """把一条源行校验并规范化为事件字段。非法即抛 _RowInvalid。"""
    key = row.get("key")
    if key is None:
        raise _RowInvalid("实体 key 为 null：历史表不接受 null 键")
    if not isinstance(key, str):
        raise _RowInvalid(f"实体 key 必须是 text，得到 {type(key).__name__}")
    value = row.get("value")
    if not isinstance(value, dict):
        raise _RowInvalid("value 必须是对象（断言或撤回）")
    op = value.get("op")
    event_id = value.get("event_id")
    if not isinstance(event_id, str):
        raise _RowInvalid("event_id 必须是 text")
    allowed = {"event_id", "op"}
    if op == "assert":
        allowed |= {"valid_from", "valid_to", "payload"}
        extra = set(value) - allowed
        if extra:
            raise _RowInvalid(f"断言含未知字段: {sorted(extra)}")
        for f in ("valid_from", "valid_to", "payload"):
            if f not in value:
                raise _RowInvalid(f"断言缺少字段 {f}")
        vf, vt = value["valid_from"], value["valid_to"]
        if not _is_int64(vf):
            raise _RowInvalid(f"valid_from 必须是有符号 64 位整数: {vf!r}")
        if vt is not None:
            if not _is_int64(vt):
                raise _RowInvalid(f"valid_to 必须是有符号 64 位整数或 null: {vt!r}")
            if not vf < vt:
                raise _RowInvalid(f"区间为空或反向：valid_from={vf} 不小于 valid_to={vt}")
        try:
            payload_json = _canon_payload(value["payload"])
        except (ValueError, TypeError) as exc:
            raise _RowInvalid(f"payload 不是标准 JSON 值: {exc}") from exc
        return {"entity_key": key, "entity_bytes": key.encode("utf-8"),
                "event_id": event_id, "op": "assert", "valid_from": vf,
                "valid_to": vt, "payload_json": payload_json,
                "target_event_id": None, "src_ord": src_ord,
                "src_row": int(row["row_number"]), "src_split": row["split_id"]}
    if op == "retract":
        allowed |= {"target_event_id"}
        extra = set(value) - allowed
        if extra:
            raise _RowInvalid(f"撤回含未知字段: {sorted(extra)}")
        target = value.get("target_event_id")
        if not isinstance(target, str):
            raise _RowInvalid("target_event_id 必须是 text")
        return {"entity_key": key, "entity_bytes": key.encode("utf-8"),
                "event_id": event_id, "op": "retract", "valid_from": None,
                "valid_to": None, "payload_json": None,
                "target_event_id": target, "src_ord": src_ord,
                "src_row": int(row["row_number"]), "src_split": row["split_id"]}
    raise _RowInvalid(f"非法 op: {op!r}（应为 assert/retract）")


def _cross_check_event(conn: sqlite3.Connection, ev: dict, kversion: int,
                       batch_id: str) -> None:
    """对照已提交事件与本批暂存做跨行语义校验（整批原子拒绝）。"""
    dup = conn.execute("SELECT 1 FROM events WHERE event_id=?",
                       (ev["event_id"],)).fetchone()
    if dup:
        raise _RowInvalid(f"event_id {ev['event_id']!r} 与已提交事实重复")
    if ev["op"] != "retract":
        return
    target = conn.execute(
        "SELECT op, entity_bytes, kversion FROM events WHERE event_id=?",
        (ev["target_event_id"],)).fetchone()
    if target is None:
        raise _RowInvalid(
            f"撤回目标 {ev['target_event_id']!r} 不存在于更早知识版本"
            "（不存在或前向引用未提交事实）"
        )
    if target["op"] != "assert":
        raise _RowInvalid(
            f"撤回目标 {ev['target_event_id']!r} 不是断言：撤回本身不接受被撤回"
        )
    if target["entity_bytes"] != ev["entity_bytes"]:
        raise _RowInvalid(
            f"撤回目标 {ev['target_event_id']!r} 属于另一实体（错实体撤回）"
        )
    if int(target["kversion"]) >= kversion:
        raise _RowInvalid(
            f"撤回目标 {ev['target_event_id']!r} 不在更早知识版本（前向引用）"
        )
    again = conn.execute("SELECT 1 FROM events WHERE target_event_id=?",
                         (ev["target_event_id"],)).fetchone()
    if again:
        raise _RowInvalid(f"断言 {ev['target_event_id']!r} 已被撤回（再次撤回）")
    again_staged = conn.execute(
        "SELECT 1 FROM batch_events WHERE batch_id=? AND target_event_id=?",
        (batch_id, ev["target_event_id"])).fetchone()
    if again_staged:
        raise _RowInvalid(f"断言 {ev['target_event_id']!r} 被本批另一撤回命中（再次撤回）")


def stage_batch(workdir: str, name: str, batch_id: str, *,
                hook: Optional[tuple[str, int]] = None) -> dict:
    """暂存阶段：流式重读接收流，整批语义校验后落入暂存表（单事务）。

    重复 ID、错实体撤回、前向引用、再次撤回、非法行都使整批被拒绝并落为
    failed；暂存内容普通查询不可见。崩溃后重入：清掉半截暂存重做。
    """
    hl, conn, meta = _open_table_conn(workdir, name)
    budget = HistBudget(int(meta["mem_bytes"]), int(meta["max_open_files"]))
    ledger = FileLedger(budget.max_open_files)
    try:
        b = _get_batch(conn, batch_id)
        if b["state"] == B_COMMITTED:
            return _batch_record(conn, b)
        if b["state"] == B_FAILED:
            raise BatchFailed(f"批次 {batch_id!r} 已失败：{b['error']}")
        if b["state"] in (B_STAGED,):
            return _batch_record(conn, b)
        if b["state"] != B_STAGING:
            raise HistoryError(f"批次 {batch_id!r} 状态为 {b['state']}，不能暂存")
        _check_next_version(conn, b)
        _release_if_held(conn, b)   # 接收已完成，补齐保护释放（防崩溃泄漏）
        kversion = int(b["kversion"])
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM batch_events WHERE batch_id=?", (batch_id,))
            seq = 0
            asserts = retracts = 0
            for payload in _iter_recv_frames(b["recv_path"], ledger):
                (src_ord,) = struct.unpack_from("<I", payload, 0)
                try:
                    row = json_loads(payload[4:])
                except (UnicodeDecodeError, ValueError) as exc:
                    raise _RowInvalid(f"接收流行 JSON 损坏: {exc}") from exc
                ev = _validate_event_row(row, src_ord)
                _cross_check_event(conn, ev, kversion, batch_id)
                try:
                    conn.execute(
                        "INSERT INTO batch_events(batch_id,seq,entity_key,entity_bytes,"
                        "event_id,op,valid_from,valid_to,payload_json,target_event_id,"
                        "src_ord,src_row,src_split) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (batch_id, seq, ev["entity_key"], ev["entity_bytes"],
                         ev["event_id"], ev["op"], ev["valid_from"], ev["valid_to"],
                         ev["payload_json"], ev["target_event_id"], ev["src_ord"],
                         ev["src_row"], ev["src_split"]),
                    )
                except sqlite3.IntegrityError as exc:
                    raise _RowInvalid(
                        f"event_id {ev['event_id']!r} 批内重复（含跨实体）"
                    ) from exc
                if ev["op"] == "assert":
                    asserts += 1
                else:
                    retracts += 1
                seq += 1
            if seq != int(b["recv_rows"]):
                raise HistoryCorrupt(
                    f"接收流条数 {seq} 与接收检查点 {b['recv_rows']} 不符（损坏）"
                )
            entities = conn.execute(
                "SELECT COUNT(DISTINCT entity_bytes) AS n FROM batch_events"
                " WHERE batch_id=?", (batch_id,)).fetchone()["n"]
            conn.execute(
                "UPDATE batches SET state=?, asserts=?, retracts=?, entities=?"
                " WHERE batch_id=?",
                (B_STAGED, asserts, retracts, int(entities), batch_id),
            )
            _hevent(conn, "batch_staged",
                    f"batch={batch_id} asserts={asserts} retracts={retracts} "
                    f"entities={entities}")
            conn.commit()
        except _RowInvalid as exc:
            conn.rollback()
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("UPDATE batches SET state=?, error=? WHERE batch_id=?",
                         (B_FAILED, f"RowInvalid: {exc}", batch_id))
            _hevent(conn, "batch_failed", f"batch={batch_id} {exc}")
            conn.commit()
            raise BatchFailed(f"批次 {batch_id!r} 语义校验失败（整批不可见）: {exc}") from exc
        except Exception:
            conn.rollback()
            raise
        hook_fire(hook, "kill-after-stage")
        return _batch_record(conn, _get_batch(conn, batch_id))
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 提交：单个 IMMEDIATE 事务——两个写进程同时提交同一版本时只有一个可见提交
# --------------------------------------------------------------------------- #
def commit_batch(workdir: str, name: str, batch_id: str, *,
                 hook: Optional[tuple[str, int]] = None) -> dict:
    """提交阶段：把暂存事件原子落入 events/buckets 并推进 head。

    失败方（版本已被占用）显式 CommitConflict，不能覆盖或拼接；批次保持
    staged 可检查后放弃。提交已持久但调用者未收到成功时，重送幂等返回。
    """
    hl, conn, meta = _open_table_conn(workdir, name)
    try:
        b = _get_batch(conn, batch_id)
        if b["state"] == B_COMMITTED:
            return _batch_record(conn, b)     # 幂等：重送不再占版本
        if b["state"] == B_FAILED:
            raise BatchFailed(f"批次 {batch_id!r} 已失败：{b['error']}")
        if b["state"] != B_STAGED:
            raise HistoryError(f"批次 {batch_id!r} 状态为 {b['state']}，不能提交")
        _release_if_held(conn, b)   # 接收已完成，补齐保护释放（防崩溃泄漏）
        kversion = int(b["kversion"])
        conn.execute("BEGIN IMMEDIATE")
        try:
            head = _head_of(conn)
            if kversion != head + 1:
                conn.rollback()
                raise CommitConflict(
                    f"批次 {batch_id!r} 目标版本 {kversion} 已被占用（当前 head={head}）："
                    "只能有一个可见提交，失败方不得覆盖或拼接"
                )
            n_events = n_buckets = 0
            cur = conn.execute(
                "SELECT * FROM batch_events WHERE batch_id=? ORDER BY seq",
                (batch_id,))
            for r in cur:   # 流式迭代，不把整批暂存装进内存
                edigest = _event_digest(
                    r["entity_bytes"], kversion, r["src_ord"], r["src_row"],
                    r["event_id"], r["op"], r["valid_from"], r["valid_to"],
                    r["payload_json"], r["target_event_id"], b["src_job"],
                    r["src_split"])
                ins = conn.execute(
                    "INSERT INTO events(entity_bytes,entity_key,kversion,src_ord,"
                    "src_row,event_id,op,valid_from,valid_to,payload_json,"
                    "target_event_id,src_job,src_split,edigest) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (r["entity_bytes"], r["entity_key"], kversion, r["src_ord"],
                     r["src_row"], r["event_id"], r["op"], r["valid_from"],
                     r["valid_to"], r["payload_json"], r["target_event_id"],
                     b["src_job"], r["src_split"], edigest))
                rid = ins.lastrowid
                n_events += 1
                if r["op"] == "assert":
                    u0 = _shift(int(r["valid_from"]))
                    u1 = _shift(r["valid_to"] if r["valid_to"] is not None else None)
                    bucket_rows = [(r["entity_bytes"], lvl, bidx - _BIAS, rid)
                                   for lvl, bidx in _cover(u0, u1)]
                    conn.executemany(
                        "INSERT INTO buckets(entity_bytes,lvl,bidx,rid) VALUES (?,?,?,?)",
                        bucket_rows)
                    n_buckets += len(bucket_rows)
            _meta_set(conn, "head", kversion)
            conn.execute(
                "UPDATE batches SET state=?, committed_at=? WHERE batch_id=?",
                (B_COMMITTED, time.time(), batch_id))
            conn.execute("DELETE FROM batch_events WHERE batch_id=?", (batch_id,))
            _counter_add(conn, "asserts_landed", int(b["asserts"]))
            _counter_add(conn, "retracts_landed", int(b["retracts"]))
            _counter_add(conn, "buckets_landed", n_buckets)
            _counter_add(conn, "batches_committed", 1)
            _hevent(conn, "batch_committed",
                    f"batch={batch_id} kversion={kversion} events={n_events} "
                    f"buckets={n_buckets}")
            conn.commit()
        except CommitConflict:
            raise
        except sqlite3.Error as exc:
            conn.rollback()
            # 提交期数据库异常（如唯一约束被绕过）：视为自身存储损坏，显式报错
            raise HistoryCorrupt(f"提交期存储异常（自身存储损坏）: {exc}") from exc
        hook_fire(hook, "kill-after-commit")
        return _batch_record(conn, _get_batch(conn, batch_id))
    finally:
        conn.close()


def run_batch(workdir: str, name: str, batch_id: str, *,
              hook: Optional[tuple[str, int]] = None) -> dict:
    """继续完成入仓：接收 → 暂存 → 提交。任何中断后再调即从持久检查点恢复。"""
    if hook is None:
        hook = parse_hook(os.environ.get("SHUFFLE_HIST_HOOK"))
    receive_batch(workdir, name, batch_id, hook=hook)
    stage_batch(workdir, name, batch_id, hook=hook)
    return commit_batch(workdir, name, batch_id, hook=hook)


def abandon_batch(workdir: str, name: str, batch_id: str) -> dict:
    """放弃未提交批次：只释放它自己的源引用与暂存/接收文件，不影响其他批次。"""
    hl, conn, meta = _open_table_conn(workdir, name)
    try:
        b = _get_batch(conn, batch_id)
        if b["state"] == B_COMMITTED:
            raise HistoryError(f"批次 {batch_id!r} 已提交，知识版本不可放弃或改写")
        if b["state"] == B_ABANDONED:
            return _batch_record(conn, b)
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM batch_events WHERE batch_id=?", (batch_id,))
        conn.execute("DELETE FROM batch_segments WHERE batch_id=?", (batch_id,))
        conn.execute("UPDATE batches SET state=?, protection_held=0 WHERE batch_id=?",
                     (B_ABANDONED, batch_id))
        _hevent(conn, "batch_abandoned", f"batch={batch_id}")
        conn.commit()
        if b["protection_held"]:
            try:
                _release_protection(b["src_root"], b["reader_id"])
            except Exception as exc:  # noqa: BLE001
                _hevent(conn, "release_warning", str(exc))
                conn.commit()
        if b["recv_path"] and os.path.exists(b["recv_path"]):
            os.remove(b["recv_path"])
        return _batch_record(conn, _get_batch(conn, batch_id))
    finally:
        conn.close()


def drop_table(workdir: str, name: str) -> dict:
    """删除历史表：只移除它自己的目录，不影响其他表或进行中的读者。"""
    hl = HistLayout(workdir, name)
    if not os.path.exists(hl.db_path):
        raise HistoryNotReady(f"历史表不存在: {hl.dir}")
    conn = _connect_hist(hl)
    try:
        held = conn.execute(
            "SELECT batch_id, src_root, reader_id FROM batches"
            " WHERE protection_held=1").fetchall()
    finally:
        conn.close()
    for b in held:   # 释放本表仍持有的源段保护（只释放自己的引用）
        try:
            _release_protection(b["src_root"], b["reader_id"])
        except Exception:
            pass
    shutil.rmtree(hl.dir, ignore_errors=True)
    return {"name": name, "dropped": True}


# --------------------------------------------------------------------------- #
# 游标：绑定表持久身份、固定知识版本、查询范围与当前位置；损坏/错用显式拒绝
# --------------------------------------------------------------------------- #
def _guard_corrupt(fn):
    """把 SQLite 层错误统一翻译为 HistoryCorrupt（不用空列表吞掉异常）。"""
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except sqlite3.DatabaseError as exc:
            raise HistoryCorrupt(f"历史表存储读取失败（自身存储损坏）: {exc}") from exc
    return wrapper


def _cursor_encode(token: str, kind: str, fields: dict) -> str:
    body = {"v": 1, "tok": token, "kind": kind}
    body.update(fields)
    sig = hashlib.sha256(b"hist-cursor:" + json_dumps(body)).hexdigest()
    body["sig"] = sig
    return json.dumps(body, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def _cursor_decode(token: str, kind: str, raw: str) -> dict:
    try:
        body = json.loads(raw)
    except (ValueError, TypeError) as exc:
        raise CursorError(f"游标无法解析（内容被破坏）: {exc}") from exc
    if not isinstance(body, dict):
        raise CursorError("游标结构非法（内容被破坏）")
    sig = body.pop("sig", None)
    if not isinstance(sig, str):
        raise CursorError("游标缺少完整性签名（内容被破坏）")
    expect = hashlib.sha256(b"hist-cursor:" + json_dumps(body)).hexdigest()
    if sig != expect:
        raise CursorError("游标完整性校验失败（内容被破坏）")
    if body.get("tok") != token:
        raise CursorError("游标属于另一张表（持久身份不符）")
    if body.get("kind") != kind:
        raise CursorError(f"游标属于另一种接口（{body.get('kind')!r}，应为 {kind!r}）")
    if body.get("v") != 1:
        raise CursorError("游标版本不受支持")
    return body


def _cursor_params_match(body: dict, given: dict) -> None:
    """游标与显式参数同时给出时必须一致——把游标用于另一组参数应明确拒绝。"""
    for key, value in given.items():
        if value is None:
            continue
        if key not in body or body[key] != value:
            raise CursorError(
                f"游标与显式参数 {key}={value!r} 不符（游标绑定 {body.get(key)!r}）："
                "不得把游标用于另一组参数"
            )


# --------------------------------------------------------------------------- #
# 查询句柄
# --------------------------------------------------------------------------- #
class HistoryTable:
    """历史表查询句柄。所有查询显式选知识版本或在开始时固定当前 head。"""

    def __init__(self, workdir: str, name: str) -> None:
        self.hl = HistLayout(workdir, name)
        if not os.path.exists(self.hl.db_path):
            raise HistoryNotReady(f"历史表不存在: {self.hl.dir}")
        probe = _connect_hist(self.hl)
        try:
            meta = _load_table(probe)
        finally:
            probe.close()
        self.workdir = workdir
        self.name = name
        self.token = meta["table_token"]
        self.budget = HistBudget(int(meta["mem_bytes"]), int(meta["max_open_files"]))

    # ------------------------------------------------------------------ #
    def _query_conn(self) -> sqlite3.Connection:
        conn = _connect_hist(self.hl, self.budget.sqlite_cache_kb)
        conn.execute("BEGIN")   # 读事务：整个查询看到同一快照（WAL 不挡写）
        conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS cand(rid INTEGER PRIMARY KEY)")
        return conn

    def _pin_version(self, conn: sqlite3.Connection, kversion: Optional[int]) -> int:
        head = _head_of(conn)
        if kversion is None:
            return head
        if not isinstance(kversion, int) or isinstance(kversion, bool):
            raise HistoryError(f"知识版本必须是整数: {kversion!r}")
        if kversion < 0:
            raise HistoryError(f"知识版本不能为负: {kversion}")
        if kversion > head:
            pending = conn.execute(
                "SELECT batch_id, state FROM batches WHERE kversion=?"
                " AND state IN (?,?,?)",
                (kversion, B_RECEIVING, B_STAGING, B_STAGED)).fetchone()
            if pending:
                raise UncommittedVersion(
                    f"知识版本 {kversion} 对应批次 {pending['batch_id']!r} "
                    f"（状态 {pending['state']}）尚未提交，查询拒绝把它当作已提交历史"
                )
            raise VersionNotFound(
                f"知识版本 {kversion} 不存在（当前 head={head}），"
                "查询不存在的未来版本不得自动改成最新版本"
            )
        return kversion

    def head(self) -> int:
        conn = self._query_conn()
        try:
            return _head_of(conn)
        finally:
            conn.rollback()
            conn.close()

    # ------------------------------------------------------------------ #
    # 候选收集：点查 65 次精确查找；区间查每层一次范围扫描（真阳性）
    # ------------------------------------------------------------------ #
    def _collect_candidates(self, conn: sqlite3.Connection, entity_bytes: bytes,
                            u0: int, u1: int, full_axis: bool, K: int,
                            stats: dict) -> None:
        """收集与扫描范围 [u0,u1) 相交的断言候选（真阳性，范围即扫描范围）。"""
        conn.execute("DELETE FROM cand")
        if full_axis:
            # 全轴查询用直接实体扫描（避免 65 次桶查找）；游标续读时扫描起点
            # 靠后，仍按扫描范围过滤，保证候选与扫描范围一致
            clauses = ["entity_bytes=?", "op='assert'", "kversion<=?"]
            args: list = [entity_bytes, K]
            if u1 < U_INF:
                clauses.append("valid_from < ?")
                args.append(u1 - _BIAS)
            if u0 > 0:
                clauses.append("(valid_to IS NULL OR valid_to > ?)")
                args.append(u0 - _BIAS)
            conn.execute(
                f"INSERT OR IGNORE INTO cand(rid) SELECT rowid FROM events"
                f" WHERE {' AND '.join(clauses)}", args)
        else:
            for lvl in range(LEVELS):
                blo = (u0 >> lvl) - _BIAS
                bhi = ((u1 - 1) >> lvl) - _BIAS
                conn.execute(
                    "INSERT OR IGNORE INTO cand(rid) SELECT rid FROM buckets"
                    " WHERE entity_bytes=? AND lvl=? AND bidx BETWEEN ? AND ?",
                    (entity_bytes, lvl, blo, bhi))
        stats["candidates"] = conn.execute(
            "SELECT COUNT(*) AS n FROM cand").fetchone()["n"]

    def _verify_event_row(self, r: sqlite3.Row, entity_bytes: bytes, K: int) -> None:
        dg = _event_digest(r["entity_bytes"], r["kversion"], r["src_ord"],
                           r["src_row"], r["event_id"], r["op"], r["valid_from"],
                           r["valid_to"], r["payload_json"], r["target_event_id"],
                           r["src_job"], r["src_split"])
        if dg != r["edigest"]:
            raise HistoryCorrupt(
                f"事件 {r['event_id']!r} 内容摘要与存储不符（历史表存储损坏）"
            )
        if r["entity_bytes"] != entity_bytes:
            raise HistoryCorrupt("桶索引引用了其他实体的事件（历史表存储损坏）")
        if int(r["kversion"]) > K:
            raise HistoryCorrupt("事件知识版本超出查询固定版本（历史表存储损坏）")

    def _is_alive(self, conn: sqlite3.Connection, event_id: str, K: int) -> bool:
        return conn.execute(
            "SELECT 1 FROM events WHERE target_event_id=? AND kversion<=? LIMIT 1",
            (event_id, K)).fetchone() is None

    # ------------------------------------------------------------------ #
    # 扫描线：候选按 valid_from 排序（SQLite 外部排序落盘），开放集合为堆
    # ------------------------------------------------------------------ #
    def _sweep(self, conn: sqlite3.Connection, entity_bytes: bytes,
               u0: int, u1: int, K: int, stats: dict) -> Iterator[tuple[int, int, Optional[dict]]]:
        """产出 [u0,u1) 的相邻片段 (start_u, end_u, winner|None)，时间升序。"""
        import heapq

        sql = ("SELECT e.rowid, e.entity_bytes, e.kversion, e.src_ord, e.src_row,"
               " e.event_id, e.valid_from, e.valid_to, e.payload_json,"
               " e.src_job, e.src_split, e.edigest, e.op, e.target_event_id"
               " FROM events e WHERE e.rowid IN (SELECT rid FROM cand)"
               " AND e.kversion<=? ORDER BY e.valid_from, e.rowid")
        cur_row = conn.execute(sql, (K,))
        open_heap: list = []   # (-kv,-sord,-srow, vt_u, payload_json, event_id, job, split, srow, kv)
        expiry: list = []      # (vt_u, seq)
        seq = 0

        def _alive_stream() -> Iterator[dict]:
            """只产出仍有效的断言（被 ≤K 撤回命中的断言不产生位置——否则会把
            空洞/片段在死断言端点处错误切开）。"""
            for row in cur_row:
                stats["events_examined"] += 1
                self._verify_event_row(row, entity_bytes, K)
                a0 = _shift(int(row["valid_from"]))
                a1 = _shift(row["valid_to"] if row["valid_to"] is not None else None)
                if a1 <= u0 or a0 >= u1:
                    raise HistoryCorrupt("桶索引命中不相交事件（历史表存储损坏）")
                if not self._is_alive(conn, row["event_id"], K):
                    continue
                yield {"a0": a0, "a1": a1, "kv": int(row["kversion"]),
                       "sord": int(row["src_ord"]), "srow": int(row["src_row"]),
                       "event_id": row["event_id"], "payload_json": row["payload_json"],
                       "src_job": row["src_job"], "src_split": row["src_split"]}

        stream = _alive_stream()
        nxt = next(stream, None)

        def _add_until(limit_u: int) -> None:
            nonlocal nxt, seq
            while nxt is not None and nxt["a0"] <= limit_u:
                ev = nxt
                heapq.heappush(open_heap, (
                    -ev["kv"], -ev["sord"], -ev["srow"], ev["a1"],
                    ev["payload_json"], ev["event_id"], ev["src_job"],
                    ev["src_split"], ev["srow"], ev["kv"]))
                heapq.heappush(expiry, (ev["a1"], seq))
                seq += 1
                if len(open_heap) > self.budget.sweep_open_limit:
                    raise BudgetExceeded(
                        f"扫描线开放集合超过预算 {self.budget.sweep_open_limit} 条"
                        "（区间内同时覆盖的断言过多）"
                    )
                nxt = next(stream, None)

        cur = u0
        _add_until(cur)
        while cur < u1:
            np = u1
            if nxt is not None:
                np = min(np, nxt["a0"])
            while expiry and expiry[0][0] <= cur:
                heapq.heappop(expiry)
            if expiry:
                np = min(np, expiry[0][0])
            while open_heap and open_heap[0][3] <= cur:
                heapq.heappop(open_heap)
            top = open_heap[0] if open_heap else None
            winner = None
            if top is not None:
                winner = {"event_id": top[5], "payload_json": top[4],
                          "kversion": top[9],
                          "source": {"job": top[6], "split": top[7],
                                     "row_number": top[8]}}
            yield cur, np, winner
            cur = np
            _add_until(cur)

    def _merged_segments(self, conn: sqlite3.Connection, entity_bytes: bytes,
                         u0: int, u1: int, K: int, stats: dict
                         ) -> Iterator[tuple[int, int, Optional[dict]]]:
        """合并"获胜 event_id 相同"的相邻片段（payload 恰好相等不合并）。"""
        open_seg: Optional[list] = None
        for s, e, winner in self._sweep(conn, entity_bytes, u0, u1, K, stats):
            if open_seg is not None and winner is not None and open_seg[2] is not None \
                    and open_seg[2]["event_id"] == winner["event_id"]:
                open_seg[1] = e
                continue
            if open_seg is not None:
                yield open_seg[0], open_seg[1], open_seg[2]
            open_seg = [s, e, winner]
        if open_seg is not None:
            yield open_seg[0], open_seg[1], open_seg[2]

    # ------------------------------------------------------------------ #
    @staticmethod
    def _fmt_segment(s_u: int, e_u: int, winner: Optional[dict]) -> dict:
        seg = {"start": _unshift(s_u), "end": _unshift(e_u), "present": False,
               "payload": None, "event_id": None, "kversion": None, "source": None}
        if winner is not None:
            seg["present"] = True
            seg["payload"] = json_loads(winner["payload_json"].encode("utf-8"))
            seg["event_id"] = winner["event_id"]
            seg["kversion"] = winner["kversion"]
            seg["source"] = winner["source"]
        return seg

    @staticmethod
    def _check_entity(entity: Any) -> bytes:
        if entity is None:
            raise HistoryError("实体 key 为 null：历史表不接受 null 键")
        if not isinstance(entity, str):
            raise HistoryError(f"实体 key 必须是 text，得到 {type(entity).__name__}")
        return entity.encode("utf-8")

    # ------------------------------------------------------------------ #
    # 点查
    # ------------------------------------------------------------------ #
    @_guard_corrupt
    def point(self, entity: str, t: int, *, kversion: Optional[int] = None) -> dict:
        """在知识版本 K 查业务时点 T：获胜断言或空洞（present=false）。"""
        entity_bytes = self._check_entity(entity)
        _check_int64_param(t, "业务时点")
        conn = self._query_conn()
        stats = {"candidates": 0, "events_examined": 0, "src_opens": 0}
        try:
            K = self._pin_version(conn, kversion)
            u = _shift(t)
            self._collect_candidates(conn, entity_bytes, u, u + 1, False, K, stats)
            best: Optional[dict] = None
            for r in conn.execute(
                    "SELECT rowid, entity_bytes, kversion, src_ord, src_row, event_id,"
                    " valid_from, valid_to, payload_json, src_job, src_split, edigest,"
                    " op, target_event_id FROM events"
                    " WHERE rowid IN (SELECT rid FROM cand) AND kversion<=?", (K,)):
                stats["events_examined"] += 1
                self._verify_event_row(r, entity_bytes, K)
                a0 = _shift(int(r["valid_from"]))
                a1 = _shift(r["valid_to"] if r["valid_to"] is not None else None)
                if not (a0 <= u < a1):
                    raise HistoryCorrupt("桶索引命中不覆盖事件（历史表存储损坏）")
                if not self._is_alive(conn, r["event_id"], K):
                    continue
                pri = (int(r["kversion"]), int(r["src_ord"]), int(r["src_row"]))
                if best is None or pri > best["pri"]:
                    best = {"pri": pri, "event_id": r["event_id"],
                            "payload_json": r["payload_json"],
                            "kversion": int(r["kversion"]),
                            "source": {"job": r["src_job"], "split": r["src_split"],
                                       "row_number": int(r["src_row"])}}
            out = {"entity": entity, "time": t, "kversion": K, "present": False,
                   "payload": None, "event_id": None, "source": None,
                   "winner_kversion": None, "stats": stats}
            if best is not None:
                out["present"] = True
                out["payload"] = json_loads(best["payload_json"].encode("utf-8"))
                out["event_id"] = best["event_id"]
                out["source"] = best["source"]
                out["winner_kversion"] = best["kversion"]
            return out
        finally:
            conn.rollback()
            conn.close()

    # ------------------------------------------------------------------ #
    # 时间线（有限业务区间，分页 + 游标）
    # ------------------------------------------------------------------ #
    @_guard_corrupt
    def timeline(self, entity: Optional[str] = None, t0: Optional[int] = None,
                 t1: Optional[int] = None, *, kversion: Optional[int] = None,
                 limit: int = 1024, cursor: Optional[str] = None) -> dict:
        """把 [t0,t1) 完整切成连续、不重叠、无遗漏的片段（空洞 present=false）。"""
        if cursor is not None:
            body = _cursor_decode(self.token, "timeline", cursor)
            _cursor_params_match(body, {"ent": entity, "t0": t0, "t1": t1,
                                        "kv": kversion})
            entity = body["ent"]
            t0, t1, kversion = body["t0"], body["t1"], body["kv"]
            start_u = int(body["pos"])
        else:
            if entity is None or t0 is None or t1 is None:
                raise HistoryError("时间线查询需要 entity/t0/t1（或游标）")
            _check_int64_param(t0, "区间起点")
            _check_int64_param(t1, "区间终点")
            if not t0 < t1:
                raise HistoryError(f"业务区间为空或反向: [{t0}, {t1})")
            start_u = _shift(t0)
        if limit < 1:
            raise HistoryError("limit 必须为正整数")
        entity_bytes = self._check_entity(entity)
        u0, u1 = _shift(t0), _shift(t1)
        if not (u0 <= start_u < u1):
            raise CursorError("游标位置越出查询区间（参数被改换）")
        conn = self._query_conn()
        stats = {"candidates": 0, "events_examined": 0, "src_opens": 0}
        try:
            K = self._pin_version(conn, kversion)
            self._collect_candidates(conn, entity_bytes, start_u, u1,
                                     start_u == 0 and u1 == U_INF, K, stats)
            segments: list[dict] = []
            end_u = start_u
            next_cursor = None
            for s, e, winner in self._merged_segments(conn, entity_bytes,
                                                      start_u, u1, K, stats):
                segments.append(self._fmt_segment(s, e, winner))
                end_u = e
                if len(segments) >= limit:
                    if e < u1:
                        next_cursor = _cursor_encode(self.token, "timeline", {
                            "ent": entity, "t0": t0, "t1": t1, "kv": K, "pos": e})
                    break
            stats["segments"] = len(segments)
            return {"entity": entity, "t0": t0, "t1": t1, "kversion": K,
                    "segments": segments, "cursor": next_cursor, "stats": stats}
        finally:
            conn.rollback()
            conn.close()

    # ------------------------------------------------------------------ #
    # 跨知识版本差异（分页 + 游标）
    # ------------------------------------------------------------------ #
    @_guard_corrupt
    def diff(self, entity: Optional[str] = None, t0: Optional[int] = None,
             t1: Optional[int] = None, *, kold: Optional[int] = None,
             knew: Optional[int] = None, limit: int = 1024,
             cursor: Optional[str] = None) -> dict:
        """比较两个知识版本在同一业务区间上的判定，只返回存在性/payload/
        获胜 event_id 改变的片段，并保留两边完整判定。"""
        if cursor is not None:
            body = _cursor_decode(self.token, "diff", cursor)
            _cursor_params_match(body, {"ent": entity, "t0": t0, "t1": t1,
                                        "kold": kold, "knew": knew})
            entity = body["ent"]
            t0, t1 = body["t0"], body["t1"]
            kold, knew = body["kold"], body["knew"]
            start_u = int(body["pos"])
        else:
            if entity is None or t0 is None or t1 is None \
                    or kold is None or knew is None:
                raise HistoryError("差异查询需要 entity/t0/t1/kold/knew（或游标）")
            _check_int64_param(t0, "区间起点")
            _check_int64_param(t1, "区间终点")
            if not t0 < t1:
                raise HistoryError(f"业务区间为空或反向: [{t0}, {t1})")
            start_u = _shift(t0)
        if limit < 1:
            raise HistoryError("limit 必须为正整数")
        entity_bytes = self._check_entity(entity)
        u0, u1 = _shift(t0), _shift(t1)
        if not (u0 <= start_u < u1):
            raise CursorError("游标位置越出查询区间（参数被改换）")
        conn_old = self._query_conn()
        conn_new = self._query_conn()
        stats = {"candidates": 0, "events_examined": 0, "src_opens": 0}
        try:
            ko = self._pin_version(conn_old, kold)
            kn = self._pin_version(conn_new, knew)
            if not ko < kn:
                raise HistoryError(f"差异查询要求旧版本小于新版本: {ko} !< {kn}")
            stats_o: dict = {"candidates": 0, "events_examined": 0}
            stats_n: dict = {"candidates": 0, "events_examined": 0}
            full = (start_u == 0 and u1 == U_INF)
            self._collect_candidates(conn_old, entity_bytes, start_u, u1, full, ko, stats_o)
            self._collect_candidates(conn_new, entity_bytes, start_u, u1, full, kn, stats_n)
            it_o = self._merged_segments(conn_old, entity_bytes, start_u, u1, ko, stats_o)
            it_n = self._merged_segments(conn_new, entity_bytes, start_u, u1, kn, stats_n)
            seg_o = next(it_o, None)
            seg_n = next(it_n, None)
            changes: list[dict] = []
            cur = start_u
            next_cursor = None
            while cur < u1 and seg_o is not None and seg_n is not None:
                b = min(seg_o[1], seg_n[1])
                key_o = self._side_key(seg_o[2])
                key_n = self._side_key(seg_n[2])
                if key_o != key_n:
                    changes.append({
                        "start": _unshift(cur), "end": _unshift(b),
                        "old": self._fmt_segment(cur, b, seg_o[2]),
                        "new": self._fmt_segment(cur, b, seg_n[2]),
                    })
                    if len(changes) >= limit:
                        if b < u1:
                            next_cursor = _cursor_encode(self.token, "diff", {
                                "ent": entity, "t0": t0, "t1": t1,
                                "kold": ko, "knew": kn, "pos": b})
                        break
                if seg_o[1] == b:
                    seg_o = next(it_o, None)
                if seg_n[1] == b:
                    seg_n = next(it_n, None)
                cur = b
            stats["candidates"] = stats_o["candidates"] + stats_n["candidates"]
            stats["events_examined"] = stats_o["events_examined"] + stats_n["events_examined"]
            stats["changes"] = len(changes)
            return {"entity": entity, "t0": t0, "t1": t1, "kold": ko, "knew": kn,
                    "changes": changes, "cursor": next_cursor, "stats": stats}
        finally:
            conn_old.rollback()
            conn_old.close()
            conn_new.rollback()
            conn_new.close()

    @staticmethod
    def _side_key(winner: Optional[dict]) -> tuple:
        if winner is None:
            return (False, None, None)
        return (True, winner["payload_json"], winner["event_id"])

    # ------------------------------------------------------------------ #
    # 流式全实体导出（实体 UTF-8 字节序，再按业务时间；分页 + 游标）
    # ------------------------------------------------------------------ #
    @_guard_corrupt
    def export(self, *, kversion: Optional[int] = None, t0: Optional[int] = None,
               t1: Optional[int] = None, limit: int = 4096,
               cursor: Optional[str] = None) -> dict:
        """按实体 UTF-8 字节序、再按业务时间导出固定版本的完整分层结果。"""
        start_entity: Optional[str] = None
        start_u: Optional[int] = None
        if cursor is not None:
            body = _cursor_decode(self.token, "export", cursor)
            _cursor_params_match(body, {"kv": kversion, "t0": t0, "t1": t1})
            kversion = body["kv"]
            t0, t1 = body["t0"], body["t1"]
            start_entity = body["ent"]
            start_u = body["pos"]      # None = 该实体已耗尽，从下一实体继续
        if limit < 1:
            raise HistoryError("limit 必须为正整数")
        t0v = INT64_MIN if t0 is None else _check_int64_param(t0, "区间起点")
        u0 = _shift(t0v)
        u1 = U_INF if t1 is None else _shift(_check_int64_param(t1, "区间终点"))
        if not u0 < u1:
            raise HistoryError("业务区间为空或反向")
        conn = self._query_conn()
        stats = {"candidates": 0, "events_examined": 0, "src_opens": 0}
        try:
            K = self._pin_version(conn, kversion)
            records: list[dict] = []
            next_cursor = None
            ent_rows = self._entity_stream(conn, K, start_entity,
                                           exhausted=(start_u is None))
            full = (u0 == 0 and u1 == U_INF)
            for ent_bytes, ent_key in ent_rows:
                lo = u0
                if start_entity is not None and start_u is not None \
                        and ent_bytes == start_entity.encode("utf-8"):
                    lo = start_u
                start_entity = None
                start_u = None
                st_one = {"candidates": 0, "events_examined": 0}
                self._collect_candidates(conn, ent_bytes, lo, u1, full, K, st_one)
                stats["candidates"] += st_one["candidates"]
                last_end = lo
                for s, e, winner in self._merged_segments(conn, ent_bytes, lo, u1,
                                                          K, st_one):
                    rec = self._fmt_segment(s, e, winner)
                    rec["entity"] = ent_key
                    records.append(rec)
                    last_end = e
                    if len(records) >= limit:
                        next_cursor = _cursor_encode(self.token, "export", {
                            "kv": K, "t0": t0v, "t1": t1, "ent": ent_key,
                            "pos": e if e < u1 else None})
                        stats["events_examined"] += st_one["events_examined"]
                        stats["records"] = len(records)
                        return {"kversion": K, "t0": t0v, "t1": t1,
                                "records": records, "cursor": next_cursor,
                                "stats": stats}
                stats["events_examined"] += st_one["events_examined"]
            stats["records"] = len(records)
            return {"kversion": K, "t0": t0v, "t1": t1, "records": records,
                    "cursor": None, "stats": stats}
        finally:
            conn.rollback()
            conn.close()

    @staticmethod
    def _entity_stream(conn: sqlite3.Connection, K: int,
                       start_entity: Optional[str],
                       exhausted: bool) -> Iterator[tuple[bytes, str]]:
        sql = ("SELECT DISTINCT entity_bytes, entity_key FROM events"
               " WHERE kversion<=?")
        args: list = [K]
        if start_entity is not None:
            op = ">" if exhausted else ">="
            sql += f" AND entity_bytes {op} ?"
            args.append(start_entity.encode("utf-8"))
        sql += " ORDER BY entity_bytes"
        for r in conn.execute(sql, args):
            yield r["entity_bytes"], r["entity_key"]

    def close(self) -> None:
        """查询句柄无持久句柄（每次查询独立短连接），关闭为空操作。"""
