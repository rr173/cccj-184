"""双输入关联算子：直接消费两个既有 shuffle 作业的 winner 段，产出可落盘关联结果。

执行链（全部流式、字节记账、检查点落 SQLite）::

    bind      建关联：校验两侧作业绑定，固定 winner 清单快照，登记持久段保护
    receive   逐源段校验（长度/身份/行摘要/段摘要）→ 追加规范化接收流 → 落检查点
    normalize 按 (split 清单序, row_number) 归并出两侧源序流（k 路归并，扇入受限）
    keysort   右侧按键字节外排 + 稀疏索引（null 键不进键流：null 永不匹配）
    emit      左流按源序探测右键流，同键组流式枚举 m×n；left/full 补未匹配左行
    emit_tail 仅 full：按右源序补未匹配右行（磁盘位图标记已匹配）
    finalize  结果整流核验（逐条摘要/序号连续/计数/流摘要）→ 写 footer 才可读

结果次序固定：先按左作业 split 清单次序、split 内 row_number 输出每个左行
（其多个匹配按右源序排列）；full 模式最后按右源序追加未匹配右行。
结果序号从 0 连续编号，支持 (start, count) 分页与迭代器，二者拼接一致。

不使用任何 SQL JOIN 或现成查询引擎；SQLite 仅保存协调状态、检查点与计数。
"""

from __future__ import annotations

import bisect
import hashlib
import os
import shutil
import sqlite3
import struct
import time
import uuid
from typing import Iterator, Optional

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
    FRAME_OVERHEAD,
    J_FOOTER_MAGIC,
    J_FOOTER_PREFIX,
    KTAG_NULL,
    Budget,
    BudgetExceeded,
    FileLedger,
    FramedWriter,
    JoinError,
    JoinFailed,
    JoinNotReady,
    ResultNotReady,
    hook_fire,
    iter_frames,
    iter_frames_from,
    iter_frames_range,
    key_to_bytes,
    pack_key_index,
    pack_keys,
    pack_norm,
    pack_recv,
    pack_result,
    pack_row_index,
    parse_hook,
    read_header,
    unpack_key_index,
    unpack_keys,
    unpack_norm,
    unpack_recv,
    unpack_result,
    unpack_row_index,
    verify_file,
)
from .segment import iter_segment_rows

MODE_INNER = "inner"
MODE_LEFT = "left"
MODE_FULL = "full"
MODES = (MODE_INNER, MODE_LEFT, MODE_FULL)
KIND_FULL = "full"          # 结果保留左右完整原行（含 value）
KIND_IDENTITY = "identity"  # 身份视图：只存 split_id/row_number/key（控制结果体积）
KINDS = (KIND_FULL, KIND_IDENTITY)

RESULT_INDEX_EVERY = 1024   # 结果稀疏索引间隔（分页定位后顺序扫描）
SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;

CREATE TABLE IF NOT EXISTS meta(
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS splits(
    side         TEXT NOT NULL,
    ord          INTEGER NOT NULL,
    split_id     TEXT NOT NULL,
    attempt_no   INTEGER NOT NULL,
    row_count    INTEGER NOT NULL,
    input_digest TEXT NOT NULL,
    PRIMARY KEY (side, ord)
);
CREATE TABLE IF NOT EXISTS segments(
    side        TEXT NOT NULL,
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
    received_at REAL,
    PRIMARY KEY (side, split_ord, partition, seq)
);
CREATE TABLE IF NOT EXISTS counters(
    name  TEXT PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS checkpoints(
    phase TEXT PRIMARY KEY,
    data  TEXT NOT NULL,
    ts    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events(
    ts     REAL NOT NULL,
    kind   TEXT NOT NULL,
    detail TEXT NOT NULL
);
"""


# --------------------------------------------------------------------------- #
# 路径布局
# --------------------------------------------------------------------------- #
class JoinLayout:
    def __init__(self, workdir: str, name: str) -> None:
        if not name or any(c in name for c in "/\\\x00") or name in (".", ".."):
            raise JoinError(f"非法关联名称: {name!r}")
        self.workdir = os.path.abspath(workdir)
        self.name = name
        self.dir = os.path.join(self.workdir, name)

    @property
    def db_path(self) -> str:
        return os.path.join(self.dir, "join.sqlite3")

    def recv_path(self, side: str) -> str:
        return os.path.join(self.dir, "recv", f"{side}.recv")

    def norm_path(self, side: str) -> str:
        return os.path.join(self.dir, "norm", f"{side}.norm")

    @property
    def keys_path(self) -> str:
        return os.path.join(self.dir, "keys", "R.keys")

    @property
    def keys_index_path(self) -> str:
        return os.path.join(self.dir, "keys", "R.index")

    @property
    def result_path(self) -> str:
        return os.path.join(self.dir, "out", "result.rows")

    @property
    def result_index_path(self) -> str:
        return os.path.join(self.dir, "out", "result.index")

    @property
    def bitmap_path(self) -> str:
        return os.path.join(self.dir, "out", "matched.bits")

    @property
    def tmp_dir(self) -> str:
        return os.path.join(self.dir, "tmp")


def _side_of(role: str) -> str:
    return "L" if role == "left" else "R"


# --------------------------------------------------------------------------- #
# 关联 DB 小工具
# --------------------------------------------------------------------------- #
def _connect_join(jl: JoinLayout) -> sqlite3.Connection:
    conn = connect(jl.db_path)
    conn.executescript(SCHEMA)
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


def _ckpt_get(conn: sqlite3.Connection, phase: str) -> Optional[dict]:
    row = conn.execute("SELECT data FROM checkpoints WHERE phase=?", (phase,)).fetchone()
    return json_loads(row["data"].encode("utf-8")) if row else None


def _ckpt_set(conn: sqlite3.Connection, phase: str, data: dict) -> None:
    conn.execute(
        "INSERT INTO checkpoints(phase,data,ts) VALUES (?,?,?) "
        "ON CONFLICT(phase) DO UPDATE SET data=excluded.data, ts=excluded.ts",
        (phase, json_dumps(data).decode("utf-8"), time.time()),
    )


def _event(conn: sqlite3.Connection, kind: str, detail: str = "") -> None:
    conn.execute("INSERT INTO events(ts,kind,detail) VALUES (?,?,?)",
                 (time.time(), kind, detail))


def _persist_peak(ctx: "_RunCtx", conn: sqlite3.Connection) -> None:
    """把文件句柄峰值随检查点一起持久化（进程被杀也不丢峰值）。"""
    if ctx.ledger.peak > _counter_get(conn, "peak_open_files"):
        conn.execute(
            "INSERT INTO counters(name,value) VALUES ('peak_open_files',?) "
            "ON CONFLICT(name) DO UPDATE SET value=excluded.value",
            (ctx.ledger.peak,),
        )


# --------------------------------------------------------------------------- #
# 建关联：校验绑定、固定 winner 快照、登记持久段保护（失败不留悬挂引用）
# --------------------------------------------------------------------------- #
def _snapshot_side(root: str, job_id: str, side: str, reader_id: str) -> dict:
    """只读校验一侧源作业并组装 winner 清单快照。失败抛 JoinNotReady。"""
    layout = Layout(root)
    if not os.path.exists(layout.db_path):
        raise JoinNotReady(f"{side} 侧源 root 不存在协调库: {layout.db_path}")
    conn = connect(layout.db_path)
    try:
        coord.init_db(conn)
        try:
            job = coord.get_job(conn, job_id)
        except ShuffleError as exc:
            raise JoinNotReady(f"{side} 侧作业绑定非法: {exc}") from exc
        if job["state"] == JOB_CANCELLED:
            raise JoinNotReady(f"{side} 侧作业 {job_id} 已取消，拒绝建关联")
        if job["state"] == JOB_DESTROYED:
            raise JoinNotReady(f"{side} 侧作业 {job_id} 已销毁，拒绝建关联")
        splits = conn.execute(
            "SELECT * FROM job_splits WHERE job_id=? ORDER BY ord", (job_id,)
        ).fetchall()
        if not splits:
            raise JoinNotReady(f"{side} 侧作业 {job_id} 没有绑定 split")
        num_p = int(job["num_partitions"])
        manifest = []
        for sp in splits:
            w = conn.execute(
                "SELECT attempt_no FROM winners WHERE job_id=? AND split_id=?",
                (job_id, sp["split_id"]),
            ).fetchone()
            if w is None:
                raise JoinNotReady(
                    f"{side} 侧 split {sp['split_id']} 尚无 winner，关联不读取未就绪输入"
                )
            attempt_no = int(w["attempt_no"])
            segs = conn.execute(
                "SELECT * FROM segments WHERE job_id=? AND split_id=? AND attempt_no=? "
                "AND state='done' ORDER BY partition_id, seq",
                (job_id, sp["split_id"], attempt_no),
            ).fetchall()
            covered = {int(s["partition_id"]) for s in segs}
            if covered != set(range(num_p)):
                raise JoinNotReady(
                    f"{side} 侧 winner {sp['split_id']}#{attempt_no} 段登记不完整"
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
        return {
            "root": os.path.abspath(root), "job_id": job_id,
            "num_partitions": num_p,
            "partition_version": int(job["partition_version"]),
            "reader_id": reader_id,
            "manifest": manifest,
        }
    finally:
        conn.close()


def _insert_protection(src: dict) -> None:
    """在源作业 readers 表登记关联的持久段保护（进程被杀后仍有效）。"""
    layout = Layout(src["root"])
    conn = connect(layout.db_path)
    try:
        conn.execute(
            "INSERT INTO readers(reader_id,job_id,opened_at,state) VALUES (?,?,?,'open')",
            (src["reader_id"], src["job_id"], time.time()),
        )
        conn.commit()
    finally:
        conn.close()


def _release_protection(src: dict) -> None:
    layout = Layout(src["root"])
    if not os.path.exists(layout.db_path):
        return
    conn = connect(layout.db_path)
    try:
        conn.execute(
            "UPDATE readers SET state='closed', closed_at=? WHERE reader_id=? AND state='open'",
            (time.time(), src["reader_id"]),
        )
        conn.commit()
    finally:
        conn.close()


def create_join(workdir: str, name: str, *, left_root: str, left_job: str,
                right_root: str, right_job: str, mode: str,
                mem_bytes: int, max_open_files: int,
                result_kind: str = KIND_FULL,
                checkpoint_rows: int = 4096) -> dict:
    """建关联：固定两侧 winner 清单并登记持久段保护。

    任一侧未就绪/已取消/绑定非法都显式失败；失败时撤销已登记的保护，
    不在另一侧留下永不释放的引用。两侧可同 root 同 job（左右角色仍区分）。
    """
    if mode not in MODES:
        raise JoinError(f"非法关联模式 {mode!r}，应为 {MODES}")
    if result_kind not in KINDS:
        raise JoinError(f"非法结果类型 {result_kind!r}，应为 {KINDS}")
    Budget(mem_bytes, max_open_files)  # 校验预算合法性
    if checkpoint_rows <= 0:
        raise JoinError("checkpoint_rows 必须为正")
    jl = JoinLayout(workdir, name)
    if os.path.exists(jl.dir):
        raise JoinError(f"关联 {name!r} 已存在: {jl.dir}")

    token = uuid.uuid4().hex[:8]
    sources: dict[str, dict] = {}
    for role, root, job in (("left", left_root, left_job),
                            ("right", right_root, right_job)):
        reader_id = f"join:{name}:{role}:{token}"
        sources[role] = _snapshot_side(root, job, role, reader_id)

    protected: list[dict] = []
    try:
        for role in ("left", "right"):
            _insert_protection(sources[role])
            protected.append(sources[role])
        os.makedirs(jl.dir, exist_ok=False)
        conn = _connect_join(jl)
        try:
            _meta_set(conn, "name", name)
            _meta_set(conn, "mode", mode)
            _meta_set(conn, "result_kind", result_kind)
            _meta_set(conn, "mem_bytes", mem_bytes)
            _meta_set(conn, "max_open_files", max_open_files)
            _meta_set(conn, "checkpoint_rows", checkpoint_rows)
            _meta_set(conn, "state", "created")
            _meta_set(conn, "phase", "bind")
            _meta_set(conn, "created_at", time.time())
            _meta_set(conn, "sources", {
                role: {k: v for k, v in src.items() if k != "manifest"}
                for role, src in sources.items()
            })
            _meta_set(conn, "protections_released", False)
            for role in ("left", "right"):
                side = _side_of(role)
                for sp in sources[role]["manifest"]:
                    conn.execute(
                        "INSERT INTO splits(side,ord,split_id,attempt_no,row_count,"
                        "input_digest) VALUES (?,?,?,?,?,?)",
                        (side, sp["ord"], sp["split_id"], sp["attempt_no"],
                         sp["row_count"], sp["input_digest"]),
                    )
                    for sg in sp["segments"]:
                        conn.execute(
                            "INSERT INTO segments(side,split_ord,partition,seq,relpath,"
                            "row_count,byte_length,seg_digest,state) "
                            "VALUES (?,?,?,?,?,?,?,?,'pending')",
                            (side, sp["ord"], sg["partition"], sg["seq"], sg["relpath"],
                             sg["row_count"], sg["byte_length"], sg["seg_digest"]),
                        )
            _event(conn, "join_created",
                   f"mode={mode} kind={result_kind} mem={mem_bytes} files={max_open_files}")
            conn.commit()
        finally:
            conn.close()
    except Exception:
        for src in protected:  # 失败必须撤销已登记的保护，不留悬挂引用
            try:
                _release_protection(src)
            except Exception:
                pass
        shutil.rmtree(jl.dir, ignore_errors=True)
        raise
    return {"name": name, "dir": jl.dir, "mode": mode, "result_kind": result_kind,
            "state": "created",
            "left": {"root": sources["left"]["root"], "job": left_job},
            "right": {"root": sources["right"]["root"], "job": right_job}}


# --------------------------------------------------------------------------- #
# 阶段一 receive：逐源段校验 → 持久接收（先校验后落检查点）
# --------------------------------------------------------------------------- #
class _BufferedWriter:
    """字节记账的写缓冲，超限即 flush。"""

    def __init__(self, writer: FramedWriter, buffer_bytes: int) -> None:
        self.writer = writer
        self.limit = max(1024, buffer_bytes)
        self._size = 0

    def write(self, payload: bytes) -> int:
        off = self.writer.write(payload)
        self._size += FRAME_OVERHEAD + len(payload)
        if self._size >= self.limit:
            self.writer.flush_fsync()
            self._size = 0
        return off

    def flush(self) -> None:
        self.writer.flush_fsync()
        self._size = 0


def _recv_recorded_end(conn: sqlite3.Connection, side: str) -> int:
    """已持久接收的接收流字节终点（含 header），即恢复截断点。"""
    row = conn.execute(
        "SELECT COALESCE(MAX(recv_offset + recv_bytes),0) AS e FROM segments"
        " WHERE side=? AND state='received'", (side,),
    ).fetchone()
    return int(row["e"])


def _do_receive(ctx: "_RunCtx", conn: sqlite3.Connection) -> None:
    budget = ctx.budget
    for role in ("left", "right"):
        side = _side_of(role)
        phase = f"receive.{side}"
        done = _ckpt_get(conn, phase)
        if done and done.get("done"):
            continue
        recv_path = ctx.jl.recv_path(side)
        recorded = _recv_recorded_end(conn, side)
        if recorded == 0:
            writer = FramedWriter(ctx.ledger, recv_path,
                                  {"type": "join-recv", "join": ctx.jl.name, "side": side})
            writer.flush_fsync()
        else:
            writer = FramedWriter(ctx.ledger, recv_path, None, resume_bytes=recorded)
        buffered = _BufferedWriter(writer, budget.recv_buffer)
        src = ctx.sources[role]
        src_layout = Layout(src["root"])
        pending = conn.execute(
            "SELECT * FROM segments WHERE side=? AND state='pending' "
            "ORDER BY split_ord, partition, seq", (side,),
        ).fetchall()
        split_rows = {int(r["ord"]): r for r in conn.execute(
            "SELECT * FROM splits WHERE side=?", (side,)).fetchall()}
        try:
            for seg in pending:
                seg_offset = writer.tell()
                seg_rows = 0
                path = coord._abs_seg_path(src_layout, src["job_id"], seg["relpath"])
                if not os.path.exists(path):
                    raise JoinFailed(f"源段丢失: {path}")
                if os.path.getsize(path) != int(seg["byte_length"]):
                    raise JoinFailed(f"源段长度与登记不符: {path}")
                sp = split_rows[int(seg["split_ord"])]
                expected = {"job_id": src["job_id"], "split_id": sp["split_id"],
                            "attempt_no": int(sp["attempt_no"]),
                            "partition_id": int(seg["partition"]), "seq": int(seg["seq"])}
                # 先整段校验（行摘要/段摘要/行数），同时逐行落入接收流
                for payload, _digest in iter_segment_rows(path, expected):
                    if len(payload) > budget.single_row_limit:
                        raise BudgetExceeded(
                            f"单行 {len(payload)} 字节超过单行预算 "
                            f"{budget.single_row_limit}（总工作内存 {budget.mem_bytes}）"
                        )
                    try:
                        row = json_loads(payload)
                    except (UnicodeDecodeError, ValueError) as exc:
                        raise JoinFailed(f"{path}: 行 JSON 损坏: {exc}") from exc
                    if row.get("split_id") != sp["split_id"]:
                        raise JoinFailed(f"{path}: 行身份与清单不符")
                    rn = int(row["row_number"])
                    if not (1 <= rn <= int(sp["row_count"])):
                        raise JoinFailed(f"{path}: row_number 越界 {rn}")
                    ktag, key = key_to_bytes(row.get("key"))
                    buffered.write(pack_recv(rn, ktag, key, payload))
                    seg_rows += 1
                if seg_rows != int(seg["row_count"]):
                    raise JoinFailed(
                        f"源段登记行数 {seg['row_count']} 与实读 {seg_rows} 不符: {path}"
                    )
                buffered.flush()  # 先 fsync 接收流，再落接收检查点
                conn.execute(
                    "UPDATE segments SET state='received', recv_offset=?, recv_bytes=?,"
                    " recv_rows=?, received_at=? WHERE side=? AND split_ord=? AND"
                    " partition=? AND seq=?",
                    (seg_offset, writer.tell() - seg_offset, seg_rows, time.time(), side,
                     int(seg["split_ord"]), int(seg["partition"]), int(seg["seq"])),
                )
                _counter_add(conn, "src_segments_read", 1)
                _counter_add(conn, "src_segment_bytes_read", int(seg["byte_length"]))
                _counter_add(conn, f"rows_received.{side}", seg_rows)
                _persist_peak(ctx, conn)
                conn.commit()
                ctx.received_total += 1
                hook_fire(ctx.hook, "kill-after-receive-segment", ctx.received_total)
            rows_total = _counter_get(conn, f"rows_received.{side}")
            _ckpt_set(conn, phase, {"done": True, "rows": rows_total,
                                    "bytes": writer.tell()})
            _event(conn, "receive_done", f"side={side} rows={rows_total}")
            conn.commit()
        finally:
            writer.abort()
    _ckpt_set(conn, "receive", {"done": True})
    conn.commit()
    hook_fire(ctx.hook, "kill-after-receive")


# --------------------------------------------------------------------------- #
# 归并：k 路归并，扇入受文件预算限制；输入为 recv 范围或中间 run 文件
# --------------------------------------------------------------------------- #
def _open_run_reader(spec, ctx: "_RunCtx") -> Iterator[bytes]:
    kind = spec[0]
    if kind == "range":
        _, path, offset, nbytes = spec
        return iter_frames_range(path, offset, nbytes, ctx.ledger)
    _, path = spec
    return iter_frames(path, ctx.ledger)


def _merge_group(specs: list, key_of, ctx: "_RunCtx", emit) -> None:
    """把若干已排序记录流归并到 emit；任一时刻内存只有每条流一条当前记录。"""
    import heapq

    readers = [_open_run_reader(s, ctx) for s in specs]
    heap: list[tuple] = []
    try:
        for i, it in enumerate(readers):
            try:
                pl = next(it)
            except StopIteration:
                continue
            heapq.heappush(heap, (key_of(pl), i, pl))
        alive = [True] * len(readers)
        while heap:
            _key, i, pl = heapq.heappop(heap)
            emit(pl)
            if alive[i]:
                try:
                    nxt = next(readers[i])
                    heapq.heappush(heap, (key_of(nxt), i, nxt))
                except StopIteration:
                    alive[i] = False
    finally:
        for it in readers:
            close = getattr(it, "close", None)
            if close:
                close()


def merge_runs(specs: list, key_of, ctx: "_RunCtx", emit, tag: str) -> None:
    """多轮归并直到扇入可容纳；中间 run 落在关联 tmp 目录并随用随清。"""
    round_no = 0
    cur = list(specs)
    intermediates: list[str] = []
    try:
        while len(cur) > ctx.budget.fan_in:
            nxt = []
            for gi in range(0, len(cur), ctx.budget.fan_in):
                group = cur[gi:gi + ctx.budget.fan_in]
                tmp_path = os.path.join(ctx.jl.tmp_dir,
                                        f"merge-{tag}-{round_no}-{gi}.run")
                writer = FramedWriter(ctx.ledger, tmp_path,
                                      {"type": "join-merge-run", "join": ctx.jl.name},
                                      tmp_rename=True)
                hasher = hashlib.sha256()
                rows = 0

                def emit_mid(pl: bytes, _w=writer, _h=hasher) -> None:
                    nonlocal rows
                    _w.write(pl)
                    _h.update(pl)
                    rows += 1

                _merge_group(group, key_of, ctx, emit_mid)
                writer.finish({"row_count": rows, "digest": hasher.hexdigest()})
                intermediates.append(tmp_path)
                nxt.append(("file", tmp_path))
            cur = nxt
            round_no += 1
        _merge_group(cur, key_of, ctx, emit)
    finally:
        for p in intermediates:
            try:
                os.remove(p)
            except OSError:
                pass


def _clean_tmp(ctx: "_RunCtx") -> None:
    """阶段开始时清理上一（被终止的）运行遗留的中间文件。"""
    if not os.path.isdir(ctx.jl.tmp_dir):
        return
    for name in os.listdir(ctx.jl.tmp_dir):
        if name.endswith(".run") or name.endswith(".run.tmp"):
            try:
                os.remove(os.path.join(ctx.jl.tmp_dir, name))
            except OSError:
                pass


def _split_chunks(conn: sqlite3.Connection, side: str, split_ord: int,
                  recv_path: str) -> list:
    """一个 split 的各分区接收范围（每分区内部 row_number 已有序）。"""
    rows = conn.execute(
        "SELECT partition, MIN(recv_offset) AS off, SUM(recv_bytes) AS nbytes"
        " FROM segments WHERE side=? AND split_ord=? AND state='received'"
        " GROUP BY partition ORDER BY partition",
        (side, split_ord),
    ).fetchall()
    return [("range", recv_path, int(r["off"]), int(r["nbytes"])) for r in rows]


def _do_normalize(ctx: "_RunCtx", conn: sqlite3.Connection, role: str) -> None:
    side = _side_of(role)
    phase = f"normalize.{side}"
    ckpt = _ckpt_get(conn, phase)
    norm_path = ctx.jl.norm_path(side)
    if ckpt and ckpt.get("done"):
        verify_file(norm_path, ctx.ledger,
                    {"type": "join-norm", "join": ctx.jl.name, "side": side},
                    int(ckpt["rows"]), ckpt["digest"])
        return
    _clean_tmp(ctx)
    recv_path = ctx.jl.recv_path(side)
    writer = FramedWriter(ctx.ledger, norm_path,
                          {"type": "join-norm", "join": ctx.jl.name, "side": side},
                          tmp_rename=True)
    hasher = hashlib.sha256()
    seq = 0
    splits = conn.execute(
        "SELECT ord FROM splits WHERE side=? ORDER BY ord", (side,)).fetchall()
    try:
        for sp in splits:
            chunks = _split_chunks(conn, side, int(sp["ord"]), recv_path)
            if not chunks:
                continue

            def emit(pl: bytes, _w=writer, _h=hasher) -> None:
                nonlocal seq
                rn, ktag, key, orig = unpack_recv(pl)
                frame = pack_norm(seq, ktag, key, orig)
                _w.write(frame)
                _h.update(frame)
                seq += 1

            merge_runs(chunks, lambda pl: unpack_recv(pl)[0], ctx, emit,
                       tag=f"norm{side}")
        writer.finish({"row_count": seq, "digest": hasher.hexdigest()})
    except Exception:
        writer.abort()
        raise
    _ckpt_set(conn, phase, {"done": True, "rows": seq, "digest": hasher.hexdigest(),
                            "bytes": os.path.getsize(norm_path)})
    _event(conn, "normalize_done", f"side={side} rows={seq}")
    _persist_peak(ctx, conn)
    conn.commit()
    hook_fire(ctx.hook, "kill-after-normalize")


# --------------------------------------------------------------------------- #
# 阶段三 keysort：右侧按键字节外排 + 稀疏索引（null 键不进键流）
# --------------------------------------------------------------------------- #
def _do_keysort(ctx: "_RunCtx", conn: sqlite3.Connection) -> None:
    phase = "keysort.R"
    ckpt = _ckpt_get(conn, phase)
    if ckpt and ckpt.get("done"):
        verify_file(ctx.jl.keys_path, ctx.ledger,
                    {"type": "join-keys", "join": ctx.jl.name},
                    int(ckpt["rows"]), ckpt["digest"])
        verify_file(ctx.jl.keys_index_path, ctx.ledger,
                    {"type": "join-keys-index", "join": ctx.jl.name},
                    int(ckpt["index_entries"]), ckpt["index_digest"])
        return
    norm_ckpt = _ckpt_get(conn, "normalize.R")
    if not norm_ckpt:
        raise JoinFailed("右侧规范化未完成，不能键排序")
    _clean_tmp(ctx)
    budget = ctx.budget
    tmp_dir = ctx.jl.tmp_dir
    os.makedirs(tmp_dir, exist_ok=True)
    # pass 1：分块内存排序 → 落 run 文件（null 键跳过：null 永不匹配）
    runs: list[tuple[str, str]] = []
    chunk: list[tuple[bytes, int, bytes]] = []
    chunk_bytes = 0
    run_no = 0

    def flush_chunk() -> None:
        nonlocal chunk, chunk_bytes, run_no
        if not chunk:
            return
        chunk.sort(key=lambda t: (t[0], t[1]))
        path = os.path.join(tmp_dir, f"keysort-{run_no}.run")
        writer = FramedWriter(ctx.ledger, path,
                              {"type": "join-keys-run", "join": ctx.jl.name},
                              tmp_rename=True)
        hasher = hashlib.sha256()
        rows = 0
        for key, seq, orig in chunk:
            pl = pack_keys(key, seq, orig)
            writer.write(pl)
            hasher.update(pl)
            rows += 1
        writer.finish({"row_count": rows, "digest": hasher.hexdigest()})
        runs.append(("file", path))
        run_no += 1
        chunk = []
        chunk_bytes = 0

    total_nonnull = 0
    for pl in iter_frames(ctx.jl.norm_path("R"), ctx.ledger,
                          {"type": "join-norm", "join": ctx.jl.name, "side": "R"}):
        seq, ktag, key, orig = unpack_norm(pl)
        if ktag == KTAG_NULL:
            continue
        chunk.append((key, seq, orig))
        chunk_bytes += len(orig) + len(key) + 32
        total_nonnull += 1
        if chunk_bytes >= budget.sort_chunk:
            flush_chunk()
    flush_chunk()

    # pass 2：归并 run → 键流 + 稀疏索引（索引条数受预算约束）
    g = max(1, (total_nonnull + budget.index_max_entries - 1) // budget.index_max_entries)
    keys_writer = FramedWriter(ctx.ledger, ctx.jl.keys_path,
                               {"type": "join-keys", "join": ctx.jl.name}, tmp_rename=True)
    index_writer = FramedWriter(ctx.ledger, ctx.jl.keys_index_path,
                                {"type": "join-keys-index", "join": ctx.jl.name,
                                 "g": g}, tmp_rename=True)
    keys_hasher = hashlib.sha256()
    index_hasher = hashlib.sha256()
    out_rows = 0
    index_entries = 0

    def emit(pl: bytes) -> None:
        nonlocal out_rows, index_entries
        if out_rows % g == 0:
            key, _seq = unpack_keys(pl)[:2]
            ipl = pack_key_index(key, keys_writer.tell())
            index_writer.write(ipl)
            index_hasher.update(ipl)
            index_entries += 1
        keys_writer.write(pl)
        keys_hasher.update(pl)
        out_rows += 1

    try:
        if runs:
            merge_runs(runs, lambda pl: unpack_keys(pl)[:2], ctx, emit, tag="keys")
        keys_writer.finish({"row_count": out_rows, "digest": keys_hasher.hexdigest()})
        index_writer.finish({"row_count": index_entries,
                             "digest": index_hasher.hexdigest()})
    except Exception:
        keys_writer.abort()
        index_writer.abort()
        raise
    for spec in runs:
        try:
            os.remove(spec[1])
        except OSError:
            pass
    _ckpt_set(conn, phase, {
        "done": True, "rows": out_rows, "digest": keys_hasher.hexdigest(),
        "index_entries": index_entries, "index_digest": index_hasher.hexdigest(),
        "g": g, "total_right_rows": int(norm_ckpt["rows"]),
    })
    _event(conn, "keysort_done", f"nonnull={total_nonnull} runs={run_no} g={g}")
    _persist_peak(ctx, conn)
    conn.commit()
    hook_fire(ctx.hook, "kill-after-keysort")


# --------------------------------------------------------------------------- #
# 磁盘位图（full 模式标记已匹配右行），带单页缓存
# --------------------------------------------------------------------------- #
class _Bitmap:
    PAGE = 4096

    def __init__(self, path: str, nbits: int, ledger: FileLedger) -> None:
        self.path = path
        self.nbits = nbits
        self.nbytes = (nbits + 7) // 8
        if not os.path.exists(path):
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as fh:
                remaining = self.nbytes
                zeros = b"\x00" * self.PAGE
                while remaining > 0:
                    chunk = zeros if remaining >= self.PAGE else zeros[:remaining]
                    fh.write(chunk)
                    remaining -= len(chunk)
                fh.flush()
                os.fsync(fh.fileno())
        if os.path.getsize(path) != self.nbytes:
            raise JoinFailed(f"位图文件长度损坏: {path}")
        self._fh = ledger.open(path, "r+b")
        self._page_no = -1
        self._page = bytearray(self.PAGE)
        self._dirty = False

    def _load(self, page_no: int) -> None:
        if page_no == self._page_no:
            return
        self._flush_page()
        self._fh.seek(page_no * self.PAGE)
        data = self._fh.read(self.PAGE)
        self._page = bytearray(data) + bytearray(self.PAGE - len(data))
        self._page_no = page_no
        self._dirty = False

    def _flush_page(self) -> None:
        if self._page_no >= 0 and self._dirty:
            off = self._page_no * self.PAGE
            self._fh.seek(off)
            self._fh.write(bytes(self._page[:max(0, min(self.PAGE,
                                                        self.nbytes - off))]))
            self._dirty = False

    def set(self, i: int) -> None:
        self._load((i // 8) // self.PAGE)
        self._page[(i // 8) % self.PAGE] |= 1 << (i % 8)
        self._dirty = True

    def get(self, i: int) -> bool:
        self._load((i // 8) // self.PAGE)
        return bool(self._page[(i // 8) % self.PAGE] & (1 << (i % 8)))

    def flush_fsync(self) -> None:
        self._flush_page()
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def close(self) -> None:
        try:
            self._flush_page()
            self._fh.flush()
        finally:
            self._fh.close()


# --------------------------------------------------------------------------- #
# 阶段四 emit：左流按源序探测右键流；同键组流式枚举 m×n
# --------------------------------------------------------------------------- #
class _KeysProber:
    """右键流探测：内存稀疏索引二分 + 顺序扫描定位同键组 [start, end)。"""

    def __init__(self, ctx: "_RunCtx", index_entries: list[tuple[bytes, int]]) -> None:
        self.ctx = ctx
        self.keys = [k for k, _ in index_entries]
        self.offsets = [o for _, o in index_entries]
        self._fh = ctx.ledger.open(ctx.jl.keys_path, "rb")
        _, self.data_start = read_header(self._fh, ctx.jl.keys_path)
        self._cache_key: Optional[bytes] = None
        self._cache_range: Optional[tuple[int, int]] = None

    def _frames_from(self, offset: int) -> Iterator[tuple[int, bytes, int, bytes]]:
        path = self.ctx.jl.keys_path
        self._fh.seek(offset)
        while True:
            frame_off = self._fh.tell()
            peek = self._fh.read(4)
            if not peek:
                raise JoinFailed(f"{path}: 记录区截断")
            if peek == J_FOOTER_PREFIX:
                rest = self._fh.read(len(J_FOOTER_MAGIC) - 4)
                if peek + rest != J_FOOTER_MAGIC:
                    raise JoinFailed(f"{path}: footer 魔数损坏")
                return
            (plen,) = struct.unpack("<I", peek)
            if plen <= 0 or plen > 256 * 1024 * 1024:
                raise JoinFailed(f"{path}: 非法记录长度 {plen}")
            payload = self._fh.read(plen)
            if len(payload) != plen:
                raise JoinFailed(f"{path}: 记录截断")
            digest = self._fh.read(32)
            if hashlib.sha256(payload).digest() != digest:
                raise JoinFailed(f"{path}: 记录摘要不符")
            key, seq, orig = unpack_keys(payload)
            yield frame_off, key, seq, orig

    def find(self, key: bytes) -> Optional[tuple[int, int]]:
        if self._cache_key == key:
            return self._cache_range
        # 最后一个键 < key 的索引条目：从那里顺序扫描必能覆盖完整的同键组
        # （组内记录相邻；条目键 == key 的条目可能已在组中间，不能从它开始）
        i = bisect.bisect_left(self.keys, key) - 1
        scan_from = self.offsets[i] if i >= 0 else self.data_start
        start: Optional[int] = None
        end: Optional[int] = None
        for frame_off, k, _seq, _orig in self._frames_from(scan_from):
            if k < key:
                continue
            if k == key:
                if start is None:
                    start = frame_off
                continue
            end = frame_off
            break
        if start is not None and end is None:
            end = self._scan_to_footer()  # 最大键的组延伸到 footer
        result = (start, end) if start is not None else None
        self._cache_key = key
        self._cache_range = result
        return result

    def _scan_to_footer(self) -> int:
        path = self.ctx.jl.keys_path
        self._fh.seek(self.data_start)
        off = self.data_start
        while True:
            peek = self._fh.read(4)
            if not peek:
                raise JoinFailed(f"{path}: 缺 footer")
            if peek == J_FOOTER_PREFIX:
                rest = self._fh.read(len(J_FOOTER_MAGIC) - 4)
                if peek + rest != J_FOOTER_MAGIC:
                    raise JoinFailed(f"{path}: footer 魔数损坏")
                return off
            (plen,) = struct.unpack("<I", peek)
            if plen <= 0 or plen > 256 * 1024 * 1024:
                raise JoinFailed(f"{path}: 非法记录长度 {plen}")
            self._fh.seek(plen + 32, os.SEEK_CUR)
            off = self._fh.tell()

    def scan_group(self, start: int, end: int) -> Iterator[tuple[int, bytes]]:
        for frame_off, _k, seq, orig in self._frames_from(start):
            if frame_off >= end:
                return
            yield seq, orig

    def close(self) -> None:
        self._fh.close()


def _load_key_index(ctx: "_RunCtx") -> list[tuple[bytes, int]]:
    entries = []
    for pl in iter_frames(ctx.jl.keys_index_path, ctx.ledger,
                          {"type": "join-keys-index", "join": ctx.jl.name}):
        entries.append(unpack_key_index(pl))
    entries.sort(key=lambda t: t[0])
    return entries


def _open_incremental(ctx: "_RunCtx", path: str, header: dict,
                      recorded_bytes: int) -> FramedWriter:
    """增量文件：无检查点字节则新建（覆盖未落定残留），否则截断到检查点。"""
    if recorded_bytes <= 0:
        writer = FramedWriter(ctx.ledger, path, header)
        writer.flush_fsync()
        return writer
    return FramedWriter(ctx.ledger, path, None, resume_bytes=recorded_bytes)


def _result_payload(ctx: "_RunCtx", orig: bytes) -> bytes:
    """full 视图保留完整原行；identity 视图只留身份字段以控制结果体积。"""
    if ctx.result_kind == KIND_FULL:
        return orig
    row = json_loads(orig)
    return json_dumps({"split_id": row["split_id"], "row_number": row["row_number"],
                       "key": row["key"]})


class _ResultSink:
    """结果流写出 + 稀疏索引 + 检查点（先 fsync 文件，再提交检查点）。"""

    def __init__(self, ctx: "_RunCtx", out_bytes: int, index_bytes: int) -> None:
        self.ctx = ctx
        self.out = _open_incremental(
            ctx, ctx.jl.result_path,
            {"type": "join-result", "join": ctx.jl.name, "mode": ctx.mode,
             "result_kind": ctx.result_kind}, out_bytes)
        self.index = _open_incremental(
            ctx, ctx.jl.result_index_path,
            {"type": "join-result-index", "join": ctx.jl.name}, index_bytes)
        self.buffered_out = _BufferedWriter(self.out, ctx.budget.emit_buffer)
        self.rows_written = 0  # 自上次检查点以来

    def write_row(self, seq: int, left: Optional[bytes], right: Optional[bytes]) -> None:
        if seq % RESULT_INDEX_EVERY == 0:
            self.index.write(pack_row_index(seq, self.out.tell()))
        self.buffered_out.write(pack_result(seq, left, right))
        self.rows_written += 1

    def fsync_files(self) -> None:
        self.buffered_out.flush()
        self.index.flush_fsync()

    def checkpoint_state(self) -> dict:
        return {"out_bytes": self.out.tell(), "index_bytes": self.index.tell()}

    def close(self) -> None:
        self.out.abort()
        self.index.abort()


def _do_emit(ctx: "_RunCtx", conn: sqlite3.Connection) -> None:
    phase = "emit"
    ckpt = _ckpt_get(conn, phase)
    if ckpt and ckpt.get("done"):
        return
    norm_ckpt = _ckpt_get(conn, "normalize.L")
    keys_ckpt = _ckpt_get(conn, "keysort.R")
    if not norm_ckpt or not keys_ckpt:
        raise JoinFailed("规范化/键排序未完成，不能 emit")
    state = ckpt or {}
    left_seq_next = int(state.get("left_seq_next", 0))
    norm_off = int(state.get("norm_off", 0))
    out_rows = int(state.get("out_rows", 0))
    out_bytes = int(state.get("out_bytes", 0))
    index_bytes = int(state.get("index_bytes", 0))
    if norm_off == 0:
        with ctx.ledger.open(ctx.jl.norm_path("L"), "rb") as fh:
            _, norm_off = read_header(fh, ctx.jl.norm_path("L"))

    sink = _ResultSink(ctx, out_bytes, index_bytes)
    bitmap: Optional[_Bitmap] = None
    if ctx.mode == MODE_FULL:
        bitmap = _Bitmap(ctx.jl.bitmap_path, int(keys_ckpt["total_right_rows"]), ctx.ledger)
    prober = _KeysProber(ctx, _load_key_index(ctx))
    norm_iter = iter_frames_from(ctx.jl.norm_path("L"), norm_off, ctx.ledger)
    ckpt_rows = ctx.checkpoint_rows
    try:
        for pl in norm_iter:
            seq, ktag, key, orig = unpack_norm(pl)
            if seq != left_seq_next:
                raise JoinFailed(
                    f"左规范化流序号 {seq} 与检查点 {left_seq_next} 不符，恢复状态损坏"
                )
            norm_off += FRAME_OVERHEAD + len(pl)
            out_payload_left = _result_payload(ctx, orig)
            group: Optional[tuple[int, int]] = None
            if ktag != KTAG_NULL:
                group = prober.find(key)
            if group is None:
                if ctx.mode != MODE_INNER:
                    sink.write_row(out_rows, out_payload_left, None)
                    out_rows += 1
                    hook_fire(ctx.hook, "kill-at-emit", out_rows)
            else:
                for rseq, rorig in prober.scan_group(*group):
                    sink.write_row(out_rows, out_payload_left,
                                   _result_payload(ctx, rorig))
                    out_rows += 1
                    if bitmap is not None:
                        bitmap.set(rseq)
                    hook_fire(ctx.hook, "kill-at-emit", out_rows)
            left_seq_next = seq + 1
            if sink.rows_written >= ckpt_rows:
                sink.fsync_files()
                if bitmap is not None:
                    bitmap.flush_fsync()
                st = {"left_seq_next": left_seq_next, "norm_off": norm_off,
                      "out_rows": out_rows}
                st.update(sink.checkpoint_state())
                _ckpt_set(conn, phase, st)
                _persist_peak(ctx, conn)
                conn.commit()
                sink.rows_written = 0
        sink.fsync_files()
        if bitmap is not None:
            bitmap.flush_fsync()
        st = {"done": True, "left_seq_next": left_seq_next, "norm_off": norm_off,
              "out_rows": out_rows}
        st.update(sink.checkpoint_state())
        _ckpt_set(conn, phase, st)
        _event(conn, "emit_done", f"out_rows={out_rows}")
        conn.commit()
    finally:
        sink.close()
        prober.close()
        norm_iter.close()
        if bitmap is not None:
            bitmap.close()


def _do_emit_tail(ctx: "_RunCtx", conn: sqlite3.Connection) -> None:
    """full 模式：按右源序追加所有未匹配右行。"""
    phase = "emit_tail"
    ckpt = _ckpt_get(conn, phase)
    if ckpt and ckpt.get("done"):
        return
    emit_ckpt = _ckpt_get(conn, "emit")
    norm_ckpt = _ckpt_get(conn, "normalize.R")
    if not emit_ckpt or not emit_ckpt.get("done") or not norm_ckpt:
        raise JoinFailed("emit 未完成，不能补未匹配右行")
    state = ckpt or {}
    right_seq_next = int(state.get("right_seq_next", 0))
    norm_off = int(state.get("norm_off", 0))
    out_rows = int(state.get("out_rows", emit_ckpt["out_rows"]))
    out_bytes = int(state.get("out_bytes", emit_ckpt["out_bytes"]))
    index_bytes = int(state.get("index_bytes", emit_ckpt["index_bytes"]))
    if norm_off == 0:
        with ctx.ledger.open(ctx.jl.norm_path("R"), "rb") as fh:
            _, norm_off = read_header(fh, ctx.jl.norm_path("R"))

    sink = _ResultSink(ctx, out_bytes, index_bytes)
    bitmap = _Bitmap(ctx.jl.bitmap_path, int(norm_ckpt["rows"]), ctx.ledger)
    ckpt_rows = ctx.checkpoint_rows
    try:
        for pl in iter_frames_from(ctx.jl.norm_path("R"), norm_off, ctx.ledger):
            seq, _ktag, _key, orig = unpack_norm(pl)
            if seq != right_seq_next:
                raise JoinFailed(
                    f"右规范化流序号 {seq} 与检查点 {right_seq_next} 不符，恢复状态损坏"
                )
            norm_off += FRAME_OVERHEAD + len(pl)
            if not bitmap.get(seq):
                sink.write_row(out_rows, None, _result_payload(ctx, orig))
                out_rows += 1
                hook_fire(ctx.hook, "kill-at-emit", out_rows)
            right_seq_next = seq + 1
            if sink.rows_written >= ckpt_rows:
                sink.fsync_files()
                st = {"right_seq_next": right_seq_next, "norm_off": norm_off,
                      "out_rows": out_rows}
                st.update(sink.checkpoint_state())
                _ckpt_set(conn, phase, st)
                _persist_peak(ctx, conn)
                conn.commit()
                sink.rows_written = 0
        sink.fsync_files()
        st = {"done": True, "right_seq_next": right_seq_next, "norm_off": norm_off,
              "out_rows": out_rows}
        st.update(sink.checkpoint_state())
        _ckpt_set(conn, phase, st)
        _event(conn, "emit_tail_done", f"out_rows={out_rows}")
        conn.commit()
    finally:
        sink.close()
        bitmap.close()


# --------------------------------------------------------------------------- #
# 阶段五 finalize：结果整流核验后才允许对外读（幂等：截断到检查点再核验）
# --------------------------------------------------------------------------- #
def _finalize_index(ctx: "_RunCtx", index_bytes: int) -> None:
    """给结果稀疏索引补 footer（截断到检查点 → 校验 → 写 footer，幂等）。"""
    path = ctx.jl.result_index_path
    with ctx.ledger.open(path, "r+b") as fh:
        fh.truncate(index_bytes)
        fh.seek(0)
        _, data_start = read_header(fh, path)
        fh.seek(data_start)
        hasher = hashlib.sha256()
        rows = 0
        consumed = 0
        remaining = index_bytes - data_start
        while consumed < remaining:
            peek = fh.read(4)
            if len(peek) != 4:
                raise JoinFailed("结果索引截断")
            (plen,) = struct.unpack("<I", peek)
            payload = fh.read(plen)
            digest = fh.read(32)
            if len(payload) != plen or len(digest) != 32 or \
                    hashlib.sha256(payload).digest() != digest:
                raise JoinFailed("结果索引记录损坏")
            hasher.update(payload)
            rows += 1
            consumed += FRAME_OVERHEAD + plen
        fb = json_dumps({"row_count": rows, "digest": hasher.hexdigest()})
        fh.seek(index_bytes)
        fh.write(J_FOOTER_MAGIC)
        fh.write(struct.pack("<I", len(fb)))
        fh.write(fb)
        fh.flush()
        os.fsync(fh.fileno())


def _do_finalize(ctx: "_RunCtx", conn: sqlite3.Connection) -> None:
    phase = "finalize"
    ckpt = _ckpt_get(conn, phase)
    if ckpt and ckpt.get("done"):
        return
    hook_fire(ctx.hook, "kill-before-finalize")
    tail = _ckpt_get(conn, "emit_tail")
    emit_ckpt = _ckpt_get(conn, "emit")
    if ctx.mode == MODE_FULL:
        if not tail or not tail.get("done"):
            raise JoinFailed("emit_tail 未完成，不能终验")
        out_rows = int(tail["out_rows"])
        out_bytes = int(tail["out_bytes"])
        index_bytes = int(tail["index_bytes"])
    else:
        if not emit_ckpt or not emit_ckpt.get("done"):
            raise JoinFailed("emit 未完成，不能终验")
        out_rows = int(emit_ckpt["out_rows"])
        out_bytes = int(emit_ckpt["out_bytes"])
        index_bytes = int(emit_ckpt["index_bytes"])
    # 整流核验：逐条摘要、序号 0..N-1 连续、计数一致、流摘要
    hasher = hashlib.sha256()
    count = 0
    with ctx.ledger.open(ctx.jl.result_path, "r+b") as fh:
        fh.truncate(out_bytes)  # 幂等：去掉可能已写的 footer，回到帧区终点
        fh.seek(0)
        _, data_start = read_header(fh, ctx.jl.result_path)
        remaining = out_bytes - data_start
        if remaining < 0:
            raise JoinFailed("结果检查点字节数小于数据区起点，状态损坏")
        consumed = 0
        while consumed < remaining:
            peek = fh.read(4)
            if len(peek) != 4:
                raise JoinFailed("结果流截断：帧头不完整")
            (plen,) = struct.unpack("<I", peek)
            payload = fh.read(plen)
            if len(payload) != plen:
                raise JoinFailed("结果流截断：帧体不完整")
            digest = fh.read(32)
            if len(digest) != 32 or hashlib.sha256(payload).digest() != digest:
                raise JoinFailed("结果记录摘要不符（内容损坏）")
            rseq, _l, _r = unpack_result(payload)
            if rseq != count:
                raise JoinFailed(f"结果序号不连续：期望 {count}，实际 {rseq}")
            hasher.update(payload)
            count += 1
            consumed += FRAME_OVERHEAD + plen
        if consumed != remaining:
            raise JoinFailed("结果流字节数与检查点不符")
        if count != out_rows:
            raise JoinFailed(f"结果行数 {count} 与检查点 {out_rows} 不符")
        # 核验通过：写 footer（结果可读的落盘标记），再落检查点
        fb = json_dumps({"row_count": count, "digest": hasher.hexdigest(),
                         "mode": ctx.mode, "result_kind": ctx.result_kind})
        fh.seek(out_bytes)
        fh.write(J_FOOTER_MAGIC)
        fh.write(struct.pack("<I", len(fb)))
        fh.write(fb)
        fh.flush()
        os.fsync(fh.fileno())
        final_bytes = fh.tell()
    _finalize_index(ctx, index_bytes)
    _ckpt_set(conn, phase, {"done": True, "rows": count, "digest": hasher.hexdigest(),
                            "frames_bytes": out_bytes, "bytes": final_bytes})
    _meta_set(conn, "result", {"rows": count, "digest": hasher.hexdigest(),
                               "bytes": final_bytes})
    _event(conn, "finalize_done", f"rows={count}")
    conn.commit()


# --------------------------------------------------------------------------- #
# 运行上下文与主入口
# --------------------------------------------------------------------------- #
class _RunCtx:
    def __init__(self, jl: JoinLayout, meta: dict, hook) -> None:
        self.jl = jl
        self.mode = meta["mode"]
        self.result_kind = meta["result_kind"]
        self.checkpoint_rows = int(meta["checkpoint_rows"])
        self.budget = Budget(int(meta["mem_bytes"]), int(meta["max_open_files"]))
        self.ledger = FileLedger(self.budget.max_open_files)
        self.sources = meta["sources"]
        self.hook = hook
        self.received_total = 0


def _load_meta(conn: sqlite3.Connection) -> dict:
    keys = ("name", "mode", "result_kind", "mem_bytes", "max_open_files",
            "checkpoint_rows", "state", "phase", "sources", "protections_released")
    return {k: _meta_get(conn, k) for k in keys}


def run_join(workdir: str, name: str, hook: Optional[str] = None) -> dict:
    """运行关联到完成；任何中断后再次调用即从持久检查点恢复。

    恢复边界：已持久接收的源段绝不重读；未落定的范围（接收中的段、
    未完成的归并/排序、检查点之后的输出）允许重做。
    """
    jl = JoinLayout(workdir, name)
    if not os.path.exists(jl.db_path):
        raise JoinError(f"关联不存在: {jl.dir}")
    conn = _connect_join(jl)
    try:
        meta = _load_meta(conn)
        if meta["state"] == "done":
            return join_status(workdir, name)
        if meta["state"] not in ("created", "running"):
            raise JoinError(f"关联状态为 {meta['state']}，不能运行")
        hook_spec = parse_hook(hook or os.environ.get("SHUFFLE_JOIN_HOOK"))
        ctx = _RunCtx(jl, meta, hook_spec)
        _meta_set(conn, "state", "running")
        conn.commit()
        try:
            for phase_name, fn in (
                ("receive", lambda: _do_receive(ctx, conn)),
                ("normalize.L", lambda: _do_normalize(ctx, conn, "left")),
                ("normalize.R", lambda: _do_normalize(ctx, conn, "right")),
                ("keysort.R", lambda: _do_keysort(ctx, conn)),
                ("emit", lambda: _do_emit(ctx, conn)),
                ("emit_tail", lambda: _do_emit_tail(ctx, conn)),
                ("finalize", lambda: _do_finalize(ctx, conn)),
            ):
                if phase_name == "emit_tail" and ctx.mode != MODE_FULL:
                    continue
                _meta_set(conn, "phase", phase_name)
                conn.commit()
                fn()
        except Exception as exc:
            _meta_set(conn, "last_error", f"{type(exc).__name__}: {exc}")
            _event(conn, "run_failed", f"{type(exc).__name__}: {exc}")
            conn.commit()
            raise
        _meta_set(conn, "state", "done")
        _meta_set(conn, "phase", "done")
        _event(conn, "join_done", "")
        conn.commit()
        # 结果已完整交付：释放两侧源段保护（不影响其他 reader/关联）
        if not meta["protections_released"]:
            for role in ("left", "right"):
                try:
                    _release_protection(meta["sources"][role])
                except Exception as exc:  # noqa: BLE001 - 源库缺失不阻断完成
                    _event(conn, "release_warning", f"{role}: {exc}")
            _meta_set(conn, "protections_released", True)
        _persist_peak(ctx, conn)
        conn.commit()
    finally:
        conn.close()
    return join_status(workdir, name)


# --------------------------------------------------------------------------- #
# 状态查看
# --------------------------------------------------------------------------- #
def join_status(workdir: str, name: str) -> dict:
    jl = JoinLayout(workdir, name)
    if not os.path.exists(jl.db_path):
        raise JoinError(f"关联不存在: {jl.dir}")
    conn = _connect_join(jl)
    try:
        meta = _load_meta(conn)
        meta["last_error"] = _meta_get(conn, "last_error")
        meta["result"] = _meta_get(conn, "result")
        receive = {}
        for side in ("L", "R"):
            tot = conn.execute(
                "SELECT COUNT(*) AS n, COALESCE(SUM(row_count),0) AS rows"
                " FROM segments WHERE side=?", (side,)).fetchone()
            got = conn.execute(
                "SELECT COUNT(*) AS n, COALESCE(SUM(recv_rows),0) AS rows,"
                " COALESCE(SUM(recv_bytes),0) AS bytes FROM segments"
                " WHERE side=? AND state='received'", (side,)).fetchone()
            per_split = [dict(r) for r in conn.execute(
                "SELECT split_ord, COUNT(*) AS segments,"
                " SUM(CASE state WHEN 'received' THEN 1 ELSE 0 END) AS received"
                " FROM segments WHERE side=? GROUP BY split_ord ORDER BY split_ord",
                (side,)).fetchall()]
            receive[side] = {
                "segments_total": int(tot["n"]), "segments_received": int(got["n"]),
                "rows_received": int(got["rows"]), "bytes_received": int(got["bytes"]),
                "rows_total": int(tot["rows"]), "per_split": per_split,
            }
        counters = {r["name"]: int(r["value"]) for r in
                    conn.execute("SELECT name, value FROM counters").fetchall()}
        ckpts = {r["phase"]: json_loads(r["data"].encode("utf-8")) for r in
                 conn.execute("SELECT phase, data FROM checkpoints").fetchall()}
        return {
            "name": name, "dir": jl.dir, "state": meta["state"], "phase": meta["phase"],
            "mode": meta["mode"], "result_kind": meta["result_kind"],
            "budgets": {"mem_bytes": meta["mem_bytes"],
                        "max_open_files": meta["max_open_files"]},
            "sources": meta["sources"],
            "protections_released": meta["protections_released"],
            "receive": receive,
            "checkpoints": ckpts,
            "counters": counters,
            "result": meta["result"],
            "last_error": meta["last_error"],
        }
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 结果读取：只有 finalize 核验通过后才可读；损坏即显式失败
# --------------------------------------------------------------------------- #
class JoinResult:
    def __init__(self, workdir: str, name: str) -> None:
        jl = JoinLayout(workdir, name)
        self.jl = jl
        if not os.path.exists(jl.db_path):
            raise JoinError(f"关联不存在: {jl.dir}")
        conn = _connect_join(jl)
        try:
            meta = _load_meta(conn)
            if meta["state"] != "done":
                raise ResultNotReady(
                    f"关联状态为 {meta['state']}，结果尚未完整核验，拒绝读取"
                )
            ckpt = _ckpt_get(conn, "finalize")
            if not ckpt or not ckpt.get("done"):
                raise ResultNotReady("结果缺少终验检查点，拒绝读取")
            self._rows = int(ckpt["rows"])
            self._digest = ckpt["digest"]
            self._frames_bytes = int(ckpt["frames_bytes"])
        finally:
            conn.close()
        self._ledger = FileLedger(64)
        self._fh = self._ledger.open(jl.result_path, "rb")
        try:
            self._header, self._data_start = read_header(self._fh, jl.result_path)
            # footer 必须恰好位于记录区之后且文件到此为止（长度损坏即报错）
            self._fh.seek(self._frames_bytes)
            magic = self._fh.read(len(J_FOOTER_MAGIC))
            if magic != J_FOOTER_MAGIC:
                raise JoinFailed("结果 footer 缺失（文件被截断或改写）")
            (flen,) = struct.unpack("<I", self._read_exact(4))
            footer_bytes = self._read_exact(flen)
            footer = json_loads(footer_bytes)
            if footer.get("row_count") != self._rows:
                raise JoinFailed("结果 footer 行数与终验检查点不符")
            if footer.get("digest") != self._digest:
                raise JoinFailed("结果 footer 摘要与终验检查点不符")
            if os.path.getsize(jl.result_path) != self._fh.tell():
                raise JoinFailed("结果文件长度与终验记录不符（尾部被改写）")
            # 稀疏索引
            self._idx_seq: list[int] = []
            self._idx_off: list[int] = []
            for pl in iter_frames(jl.result_index_path, self._ledger,
                                  {"type": "join-result-index", "join": name}):
                s, o = unpack_row_index(pl)
                self._idx_seq.append(s)
                self._idx_off.append(o)
        except Exception:
            self._fh.close()
            raise

    def _read_exact(self, n: int) -> bytes:
        data = self._fh.read(n)
        if len(data) != n:
            raise JoinFailed("结果 footer 截断")
        return data

    @property
    def row_count(self) -> int:
        return self._rows

    def _row_from_payload(self, payload: bytes) -> dict:
        seq, left, right = unpack_result(payload)
        return {
            "seq": seq,
            "left": json_loads(left) if left is not None else None,
            "right": json_loads(right) if right is not None else None,
        }

    def _read_one(self) -> Optional[bytes]:
        peek = self._fh.read(4)
        if not peek:
            return None
        if peek == J_FOOTER_PREFIX:
            rest = self._fh.read(len(J_FOOTER_MAGIC) - 4)
            if peek + rest == J_FOOTER_MAGIC:
                return None
            raise JoinFailed("结果 footer 魔数损坏")
        (plen,) = struct.unpack("<I", peek)
        if plen <= 0 or plen > 256 * 1024 * 1024:
            raise JoinFailed(f"结果记录长度非法 {plen}")
        payload = self._fh.read(plen)
        if len(payload) != plen:
            raise JoinFailed("结果记录截断")
        digest = self._fh.read(32)
        if len(digest) != 32 or hashlib.sha256(payload).digest() != digest:
            raise JoinFailed("结果记录摘要不符（内容损坏）")
        return payload

    def _scan_from(self, start: int) -> Iterator[bytes]:
        i = bisect.bisect_right(self._idx_seq, start) - 1
        offset = self._idx_off[i] if i >= 0 else self._data_start
        self._fh.seek(offset)
        expected = self._idx_seq[i] if i >= 0 else 0
        while expected <= start - 1:  # 稀疏索引定位后顺序扫描到起点
            payload = self._read_one()
            if payload is None:
                return
            rseq, _l, _r = unpack_result(payload)
            if rseq != expected:
                raise JoinFailed(f"结果序号不连续：期望 {expected}，实际 {rseq}")
            expected += 1
        while True:
            payload = self._read_one()
            if payload is None:
                return
            yield payload

    def read(self, start: int, count: int) -> list[dict]:
        """按起始序号读取 count 条；越界部分返回空。分页拼接与整次流一致。"""
        if start < 0 or count < 0:
            raise JoinError("start/count 必须非负")
        out: list[dict] = []
        if count == 0 or start >= self._rows:
            return out
        for payload in self._scan_from(start):
            row = self._row_from_payload(payload)
            if row["seq"] != start + len(out):
                raise JoinFailed("结果序号不连续（顺序损坏）")
            out.append(row)
            if len(out) >= count:
                break
        return out

    def iter(self, start: int = 0) -> Iterator[dict]:
        """从 start 迭代到末尾；读完全程时核验总条数与流摘要。"""
        if start < 0:
            raise JoinError("start 必须非负")
        hasher = hashlib.sha256() if start == 0 else None
        n = 0
        for payload in self._scan_from(start):
            row = self._row_from_payload(payload)
            if row["seq"] != start + n:
                raise JoinFailed("结果序号不连续（顺序损坏）")
            if hasher is not None:
                hasher.update(payload)
            n += 1
            yield row
        if start + n != self._rows:
            raise JoinFailed(
                f"结果条数 {start + n} 与终验 {self._rows} 不符（数量损坏）"
            )
        if hasher is not None and hasher.hexdigest() != self._digest:
            raise JoinFailed("结果流摘要与终验不符（内容损坏）")

    def close(self) -> None:
        self._fh.close()

    def __enter__(self) -> "JoinResult":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def delete_join(workdir: str, name: str) -> dict:
    """删除关联：释放它自己的源段保护（不影响别的 reader/关联），移除工作目录。"""
    jl = JoinLayout(workdir, name)
    if not os.path.exists(jl.db_path):
        raise JoinError(f"关联不存在: {jl.dir}")
    conn = _connect_join(jl)
    try:
        meta = _load_meta(conn)
        sources = meta["sources"] or {}
        released = bool(meta["protections_released"])
        state = meta["state"]
    finally:
        conn.close()
    if not released:
        for role in ("left", "right"):
            if role in sources:
                try:
                    _release_protection(sources[role])
                except Exception:
                    pass
    shutil.rmtree(jl.dir, ignore_errors=True)
    return {"name": name, "deleted": True, "previous_state": state,
            "protections_released": True}
