"""SQLite 协调器：作业绑定、attempt 派工、winner 确认、恢复与回收。

关键不变量：
  * 建作业时固定 split 清单、源摘要、分区数与分区函数版本，之后不可改；
  * winner 确认在一个 ``IMMEDIATE`` 事务内完成整 attempt 全分区核对，
    两个工人同时申请时只有一个能成功，先到者不可被替换；
  * 核对全部流式进行（逐段逐行），不按分区攒 Python 列表；
  * 协调器重启只靠 SQLite/文件现场恢复：已确认 winner 继续可见，
    未完成 attempt 在确认进程已死后标记 failed，绝不变成可见结果。
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import time
from typing import Callable, Optional

from .common import (
    ATT_COMPLETED,
    ATT_FAILED,
    ATT_LOST,
    ATT_RUNNING,
    ATT_WINNER,
    JOB_CANCELLED,
    JOB_DESTROYED,
    JOB_OPEN,
    IdentityChanged,
    SegmentCorrupt,
    ShuffleError,
    connect,
    init_db,
    json_loads,
)
from .common import Layout, canonical_row_bytes, log_event, partition_of
from .segment import SegmentRecord, iter_segment_rows


class AlreadyWinner(ShuffleError):
    """该 split 已有持久 winner；晚到的完整 attempt 只能成为输者。"""


# --------------------------------------------------------------------------- #
# 建作业
# --------------------------------------------------------------------------- #
def _stream_file_digest(path: str) -> tuple[str, int]:
    """流式计算源 split 原始字节摘要与行数（JSON Lines）。"""
    import hashlib

    if not os.path.isfile(path):
        raise ShuffleError(f"输入文件不存在或不是普通文件: {path}")
    h = hashlib.sha256()
    rows = 0
    with open(path, "rb") as fh:
        for line in fh:
            if not line.strip():
                raise ShuffleError(f"{path}: 存在空行，输入必须是规整 JSON Lines")
            obj = json_loads(line)
            if not isinstance(obj, dict) or "key" not in obj or "value" not in obj:
                raise ShuffleError(f"{path}: 第 {rows + 1} 行必须是含 key/value 的 JSON 对象")
            key = obj["key"]
            if key is not None and not isinstance(key, str):
                raise ShuffleError(f"{path}: 第 {rows + 1} 行 key 必须是 text 或 null")
            h.update(line)
            rows += 1
    return h.hexdigest(), rows


def create_job(layout: Layout, job_id: str, splits: list[tuple[str, str]],
               num_partitions: int, partition_version: int = 0) -> dict:
    """建作业并固定输入绑定。

    splits 为 ``(split_id, input_path)`` 的有序清单，次序就是将来读取器
    跨 split 交付的固定次序。
    """
    if not splits:
        raise ShuffleError("至少需要一个 split")
    ids = [s[0] for s in splits]
    if len(set(ids)) != len(ids):
        raise ShuffleError("split_id 清单存在重复")
    conn = connect(layout.db_path)
    try:
        init_db(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            exists = conn.execute("SELECT 1 FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if exists:
                raise ShuffleError(f"作业 {job_id} 已存在")
            conn.execute(
                "INSERT INTO jobs(job_id,num_partitions,partition_version,state,created_at)"
                " VALUES (?,?,?,?,?)",
                (job_id, num_partitions, partition_version, JOB_OPEN, time.time()),
            )
            bound = []
            for ord_, (split_id, input_path) in enumerate(splits):
                digest, rows = _stream_file_digest(input_path)
                conn.execute(
                    "INSERT INTO job_splits(job_id,ord,split_id,input_path,input_digest,row_count)"
                    " VALUES (?,?,?,?,?,?)",
                    (job_id, ord_, split_id, os.path.abspath(input_path), digest, rows),
                )
                bound.append({"split_id": split_id, "ord": ord_, "row_count": rows,
                              "input_digest": digest})
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        log_event(conn, job_id, "job_created",
                  f"partitions={num_partitions} version={partition_version} splits={len(splits)}")
        conn.commit()
        return {"job_id": job_id, "num_partitions": num_partitions,
                "partition_version": partition_version, "splits": bound}
    finally:
        conn.close()


def get_job(conn: sqlite3.Connection, job_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
    if row is None:
        raise ShuffleError(f"作业不存在: {job_id}")
    return row


def require_open(conn: sqlite3.Connection, job_id: str) -> sqlite3.Row:
    job = get_job(conn, job_id)
    if job["state"] == JOB_CANCELLED:
        from .common import JobCancelled

        raise JobCancelled(f"作业 {job_id} 已取消")
    if job["state"] == JOB_DESTROYED:
        raise ShuffleError(f"作业 {job_id} 已销毁")
    return job


# --------------------------------------------------------------------------- #
# 派工
# --------------------------------------------------------------------------- #
def launch_attempt(layout: Layout, job_id: str, split_id: str, mem_bytes: int) -> dict:
    """为某个 split 登记新的 attempt（同一 split 允许并行 attempt）。

    已存在 winner 后拒绝派工；attempt 编号在该 split 内单调递增。
    """
    conn = connect(layout.db_path)
    try:
        init_db(conn)
        require_open(conn, job_id)
        split = conn.execute(
            "SELECT * FROM job_splits WHERE job_id=? AND split_id=?", (job_id, split_id)
        ).fetchone()
        if split is None:
            raise ShuffleError(f"作业 {job_id} 未绑定 split {split_id}")
        winner = conn.execute(
            "SELECT 1 FROM winners WHERE job_id=? AND split_id=?", (job_id, split_id)
        ).fetchone()
        if winner:
            raise AlreadyWinner(f"{split_id} 已有 winner，不能再派工")
        row = conn.execute(
            "SELECT COALESCE(MAX(attempt_no), -1) + 1 AS next FROM attempts"
            " WHERE job_id=? AND split_id=?",
            (job_id, split_id),
        ).fetchone()
        attempt_no = int(row["next"])
        conn.execute(
            "INSERT INTO attempts(job_id,split_id,attempt_no,state,pid,mem_bytes,launched_at)"
            " VALUES (?,?,?,?,?,?,?)",
            (job_id, split_id, attempt_no, ATT_RUNNING, os.getpid(), mem_bytes, time.time()),
        )
        log_event(conn, job_id, "attempt_launched", f"mem_bytes={mem_bytes}",
                  split_id, attempt_no)
        conn.commit()
        return {"job_id": job_id, "split_id": split_id, "attempt_no": attempt_no,
                "mem_bytes": mem_bytes}
    finally:
        conn.close()


def attach_pid(layout: Layout, job_id: str, split_id: str, attempt_no: int, pid: int) -> None:
    """工人进程启动后把真实 PID 登记到 attempt（恢复时据此判断死活）。"""
    conn = connect(layout.db_path)
    try:
        conn.execute(
            "UPDATE attempts SET pid=? WHERE job_id=? AND split_id=? AND attempt_no=?",
            (pid, job_id, split_id, attempt_no),
        )
        conn.commit()
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 工人侧段登记
# --------------------------------------------------------------------------- #
def register_segment_tmp(layout: Layout, job_id: str, split_id: str, attempt_no: int,
                         partition_id: int, seq: int, relpath: str) -> None:
    conn = connect(layout.db_path)
    try:
        conn.execute(
            "INSERT INTO segments(job_id,split_id,attempt_no,partition_id,seq,relpath,"
            "row_count,byte_length,seg_digest,state) VALUES (?,?,?,?,?,?,0,0,'','tmp')",
            (job_id, split_id, attempt_no, partition_id, seq, relpath),
        )
        conn.commit()
    finally:
        conn.close()


def register_segment_done(layout: Layout, job_id: str, split_id: str, attempt_no: int,
                          rec: SegmentRecord) -> None:
    conn = connect(layout.db_path)
    try:
        conn.execute(
            "UPDATE segments SET row_count=?, byte_length=?, seg_digest=?, state='done'"
            " WHERE job_id=? AND split_id=? AND attempt_no=? AND partition_id=? AND seq=?",
            (rec.row_count, rec.byte_length, rec.seg_digest,
             job_id, split_id, attempt_no, rec.partition_id, rec.seq),
        )
        conn.commit()
    finally:
        conn.close()


def mark_attempt_completed(layout: Layout, job_id: str, split_id: str, attempt_no: int) -> None:
    conn = connect(layout.db_path)
    try:
        conn.execute(
            "UPDATE attempts SET state=?, finished_at=? "
            "WHERE job_id=? AND split_id=? AND attempt_no=? AND state=?",
            (ATT_COMPLETED, time.time(), job_id, split_id, attempt_no, ATT_RUNNING),
        )
        log_event(conn, job_id, "attempt_segments_complete", "", split_id, attempt_no)
        conn.commit()
    finally:
        conn.close()


def mark_attempt_failed(layout: Layout, job_id: str, split_id: str, attempt_no: int,
                        detail: str) -> None:
    conn = connect(layout.db_path)
    try:
        conn.execute(
            "UPDATE attempts SET state=?, finished_at=? "
            "WHERE job_id=? AND split_id=? AND attempt_no=? AND state IN (?,?)",
            (ATT_FAILED, time.time(), job_id, split_id, attempt_no, ATT_RUNNING, ATT_COMPLETED),
        )
        log_event(conn, job_id, "attempt_failed", detail, split_id, attempt_no)
        conn.commit()
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# winner 确认：整 attempt 全分区实际核对
# --------------------------------------------------------------------------- #
def _abs_seg_path(layout: Layout, job_id: str, relpath: str) -> str:
    base = os.path.realpath(layout.job_dir(job_id))
    path = os.path.realpath(os.path.join(base, relpath))
    if not path.startswith(base + os.sep):
        raise ShuffleError(f"段路径逃逸作业目录: {relpath}")
    return path


def verify_attempt(layout: Layout, conn: sqlite3.Connection, job: sqlite3.Row,
                   split: sqlite3.Row, attempt_no: int) -> dict:
    """流式核对一个 attempt 的全部目标分区。

    检查项：每个分区（含空分区）至少有一个 done 段；文件存在且长度与登记一致；
    逐行行摘要、段摘要、行数；行身份的 split_id 与 row_number 严格递增；
    分区键复算与所在分区一致；总行数与建作业绑定行数一致。
    """
    job_id = job["job_id"]
    num_p = int(job["num_partitions"])
    version = int(job["partition_version"])
    segs = conn.execute(
        "SELECT * FROM segments WHERE job_id=? AND split_id=? AND attempt_no=? "
        "ORDER BY partition_id, seq",
        (job_id, split["split_id"], attempt_no),
    ).fetchall()
    by_p: dict[int, list[sqlite3.Row]] = {p: [] for p in range(num_p)}
    for s in segs:
        if s["state"] != "done":
            raise ShuffleError(
                f"分区 {s['partition_id']} 段 seq={s['seq']} 状态为 {s['state']}，"
                "存在未写完的段，不能确认"
            )
        by_p.setdefault(int(s["partition_id"]), []).append(s)
    missing = [p for p, lst in by_p.items() if not lst]
    if missing:
        raise ShuffleError(f"attempt 未覆盖全部目标分区，缺少: {missing}（空分区也必须有完成段）")

    total_rows = 0
    per_partition: dict[int, dict] = {}
    for p in range(num_p):
        last_row_no = 0
        p_rows = 0
        p_digest = hashlib.sha256()
        for s in by_p[p]:
            path = _abs_seg_path(layout, job_id, s["relpath"])
            if not os.path.exists(path):
                raise ShuffleError(f"段文件丢失: {path}")
            if os.path.getsize(path) != int(s["byte_length"]):
                raise ShuffleError(f"段长度与登记不符: {path}")
            expected = {"job_id": job_id, "split_id": split["split_id"],
                        "attempt_no": attempt_no, "partition_id": p, "seq": int(s["seq"])}
            seg_rows = 0
            for payload, _digest in iter_segment_rows(path, expected):
                try:
                    row = json_loads(payload)
                except (UnicodeDecodeError, ValueError) as exc:
                    raise SegmentCorrupt(f"{path}: 行 JSON 损坏: {exc}") from exc
                if row.get("split_id") != split["split_id"]:
                    raise IdentityChanged(
                        f"段内出现他 split 的行: {row.get('split_id')!r} != {split['split_id']!r}"
                    )
                rn = int(row["row_number"])
                if rn <= last_row_no:
                    raise ShuffleError(
                        f"分区 {p} 内 row_number 非严格递增: {last_row_no} -> {rn}"
                    )
                last_row_no = rn
                if not (1 <= rn <= int(split["row_count"])):
                    raise ShuffleError(f"row_number 越界: {rn}")
                if partition_of(row["key"], num_p, version) != p:
                    raise ShuffleError(
                        f"行 (row_number={rn}) 的键复算分区与所在段分区 {p} 不一致"
                    )
                # 复算规范行字节，防止段内 payload 与登记身份被改写
                if canonical_row_bytes(row["split_id"], rn, row["key"], row["value"]) != payload:
                    raise ShuffleError(f"行规范化编码被破坏: row_number={rn}")
                p_digest.update(payload)
                seg_rows += 1
            if seg_rows != int(s["row_count"]):
                raise ShuffleError(f"段登记行数 {s['row_count']} 与实读 {seg_rows} 不符")
            p_rows += seg_rows
        per_partition[p] = {"rows": p_rows, "digest": p_digest.hexdigest(),
                            "segments": [int(s["seq"]) for s in by_p[p]]}
        total_rows += p_rows
    if total_rows != int(split["row_count"]):
        raise ShuffleError(
            f"attempt 总行数 {total_rows} 与建作业绑定 {split['row_count']} 不符"
        )
    return {"row_count": total_rows, "partitions": per_partition}


def claim_winner(layout: Layout, job_id: str, split_id: str, attempt_no: int,
                 before_commit_hook: Optional[Callable[[], None]] = None) -> dict:
    """把一个完整 attempt 持久确认为 winner。

    两个工人并发调用时，SQLite ``BEGIN IMMEDIATE`` 串行化事务：先提交者写入
    winners 行；后者在同一事务里看到既有 winner，本 attempt 转 lost 并显式失败。
    winner 一经提交，任何后续调用都不能替换它。
    """
    conn = connect(layout.db_path)
    try:
        init_db(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            job = require_open(conn, job_id)
            split = conn.execute(
                "SELECT * FROM job_splits WHERE job_id=? AND split_id=?", (job_id, split_id)
            ).fetchone()
            if split is None:
                raise ShuffleError(f"未绑定 split: {split_id}")
            existing = conn.execute(
                "SELECT attempt_no FROM winners WHERE job_id=? AND split_id=?",
                (job_id, split_id),
            ).fetchone()
            att = conn.execute(
                "SELECT * FROM attempts WHERE job_id=? AND split_id=? AND attempt_no=?",
                (job_id, split_id, attempt_no),
            ).fetchone()
            if att is None:
                raise ShuffleError(f"attempt 不存在: {split_id}#{attempt_no}")
            if existing is not None:
                if int(existing["attempt_no"]) == attempt_no:
                    # 现任 winner 重复确认（工人未收到响应后重试）：幂等返回，
                    # 不重新核对、不改状态，winner 一旦可读即固定
                    conn.commit()
                    log_event(conn, job_id, "winner_reconfirm_idempotent", "",
                              split_id, attempt_no)
                    conn.commit()
                    return {"job_id": job_id, "split_id": split_id,
                            "winner_attempt": attempt_no, "idempotent": True}
                # 晚到的其他 attempt：登记为输者；winner 永不替换
                if att["state"] != ATT_WINNER:
                    conn.execute(
                        "UPDATE attempts SET state=? WHERE job_id=? AND split_id=? AND attempt_no=?",
                        (ATT_LOST, job_id, split_id, attempt_no),
                    )
                    log_event(conn, job_id, "attempt_lost",
                              f"winner 已是 attempt {existing['attempt_no']}",
                              split_id, attempt_no)
                conn.commit()
                raise AlreadyWinner(
                    f"{split_id} 的 winner 已固定为 attempt {existing['attempt_no']}，"
                    f"attempt {attempt_no} 作为输者持久化"
                )
            if att["state"] != ATT_COMPLETED:
                raise ShuffleError(
                    f"attempt 状态为 {att['state']}，段未齐备不能确认 winner"
                )
            verify_attempt(layout, conn, job, split, attempt_no)
            if before_commit_hook is not None:
                # 故障注入点：核对已过、提交前被杀（演示“确认后未收到响应”）
                before_commit_hook()
            conn.execute(
                "INSERT INTO winners(job_id,split_id,attempt_no,confirmed_at) VALUES (?,?,?,?)",
                (job_id, split_id, attempt_no, time.time()),
            )
            conn.execute(
                "UPDATE attempts SET state=?, claimed_at=? "
                "WHERE job_id=? AND split_id=? AND attempt_no=?",
                (ATT_WINNER, time.time(), job_id, split_id, attempt_no),
            )
            conn.execute(
                "UPDATE attempts SET state=? WHERE job_id=? AND split_id=? AND attempt_no<>?",
                (ATT_LOST, job_id, split_id, attempt_no),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        log_event(conn, job_id, "winner_confirmed", "", split_id, attempt_no)
        conn.commit()
        return {"job_id": job_id, "split_id": split_id, "winner_attempt": attempt_no}
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 状态查看
# --------------------------------------------------------------------------- #
def inspect(layout: Layout, job_id: str) -> dict:
    conn = connect(layout.db_path)
    try:
        init_db(conn)
        job = get_job(conn, job_id)
        splits = []
        for sp in conn.execute(
            "SELECT * FROM job_splits WHERE job_id=? ORDER BY ord", (job_id,)
        ).fetchall():
            attempts = []
            for a in conn.execute(
                "SELECT * FROM attempts WHERE job_id=? AND split_id=? ORDER BY attempt_no",
                (job_id, sp["split_id"]),
            ).fetchall():
                seg_rows = conn.execute(
                    "SELECT partition_id, COUNT(*) AS n, SUM(row_count) AS rows,"
                    " SUM(CASE state WHEN 'done' THEN 1 ELSE 0 END) AS done_n"
                    " FROM segments WHERE job_id=? AND split_id=? AND attempt_no=?"
                    " GROUP BY partition_id ORDER BY partition_id",
                    (job_id, sp["split_id"], a["attempt_no"]),
                ).fetchall()
                attempts.append({
                    "attempt_no": a["attempt_no"], "state": a["state"], "pid": a["pid"],
                    "partitions": [
                        {"partition_id": r["partition_id"], "segments": r["n"],
                         "done_segments": r["done_n"], "rows": r["rows"] or 0}
                        for r in seg_rows
                    ],
                })
            w = conn.execute(
                "SELECT attempt_no FROM winners WHERE job_id=? AND split_id=?",
                (job_id, sp["split_id"]),
            ).fetchone()
            splits.append({
                "ord": sp["ord"], "split_id": sp["split_id"], "row_count": sp["row_count"],
                "input_digest": sp["input_digest"],
                "winner_attempt": w["attempt_no"] if w else None,
                "attempts": attempts,
            })
        readers = conn.execute(
            "SELECT reader_id, state, opened_at, closed_at FROM readers"
            " WHERE job_id=? ORDER BY opened_at", (job_id,)
        ).fetchall()
        events = conn.execute(
            "SELECT ts, split_id, attempt_no, kind, detail FROM events"
            " WHERE job_id=? ORDER BY rowid", (job_id,)
        ).fetchall()
        return {
            "job_id": job_id, "state": job["state"],
            "num_partitions": job["num_partitions"],
            "partition_version": job["partition_version"],
            "splits": splits,
            "readers": [dict(r) for r in readers],
            "events": [dict(e) for e in events],
        }
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 取消 / 恢复 / 清理
# --------------------------------------------------------------------------- #
def cancel_job(layout: Layout, job_id: str) -> None:
    conn = connect(layout.db_path)
    try:
        init_db(conn)
        job = get_job(conn, job_id)
        if job["state"] != JOB_OPEN:
            raise ShuffleError(f"作业状态为 {job['state']}，无法取消")
        conn.execute("UPDATE jobs SET state=? WHERE job_id=?", (JOB_CANCELLED, job_id))
        log_event(conn, job_id, "job_cancelled", "新读取被拒绝；已开 reader 按原清单结束或失败")
        conn.commit()
    finally:
        conn.close()


def _pid_alive(pid: Optional[int]) -> bool:
    """受控进程状态检查（本机演示，不实现跨主机租约）。"""
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def recover(layout: Layout, job_id: str) -> dict:
    """协调器重启后的现场恢复。

      * 已确认 winner：重新核对其段可读性，保持可见；损坏则显式报错而不重派；
      * running/completed 但进程已死的 attempt：标记 failed（确认其不再执行后
        才能重新派工）；进程仍活着则保留 running；
      * 段表中的 tmp 段（写一半被杀）随 failed attempt 留给 cleanup 回收。
    """
    conn = connect(layout.db_path)
    try:
        init_db(conn)
        job = get_job(conn, job_id)
        result = {"winners": [], "failed_attempts": [], "still_running": [], "errors": []}
        winners = conn.execute(
            "SELECT * FROM winners WHERE job_id=?", (job_id,)
        ).fetchall()
        for w in winners:
            split = conn.execute(
                "SELECT * FROM job_splits WHERE job_id=? AND split_id=?",
                (job_id, w["split_id"]),
            ).fetchone()
            try:
                verify_attempt(layout, conn, job, split, int(w["attempt_no"]))
                result["winners"].append(
                    {"split_id": w["split_id"], "attempt_no": w["attempt_no"], "verified": True}
                )
            except ShuffleError as exc:
                result["errors"].append(
                    {"split_id": w["split_id"], "attempt_no": w["attempt_no"], "error": str(exc)}
                )
                log_event(conn, job_id, "winner_verify_failed", str(exc),
                          w["split_id"], w["attempt_no"])
        for a in conn.execute(
            "SELECT * FROM attempts WHERE job_id=? AND state IN (?,?)",
            (job_id, ATT_RUNNING, ATT_COMPLETED),
        ).fetchall():
            if _pid_alive(a["pid"]):
                result["still_running"].append(
                    {"split_id": a["split_id"], "attempt_no": a["attempt_no"], "pid": a["pid"]}
                )
                continue
            conn.execute(
                "UPDATE attempts SET state=?, finished_at=? "
                "WHERE job_id=? AND split_id=? AND attempt_no=?",
                (ATT_FAILED, time.time(), job_id, a["split_id"], a["attempt_no"]),
            )
            log_event(conn, job_id, "attempt_reaped_as_failed",
                      "协调器恢复：进程已不存在，未完成尝试不可见",
                      a["split_id"], a["attempt_no"])
            result["failed_attempts"].append(
                {"split_id": a["split_id"], "attempt_no": a["attempt_no"]}
            )
        conn.commit()
        if result["errors"]:
            raise ShuffleError(f"已确认 winner 的段核对失败，作业输入绑定不得更改: {result['errors']}")
        return result
    finally:
        conn.close()


def _open_reader_ids(conn: sqlite3.Connection, job_id: str) -> list[str]:
    return [r["reader_id"] for r in conn.execute(
        "SELECT reader_id FROM readers WHERE job_id=? AND state='open'", (job_id,)
    ).fetchall()]


def cleanup(layout: Layout, job_id: str, destroy: bool = False) -> dict:
    """回收本作业受管文件。

    * failed/lost  attempt 以及被取代的 completed 段：直接回收；
    * winner 段：只要还有打开的 reader 就保留；destroy 时若有 open reader
      则拒绝（调用方应等 reader 结束或令其显式失败）；
    * 只删除作业目录内登记过/遗留的 .tmp/.seg 文件，绝不触碰作业外路径。
    """
    conn = connect(layout.db_path)
    try:
        init_db(conn)
        job = get_job(conn, job_id)
        open_readers = _open_reader_ids(conn, job_id)
        reaped: list[str] = []
        base = layout.job_dir(job_id)
        # 1) 非 winner attempt 的段（输者/失败/未确认残留）
        non_winner = conn.execute(
            "SELECT DISTINCT relpath FROM segments s WHERE s.job_id=? AND NOT EXISTS ("
            " SELECT 1 FROM winners w WHERE w.job_id=s.job_id AND w.split_id=s.split_id"
            " AND w.attempt_no=s.attempt_no)",
            (job_id,),
        ).fetchall()
        for r in non_winner:
            path = _abs_seg_path(layout, job_id, r["relpath"])
            if os.path.exists(path):
                os.remove(path)
                reaped.append(r["relpath"])
            conn.execute(
                "UPDATE segments SET state='reaped' WHERE job_id=? AND relpath=?",
                (job_id, r["relpath"]),
            )
        # 2) 孤儿文件：写一半留下的 .tmp，或已封尾改名但来不及登记的 .seg
        seg_root = os.path.join(base, "segments")
        registered = {
            _abs_seg_path(layout, job_id, r["relpath"])
            for r in conn.execute(
                "SELECT DISTINCT relpath FROM segments WHERE job_id=?", (job_id,)
            ).fetchall()
        }
        if os.path.isdir(seg_root):
            for dirpath, _dirs, files in os.walk(seg_root):
                for name in files:
                    fp = os.path.join(dirpath, name)
                    if name.endswith(".tmp") or (name.endswith(".seg") and fp not in registered):
                        os.remove(fp)
                        reaped.append(os.path.relpath(fp, base))
        # 3) 空目录清掉
        if os.path.isdir(seg_root):
            for dirpath, dirs, files in os.walk(seg_root, topdown=False):
                if not os.listdir(dirpath):
                    os.rmdir(dirpath)
        result = {"reaped": reaped, "open_readers": open_readers, "destroyed": False}

        if destroy:
            if open_readers:
                raise ShuffleError(
                    f"仍有 {len(open_readers)} 个打开的 reader，不能销毁 winner 数据"
                )
            conn.execute("UPDATE jobs SET state=? WHERE job_id=?", (JOB_DESTROYED, job_id))
            log_event(conn, job_id, "job_destroyed", f"回收文件 {len(reaped) + 1} 项")
            conn.commit()
            conn.close()
            if os.path.isdir(base):
                # 整个作业目录都是受管目录；db 文件本身在 root 下，保留 db 行供审计
                import shutil

                shutil.rmtree(base)
            result["destroyed"] = True
            return result
        log_event(conn, job_id, "cleanup", f"回收 {len(reaped)} 个受管文件")
        conn.commit()
        return result
    finally:
        try:
            conn.close()
        except sqlite3.ProgrammingError:
            pass
