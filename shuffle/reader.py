"""分区读取器：只消费已确认 winner 的段，绑定固定获胜清单。

语义保证：
  * ``open_reader`` 时刻把每个 split 的 winner attempt 固定为快照；之后任何
    winner 变化都不影响本次读取（winner 本身也不可变）；
  * 未就绪 split：``wait=True`` 时限时等待，等待期间不会漏过任何 split；
    超时按 :class:`NotReady` 显式失败，绝不返回缺行的成功流；
  * 分区内按固定清单次序遍历 split，split 内按段 seq、段内按原 row_number
    交付（工人保证 row_number 递增，读取器再次核对）；
  * 全程逐行流式，不按分区攒列表；打开的 reader 在 readers 表登记，
    cleanup 不得提前删除其正在使用的 winner 段。
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Iterator

from . import coordinator as coord
from .common import (
    JOB_CANCELLED,
    NotReady,
    SegmentCorrupt,
    ShuffleError,
    connect,
    json_loads,
    log_event,
)
from .common import Layout
from .segment import iter_segment_rows


class PartitionReader:
    def __init__(self, layout: Layout, job_id: str, reader_id: str,
                 winners: dict[str, int]) -> None:
        self.layout = layout
        self.job_id = job_id
        self.reader_id = reader_id
        # split_id -> winner attempt_no，快照在 open 时固定
        self.winners = dict(winners)
        self._closed = False

    # ------------------------------------------------------------------ #
    def read_partition(self, partition_id: int) -> Iterator[dict]:
        """逐行产出该分区的完整行（含 split_id/row_number/key/value）。

        split 之间按建作业固定清单次序；每个 split 只取其 winner 的段，
        不可能混到其他 attempt 的行。
        """
        if self._closed:
            raise ShuffleError("reader 已关闭")
        conn = connect(self.layout.db_path)
        try:
            job = coord.get_job(conn, self.job_id)
            splits = conn.execute(
                "SELECT split_id, ord FROM job_splits WHERE job_id=? ORDER BY ord",
                (self.job_id,),
            ).fetchall()
            num_p = int(job["num_partitions"])
            if not 0 <= partition_id < num_p:
                raise ShuffleError(f"分区 {partition_id} 越界（共 {num_p} 个）")
            # 再次核对快照 winner 仍然是持久 winner（正常协议下不可能变化）
            for sp in splits:
                row = conn.execute(
                    "SELECT attempt_no FROM winners WHERE job_id=? AND split_id=?",
                    (self.job_id, sp["split_id"]),
                ).fetchone()
                if row is None or int(row["attempt_no"]) != self.winners[sp["split_id"]]:
                    raise ShuffleError(
                        "固定获胜清单与当前 winners 表不一致，读取按显式失败处理"
                    )
        finally:
            conn.close()

        for sp in splits:
            split_id = sp["split_id"]
            attempt_no = self.winners[split_id]
            yield from self._read_split_partition(split_id, attempt_no, partition_id)

    def _read_split_partition(self, split_id: str, attempt_no: int,
                              partition_id: int) -> Iterator[dict]:
        conn = connect(self.layout.db_path)
        try:
            segs = conn.execute(
                "SELECT * FROM segments WHERE job_id=? AND split_id=? AND attempt_no=? "
                "AND partition_id=? AND state='done' ORDER BY seq",
                (self.job_id, split_id, attempt_no, partition_id),
            ).fetchall()
            if not segs:
                # winner 已确认意味着全分区都有完成证据段；查不到只能是数据被破坏
                raise ShuffleError(
                    f"winner {split_id}#{attempt_no} 的 p{partition_id} 段缺失"
                )
        finally:
            conn.close()

        last_row_no = 0
        for s in segs:
            path = coord._abs_seg_path(self.layout, self.job_id, s["relpath"])
            expected = {"job_id": self.job_id, "split_id": split_id,
                        "attempt_no": attempt_no, "partition_id": partition_id,
                        "seq": int(s["seq"])}
            for payload, _digest in iter_segment_rows(path, expected):
                try:
                    row = json_loads(payload)
                except (UnicodeDecodeError, ValueError) as exc:
                    raise SegmentCorrupt(f"{path}: 行 JSON 损坏: {exc}") from exc
                if row["split_id"] != split_id:
                    raise ShuffleError("winner 段内出现他 split 行，拒绝混拼交付")
                if row["row_number"] <= last_row_no:
                    raise ShuffleError("段内 row_number 顺序被破坏")
                last_row_no = row["row_number"]
                yield row

    # ------------------------------------------------------------------ #
    def close(self, state: str = "closed") -> None:
        if self._closed:
            return
        self._closed = True
        conn = connect(self.layout.db_path)
        try:
            conn.execute(
                "UPDATE readers SET closed_at=?, state=? WHERE reader_id=?",
                (time.time(), state, self.reader_id),
            )
            conn.commit()
        finally:
            conn.close()

    def __enter__(self) -> "PartitionReader":
        return self

    def __exit__(self, *exc) -> None:
        self.close("failed" if exc and exc[0] else "closed")


def open_reader(layout: Layout, job_id: str, *, wait: bool = True,
                timeout: float = 30.0, poll: float = 0.05) -> PartitionReader:
    """打开 reader 并绑定固定获胜清单。

    wait=False 时若有 split 未就绪立即抛 :class:`NotReady`；
    wait=True 时轮询到全部就绪或超时。作业取消后拒绝新读取。
    """
    conn = connect(layout.db_path)
    try:
        coord.init_db(conn)
        job = coord.get_job(conn, job_id)
        if job["state"] == JOB_CANCELLED:
            from .common import JobCancelled

            raise JobCancelled(f"作业 {job_id} 已取消，拒绝新读取")
        split_ids = [r["split_id"] for r in conn.execute(
            "SELECT split_id FROM job_splits WHERE job_id=? ORDER BY ord", (job_id,)
        ).fetchall()]
    finally:
        conn.close()

    deadline = time.time() + timeout
    while True:
        conn = connect(layout.db_path)
        try:
            job = coord.get_job(conn, job_id)
            if job["state"] == JOB_CANCELLED:
                from .common import JobCancelled

                raise JobCancelled(f"作业 {job_id} 在等待期间被取消，读取失败，不返回缺行流")
            winners = {
                r["split_id"]: int(r["attempt_no"])
                for r in conn.execute(
                    "SELECT split_id, attempt_no FROM winners WHERE job_id=?", (job_id,)
                ).fetchall()
            }
        finally:
            conn.close()
        missing = [s for s in split_ids if s not in winners]
        if not missing:
            break
        if not wait:
            raise NotReady(f"以下 split 尚无 winner: {missing}")
        if time.time() >= deadline:
            raise NotReady(f"等待 winner 超时，未就绪: {missing}")
        time.sleep(poll)

    reader_id = uuid.uuid4().hex
    conn = connect(layout.db_path)
    try:
        conn.execute(
            "INSERT INTO readers(reader_id,job_id,opened_at,state) VALUES (?,?,?,?)",
            (reader_id, job_id, time.time(), "open"),
        )
        log_event(conn, job_id, "reader_opened",
                  "winners=" + json.dumps(winners, sort_keys=True))
        conn.commit()
    finally:
        conn.close()
    return PartitionReader(layout, job_id, reader_id, winners)


def read_partition_rows(layout: Layout, job_id: str, partition_id: int,
                        *, timeout: float = 30.0) -> list[dict]:
    """便捷封装：打开 reader、流式收集一个分区并关闭。

    生产消费方应直接用 ``open_reader`` 的迭代器；这里收集仅用于测试/CLI。
    """
    with open_reader(layout, job_id, timeout=timeout) as reader:
        return list(reader.read_partition(partition_id))
