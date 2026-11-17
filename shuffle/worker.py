"""真实工人进程：一次 attempt 绑定唯一 (split, attempt_no)，流式加工整个 split。

工人在独立 OS 进程中运行（见 ``__main__`` / CLI ``worker``），与协调器之间
只通过 SQLite 元数据与段文件交换数据。内存上限按“每分区”生效，缓冲写满
即溢写新段；输入与输出都逐行流式处理。

受控故障/暂停钩子（仅演示与测试用，经环境变量传递，不参与正常协议）：
  SHUFFLE_HOOK=pause-after-first  写完第一行后 SIGSTOP 暂停（等驱动 SIGCONT）
  SHUFFLE_HOOK=kill-mid-segment   写完第一行后 os._exit，留下 .tmp 写一半现场
"""

from __future__ import annotations

import hashlib
import json
import os
import signal
import sys
from typing import Optional

from . import coordinator as coord
from .common import (
    ATT_RUNNING,
    IdentityChanged,
    ShuffleError,
    connect,
    json_loads,
)
from .common import Layout
from .segment import SpillingShuffleWriter


def _verify_input_digest(input_path: str, expected_digest: str) -> int:
    """开工前复算源 split 摘要；身份改变直接失败，绝不改作业绑定。"""
    h = hashlib.sha256()
    rows = 0
    with open(input_path, "rb") as fh:
        for line in fh:
            h.update(line)
            rows += 1
    if h.hexdigest() != expected_digest:
        raise IdentityChanged(
            f"源 split 摘要改变：期望 {expected_digest}，实际 {h.hexdigest()}；"
            "作业输入绑定不可修改，拒绝执行"
        )
    return rows


def run_worker(layout: Layout, job_id: str, split_id: str, attempt_no: int,
               mem_bytes: int, hook: Optional[str] = None) -> dict:
    """工人主流程。返回 attempt 的完成摘要；任何失败都会把 attempt 落为 failed。"""
    conn = connect(layout.db_path)
    try:
        job = coord.get_job(conn, job_id)
        split = conn.execute(
            "SELECT * FROM job_splits WHERE job_id=? AND split_id=?", (job_id, split_id)
        ).fetchone()
        if split is None:
            raise ShuffleError(f"未绑定 split: {split_id}")
        att = conn.execute(
            "SELECT state FROM attempts WHERE job_id=? AND split_id=? AND attempt_no=?",
            (job_id, split_id, attempt_no),
        ).fetchone()
        if att is None or att["state"] != ATT_RUNNING:
            raise ShuffleError(f"attempt {split_id}#{attempt_no} 不处于 running，工人拒绝执行")
        num_p = int(job["num_partitions"])
        version = int(job["partition_version"])
    finally:
        conn.close()

    coord.attach_pid(layout, job_id, split_id, attempt_no, os.getpid())
    try:
        input_rows = _verify_input_digest(split["input_path"], split["input_digest"])
    except IdentityChanged:
        coord.mark_attempt_failed(layout, job_id, split_id, attempt_no, "源 split 身份改变")
        raise

    writer = SpillingShuffleWriter(
        job_id, split_id, attempt_no, num_p, version, mem_bytes, layout
    )
    written = 0
    try:
        with open(split["input_path"], "rb") as fh:
            for row_number, line in enumerate(fh, start=1):
                if not line.strip():
                    raise ShuffleError(f"输入第 {row_number} 行为空")
                obj = json_loads(line)
                # 原行身份 (split_id, row_number) 由工人显式带上；重复内容不合并
                writer.write_row(split_id, row_number, obj["key"], obj["value"])
                written += 1
                if hook == "pause-after-first" and written == 1:
                    os.kill(os.getpid(), signal.SIGSTOP)
                if hook == "kill-mid-segment" and written == 1:
                    # 段写一半被终止：fd 不 flush、不改名、状态不推进
                    sys.stderr.write(
                        f"[worker {os.getpid()}] 受控终止：段写一半 (row 1 已入 .tmp)\n"
                    )
                    os._exit(2)
        records = writer.close()
    except IdentityChanged:
        writer.abort()
        coord.mark_attempt_failed(layout, job_id, split_id, attempt_no, "源 split 身份改变")
        raise
    except BaseException:
        writer.abort()
        coord.mark_attempt_failed(layout, job_id, split_id, attempt_no, "工人异常退出")
        raise

    if written != input_rows:
        coord.mark_attempt_failed(
            layout, job_id, split_id, attempt_no,
            f"声明行数 {input_rows} 与实写 {written} 不符"
        )
        raise ShuffleError(f"行数核对失败: 声明 {input_rows} 实写 {written}")

    # 登记所有段（含空分区的 0 行段），先 tmp 后 done 的两阶段状态
    for rec in records:
        rel = os.path.relpath(
            layout.seg_path(job_id, split_id, attempt_no, rec.partition_id, rec.seq),
            layout.job_dir(job_id),
        )
        coord.register_segment_tmp(layout, job_id, split_id, attempt_no,
                                   rec.partition_id, rec.seq, rel)
        coord.register_segment_done(layout, job_id, split_id, attempt_no, rec)

    coord.mark_attempt_completed(layout, job_id, split_id, attempt_no)
    return {"job_id": job_id, "split_id": split_id, "attempt_no": attempt_no,
            "rows": written, "segments": len(records), "pid": os.getpid()}


def main(argv: Optional[list[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="python -m shuffle.worker")
    parser.add_argument("--root", required=True)
    parser.add_argument("--job", required=True)
    parser.add_argument("--split", required=True)
    parser.add_argument("--attempt", type=int, required=True)
    parser.add_argument("--mem-bytes", type=int, required=True)
    parser.add_argument("--hook", default=os.environ.get("SHUFFLE_HOOK"))
    args = parser.parse_args(argv)
    layout = Layout(args.root)
    try:
        result = run_worker(layout, args.job, args.split, args.attempt,
                            args.mem_bytes, args.hook)
    except ShuffleError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False),
              file=sys.stderr)
        return 1
    print(json.dumps({"ok": True, **result}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
