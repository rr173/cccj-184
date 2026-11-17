"""故障与并发关卡，全部使用真实独立工人进程。

  F1 两个工人同时为同一 split 申请 winner：只有一个成功，另一个持久为 lost
  F2 段文件实际损坏：确认/恢复/读取三处都必须显式失败，不静默丢行
  F3 源 split 身份改变（建作业后输入被换）：工人拒绝执行且不改作业绑定
  F4 取消作业：拒绝新读取；已开始的 reader 按原清单完成或显式失败
  F5 核对通过、winner 提交前协调器退出：重启后无 winner，可安全重新派工
"""

from __future__ import annotations

import multiprocessing as mp
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shuffle import coordinator as coord
from shuffle.cli import _spawn_worker
from shuffle.common import (
    ATT_LOST,
    ATT_WINNER,
    JOB_CANCELLED,
    Layout,
    ShuffleError,
)
from shuffle import reader as reader_mod
from demo.demo_data import expected_identities, write_jsonl

ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".run", "fault")
NUM_P = 4
VERSION = 13
MEM = 128 * 1024


def banner(t: str) -> None:
    print(f"\n{'-' * 72}\nF: {t}\n{'-' * 72}")


def check(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def _claim_worker(root: str, job: str, split: str, attempt: int, q) -> None:
    """独立子进程里执行 winner 申请，制造真正的并发事务竞争。"""
    try:
        coord.claim_winner(Layout(root), job, split, attempt)
        q.put((attempt, True, ""))
    except coord.AlreadyWinner as exc:
        q.put((attempt, False, str(exc)))
    except Exception as exc:  # noqa: BLE001
        q.put((attempt, False, f"UNEXPECTED: {exc!r}"))


def _precommit_child(root: str, job: str) -> None:
    def _die() -> None:
        sys.stderr.write("[coordinator-child] 受控退出：winner INSERT 前 os._exit(3)\n")
        sys.stderr.flush()
        os._exit(3)

    coord.claim_winner(Layout(root), job, "S", 0, before_commit_hook=_die)


def main() -> None:
    os.makedirs(ROOT, exist_ok=True)
    layout = Layout(ROOT)
    in_dir = os.path.join(ROOT, "inputs")
    rows = [("a1", 1), ("a2", 2), ("b2", 3)]
    write_jsonl(os.path.join(in_dir, "S.jsonl"), rows)

    # ------------------------------------------------------------------ #
    banner("F1 两个真实工人并行完成，同刻申请 winner（BEGIN IMMEDIATE 竞争）")
    job = "job-race"
    coord.create_job(layout, job, [("S", os.path.join(in_dir, "S.jsonl"))], NUM_P, VERSION)
    procs = []
    for attempt in (0, 1):
        info = coord.launch_attempt(layout, job, "S", MEM)
        assert info["attempt_no"] == attempt
        procs.append(_spawn_worker(layout, job, "S", attempt, MEM, None))
    for p in procs:
        out, err = p.communicate(timeout=30)
        check(p.returncode == 0, f"工人 pid={p.pid} 完整跑完: {out.decode().strip()}")

    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    claimers = [
        ctx.Process(target=_claim_worker, args=(ROOT, job, "S", 0, q)),
        ctx.Process(target=_claim_worker, args=(ROOT, job, "S", 1, q)),
    ]
    for c in claimers:
        c.start()
    results = sorted([q.get(timeout=30) for _ in claimers], key=lambda x: x[0])
    for c in claimers:
        c.join()
    winners = [r[0] for r in results if r[1]]
    losers = [r for r in results if not r[1]]
    check(len(winners) == 1, f"恰好一个 attempt 成为 winner: {winners}")
    check(len(losers) == 1 and not losers[0][2].startswith("UNEXPECTED"),
          f"另一个收到 AlreadyWinner 并持久为 lost: {losers[0][2][:60]}...")
    st = coord.inspect(layout, job)
    states = {a["attempt_no"]: a["state"] for a in st["splits"][0]["attempts"]}
    check(states == {0: ATT_WINNER if winners[0] == 0 else ATT_LOST,
                     1: ATT_WINNER if winners[0] == 1 else ATT_LOST},
          f"attempt 终态: {states}")
    check(st["splits"][0]["winner_attempt"] == winners[0], "winners 表唯一且持久")
    try:
        coord.launch_attempt(layout, job, "S", MEM)
        raise AssertionError("已有 winner 后不应能派工")
    except coord.AlreadyWinner:
        check(True, "winner 固定后拒绝新 attempt")

    # ------------------------------------------------------------------ #
    banner("F2 段文件实际损坏：确认、读取、恢复三处显式失败")
    job2 = "job-corrupt"
    coord.create_job(layout, job2, [("S", os.path.join(in_dir, "S.jsonl"))], NUM_P, VERSION)
    info = coord.launch_attempt(layout, job2, "S", MEM)
    p = _spawn_worker(layout, job2, "S", info["attempt_no"], MEM, None)
    p.wait(timeout=30)
    target = os.path.join(ROOT, job2, "segments", "S", "attempt-0", "p0-s0.seg")
    size = os.path.getsize(target)
    with open(target, "r+b") as fh:
        fh.truncate(size - 9)  # 砍掉 footer 尾部，制造真实截断
    try:
        coord.claim_winner(layout, job2, "S", 0)
        raise AssertionError("损坏段不应确认成功")
    except ShuffleError as exc:
        check(True, f"确认时显式失败（段截断）: {type(exc).__name__}")

    job2b = "job-corrupt2"
    coord.create_job(layout, job2b, [("S", os.path.join(in_dir, "S.jsonl"))], NUM_P, VERSION)
    info2 = coord.launch_attempt(layout, job2b, "S", MEM)
    p2 = _spawn_worker(layout, job2b, "S", info2["attempt_no"], MEM, None)
    p2.wait(timeout=30)
    coord.claim_winner(layout, job2b, "S", 0)
    t2 = os.path.join(ROOT, job2b, "segments", "S", "attempt-0", "p1-s0.seg")
    with open(t2, "r+b") as fh:
        fh.seek(20)
        fh.write(b"\xff" * 8)  # 破坏行 payload，行摘要立即不符
    try:
        with reader_mod.open_reader(layout, job2b) as rdr:
            list(rdr.read_partition(1))
        raise AssertionError("损坏 winner 段不应读出成功流")
    except ShuffleError as exc:
        check(True, f"读取时显式失败（行摘要不符）: {type(exc).__name__}")
    try:
        coord.recover(layout, job2b)
        raise AssertionError("recover 不应放过损坏的 winner 段")
    except ShuffleError as exc:
        check(True, f"恢复核对显式失败且不重派/不改绑定: {type(exc).__name__}")

    # ------------------------------------------------------------------ #
    banner("F3 源 split 身份改变：工人拒绝执行，作业输入绑定保持不变")
    job3 = "job-identity"
    coord.create_job(layout, job3, [("S", os.path.join(in_dir, "S.jsonl"))], NUM_P, VERSION)
    before = coord.inspect(layout, job3)["splits"][0]["input_digest"]
    original = os.path.join(in_dir, "S.jsonl")
    backup = original + ".bak"
    swapped = os.path.join(in_dir, "S_swapped.jsonl")
    write_jsonl(swapped, [("a1", 999)])
    os.replace(original, backup)
    os.replace(swapped, original)
    try:
        info3 = coord.launch_attempt(layout, job3, "S", MEM)
        p3 = _spawn_worker(layout, job3, "S", info3["attempt_no"], MEM, None)
        _out, err3 = p3.communicate(timeout=30)
        check(p3.returncode != 0, "工人进程以非零码退出")
        check("摘要" in err3.decode() or "身份" in err3.decode(),
              f"失败原因为源身份改变: {err3.decode().strip()[:80]}")
    finally:
        os.replace(backup, original)
    after = coord.inspect(layout, job3)["splits"][0]
    check(after["input_digest"] == before, "作业绑定的源摘要未被修改")
    check(after["attempts"][0]["state"] == "failed", "身份改变的 attempt 落为 failed")
    info4 = coord.launch_attempt(layout, job3, "S", MEM)
    p4 = _spawn_worker(layout, job3, "S", info4["attempt_no"], MEM, None)
    p4.wait(timeout=30)
    coord.claim_winner(layout, job3, "S", info4["attempt_no"])
    with reader_mod.open_reader(layout, job3) as rdr:
        got = sum(len(list(rdr.read_partition(p))) for p in range(NUM_P))
    check(got == len(rows), f"绑定不变、重新派工后读全 {len(rows)} 行")

    # ------------------------------------------------------------------ #
    banner("F4 取消作业：拒绝新读取；已开始 reader 按原清单完成")
    job4 = "job-cancel"
    coord.create_job(layout, job4, [("S", os.path.join(in_dir, "S.jsonl"))], NUM_P, VERSION)
    info5 = coord.launch_attempt(layout, job4, "S", MEM)
    p5 = _spawn_worker(layout, job4, "S", info5["attempt_no"], MEM, None)
    p5.wait(timeout=30)
    coord.claim_winner(layout, job4, "S", 0)
    rdr = reader_mod.open_reader(layout, job4)
    it = rdr.read_partition(0)
    first = next(it)
    coord.cancel_job(layout, job4)
    try:
        reader_mod.open_reader(layout, job4)
        raise AssertionError("取消后不得接受新 reader")
    except ShuffleError as exc:
        check(True, f"取消后新读取被拒: {type(exc).__name__}")
    rest = list(it)
    rdr.close()
    expect0 = expected_identities(rows, "S", NUM_P, VERSION)[0]
    got_ids = [(first["split_id"], first["row_number"])] + \
              [(r["split_id"], r["row_number"]) for r in rest]
    check(got_ids == expect0, f"已开 reader 不受取消影响，p0 交付完整: {got_ids}")
    check(coord.inspect(layout, job4)["state"] == JOB_CANCELLED, "作业状态为 cancelled")

    # ------------------------------------------------------------------ #
    banner("F5 协调器在 winner 提交前退出（独立子进程 + 故障钩子）")
    job5 = "job-precommit"
    coord.create_job(layout, job5, [("S", os.path.join(in_dir, "S.jsonl"))], NUM_P, VERSION)
    info6 = coord.launch_attempt(layout, job5, "S", MEM)
    p6 = _spawn_worker(layout, job5, "S", info6["attempt_no"], MEM, None)
    p6.wait(timeout=30)
    child = mp.get_context("spawn").Process(target=_precommit_child, args=(ROOT, job5))
    child.start()
    child.join(timeout=30)
    check(child.exitcode == 3, f"协调器子进程以钩子退出码 3 终止（实际 {child.exitcode}）")
    st5 = coord.inspect(layout, job5)
    check(st5["splits"][0]["winner_attempt"] is None, "提交前退出：winners 表无行")
    rec = coord.recover(layout, job5)
    check(any(x["attempt_no"] == 0 for x in rec["failed_attempts"]),
          "未确认 attempt 恢复为 failed，数据不可见")
    new_info = coord.launch_attempt(layout, job5, "S", MEM)
    p7 = _spawn_worker(layout, job5, "S", new_info["attempt_no"], MEM, None)
    p7.wait(timeout=30)
    coord.claim_winner(layout, job5, "S", new_info["attempt_no"])
    with reader_mod.open_reader(layout, job5) as rdr:
        got = sum(len(list(rdr.read_partition(p))) for p in range(NUM_P))
    check(got == len(rows), f"重试后完整读取 {len(rows)} 行")

    print("\n全部故障/并发关卡通过。")


if __name__ == "__main__":
    main()
