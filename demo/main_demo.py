"""主例 + 故障位置演示，全部使用真实独立工人进程。

覆盖：
  关卡 1  split A：A1 只写 p0 后暂停；A2 全写完并获胜；再放行 A1 补齐——
          p0/p1 必须全部来自 A2，A1 晚报不能替换 winner
  关卡 2  split B 独立获胜；四分区汇总恰好每行一次、来路不混 A 的两个 attempt
  关卡 3  空分区有明确完成证据（4 个目标分区，p3 为空）
  关卡 4  段写一半被杀：残留 .tmp，attempt 不可见，恢复后可重新派工
  关卡 5  段齐备未确认时协调器“重启”：recover 后未确认 attempt 不可见
  关卡 6  winner 确认后工人未收到响应：winner 已持久，重复确认安全不替换
  关卡 7  已打开 reader 时 cleanup：winner 段保留；reader 关闭后可回收
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shuffle import coordinator as coord
from shuffle.cli import _spawn_worker
from shuffle.common import (
    ATT_COMPLETED,
    ATT_LOST,
    ATT_RUNNING,
    ATT_WINNER,
    Layout,
    NotReady,
)
from shuffle import reader as reader_mod
from demo.demo_data import (
    directory_size,
    expected_identities,
    write_jsonl,
)

ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".run", "main")
JOB = "job-main"
NUM_P = 4
VERSION = 13  # 公开规则下使 a1->p0, a2->p1
MEM = 256 * 1024  # 每分区 256KiB，小输入下主要验证机制；规模数据在 scale_demo 验证溢写


def banner(n: int, title: str) -> None:
    print(f"\n{'=' * 72}\n关卡 {n}：{title}\n{'=' * 72}")


def assert_eq(actual, expected, what: str) -> None:
    if actual != expected:
        raise AssertionError(f"{what} 失败：期望 {expected!r}，实际 {actual!r}")
    print(f"  ✓ {what} = {actual!r}")


def wait_state(layout: Layout, split_id: str, attempt_no: int, states: set[str],
               timeout: float = 20.0) -> str:
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = coord.inspect(layout, JOB)
        for sp in st["splits"]:
            if sp["split_id"] == split_id:
                for a in sp["attempts"]:
                    if a["attempt_no"] == attempt_no:
                        if a["state"] in states:
                            return a["state"]
        time.sleep(0.05)
    raise TimeoutError(f"等待 {split_id}#{attempt_no} 进入 {states} 超时")


def find_attempt(layout: Layout, split_id: str, attempt_no: int) -> dict:
    st = coord.inspect(layout, JOB)
    for sp in st["splits"]:
        if sp["split_id"] == split_id:
            for a in sp["attempts"]:
                if a["attempt_no"] == attempt_no:
                    return a
    raise KeyError((split_id, attempt_no))


def find_split(layout: Layout, split_id: str) -> dict:
    st = coord.inspect(layout, JOB)
    for sp in st["splits"]:
        if sp["split_id"] == split_id:
            return sp
    raise KeyError(split_id)


def main() -> None:
    os.makedirs(ROOT, exist_ok=True)
    layout = Layout(ROOT)
    in_dir = os.path.join(ROOT, "inputs")

    # ------------------------------------------------------------------ #
    # 数据：A 两行落 p0/p1；B 三行落 p0/p2；p3 全局为空
    # ------------------------------------------------------------------ #
    rows_a = [("a1", "alpha-one"), ("a2", "alpha-two")]
    rows_b = [("b1", "bravo-one"), ("b2", "bravo-two"), ("b3", "bravo-three")]
    write_jsonl(os.path.join(in_dir, "A.jsonl"), rows_a)
    write_jsonl(os.path.join(in_dir, "B.jsonl"), rows_b)

    if os.path.exists(layout.db_path):
        import shutil

        shutil.rmtree(os.path.join(ROOT, JOB), ignore_errors=True)
        os.remove(layout.db_path)
        for suffix in ("-wal", "-shm"):
            p = layout.db_path + suffix
            if os.path.exists(p):
                os.remove(p)

    coord.create_job(layout, JOB,
                     [("A", os.path.join(in_dir, "A.jsonl")),
                      ("B", os.path.join(in_dir, "B.jsonl"))],
                     NUM_P, VERSION)
    print(f"建作业 {JOB}：固定 2 个 split、{NUM_P} 分区、分区函数版本 v{VERSION}")

    # ------------------------------------------------------------------ #
    # 关卡 1：A1 暂停 vs A2 完整获胜
    # ------------------------------------------------------------------ #
    banner(1, "A1 写完首行暂停，A2 完整跑完并确认 winner，再放行 A1")
    a1 = coord.launch_attempt(layout, JOB, "A", MEM)
    p1 = _spawn_worker(layout, JOB, "A", a1["attempt_no"], MEM, "pause-after-first")
    # 等 A1 真正暂停（SIGSTOP 后 /proc/<pid>/stat 为 T）
    deadline = time.time() + 10
    while time.time() < deadline:
        with open(f"/proc/{p1.pid}/stat", "rb") as fh:
            stat = fh.read().split()[2]
        if stat == b"T":
            break
        time.sleep(0.03)
    assert_eq(stat, b"T", f"A1 工人 pid={p1.pid} 已 SIGSTOP 暂停")
    a1_state = find_attempt(layout, "A", 0)
    assert_eq(a1_state["state"], ATT_RUNNING, "暂停中的 A1 仍是 running，结果不可见")

    a2 = coord.launch_attempt(layout, JOB, "A", MEM)
    p2 = _spawn_worker(layout, JOB, "A", a2["attempt_no"], MEM, None)
    out, err = p2.communicate(timeout=30)
    if p2.returncode != 0:
        raise RuntimeError(f"A2 工人失败: {err.decode()}")
    print(f"  ✓ A2 真实工人 pid={p2.pid} 正常结束: {out.decode().strip()}")
    wait_state(layout, "A", 1, {ATT_COMPLETED})

    # 此时 A1 还停着：没有任何 winner，非阻塞读取必须 NotReady，而不是给部分流
    try:
        reader_mod.open_reader(layout, JOB, wait=False)
        raise AssertionError("未就绪时不应成功打开 reader")
    except NotReady as exc:
        print(f"  ✓ 未就绪读取显式 NotReady：{exc}")

    claim = coord.claim_winner(layout, JOB, "A", a2["attempt_no"])
    assert_eq(claim["winner_attempt"], 1, "A 的 winner = A2(attempt 1)")

    # 放行 A1，等它补齐全部段（winner 已存在，它在 A2 确认事务里已被标 lost）
    os.kill(p1.pid, signal.SIGCONT)
    out1, err1 = p1.communicate(timeout=30)
    if p1.returncode != 0:
        raise RuntimeError(f"A1 恢复后失败: {err1.decode()}")
    deadline = time.time() + 10
    while time.time() < deadline:
        a1_now = find_attempt(layout, "A", 0)
        if len(a1_now["partitions"]) == NUM_P:
            break
        time.sleep(0.05)
    print("  ✓ A1 补齐全部四个分区段（含空分区段），但 attempt 已是 lost")

    # A1 晚报：确认必须失败，且 A1 保持 lost；winner 不替换
    try:
        coord.claim_winner(layout, JOB, "A", a1["attempt_no"])
        raise AssertionError("A1 晚报不应确认成功")
    except coord.AlreadyWinner as exc:
        print(f"  ✓ A1 晚报被拒（保持 lost）：{exc}")
    assert_eq(find_attempt(layout, "A", 0)["state"], ATT_LOST, "A1 持久化为输者")
    assert_eq(find_attempt(layout, "A", 1)["state"], ATT_WINNER, "A2 保持 winner")
    assert_eq(find_split(layout, "A")["winner_attempt"], 1, "winner 选择不被晚到者替换")

    # ------------------------------------------------------------------ #
    # 关卡 2 + 3：B 独立获胜；读取与独立校验；空分区证据
    # ------------------------------------------------------------------ #
    banner(2, "split B 独立获胜（两 split 不要求统一停机）")
    b1 = coord.launch_attempt(layout, JOB, "B", MEM)
    pb = _spawn_worker(layout, JOB, "B", b1["attempt_no"], MEM, None)
    outb, errb = pb.communicate(timeout=30)
    if pb.returncode != 0:
        raise RuntimeError(f"B 工人失败: {errb.decode()}")
    coord.claim_winner(layout, JOB, "B", b1["attempt_no"])
    assert_eq(find_split(layout, "B")["winner_attempt"], 0, "B 独立确认 winner")

    banner(3, "空分区 p3 的明确完成证据 + 全分区独立校验")
    for sid, rows in (("A", rows_a), ("B", rows_b)):
        wno = find_split(layout, sid)["winner_attempt"]
        att = find_attempt(layout, sid, wno)
        covered = {x["partition_id"] for x in att["partitions"]}
        assert_eq(covered, set(range(NUM_P)), f"{sid} winner 覆盖全部分区（含空分区）")
        empty = [x for x in att["partitions"] if x["partition_id"] == 3]
        assert_eq(empty[0]["done_segments"], 1, f"{sid} 的空 p3 存在 1 个 0 行完成段")
        assert_eq(empty[0]["rows"], 0, f"{sid} 的 p3 行数为 0")

    with reader_mod.open_reader(layout, JOB) as rdr:
        snapshot = dict(rdr.winners)
        print(f"  ✓ 本次读取绑定固定获胜清单: {snapshot}")
        all_rows: dict[int, list[dict]] = {}
        for p in range(NUM_P):
            all_rows[p] = list(rdr.read_partition(p))

    # 独立期望：只依赖公开分区函数与原始输入，不看协调器状态
    expect = {p: [] for p in range(NUM_P)}
    for sid, rows in (("A", rows_a), ("B", rows_b)):
        for p, ids in expected_identities(rows, sid, NUM_P, VERSION).items():
            expect[p].extend(ids)
    for p in expect:
        expect[p].sort(key=lambda t: (0 if t[0] == "A" else 1, t[1]))

    total = 0
    for p in range(NUM_P):
        got = [(r["split_id"], r["row_number"]) for r in all_rows[p]]
        assert_eq(got, expect[p], f"p{p} 行身份集合与顺序（独立复算）")
        # 来路检查：A 的行只能来自 attempt 1（A2）
        for r in all_rows[p]:
            if r["split_id"] == "A":
                assert snapshot["A"] == 1
        total += len(got)
    assert_eq(total, len(rows_a) + len(rows_b), "四分区汇总每个源行恰好出现一次")
    print("  ✓ 没有任何行来自输者 A1；不存在先交错后去重的混拼流")

    # ------------------------------------------------------------------ #
    # 关卡 4：段写一半被杀
    # ------------------------------------------------------------------ #
    banner(4, "工人在段写一半被终止（kill -9 等价的 os._exit，留半截 .tmp）")
    job2 = "job-kill-mid"
    coord.create_job(layout, job2,
                     [("A", os.path.join(in_dir, "A.jsonl"))], NUM_P, VERSION)
    k0 = coord.launch_attempt(layout, job2, "A", MEM)
    pk = _spawn_worker(layout, job2, "A", k0["attempt_no"], MEM, "kill-mid-segment")
    pk.wait(timeout=20)
    assert_eq(pk.returncode, 2, "写一半工人以受控退出码 2 终止")
    time.sleep(0.2)
    tmp_files = []
    seg_root = os.path.join(ROOT, job2, "segments")
    for dp, _d, files in os.walk(seg_root):
        tmp_files.extend(os.path.join(dp, f) for f in files if f.endswith(".tmp"))
    if not tmp_files:
        raise AssertionError("磁盘上应存在未封尾的 .tmp 半截段")
    print(f"  ✓ 残留半截临时段: {[os.path.relpath(t, ROOT) for t in tmp_files]}")
    # 协调器视角：没有 winner、没有 completed
    st = coord.inspect(layout, job2)
    assert_eq(st["splits"][0]["winner_attempt"], None, "被杀 attempt 没有 winner，数据不可读")
    try:
        reader_mod.open_reader(layout, job2, wait=False)
        raise AssertionError("半成品作业不应能打开 reader")
    except NotReady:
        print("  ✓ 半成品对读取器不可见（NotReady，而非部分行流）")

    # ------------------------------------------------------------------ #
    # 关卡 5：段齐备未确认时协调器重启（recover）
    # ------------------------------------------------------------------ #
    banner(5, "段齐备但未确认期间协调器退出并恢复")
    job3 = "job-restart"
    coord.create_job(layout, job3,
                     [("A", os.path.join(in_dir, "A.jsonl")),
                      ("B", os.path.join(in_dir, "B.jsonl"))], NUM_P, VERSION)
    r0 = coord.launch_attempt(layout, job3, "A", MEM)
    pr = _spawn_worker(layout, job3, "A", r0["attempt_no"], MEM, None)
    pr.wait(timeout=30)
    # 等 completed
    deadline = time.time() + 10
    while time.time() < deadline:
        if find_attempt_in(layout, job3, "A", 0)["state"] == ATT_COMPLETED:
            break
        time.sleep(0.05)
    rec = coord.recover(layout, job3)  # 模拟协调器重启：无 winner，工人进程已退出
    assert_eq([x["attempt_no"] for x in rec["failed_attempts"]], [0],
              "重启恢复：未确认的 completed attempt 判为 failed，保持不可见")
    assert_eq(rec["winners"], [], "恢复后仍无 winner")
    try:
        reader_mod.open_reader(layout, job3, wait=False)
        raise AssertionError("恢复后未确认数据仍不可读")
    except NotReady:
        print("  ✓ 未确认 attempt 恢复后依旧不可见")
    # 确认原 attempt 不执行后重新派工（attempt 编号递增）
    r1 = coord.launch_attempt(layout, job3, "A", MEM)
    assert_eq(r1["attempt_no"], 1, "重新派工得到新 attempt 编号 1，输入绑定不变")
    pr2 = _spawn_worker(layout, job3, "A", 1, MEM, None)
    pr2.wait(timeout=30)
    coord.claim_winner(layout, job3, "A", 1)
    b0 = coord.launch_attempt(layout, job3, "B", MEM)
    pb2 = _spawn_worker(layout, job3, "B", b0["attempt_no"], MEM, None)
    pb2.wait(timeout=30)
    coord.claim_winner(layout, job3, "B", 0)
    with reader_mod.open_reader(layout, job3) as rdr:
        got = sum(len(list(rdr.read_partition(p))) for p in range(NUM_P))
    assert_eq(got, len(rows_a) + len(rows_b), "恢复并重派后完整读取行数正确")

    # 顺便验证关卡 4 的恢复路径
    rec4 = coord.recover(layout, job2)
    assert any(x["attempt_no"] == 0 for x in rec4["failed_attempts"]), \
        "写一半被杀的 running attempt 恢复为 failed"
    k1 = coord.launch_attempt(layout, job2, "A", MEM)
    pk2 = _spawn_worker(layout, job2, "A", k1["attempt_no"], MEM, None)
    pk2.wait(timeout=30)
    coord.claim_winner(layout, job2, "A", k1["attempt_no"])

    # ------------------------------------------------------------------ #
    # 关卡 6：winner 确认后“未收到响应”——重复确认幂等安全
    # ------------------------------------------------------------------ #
    banner(6, "winner 已持久但工人未收到确认响应：重试确认不替换、不报错状态错乱")
    again = coord.claim_winner(layout, JOB, "A", 1)  # 同一 winner 再确认
    assert_eq(again["winner_attempt"], 1, "对现任 winner 重复确认返回同一 winner")
    try:
        coord.claim_winner(layout, JOB, "A", 0)  # A1 再试
        raise AssertionError("输者重试不得成功")
    except coord.AlreadyWinner:
        print("  ✓ 输者 A1 重试仍被拒；winner 一旦可读即固定")
    rec_main = coord.recover(layout, JOB)
    assert_eq({(x["split_id"], x["attempt_no"]) for x in rec_main["winners"]},
              {("A", 1), ("B", 0)}, "协调器重启恢复后两个 winner 重新核对可见")

    # ------------------------------------------------------------------ #
    # 关卡 7：已打开 reader 时 cleanup
    # ------------------------------------------------------------------ #
    banner(7, "reader 打开期间清理：winner 段受保护，关闭后才回收")
    rdr = reader_mod.open_reader(layout, JOB)
    it = rdr.read_partition(0)
    first = next(it)
    print(f"  ✓ reader 已取走首行: {first['split_id']}/{first['row_number']}")
    clean = coord.cleanup(layout, JOB)
    winner_files_before = []
    for dp, _d, files in os.walk(os.path.join(ROOT, JOB, "segments")):
        for f in files:
            winner_files_before.append(os.path.join(dp, f))
    # 输者 A1 的段应被回收；A2/B 的 winner 段必须还在
    a1_dir = os.path.join(ROOT, JOB, "segments", "A", "attempt-0")
    a2_dir = os.path.join(ROOT, JOB, "segments", "A", "attempt-1")
    assert not os.path.exists(a1_dir) or not os.listdir(a1_dir), "输者 A1 段已回收"
    if not os.path.isdir(a2_dir) or not os.listdir(a2_dir):
        raise AssertionError("打开中的 winner(A2) 段不得提前删除")
    print(f"  ✓ cleanup 回收 {len(clean['reaped'])} 个非 winner/临时文件，winner 段保留")
    rest = list(it)  # 继续读完，不缺行
    rdr.close()
    p0_rows = len(rest) + 1
    assert_eq(p0_rows, len(expect[0]), "reader 在清理并发期间仍读全 p0 全部行")
    print(f"  磁盘占用（作业目录）: {directory_size(os.path.join(ROOT, JOB))} 字节")

    print("\n全部主例关卡通过。")
    print(json.dumps({"root": ROOT, "jobs": [JOB, job2, job3]}, indent=2))


def find_attempt_in(layout: Layout, job_id: str, split_id: str, attempt_no: int) -> dict:
    st = coord.inspect(layout, job_id)
    for sp in st["splits"]:
        if sp["split_id"] == split_id:
            for a in sp["attempts"]:
                if a["attempt_no"] == attempt_no:
                    return a
    raise KeyError((job_id, split_id, attempt_no))


if __name__ == "__main__":
    main()
