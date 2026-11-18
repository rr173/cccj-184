"""双时间历史表功能关卡：真实上游工人建输入，独立参考实现逐项对照。

  H1  手算演示：v1 A[0,20)"old"；v2 B[5,15)"new"；v3 C[8,12)null；v4 撤回 B；
      v5 撤回 C。逐版本时间线 = A / ABA / ABCBA / ACA / A；v3→v4 差异只有
      [5,8) 与 [12,15) 两段 B→A；C 的 null 不是空洞；后续操作不改变 v2 查询。
  H2  语义关卡：同批重叠、不同分区规则、同内容不同 ID、相同 ID 跨实体冲突、
      错误撤回（不存在/错实体/再次/前向/撤回撤回）、批尾错误（坏段、非法
      末行、语义错误在批尾），全部对照独立参考。
  H3  并发提交裁决（两个独立进程同时提交同一版本，只有一个可见提交）与
      游标快照（翻页途中他进程提交新批次，后续页仍与原查询逐条一致）。
  H4  同步点杀进程：接收一部分后/暂存未提交/提交已持久未响应；重开后每个
      版本要么整体存在要么整体不存在，重送不再占版本。
  H5  键与值语义：空串/NUL/Unicode 键、null 键拒绝、payload 任意 JSON、
      null payload 与空洞的存在标记、int64 边界、负时间、无穷终点。
  H6  分页与游标：多页宽拼接一致；游标跨表/跨接口/改参数/破坏内容均拒绝；
      进程退出再打开可接着读；未来版本不自动改成最新版本。
  H7  自身存储损坏：事件内容字节翻转 → 查询显式 HistoryCorrupt，不用空
      列表吞掉异常。
  H8  CLI 端到端。
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shuffle import coordinator as coord
from shuffle.cli import _spawn_worker
from shuffle.common import Layout, ShuffleError, connect
from shuffle import history as H
from demo.history_ref import RefTable, load_batch_rows

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.path.join(PROJECT, ".run", "hist-demo")
WORK = os.path.join(ROOT, "work")
MEM = 1 << 20
FILES = 24


def banner(t: str) -> None:
    print(f"\n{'=' * 72}\n{t}\n{'=' * 72}")


def check(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def write_input(path: str, rows: list) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for key, value in rows:
            fh.write(json.dumps({"key": key, "value": value}, ensure_ascii=False) + "\n")


def build_job(layout: Layout, job: str, splits: list, num_p: int, version: int,
              mem: int = 65536) -> None:
    """用真实独立工人进程建立作业并确认全部 winner。"""
    coord.create_job(layout, job, splits, num_p, version)
    for sid, _path in splits:
        info = coord.launch_attempt(layout, job, sid, mem)
        proc = _spawn_worker(layout, job, sid, info["attempt_no"], mem, None)
        out, err = proc.communicate(timeout=60)
        if proc.returncode != 0:
            raise RuntimeError(f"工人 {job}/{sid} 失败: {err.decode()}")
        coord.claim_winner(layout, job, sid, info["attempt_no"])


def assertion(eid, vf, vt, payload):
    return {"event_id": eid, "op": "assert", "valid_from": vf,
            "valid_to": vt, "payload": payload}


def retraction(eid, target):
    return {"event_id": eid, "op": "retract", "target_event_id": target}


def seg_ids(page):
    return [(s["start"], s["end"], s["event_id"]) for s in page["segments"]]


def make_table(name: str) -> None:
    H.create_table(WORK, name, mem_bytes=MEM, max_open_files=FILES)


def ingest(name: str, layout: Layout, job: str, batch_id: str) -> dict:
    H.submit_batch(WORK, name, src_root=layout.root, src_job=job, batch_id=batch_id)
    return H.run_batch(WORK, name, batch_id)


# --------------------------------------------------------------------------- #
# H1 手算演示
# --------------------------------------------------------------------------- #
def gate_h1(lay: Layout) -> None:
    banner("H1 手算演示：A/B/C 五版本 + 撤回；逐版本时间线与差异精确核对")
    specs = [
        ("h1j1", [("cust", assertion("A", 0, 20, "old"))]),
        ("h1j2", [("cust", assertion("B", 5, 15, "new"))]),
        ("h1j3", [("cust", assertion("C", 8, 12, None))]),
        ("h1j4", [("cust", retraction("RB", "B"))]),
        ("h1j5", [("cust", retraction("RC", "C"))]),
    ]
    ref = RefTable()
    for i, (job, rows) in enumerate(specs, start=1):
        p = os.path.join(ROOT, "in", f"{job}.jsonl")
        write_input(p, rows)
        build_job(lay, job, [("S", p)], 4, 13)
        ref.validate_and_add_batch(load_batch_rows([(p, 0)]))
    make_table("h1")
    for i, (job, _) in enumerate(specs, start=1):
        rec = ingest("h1", lay, job, f"h1-b{i}")
        check(rec["state"] == "committed" and rec["kversion"] == i,
              f"批次 h1-b{i} 提交占知识版本 {i}")
    t = H.HistoryTable(WORK, "h1")
    check(t.head() == 5, "head = 5")

    expect = {
        1: [(0, 20, "A")],
        2: [(0, 5, "A"), (5, 15, "B"), (15, 20, "A")],
        3: [(0, 5, "A"), (5, 8, "B"), (8, 12, "C"), (12, 15, "B"), (15, 20, "A")],
        4: [(0, 8, "A"), (8, 12, "C"), (12, 20, "A")],
        5: [(0, 20, "A")],
    }
    for k, exp in expect.items():
        got = seg_ids(t.timeline("cust", 0, 20, kversion=k))
        check(got == exp, f"版本 {k} 时间线 = {got}")
        ref_got = [(s["start"], s["end"], s["event_id"])
                   for s in ref.timeline("cust", 0, 20, k)]
        check(got == ref_got, f"版本 {k} 与独立参考一致")

    # 来源身份：获胜行的原上游行身份
    segs = t.timeline("cust", 0, 20, kversion=3)["segments"]
    check(segs[1]["source"] == {"job": "h1j2", "split": "S", "row_number": 1},
          "获胜片段携带原上游行身份（job/split/row_number）")
    check(segs[2]["payload"] is None and segs[2]["present"] is True,
          "C 的 payload 为 null 但 present=true（不是空洞）")

    # 差异 v3→v4：只有 [5,8) 与 [12,15) 两段 B→A
    d = t.diff("cust", 0, 20, kold=3, knew=4)
    got = [(c["start"], c["end"], c["old"]["event_id"], c["new"]["event_id"])
           for c in d["changes"]]
    check(got == [(5, 8, "B", "A"), (12, 15, "B", "A")],
          "v3→v4 差异只有 [5,8)、[12,15) 两段 B→A（C 的 null 不当成空洞）")
    ref_d = [(c["start"], c["end"], c["old"]["event_id"], c["new"]["event_id"])
             for c in ref.diff("cust", 0, 20, 3, 4)]
    check(got == ref_d, "差异与独立参考一致")
    d45 = t.diff("cust", 0, 20, kold=4, knew=5)
    check([(c["start"], c["end"], c["old"]["event_id"], c["new"]["event_id"])
           for c in d45["changes"]] == [(8, 12, "C", "A")],
          "v4→v5 差异只有 [8,12) C→A")

    # 后续操作不改变 v2 查询：再提交空批次 v6 后重查 v2
    p6 = os.path.join(ROOT, "in", "h1j6.jsonl")
    write_input(p6, [])
    build_job(lay, "h1j6", [("S", p6)], 4, 13)
    r6 = ingest("h1", lay, "h1j6", "h1-b6")
    check(r6["kversion"] == 6 and t.head() == 6, "空批次也占一个版本（v6）")
    check(seg_ids(t.timeline("cust", 0, 20, kversion=2)) == expect[2],
          "提交 v6 后版本 2 的查询不变")
    check(seg_ids(t.timeline("cust", 0, 20, kversion=6)) == expect[5],
          "版本 6 时间线 = 版本 5（空批次无新事实）")
    p = t.point("cust", 9, kversion=2)
    check(p["present"] and p["event_id"] == "B" and p["payload"] == "new",
          "点查 v2 t=9 → B/new")
    p = t.point("cust", 9, kversion=4)
    check(p["event_id"] == "C" and p["payload"] is None and p["present"],
          "点查 v4 t=9 → C/null（present=true）")
    p = t.point("cust", 9, kversion=5)
    check(p["event_id"] == "A" and p["payload"] == "old",
          "撤回 C 后点查 t=9 → A/old（下层重新露出）")


# --------------------------------------------------------------------------- #
# H2 语义关卡
# --------------------------------------------------------------------------- #
def gate_h2(lay: Layout) -> None:
    banner("H2 语义关卡：同批重叠/分区规则/同内容不同ID/冲突撤回/批尾错误")

    # (a) 同批重叠：split 清单序 + row_number 决胜
    pa = os.path.join(ROOT, "in", "h2a1.jsonl")
    pb = os.path.join(ROOT, "in", "h2a2.jsonl")
    write_input(pa, [("x", assertion("A1", 0, 10, "a1")),
                     ("x", assertion("A2", 5, 15, "a2"))])
    write_input(pb, [("x", assertion("A3", 3, 8, "a3"))])
    build_job(lay, "h2a", [("S1", pa), ("S2", pb)], 4, 13)
    make_table("h2a")
    ingest("h2a", lay, "h2a", "h2a-b1")
    t = H.HistoryTable(WORK, "h2a")
    ref = RefTable()
    ref.validate_and_add_batch(load_batch_rows([(pa, 0), (pb, 1)]))
    got = seg_ids(t.timeline("x", 0, 20, kversion=1))
    exp = [(0, 3, "A1"), (3, 8, "A3"), (8, 15, "A2"), (15, 20, None)]
    check(got == exp, f"同批重叠：S2 的 A3 赢 [3,8)，A2 赢 [8,15)，[15,20) 空洞：{got}")
    ref_got = [(s["start"], s["end"], s["event_id"]) for s in ref.timeline("x", 0, 20, 1)]
    check(got == ref_got, "同批重叠与独立参考一致")

    # (b) 不同分区规则：同样输入、不同分区数/版本 → 结果一致
    build_job(lay, "h2b2", [("S1", pa), ("S2", pb)], 2, 0)
    make_table("h2b")
    ingest("h2b", lay, "h2b2", "h2b-b1")
    t2 = H.HistoryTable(WORK, "h2b")
    check(seg_ids(t2.timeline("x", 0, 20, kversion=1)) == got,
          "分区数 4/版本 13 与 2/版本 0 的时间线完全一致")

    # (c) 同内容不同 event_id 仍是两条事实；撤回上层露出下层
    pc = os.path.join(ROOT, "in", "h2c1.jsonl")
    write_input(pc, [("y", assertion("D1", 0, 10, "same")),
                     ("y", assertion("D2", 0, 10, "same"))])
    pc2 = os.path.join(ROOT, "in", "h2c2.jsonl")
    write_input(pc2, [("y", retraction("RD2", "D2"))])
    build_job(lay, "h2c1", [("S", pc)], 4, 13)
    build_job(lay, "h2c2", [("S", pc2)], 4, 13)
    make_table("h2c")
    ingest("h2c", lay, "h2c1", "h2c-b1")
    ingest("h2c", lay, "h2c2", "h2c-b2")
    t3 = H.HistoryTable(WORK, "h2c")
    check(seg_ids(t3.timeline("y", 0, 20, kversion=1)) == [(0, 10, "D2"), (10, 20, None)],
          "同内容不同 ID：后者 D2 获胜（两条事实都保留）")
    check(seg_ids(t3.timeline("y", 0, 20, kversion=2)) == [(0, 10, "D1"), (10, 20, None)],
          "撤回 D2 后 D1 重新露出")
    st = H.table_status(WORK, "h2c")
    check(st["counters"]["asserts_landed"] == 2, "两条同内容断言都落仓（不静默去重）")

    # (d) 相同 event_id 跨实体冲突 → 整批拒绝
    pd = os.path.join(ROOT, "in", "h2d.jsonl")
    write_input(pd, [("a", assertion("E1", 0, 5, "x")),
                     ("b", assertion("E1", 0, 5, "y"))])
    build_job(lay, "h2d", [("S", pd)], 4, 13)
    make_table("h2d")
    H.submit_batch(WORK, "h2d", src_root=lay.root, src_job="h2d", batch_id="h2d-b1")
    try:
        H.run_batch(WORK, "h2d", "h2d-b1")
        raise AssertionError("相同 ID 跨实体不应提交")
    except H.BatchFailed as exc:
        check(True, f"相同 ID 跨实体冲突：整批拒绝（{exc}）")
    t4 = H.HistoryTable(WORK, "h2d")
    check(t4.head() == 0, "冲突批次不占版本（head 仍为 0）")
    st = H.table_status(WORK, "h2d")
    check(st["batches"][0]["state"] == "failed" and "重复" in st["batches"][0]["error"],
          "失败原因可检查（event_id 批内重复）")
    H.abandon_batch(WORK, "h2d", "h2d-b1")
    check(H.table_status(WORK, "h2d")["batches"][0]["state"] == "abandoned",
          "失败批次可放弃")

    # (e) 错误撤回：目标不存在 / 错实体 / 再次撤回 / 前向引用 / 撤回撤回
    make_table("h2e")
    pe0 = os.path.join(ROOT, "in", "h2e0.jsonl")
    write_input(pe0, [("e1", assertion("EA", 0, 10, "a")),
                      ("e2", assertion("EB", 0, 10, "b"))])
    build_job(lay, "h2e0", [("S", pe0)], 4, 13)
    ingest("h2e", lay, "h2e0", "h2e-b1")
    bad_batches = [
        ("h2e-x1", [("e1", retraction("R1", "NOPE"))], "目标不存在"),
        ("h2e-x2", [("e2", retraction("R2", "EA"))], "错实体撤回"),
        ("h2e-x3", [("e1", retraction("R3", "EA")), ("e1", retraction("R4", "EA"))],
         "同批再次撤回"),
        ("h2e-x4", [("e1", assertion("FWD", 0, 3, "f")),
                    ("e1", retraction("R5", "FWD"))], "前向引用同批断言"),
    ]
    for job, rows, tag in bad_batches:
        p = os.path.join(ROOT, "in", f"{job}.jsonl")
        write_input(p, rows)
        build_job(lay, job, [("S", p)], 4, 13)
        H.submit_batch(WORK, "h2e", src_root=lay.root, src_job=job, batch_id=f"{job}-b")
        try:
            H.run_batch(WORK, "h2e", f"{job}-b")
            raise AssertionError(f"{tag} 不应提交")
        except H.BatchFailed as exc:
            check(True, f"错误撤回（{tag}）：整批拒绝")
        check(H.HistoryTable(WORK, "h2e").head() == 1,
              f"{tag} 后 head 仍为 1")
        H.abandon_batch(WORK, "h2e", f"{job}-b")
    # 再次撤回（跨批）：先合法撤回 EA，再撤一次
    pe1 = os.path.join(ROOT, "in", "h2e-ok.jsonl")
    write_input(pe1, [("e1", retraction("ROK", "EA"))])
    build_job(lay, "h2e-ok", [("S", pe1)], 4, 13)
    ingest("h2e", lay, "h2e-ok", "h2e-b2")
    pe2 = os.path.join(ROOT, "in", "h2e-again.jsonl")
    write_input(pe2, [("e1", retraction("RAGAIN", "EA"))])
    build_job(lay, "h2e-again", [("S", pe2)], 4, 13)
    H.submit_batch(WORK, "h2e", src_root=lay.root, src_job="h2e-again", batch_id="h2e-b3")
    try:
        H.run_batch(WORK, "h2e", "h2e-b3")
        raise AssertionError("再次撤回不应提交")
    except H.BatchFailed:
        check(True, "跨批再次撤回：整批拒绝")
    H.abandon_batch(WORK, "h2e", "h2e-b3")
    # 撤回撤回（目标是撤回本身）
    pe3 = os.path.join(ROOT, "in", "h2e-rr.jsonl")
    write_input(pe3, [("e1", retraction("RRR", "ROK"))])
    build_job(lay, "h2e-rr", [("S", pe3)], 4, 13)
    H.submit_batch(WORK, "h2e", src_root=lay.root, src_job="h2e-rr", batch_id="h2e-b4")
    try:
        H.run_batch(WORK, "h2e", "h2e-b4")
        raise AssertionError("撤回撤回不应提交")
    except H.BatchFailed:
        check(True, "撤回本身不接受被撤回：整批拒绝")
    H.abandon_batch(WORK, "h2e", "h2e-b4")
    check(H.HistoryTable(WORK, "h2e").head() == 2,
          "全部错误撤回后 head=2（只有合法批次占版本）")

    # (f) 批尾错误：语义错误在批尾 → 整批不可见
    make_table("h2f")
    pf = os.path.join(ROOT, "in", "h2f.jsonl")
    write_input(pf, [("z", assertion("G1", 0, 5, "ok1")),
                     ("z", assertion("G2", 5, 10, "ok2")),
                     ("z", assertion("G3", 7, 7, "bad"))])   # 末行区间为空
    build_job(lay, "h2f", [("S", pf)], 4, 13)
    H.submit_batch(WORK, "h2f", src_root=lay.root, src_job="h2f", batch_id="h2f-b1")
    try:
        H.run_batch(WORK, "h2f", "h2f-b1")
        raise AssertionError("批尾语义错误不应提交")
    except H.BatchFailed as exc:
        check(True, f"批尾语义错误：整批拒绝（{exc}）")
    check(H.HistoryTable(WORK, "h2f").head() == 0,
          "批尾错误使整批不可见（含前面的合法行）")
    H.abandon_batch(WORK, "h2f", "h2f-b1")

    # (f2) 坏段：winner 段尾部字节被改写 → 接收期对照批次清单显式失败
    make_table("h2g")
    pg = os.path.join(ROOT, "in", "h2g.jsonl")
    write_input(pg, [("w", assertion("H1", 0, 5, "v1")),
                     ("w", assertion("H2", 5, 9, "v2"))])
    build_job(lay, "h2g", [("S", pg)], 4, 13)
    conn = connect(lay.db_path)
    try:
        seg_row = conn.execute(
            "SELECT relpath FROM segments WHERE job_id='h2g' AND state='done'"
            " AND row_count>0 LIMIT 1").fetchone()
    finally:
        conn.close()
    seg_path = coord._abs_seg_path(lay, "h2g", seg_row["relpath"])
    size = os.path.getsize(seg_path)
    with open(seg_path, "r+b") as fh:   # 改写段内最后一行区域一个字节
        fh.seek(size - 40)
        b = fh.read(1)
        fh.seek(size - 40)
        fh.write(bytes([b[0] ^ 0xFF]))
    H.submit_batch(WORK, "h2g", src_root=lay.root, src_job="h2g", batch_id="h2g-b1")
    try:
        H.run_batch(WORK, "h2g", "h2g-b1")
        raise AssertionError("坏段不应入仓")
    except H.BatchFailed as exc:
        check(True, f"坏段（尾部字节改写）：接收期整批拒绝（{type(exc).__name__}）")
    st = H.table_status(WORK, "h2g")
    check(st["batches"][0]["state"] == "failed" and st["head"] == 0,
          "坏段批次失败可查，head 不变")
    H.abandon_batch(WORK, "h2g", "h2g-b1")
    check(H.table_status(WORK, "h2g")["batches"][0]["state"] == "abandoned",
          "坏段批次可放弃（释放自己的引用与暂存）")


# --------------------------------------------------------------------------- #
# H3 并发提交裁决 + 游标快照（独立进程）
# --------------------------------------------------------------------------- #
def gate_h3(lay: Layout) -> None:
    banner("H3 并发提交裁决与游标快照（真实独立进程交错写入与翻页）")
    pa = os.path.join(ROOT, "in", "h3a.jsonl")
    pb = os.path.join(ROOT, "in", "h3b.jsonl")
    write_input(pa, [("e1", assertion("JA1", 0, 100, "from-A"))])
    write_input(pb, [("e2", assertion("JB1", 0, 100, "from-B"))])
    build_job(lay, "h3jA", [("S", pa)], 4, 13)
    build_job(lay, "h3jB", [("S", pb)], 4, 13)
    make_table("h3")
    # 两个批次都绑定下一版本 1
    rx = H.submit_batch(WORK, "h3", src_root=lay.root, src_job="h3jA", batch_id="h3-X")
    ry = H.submit_batch(WORK, "h3", src_root=lay.root, src_job="h3jB", batch_id="h3-Y")
    check(rx["kversion"] == 1 and ry["kversion"] == 1,
          "两个批次都绑定下一知识版本 1")
    procs = []
    for bid, tag in (("h3-X", "x"), ("h3-Y", "y")):
        ready = os.path.join(ROOT, f"ready-{tag}")
        go = os.path.join(ROOT, "go")
        procs.append((bid, subprocess.Popen(
            [sys.executable, "-m", "demo.hist_child", "commit-race",
             WORK, "h3", bid, ready, go],
            cwd=PROJECT, stdout=subprocess.PIPE, stderr=subprocess.PIPE)))
    for _bid, tag in (("h3-X", "x"), ("h3-Y", "y")):
        deadline = time.time() + 60
        while not os.path.exists(os.path.join(ROOT, f"ready-{tag}")):
            if time.time() > deadline:
                raise RuntimeError("子进程未就绪")
            time.sleep(0.02)
    with open(os.path.join(ROOT, "go"), "w") as fh:   # 同时放行提交
        fh.write("go")
    results = {}
    for bid, proc in procs:
        out, err = proc.communicate(timeout=60)
        results[bid] = json.loads(out.decode().strip().splitlines()[-1])
    oks = [b for b, r in results.items() if r.get("ok")]
    conflicts = [b for b, r in results.items() if not r.get("ok")]
    check(len(oks) == 1 and len(conflicts) == 1,
          f"两个写进程同时提交同一版本：只有一个可见提交 {results}")
    t = H.HistoryTable(WORK, "h3")
    check(t.head() == 1, "head 只推进一次（=1）")
    loser = conflicts[0]
    st = H.table_status(WORK, "h3")
    loser_state = [b for b in st["batches"] if b["batch_id"] == loser][0]
    check(loser_state["state"] == "staged",
          "失败方保持 staged（不能覆盖或拼接），可检查后放弃")
    winner = oks[0]
    win_job = "h3jA" if winner == "h3-X" else "h3jB"
    exp_entity = "e1" if winner == "h3-X" else "e2"
    check(t.point(exp_entity, 50, kversion=1)["present"],
          "可见提交的事实可查；失败方事实不可见")
    H.abandon_batch(WORK, "h3", loser)
    # 失败方数据以新批次标识重送 → 占版本 2
    r2 = H.submit_batch(WORK, "h3", src_root=lay.root, src_job="h3jB" if loser == "h3-Y" else "h3jA",
                        batch_id="h3-Z")
    check(r2["kversion"] == 2, "放弃后以新标识重送，绑定版本 2")
    H.run_batch(WORK, "h3", "h3-Z")
    check(t.head() == 2, "重送后 head=2")

    # 游标快照：翻页途中他进程提交新批次，后续页仍与原查询逐条一致
    make_table("h3s")
    rows1 = [(f"e{i}", assertion(f"V1-{i}", 0, 50, f"v1-{i}")) for i in range(10)]
    p1 = os.path.join(ROOT, "in", "h3s1.jsonl")
    write_input(p1, rows1)
    build_job(lay, "h3s1", [("S", p1)], 4, 13)
    ingest("h3s", lay, "h3s1", "h3s-b1")
    ts = H.HistoryTable(WORK, "h3s")
    page1 = ts.export(kversion=1, limit=7)
    check(len(page1["records"]) == 7 and page1["cursor"], "导出第 1 页 7 条")
    # 另一“进程”提交 v2（改写全部实体）
    rows2 = [(f"e{i}", assertion(f"V2-{i}", 0, 50, f"v2-{i}")) for i in range(10)]
    p2 = os.path.join(ROOT, "in", "h3s2.jsonl")
    write_input(p2, rows2)
    build_job(lay, "h3s2", [("S", p2)], 4, 13)
    ingest("h3s", lay, "h3s2", "h3s-b2")
    check(ts.head() == 2, "翻页途中新批次提交，head=2")
    # 跨进程续读：用 CLI 子进程拿后续页
    proc = subprocess.run(
        [sys.executable, "-m", "shuffle.hist_cli", "export",
         "--workdir", WORK, "--name", "h3s", "--limit", "100",
         "--cursor", page1["cursor"]],
        capture_output=True, text=True, cwd=PROJECT)
    check(proc.returncode == 0, f"跨进程游标续读成功（{proc.stderr.strip()[:100]}）")
    page2 = json.loads(proc.stdout)
    all_records = page1["records"] + page2["records"]
    check(all(r["payload"].startswith("v1-") for r in all_records if r["present"]),
          "翻页途中提交 v2 后，后续页仍逐条反映固定的版本 1")
    full_v1 = ts.export(kversion=1, limit=1000)["records"]
    check([(r["entity"], r["start"], r["end"], r["event_id"]) for r in all_records]
          == [(r["entity"], r["start"], r["end"], r["event_id"]) for r in full_v1],
          "分页拼接 == 固定版本的整次导出")
    check(ts.export(kversion=2, limit=1000)["records"][1]["payload"].startswith("v2-"),
          "显式选版本 2 则看到新事实")

    # 两个进程同时提交同标识批次：幂等，只有一个落库，结果一致
    make_table("h3dup")
    pd = os.path.join(ROOT, "in", "h3dup.jsonl")
    write_input(pd, [("e", assertion("DUP1", 0, 9, "x"))])
    build_job(lay, "h3dupj", [("S", pd)], 4, 13)
    procs = [subprocess.Popen(
        [sys.executable, "-c",
         "import sys; sys.path.insert(0, '.'); from shuffle import history as H; "
         "H.submit_batch('.run/hist-demo/work', 'h3dup', src_root='.run/hist-demo/root', "
         "src_job='h3dupj', batch_id='h3dup-b')"],
        cwd=PROJECT, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for _ in range(2)]
    rcs = [p.wait() for p in procs]
    check(all(rc == 0 for rc in rcs), "并发同标识建批：两个进程都幂等成功")
    st = H.table_status(WORK, "h3dup")
    check(len(st["batches"]) == 1 and st["batches"][0]["kversion"] == 1,
          "并发同标识建批：只落一个批次，绑定版本 1")


# --------------------------------------------------------------------------- #
# H4 同步点杀进程恢复
# --------------------------------------------------------------------------- #
def _run_cli_hook(name: str, batch_id: str, hook: str | None) -> subprocess.CompletedProcess:
    cmd = [sys.executable, "-m", "shuffle.hist_cli", "run",
           "--workdir", WORK, "--name", name, "--batch-id", batch_id]
    if hook:
        cmd += ["--hook", hook]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT)


def gate_h4(lay: Layout) -> None:
    banner("H4 同步点杀进程：接收途中/暂存未提交/提交已持久未响应；重开恢复")
    # 多 split 多行 → 多个段
    p1 = os.path.join(ROOT, "in", "h4a.jsonl")
    p2 = os.path.join(ROOT, "in", "h4b.jsonl")
    write_input(p1, [(f"k{i}", assertion(f"H4-{i}", i * 10, i * 10 + 5, f"v{i}"))
                     for i in range(12)])
    write_input(p2, [(f"m{i}", assertion(f"H4M-{i}", 0, 100, f"w{i}"))
                     for i in range(6)])
    build_job(lay, "h4j", [("S1", p1), ("S2", p2)], 4, 13)
    make_table("h4")
    H.submit_batch(WORK, "h4", src_root=lay.root, src_job="h4j", batch_id="h4-b1")

    proc = _run_cli_hook("h4", "h4-b1", "kill-after-receive-segment=2")
    check(proc.returncode == 42, f"接收第 2 段后受控终止（退出码 {proc.returncode}）")
    st = H.table_status(WORK, "h4")
    check(st["batches"][0]["receive"]["segments_done"] == 2 and st["head"] == 0,
          "已持久接收 2 段（状态可查），head 不变")
    t = H.HistoryTable(WORK, "h4")
    check(t.head() == 0 and not t.point("k1", 12, kversion=0)["present"],
          "被杀后查询正常（版本 0 合法空结果）")

    proc = _run_cli_hook("h4", "h4-b1", "kill-after-receive")
    check(proc.returncode == 42, "接收完成后受控终止")
    st = H.table_status(WORK, "h4")
    check(st["batches"][0]["state"] == "staging" and st["head"] == 0,
          "接收完成未暂存：状态 staging，版本仍不可见")
    reads_before = st["counters"]["src_segments_read"]

    proc = _run_cli_hook("h4", "h4-b1", "kill-after-stage")
    check(proc.returncode == 42, "暂存后提交前受控终止")
    st = H.table_status(WORK, "h4")
    check(st["batches"][0]["state"] == "staged" and st["head"] == 0,
          "历史改动已暂存但未提交：版本整体不可见")
    check(st["counters"]["src_segments_read"] == reads_before,
          "恢复复用已持久接收的源范围（源段不重读）")

    proc = _run_cli_hook("h4", "h4-b1", "kill-after-commit")
    check(proc.returncode == 42, "提交已持久但调用者未收到成功时受控终止")
    st = H.table_status(WORK, "h4")
    check(st["head"] == 1 and st["batches"][0]["state"] == "committed",
          "重开后版本 1 整体存在（提交原子）")
    # 重送：不再占版本，幂等返回原结果
    r = H.submit_batch(WORK, "h4", src_root=lay.root, src_job="h4j", batch_id="h4-b1")
    check(r["state"] == "committed" and r["kversion"] == 1,
          "重送同标识同绑定：返回已提交原结果，不再占版本")
    check(H.HistoryTable(WORK, "h4").head() == 1, "head 仍为 1")
    # 恢复运行：幂等完成
    proc = _run_cli_hook("h4", "h4-b1", None)
    check(proc.returncode == 0, "恢复运行幂等完成")
    t = H.HistoryTable(WORK, "h4")
    check(t.point("k3", 32, kversion=1)["event_id"] == "H4-3",
          "恢复后事实可查（k3 → H4-3）")
    check(t.point("m2", 50, kversion=1)["event_id"] == "H4M-2",
          "第二 split 的事实同样入仓")

    # 每个版本要么整体存在要么整体不存在：提交前杀 → 版本 2 不可见
    p3 = os.path.join(ROOT, "in", "h4c.jsonl")
    write_input(p3, [("k0", assertion("H4-NEW", 0, 3, "new"))])
    build_job(lay, "h4j2", [("S", p3)], 4, 13)
    H.submit_batch(WORK, "h4", src_root=lay.root, src_job="h4j2", batch_id="h4-b2")
    proc = _run_cli_hook("h4", "h4-b2", "kill-after-stage")
    check(proc.returncode == 42, "版本 2 暂存后被杀")
    check(H.HistoryTable(WORK, "h4").head() == 1, "版本 2 整体不存在（head 仍为 1）")
    try:
        t.point("k0", 1, kversion=2)
        raise AssertionError("未提交版本不应可查")
    except H.UncommittedVersion as exc:
        check(True, f"查询未提交版本显式拒绝（{exc}）")
    proc = _run_cli_hook("h4", "h4-b2", None)
    check(proc.returncode == 0 and H.HistoryTable(WORK, "h4").head() == 2,
          "恢复后版本 2 整体存在")

    # 删除源作业不妨碍继续提交或查询已入仓历史
    out = coord.cleanup(lay, "h4j", destroy=True)
    check(out["destroyed"] is True, "接收完成后源段保护已释放，源作业可销毁")
    t = H.HistoryTable(WORK, "h4")
    check(t.point("k3", 32, kversion=1)["event_id"] == "H4-3",
          "源作业销毁后已入仓历史仍可查")
    r = H.submit_batch(WORK, "h4", src_root=lay.root, src_job="h4j", batch_id="h4-b1")
    check(r["state"] == "committed" and r["kversion"] == 1,
          "源作业销毁后重送已提交批次：仍幂等返回（按 root/job 核对）")
    p4 = os.path.join(ROOT, "in", "h4d.jsonl")
    write_input(p4, [("k0", assertion("H4-V3", 0, 1, "v3"))])
    build_job(lay, "h4j3", [("S", p4)], 4, 13)
    ingest("h4", lay, "h4j3", "h4-b3")
    check(H.HistoryTable(WORK, "h4").head() == 3,
          "源作业销毁不影响后续批次继续提交占版本")


# --------------------------------------------------------------------------- #
# H5 键与值语义
# --------------------------------------------------------------------------- #
def gate_h5(lay: Layout) -> None:
    banner("H5 键与值语义：空串/NUL/Unicode 键、null 键拒绝、payload 任意 JSON、int64 边界")
    make_table("h5")
    special_keys = ["", "a\x00b", "µnïçødé", "汉", "K", "k"]
    rows = []
    for i, key in enumerate(special_keys):
        rows.append((key, assertion(f"K{i}", -10, 10, {"idx": i, "nested": [1, None, {"x": "y"}]})))
    p = os.path.join(ROOT, "in", "h5keys.jsonl")
    write_input(p, rows)
    build_job(lay, "h5keys", [("S", p)], 4, 13)
    ingest("h5", lay, "h5keys", "h5-b1")
    t = H.HistoryTable(WORK, "h5")
    for i, key in enumerate(special_keys):
        pt = t.point(key, 0, kversion=1)
        check(pt["present"] and pt["payload"]["idx"] == i,
              f"键 {key!r} 精确可查（不修剪不折叠大小写）")
    check(t.point("K", 0, kversion=1)["event_id"] == "K4"
          and t.point("k", 0, kversion=1)["event_id"] == "K5",
          "大小写不折叠：K 与 k 是两个实体")
    check(not t.point("\x00NULL", 0, kversion=1)["present"],
          "不存在的键合法空结果（present=false）")

    # null 键 → 整批拒绝
    pn = os.path.join(ROOT, "in", "h5null.jsonl")
    write_input(pn, [(None, assertion("NUL1", 0, 5, "x"))])
    build_job(lay, "h5null", [("S", pn)], 4, 13)
    H.submit_batch(WORK, "h5", src_root=lay.root, src_job="h5null", batch_id="h5-badnull")
    try:
        H.run_batch(WORK, "h5", "h5-bad1")
        raise AssertionError("null 键不应入仓")
    except H.BatchFailed:
        check(True, "null 键非法：整批拒绝")
    H.abandon_batch(WORK, "h5", "h5-badnull")
    check(t.head() == 1, "非法批次不占版本")

    # int64 边界与无穷终点、负时间
    pb = os.path.join(ROOT, "in", "h5bounds.jsonl")
    write_input(pb, [
        ("b1", assertion("BMIN", -(2**63), -(2**63) + 100, "min")),
        ("b1", assertion("BINF", 0, None, "inf")),
        ("b2", assertion("BMAX", (2**63) - 100, (2**63) - 1, "max")),
        ("b2", assertion("BNEG", -50, -10, "neg")),
    ])
    build_job(lay, "h5bounds", [("S", pb)], 4, 13)
    ingest("h5", lay, "h5bounds", "h5-b2")
    check(t.point("b1", -(2**63), kversion=2)["event_id"] == "BMIN",
          "valid_from = -2^63 合法")
    check(t.point("b1", 2**62, kversion=2)["event_id"] == "BINF",
          "valid_to = null 表示正无穷")
    check(t.point("b1", -(2**63) + 50, kversion=2)["event_id"] == "BMIN",
          "负时间区间可查")
    check(t.point("b2", (2**63) - 2, kversion=2)["event_id"] == "BMAX",
          "valid_to = 2^63-1 合法")
    check(t.point("b2", -30, kversion=2)["event_id"] == "BNEG", "全负区间可查")
    tl = t.timeline("b1", -(2**63), 100, kversion=2)
    check([(s["start"], s["end"], s["event_id"]) for s in tl["segments"]] ==
          [(-(2**63), -(2**63) + 100, "BMIN"), (-(2**63) + 100, 0, None), (0, 100, "BINF")],
          "时间线含空洞（present=false）且完整无遗漏")
    gap = [s for s in tl["segments"] if s["event_id"] is None][0]
    check(gap["present"] is False and gap["payload"] is None,
          "空洞片段 present=false（与 null payload 区分）")

    # 非法时间：反向区间 / 布尔 / 浮点 / 越界
    bad_rows = [
        [("t", assertion("BAD1", 5, 5, "x"))],                 # 空区间
        [("t", assertion("BAD2", 9, 5, "x"))],                 # 反向
        [("t", assertion("BAD3", True, 8, "x"))],              # 布尔时间
        [("t", assertion("BAD4", 0, 2.5, "x"))],               # 浮点
        [("t", assertion("BAD5", 0, 2**63, "x"))],             # 越界
        [("t", {"event_id": "BAD6", "op": "assert", "valid_from": 0,
                "valid_to": 5, "payload": float("nan")})],     # 非标准 JSON
    ]
    for i, rows_bad in enumerate(bad_rows):
        pbad = os.path.join(ROOT, "in", f"h5bad{i}.jsonl")
        write_input(pbad, rows_bad)
        build_job(lay, f"h5bad{i}", [("S", pbad)], 4, 13)
        H.submit_batch(WORK, "h5", src_root=lay.root, src_job=f"h5bad{i}",
                       batch_id=f"h5-bad{i}")
        try:
            H.run_batch(WORK, "h5", f"h5-bad{i}")
            raise AssertionError(f"非法行 {i} 不应入仓")
        except H.BatchFailed:
            pass
        H.abandon_batch(WORK, "h5", f"h5-bad{i}")
    check(True, "空区间/反向/布尔/浮点/越界/NaN 全部整批拒绝")
    check(t.head() == 2, "全部非法批次后 head=2")

    # payload 任意标准 JSON：字符串/数字/布尔/数组/对象/null
    pj = os.path.join(ROOT, "in", "h5json.jsonl")
    payloads = ["s", 3.5, True, [1, 2], {"a": {"b": [None]}}, None]
    write_input(pj, [("j", assertion(f"J{i}", i * 10, i * 10 + 10, pl))
                     for i, pl in enumerate(payloads)])
    build_job(lay, "h5json", [("S", pj)], 4, 13)
    ingest("h5", lay, "h5json", "h5-b3")
    for i, pl in enumerate(payloads):
        got = t.point("j", i * 10 + 5, kversion=3)
        check(got["present"] and got["payload"] == pl,
              f"payload {pl!r} 原样可查（present=true）")
    check(t.point("j", 55, kversion=3)["payload"] is None
          and t.point("j", 55, kversion=3)["present"],
          "payload=null 与空洞都有独立存在标记")


# --------------------------------------------------------------------------- #
# H6 分页与游标
# --------------------------------------------------------------------------- #
def gate_h6(lay: Layout) -> None:
    banner("H6 分页与游标：拼接一致、跨表/跨接口/破坏拒绝、进程退出续读")
    make_table("h6")
    # 实体 pg：40 个不重叠断言 → 多片段；实体字典序混合
    keys = ["b", "a", "µ", "汉", "", "A", "aa"]
    rows = []
    for key in keys:
        for i in range(6):
            rows.append((key, assertion(f"P-{key}-{i}", i * 4, i * 4 + 2, f"{key}-{i}")))
    rows += [("pg", assertion(f"PG-{i}", i * 2, i * 2 + 1, i)) for i in range(40)]
    p = os.path.join(ROOT, "in", "h6.jsonl")
    write_input(p, rows)
    build_job(lay, "h6j", [("S", p)], 4, 13)
    ingest("h6", lay, "h6j", "h6-b1")
    t = H.HistoryTable(WORK, "h6")

    full = t.timeline("pg", 0, 80, kversion=1, limit=1000)["segments"]
    check(len(full) == 80, f"pg 全时间线 80 段（40 事实 + 40 空洞），实际 {len(full)}")
    for page_w in (1, 2, 7, 1000):
        got, cur = [], None
        while True:
            if cur is None:
                page = t.timeline("pg", 0, 80, kversion=1, limit=page_w)
            else:
                page = t.timeline(cursor=cur, limit=page_w)
            got.extend(page["segments"])
            cur = page["cursor"]
            if not cur:
                break
        check(seg_ids({"segments": got}) == seg_ids({"segments": full}),
              f"时间线页宽 {page_w} 拼接 == 整次查询")

    # 导出：实体 UTF-8 字节序
    exp_order = sorted(keys + ["pg"], key=lambda k: k.encode("utf-8"))
    got, cur = [], None
    while True:
        page = t.export(kversion=1, limit=5) if cur is None else \
            t.export(limit=5, cursor=cur)
        got.extend(page["records"])
        cur = page["cursor"]
        if not cur:
            break
    ents = []
    for r in got:
        if not ents or ents[-1] != r["entity"]:
            ents.append(r["entity"])
    check(ents == exp_order, f"导出实体序 == UTF-8 字节序 {ents}")
    check(len(got) == len(t.export(kversion=1, limit=10000)["records"]),
          "导出分页拼接 == 整次导出")

    # 差异分页
    p2 = os.path.join(ROOT, "in", "h6b.jsonl")
    write_input(p2, [("pg", assertion(f"PG2-{i}", i * 10, i * 10 + 5, "x"))
                     for i in range(8)])
    build_job(lay, "h6j2", [("S", p2)], 4, 13)
    ingest("h6", lay, "h6j2", "h6-b2")
    d_full = t.diff("pg", 0, 80, kold=1, knew=2, limit=1000)["changes"]
    got, cur = [], None
    while True:
        page = t.diff("pg", 0, 80, kold=1, knew=2, limit=3) if cur is None else \
            t.diff(cursor=cur, limit=3)
        got.extend(page["changes"])
        cur = page["cursor"]
        if not cur:
            break
    check([(c["start"], c["end"]) for c in got] == [(c["start"], c["end"]) for c in d_full],
          "差异分页拼接 == 整次差异")

    # 游标误用
    make_table("h6other")
    cur1 = t.timeline("pg", 0, 80, kversion=1, limit=3)["cursor"]
    t_other = H.HistoryTable(WORK, "h6other")
    try:
        t_other.timeline(cursor=cur1, limit=3)
        raise AssertionError("跨表游标不应接受")
    except H.CursorError as exc:
        check(True, f"游标用于另一张表：显式拒绝（{exc}）")
    try:
        t.diff(cursor=cur1, limit=3)
        raise AssertionError("跨接口游标不应接受")
    except H.CursorError as exc:
        check(True, "游标用于另一种接口：显式拒绝")
    bad = cur1[:-3] + ("AAA" if cur1[-3:] != "AAA" else "BBB")
    try:
        t.timeline(cursor=bad, limit=3)
        raise AssertionError("破坏的游标不应接受")
    except H.CursorError:
        check(True, "破坏游标内容：显式拒绝")
    obj = json.loads(cur1)
    obj["t1"] = 999
    forged = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    try:
        t.timeline(cursor=forged, limit=3)
        raise AssertionError("改参数的游标不应接受")
    except H.CursorError:
        check(True, "改换查询参数的游标：显式拒绝（完整性校验失败）")
    # 游标与显式参数同时给出但不一致
    try:
        t.timeline("pg", 0, 999, cursor=cur1, limit=3)
        raise AssertionError("游标+不一致参数不应接受")
    except H.CursorError:
        check(True, "游标与不一致的显式参数同给：显式拒绝")
    ok_same = t.timeline("pg", 0, 80, kversion=1, cursor=cur1, limit=3)
    check(len(ok_same["segments"]) == 3, "游标与一致参数同给：正常续读")

    # 进程退出再打开可接着读（CLI 子进程翻页）
    proc = subprocess.run(
        [sys.executable, "-m", "shuffle.hist_cli", "timeline", "--workdir", WORK,
         "--name", "h6", "--entity", "pg", "--t0", "0", "--t1", "80",
         "--kversion", "1", "--limit", "10"],
        capture_output=True, text=True, cwd=PROJECT)
    page = json.loads(proc.stdout)
    proc2 = subprocess.run(
        [sys.executable, "-m", "shuffle.hist_cli", "timeline", "--workdir", WORK,
         "--name", "h6", "--limit", "1000", "--cursor", page["cursor"]],
        capture_output=True, text=True, cwd=PROJECT)
    page2 = json.loads(proc2.stdout)
    check(len(page["segments"]) + len(page2["segments"]) == 80,
          "进程退出再打开：游标续读拼接完整")

    # 未来版本 / 未提交版本
    try:
        t.timeline("pg", 0, 80, kversion=99)
        raise AssertionError("未来版本不应自动改成最新")
    except H.VersionNotFound:
        check(True, "查询不存在的未来版本：显式拒绝（不自动改成最新版本）")
    p3 = os.path.join(ROOT, "in", "h6c.jsonl")
    write_input(p3, [("pg", assertion("PG-NEW", 0, 1, "x"))])
    build_job(lay, "h6j3", [("S", p3)], 4, 13)
    H.submit_batch(WORK, "h6", src_root=lay.root, src_job="h6j3", batch_id="h6-b3")
    try:
        t.point("pg", 0, kversion=3)
        raise AssertionError("未提交版本不应可查")
    except H.UncommittedVersion:
        check(True, "查询未提交批次版本：显式拒绝（区分未提交与合法空结果）")
    H.abandon_batch(WORK, "h6", "h6-b3")


# --------------------------------------------------------------------------- #
# H7 自身存储损坏
# --------------------------------------------------------------------------- #
def gate_h7(lay: Layout) -> None:
    banner("H7 自身存储损坏：事件内容字节翻转 → 查询显式失败，不用空列表吞掉")
    make_table("h7")
    p = os.path.join(ROOT, "in", "h7.jsonl")
    write_input(p, [("victim", assertion("V1", 0, 100, "PAYLOAD_MARK_XYZ"))])
    build_job(lay, "h7j", [("S", p)], 4, 13)
    ingest("h7", lay, "h7j", "h7-b1")
    t = H.HistoryTable(WORK, "h7")
    check(t.point("victim", 50, kversion=1)["payload"] == "PAYLOAD_MARK_XYZ",
          "损坏前查询正常")
    db = os.path.join(WORK, "h7", "history.sqlite3")
    conn = connect(db)
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()
    with open(db, "rb") as fh:
        data = fh.read()
    off = data.find(b"PAYLOAD_MARK_XYZ")
    check(off > 0, "在库文件中定位事件内容字节")
    with open(db, "r+b") as fh:
        fh.seek(off + 3)
        b = fh.read(1)
        fh.seek(off + 3)
        fh.write(bytes([b[0] ^ 0xFF]))
    try:
        H.HistoryTable(WORK, "h7").point("victim", 50, kversion=1)
        raise AssertionError("内容损坏不应读出成功结果")
    except H.HistoryCorrupt as exc:
        check(True, f"事件内容字节翻转：查询显式 HistoryCorrupt（{exc}）")
    try:
        H.HistoryTable(WORK, "h7").timeline("victim", 0, 100, kversion=1)
        raise AssertionError("时间线也不应吞掉损坏")
    except H.HistoryCorrupt:
        check(True, "时间线查询同样显式失败（不用空列表吞掉异常）")


# --------------------------------------------------------------------------- #
# H8 CLI 端到端
# --------------------------------------------------------------------------- #
def gate_h8(lay: Layout) -> None:
    banner("H8 CLI 端到端：create/submit/run/status/point/timeline/diff/export/abandon/drop")
    def cli(*args, ok=True):
        proc = subprocess.run([sys.executable, "-m", "shuffle.hist_cli", *args],
                              capture_output=True, text=True, cwd=PROJECT)
        if ok and proc.returncode != 0:
            raise RuntimeError(f"CLI 失败 {args}: {proc.stderr}")
        return proc

    p = os.path.join(ROOT, "in", "h8.jsonl")
    write_input(p, [("c1", assertion("C1", 0, 10, "x")),
                    ("c2", assertion("C2", 3, 8, None))])
    build_job(lay, "h8j", [("S", p)], 4, 13)
    out = json.loads(cli("create", "--workdir", WORK, "--name", "h8",
                         "--mem-bytes", str(MEM), "--max-open-files", str(FILES)).stdout)
    check(out["head"] == 0 and out["table_token"], "CLI create：表建成（含持久身份）")
    out = json.loads(cli("submit", "--workdir", WORK, "--name", "h8",
                         "--root", lay.root, "--job", "h8j",
                         "--batch-id", "h8-b1").stdout)
    check(out["state"] == "receiving" and out["kversion"] == 1, "CLI submit：批次登记")
    out = json.loads(cli("run", "--workdir", WORK, "--name", "h8",
                         "--batch-id", "h8-b1").stdout)
    check(out["state"] == "committed" and out["kversion"] == 1, "CLI run：入仓完成")
    out = json.loads(cli("status", "--workdir", WORK, "--name", "h8").stdout)
    check(out["head"] == 1 and out["counters"]["asserts_landed"] == 2,
          "CLI status：head/计数器可查")
    out = json.loads(cli("point", "--workdir", WORK, "--name", "h8",
                         "--entity", "c1", "--time", "5", "--kversion", "1").stdout)
    check(out["present"] and out["event_id"] == "C1", "CLI point")
    out = json.loads(cli("timeline", "--workdir", WORK, "--name", "h8",
                         "--entity", "c2", "--t0", "0", "--t1", "10",
                         "--kversion", "1").stdout)
    check(seg_ids(out) == [(0, 3, None), (3, 8, "C2"), (8, 10, None)],
          "CLI timeline（含空洞）")
    out = json.loads(cli("diff", "--workdir", WORK, "--name", "h8",
                         "--entity", "c1", "--t0", "0", "--t1", "10",
                         "--kold", "0", "--knew", "1").stdout)
    check(len(out["changes"]) == 1 and out["changes"][0]["new"]["event_id"] == "C1",
          "CLI diff（v0→v1 新增 C1）")
    out = json.loads(cli("export", "--workdir", WORK, "--name", "h8",
                         "--kversion", "1", "--limit", "100").stdout)
    check({r["entity"] for r in out["records"]} == {"c1", "c2"}, "CLI export")
    # abandon：先 submit 一个不运行的批次再放弃
    p2 = os.path.join(ROOT, "in", "h8b.jsonl")
    write_input(p2, [("c1", assertion("C9", 0, 1, "y"))])
    build_job(lay, "h8j2", [("S", p2)], 4, 13)
    cli("submit", "--workdir", WORK, "--name", "h8", "--root", lay.root,
        "--job", "h8j2", "--batch-id", "h8-b2")
    out = json.loads(cli("abandon", "--workdir", WORK, "--name", "h8",
                         "--batch-id", "h8-b2").stdout)
    check(out["state"] == "abandoned", "CLI abandon：未提交批次可放弃")
    out = json.loads(cli("drop", "--workdir", WORK, "--name", "h8").stdout)
    check(out["dropped"] and not os.path.exists(os.path.join(WORK, "h8")),
          "CLI drop：表删除（只移除自己的目录）")


def main() -> None:
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(WORK, exist_ok=True)
    lay = Layout(os.path.join(ROOT, "root"))
    gate_h1(lay)
    gate_h2(lay)
    gate_h3(lay)
    gate_h4(lay)
    gate_h5(lay)
    gate_h6(lay)
    gate_h7(lay)
    gate_h8(lay)
    print("\n全部历史表功能关卡通过。")


if __name__ == "__main__":
    main()
