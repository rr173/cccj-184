"""双输入关联算子功能关卡：真实上游工人建输入，再驱动关联接口。

  J1  规定小例：三种模式精确结果（full 8 条：L1配R1/R2、L2配R1/R2、L3单边、
      L4配R4、R3单边、R5单边）；完整原行四字段、未匹配侧为 null
  J2  键语义：null 与文本 "\\u0000NULL" 同桶但不误配；空串/Unicode/含 NUL 文本；
      合法 null value 与“这一侧没有行”严格区分
  J3  随机化交叉核对：两种分区数/版本、同名 split、重复内容行 × 三种模式
  J4  分页边界：多种页宽拼接 == 整次流；越界/零宽/末条/中段迭代
  J5  空侧：左空/右空/双空 × 三种模式
  J6  同一作业同时作为左右输入（自关联），左右角色仍区分
  J7  建关联失败：右侧未就绪/已取消/不存在/已销毁；失败不留悬挂保护
  J8  段保护生命周期：加工期间 destroy 被拒；源作业取消不换输入；
      完成后释放保护可销毁源；删除关联只释放它自己的保护
  J9  同步关卡：接收源段后/热点乘积途中/结果可读前杀进程并恢复；
      已接收源段不重读；恢复后结果与朴素参考一致
  J10 结果损坏：内容字节翻转/截断 → 读取显式失败
  J11 单行超过预算：明确报错，关联保持可诊断状态
  J12 上游分区数与版本改变，不改变规定身份、重数与次序
"""

from __future__ import annotations

import json
import os
import random
import shutil
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shuffle import coordinator as coord
from shuffle.cli import _spawn_worker
from shuffle.common import Layout, ShuffleError, connect, partition_of
from shuffle import join as J
from shuffle.join_common import BudgetExceeded, JoinFailed, JoinNotReady, ResultNotReady
from demo.join_ref import (
    comparable,
    expected_comparable,
    load_source_rows,
    naive_join,
    write_input,
)

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.path.join(PROJECT, ".run", "join-demo")
WORK = os.path.join(ROOT, "work")
MEM = 16 * 1024
FILES = 24


def banner(t: str) -> None:
    print(f"\n{'=' * 72}\n{t}\n{'=' * 72}")


def check(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


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


def open_join_readers(layout: Layout, job: str) -> int:
    conn = connect(layout.db_path)
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM readers WHERE job_id=? AND state='open'"
            " AND reader_id LIKE 'join:%'", (job,)).fetchone()
        return int(row["c"])
    finally:
        conn.close()


def run_join_cli(name: str, hook: str | None = None) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env.pop("SHUFFLE_JOIN_HOOK", None)
    cmd = [sys.executable, "-m", "shuffle.join_cli", "run",
           "--workdir", WORK, "--name", name]
    if hook:
        cmd += ["--hook", hook]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT)


def make_join(name: str, left, right, mode: str, **kw) -> None:
    J.create_join(WORK, name, left_root=left[0].root, left_job=left[1],
                  right_root=right[0].root, right_job=right[1], mode=mode,
                  mem_bytes=kw.pop("mem_bytes", MEM),
                  max_open_files=kw.pop("max_open_files", FILES),
                  checkpoint_rows=kw.pop("checkpoint_rows", 3), **kw)


def result_rows(name: str) -> list[dict]:
    with J.JoinResult(WORK, name) as res:
        return list(res.iter())


def gate_j1(layL, layR) -> None:
    banner("J1 规定小例：inner=5 / left=6 / full=8，完整原行与次序精确核对")
    inL, inR = os.path.join(ROOT, "in", "j1L.jsonl"), os.path.join(ROOT, "in", "j1R.jsonl")
    write_input(inL, [("x", "L1"), ("x", "L2"), (None, "L3"), ("", "L4")])
    write_input(inR, [("x", "R1"), ("x", "R2"), (None, "R3"), ("", "R4"), ("z", "R5")])
    build_job(layL, "j1L", [("A", inL)], 4, 13)
    build_job(layR, "j1R", [("B", inR)], 3, 7)
    left = load_source_rows(inL, "A")
    right = load_source_rows(inR, "B")
    for mode, n in (("inner", 5), ("left", 6), ("full", 8)):
        make_join(f"j1-{mode}", (layL, "j1L"), (layR, "j1R"), mode)
        J.run_join(WORK, f"j1-{mode}")
        rows = result_rows(f"j1-{mode}")
        exp = naive_join(left, right, mode)
        check(comparable(rows) == expected_comparable(exp),
              f"{mode}: {len(rows)} 条与朴素参考完全一致（次序+重数+内容）")
        check(len(rows) == n, f"{mode}: 条数 = {n}")
    rows = result_rows("j1-full")
    seq8 = [(r["left"]["value"] if r["left"] else None,
             r["right"]["value"] if r["right"] else None) for r in rows]
    check(seq8 == [("L1", "R1"), ("L1", "R2"), ("L2", "R1"), ("L2", "R2"),
                   ("L3", None), ("L4", "R4"), (None, "R3"), (None, "R5")],
          "full 8 条正是规定序列：L1配R1/R2、L2配R1/R2、L3单边、L4配R4、R3单边、R5单边")
    r0 = rows[0]
    check(set(r0["left"].keys()) == {"split_id", "row_number", "key", "value"},
          "结果左字段为完整原行（split_id/row_number/key/value 四字段齐全）")
    check(rows[4]["right"] is None and rows[6]["left"] is None,
          "未匹配侧整个字段为 null")


def gate_j2(layL, layR) -> None:
    banner("J2 键语义：null 与文本 \\u0000NULL 同桶不误配；空串/Unicode/NUL；null value")
    nul_text = "\u0000NULL"
    check(partition_of(None, 4, 13) == partition_of(nul_text, 4, 13),
          "null 与文本 \"\\u0000NULL\" 在公开分区规则下同桶（前置条件成立）")
    inL = os.path.join(ROOT, "in", "j2L.jsonl")
    inR = os.path.join(ROOT, "in", "j2R.jsonl")
    write_input(inL, [(None, "Ln"), (nul_text, "Lt"), ("", "e1"), ("µnïçødé", "u1"),
                      ("a\x00b", "nul1"), ("nv", None)])
    write_input(inR, [(nul_text, "Rt"), (None, "Rn"), ("", "e2"), ("µnïçødé", "u2"),
                      ("a\x00b", "nul2"), ("nv", "has")])
    build_job(layL, "j2L", [("S", inL)], 4, 13)
    build_job(layR, "j2R", [("S", inR)], 4, 13)
    make_join("j2-full", (layL, "j2L"), (layR, "j2R"), "full")
    J.run_join(WORK, "j2-full")
    rows = result_rows("j2-full")
    exp = naive_join(load_source_rows(inL, "S"), load_source_rows(inR, "S"), "full")
    check(comparable(rows) == expected_comparable(exp), "full 结果与朴素参考一致")
    pairs = [(r["left"]["value"] if r["left"] else None,
              r["right"]["value"] if r["right"] else None) for r in rows]
    check(("Ln", "Rn") not in pairs and ("Ln", None) in pairs and (None, "Rn") in pairs,
          "null 键左右各自单边：null 永不匹配（含 null 对 null）")
    check(("Lt", "Rt") in pairs, "文本 \"\\u0000NULL\" 与同文本精确匹配")
    check(("e1", "e2") in pairs and ("u1", "u2") in pairs and ("nul1", "nul2") in pairs,
          "空串/Unicode/含 NUL 文本按自身值精确匹配")
    nv = [r for r in rows if r["left"] and r["left"]["key"] == "nv"][0]
    check(nv["left"] is not None and nv["left"]["value"] is None and nv["right"] is not None,
          "合法 null value（左字段非 null 而 value 为 null）不与“这一侧没有行”混淆")


def gate_j3(layL, layR) -> None:
    banner("J3 随机化交叉核对：不同分区数/版本、同名 split、重复内容 × 三种模式")
    rng = random.Random(20261117)
    key_pool = [None, "", "x", "k1", "k2", "µé", "\u0000NULL", "a\x00b", "z", "汉"]
    val_pool = [1, "v", None, "dup", "dup", 3.5, "x"]

    def gen(n):
        return [(rng.choice(key_pool), rng.choice(val_pool)) for _ in range(n)]

    inL1 = os.path.join(ROOT, "in", "j3L1.jsonl")
    inL2 = os.path.join(ROOT, "in", "j3L2.jsonl")
    inR1 = os.path.join(ROOT, "in", "j3R1.jsonl")
    inR2 = os.path.join(ROOT, "in", "j3R2.jsonl")
    rowsL1, rowsL2, rowsR1, rowsR2 = gen(37), gen(29), gen(41), gen(31)
    write_input(inL1, rowsL1)
    write_input(inL2, rowsL2)
    write_input(inR1, rowsR1)
    write_input(inR2, rowsR2)
    # 两侧 split 同名（S1/S2），分区数与版本均不同
    build_job(layL, "j3L", [("S1", inL1), ("S2", inL2)], 5, 9)
    build_job(layR, "j3R", [("S1", inR1), ("S2", inR2)], 3, 21)
    left = load_source_rows(inL1, "S1") + load_source_rows(inL2, "S2")
    right = load_source_rows(inR1, "S1") + load_source_rows(inR2, "S2")
    for mode in ("inner", "left", "full"):
        make_join(f"j3-{mode}", (layL, "j3L"), (layR, "j3R"), mode)
        J.run_join(WORK, f"j3-{mode}")
        rows = result_rows(f"j3-{mode}")
        exp = naive_join(left, right, mode)
        check(comparable(rows) == expected_comparable(exp),
              f"{mode}: {len(rows)} 条与朴素参考一致（含重复键/重复内容行）")
    # identity 视图：只存身份字段
    make_join("j3-id", (layL, "j3L"), (layR, "j3R"), "inner",
              result_kind="identity")
    J.run_join(WORK, "j3-id")
    id_rows = result_rows("j3-id")
    exp_inner = naive_join(left, right, "inner")
    check(all(set(r["left"].keys()) == {"split_id", "row_number", "key"}
              for r in id_rows if r["left"]),
          "identity 视图只含身份字段（split_id/row_number/key）")
    id_pairs = [(r["left"]["row_number"], r["right"]["row_number"]) for r in id_rows]
    exp_pairs = [(l["row_number"], r["row_number"]) for l, r in exp_inner]
    check(id_pairs == exp_pairs, "identity 视图的身份与次序和完整结果一致")


def gate_j4(layL, layR) -> None:
    banner("J4 分页边界：多种页宽拼接 == 整次流；越界/零宽/末条/中段迭代")
    rows = result_rows("j1-full")
    n = len(rows)
    with J.JoinResult(WORK, "j1-full") as res:
        check(res.row_count == n, f"row_count = {n}")
        for page in (1, 2, 3, 5, 7, 8, 100):
            got = []
            for start in range(0, n, page):
                got.extend(res.read(start, page))
            check(comparable(got) == comparable(rows), f"页宽 {page} 拼接与整次流一致")
        check(res.read(n, 5) == [] and res.read(n + 10, 3) == [], "起始序号越界返回空")
        check(res.read(0, 0) == [], "零宽读取返回空")
        check(res.read(n - 1, 1) == rows[-1:], "末条单页读取正确")
        check(res.read(2, 3) == rows[2:5], "中段窗口读取正确")
        check(list(res.iter(start=3)) == rows[3:], "中段开始的迭代器与切片一致")
        check(list(res.iter()) == res.read(0, n), "迭代器与整页读取一致")


def gate_j5(layL, layR) -> None:
    banner("J5 空侧：左空/右空/双空 × 三种模式")
    inE = os.path.join(ROOT, "in", "j5empty.jsonl")
    inL = os.path.join(ROOT, "in", "j5L.jsonl")
    inR = os.path.join(ROOT, "in", "j5R.jsonl")
    write_input(inE, [])
    write_input(inL, [("a", 1), (None, 2), ("", 3)])
    write_input(inR, [("a", 9), ("b", 8)])
    build_job(layL, "j5emptyL", [("E", inE)], 4, 13)
    build_job(layR, "j5emptyR", [("E", inE)], 4, 13)
    build_job(layL, "j5L", [("S", inL)], 4, 13)
    build_job(layR, "j5R", [("S", inR)], 4, 13)
    cases = [
        ("emptyL-inner", (layL, "j5emptyL"), (layR, "j5R"), "inner", 0),
        ("emptyL-left", (layL, "j5emptyL"), (layR, "j5R"), "left", 0),
        ("emptyL-full", (layL, "j5emptyL"), (layR, "j5R"), "full", 2),
        ("emptyR-inner", (layL, "j5L"), (layR, "j5emptyR"), "inner", 0),
        ("emptyR-left", (layL, "j5L"), (layR, "j5emptyR"), "left", 3),
        ("emptyR-full", (layL, "j5L"), (layR, "j5emptyR"), "full", 3),
        ("both-empty", (layL, "j5emptyL"), (layR, "j5emptyR"), "full", 0),
    ]
    for name, left, right, mode, n in cases:
        make_join(f"j5-{name}", left, right, mode)
        J.run_join(WORK, f"j5-{name}")
        rows = result_rows(f"j5-{name}")
        check(len(rows) == n, f"{name}/{mode}: {len(rows)} 条（期望 {n}）")
    rows = result_rows("j5-emptyL-full")
    check(all(r["left"] is None for r in rows), "左空 full：全部右行作为未匹配右侧输出")
    rows = result_rows("j5-emptyR-full")
    check(all(r["right"] is None for r in rows), "右空 full：全部左行作为未匹配左侧输出")


def gate_j6(layL) -> None:
    banner("J6 同一 root/job 同时作为左右输入（自关联），左右角色区分")
    inS = os.path.join(ROOT, "in", "j6.jsonl")
    write_input(inS, [("k", "v1"), ("k", "v2"), (None, "n")])
    build_job(layL, "j6job", [("S", inS)], 4, 13)
    make_join("j6-self", (layL, "j6job"), (layL, "j6job"), "full")
    st0 = J.join_status(WORK, "j6-self")
    check(open_join_readers(layL, "j6job") == 2,
          "左右角色各登记一份独立段保护（reader_id 含角色）")
    J.run_join(WORK, "j6-self")
    rows = result_rows("j6-self")
    src = load_source_rows(inS, "S")
    exp = naive_join(src, src, "full")
    check(comparable(rows) == expected_comparable(exp),
          "自关联结果与朴素参考一致（k×k 4 对 + null 行两侧各单边）")
    check(len(rows) == 6, "自关联 full 共 6 条")
    check(J.join_status(WORK, "j6-self")["protections_released"] is True,
          "完成后两份保护均已释放")


def gate_j7(layL, layR) -> None:
    banner("J7 建关联失败：未就绪/已取消/不存在/已销毁；失败不留悬挂保护")
    inR = os.path.join(ROOT, "in", "j7R.jsonl")
    write_input(inR, [("a", 1)])
    build_job(layL, "j7ok", [("S", os.path.join(ROOT, "in", "j2L.jsonl"))], 4, 13)
    coord.create_job(layR, "j7notready", [("S", inR)], 4, 13)  # 不派工：无 winner
    try:
        make_join("j7-fail1", (layL, "j7ok"), (layR, "j7notready"), "inner")
        raise AssertionError("右侧未就绪不应建成")
    except JoinNotReady as exc:
        check(True, f"右侧无 winner：显式失败 {exc}")
    check(open_join_readers(layL, "j7ok") == 0, "失败后左侧没有任何悬挂段保护")
    check(not os.path.exists(os.path.join(WORK, "j7-fail1")), "失败关联不残留工作目录")

    build_job(layR, "j7cancel", [("S", inR)], 4, 13)
    coord.cancel_job(layR, "j7cancel")
    try:
        make_join("j7-fail2", (layL, "j7ok"), (layR, "j7cancel"), "inner")
        raise AssertionError("已取消作业不应建成")
    except JoinNotReady as exc:
        check(True, f"右侧已取消：显式失败 {exc}")
    check(open_join_readers(layL, "j7ok") == 0, "取消场景同样不留悬挂保护")

    try:
        make_join("j7-fail3", (layL, "j7ok"), (layR, "job-not-exist"), "inner")
        raise AssertionError("作业不存在不应建成")
    except JoinNotReady as exc:
        check(True, f"作业不存在：显式失败 {exc}")

    build_job(layR, "j7gone", [("S", inR)], 4, 13)
    coord.cleanup(layR, "j7gone", destroy=True)
    try:
        make_join("j7-fail4", (layL, "j7ok"), (layR, "j7gone"), "inner")
        raise AssertionError("已销毁作业不应建成")
    except JoinNotReady as exc:
        check(True, f"已销毁作业：显式失败 {exc}")
    check(open_join_readers(layL, "j7ok") == 0, "全部失败场景左侧保护计数为 0")

    # 关联库写入阶段失败（故障注入）：两侧保护都必须撤销
    build_job(layR, "j7okR", [("S", inR)], 4, 13)
    orig = J._connect_join
    def _boom(jl):
        raise RuntimeError("注入：关联库写入失败")
    J._connect_join = _boom
    try:
        make_join("j7-fail5", (layL, "j7ok"), (layR, "j7okR"), "inner")
        raise AssertionError("注入失败不应建成")
    except RuntimeError:
        check(True, "关联库写入失败：建关联显式失败")
    finally:
        J._connect_join = orig
    check(open_join_readers(layL, "j7ok") == 0 and open_join_readers(layR, "j7okR") == 0,
          "保护登记后失败：已登记的保护被补偿撤销，两侧均无悬挂引用")


def gate_j8(layL, layR) -> None:
    banner("J8 段保护生命周期：加工中 destroy 被拒；源取消不换输入；完成后释放")
    inL = os.path.join(ROOT, "in", "j8L.jsonl")
    inR = os.path.join(ROOT, "in", "j8R.jsonl")
    write_input(inL, [("a", "l1"), ("b", "l2"), ("c", "l3")])
    write_input(inR, [("a", "r1"), ("b", "r2"), ("d", "r4")])
    build_job(layL, "j8L", [("S", inL)], 4, 13)
    build_job(layR, "j8R", [("S", inR)], 4, 13)
    make_join("j8", (layL, "j8L"), (layR, "j8R"), "full")
    try:
        coord.cleanup(layL, "j8L", destroy=True)
        raise AssertionError("有关联保护时不应允许销毁")
    except ShuffleError as exc:
        check(True, f"关联登记保护后，源作业 destroy 被拒: {exc}")
    coord.cancel_job(layL, "j8L")  # 建关联后源作业被取消：不得换输入/吞行
    J.run_join(WORK, "j8")
    rows = result_rows("j8")
    exp = naive_join(load_source_rows(inL, "S"), load_source_rows(inR, "S"), "full")
    check(comparable(rows) == expected_comparable(exp),
          "源作业取消后关联仍按建关联时固定的 winner 清单完成，结果与参考一致")
    st = J.join_status(WORK, "j8")
    check(st["protections_released"] is True, "结果交付后源引用已释放")
    out = coord.cleanup(layL, "j8L", destroy=True)
    check(out["destroyed"] is True, "保护释放后源作业可销毁")
    rows2 = result_rows("j8")
    check(comparable(rows2) == comparable(rows), "源作业销毁后关联结果仍完整可读")

    # 删除关联：只释放它自己的保护（j8L 已销毁，改用仍存活的 j8R 两侧自关联）
    make_join("j8-del", (layR, "j8R"), (layR, "j8R"), "inner")
    check(open_join_readers(layR, "j8R") == 2, "删除前左右角色各有一份关联保护")
    J.delete_join(WORK, "j8-del")
    check(open_join_readers(layR, "j8R") == 0, "删除关联后它自己的保护被释放")
    check(not os.path.exists(os.path.join(WORK, "j8-del")), "删除关联移除工作目录")
    rows3 = result_rows("j8")
    check(len(rows3) == len(rows), "删除别的关联不影响已完成关联的结果")


def gate_j9(layL, layR) -> None:
    banner("J9 同步关卡：接收后/热点乘积途中/可读前杀进程并恢复（真实子进程）")
    inL = os.path.join(ROOT, "in", "j9L.jsonl")
    inR = os.path.join(ROOT, "in", "j9R.jsonl")
    rowsL = [("hot", f"L{i}") for i in range(30)] + \
            [(f"k{i % 7}", f"x{i}") for i in range(40)] + [(None, "nl")]
    rowsR = [("hot", f"R{i}") for i in range(30)] + \
            [(f"k{i % 7}", f"y{i}") for i in range(50)] + [(None, "nr1"), (None, "nr2")]
    write_input(inL, rowsL)
    write_input(inR, rowsR)
    build_job(layL, "j9L", [("A", inL)], 4, 13)
    build_job(layR, "j9R", [("B", inR)], 3, 7)
    make_join("j9", (layL, "j9L"), (layR, "j9R"), "full", checkpoint_rows=50)

    proc = run_join_cli("j9", hook="kill-after-receive-segment=3")
    check(proc.returncode == 42, f"接收第 3 段后受控终止（退出码 {proc.returncode}）")
    st = J.join_status(WORK, "j9")
    got = st["receive"]["L"]["segments_received"] + st["receive"]["R"]["segments_received"]
    check(got == 3 and st["counters"]["src_segments_read"] == 3,
          f"已持久接收 3 段（状态可查），源段读取计数 = {st['counters']['src_segments_read']}")
    try:
        coord.cleanup(layL, "j9L", destroy=True)
        raise AssertionError("被杀关联的保护仍应有效")
    except ShuffleError:
        check(True, "进程被杀后关联的段保护依然有效（destroy 被拒）")

    proc = run_join_cli("j9", hook="kill-at-emit=500")
    check(proc.returncode == 42, f"热点乘积途中受控终止（退出码 {proc.returncode}）")
    st = J.join_status(WORK, "j9")
    ck = st["checkpoints"]["emit"]
    check(ck["out_rows"] < 500 and ck["left_seq_next"] > 0,
          f"emit 检查点停在乘积途中：out_rows={ck['out_rows']} left_seq_next={ck['left_seq_next']}")

    proc = run_join_cli("j9", hook="kill-before-finalize")
    check(proc.returncode == 42, "结果可读前受控终止")
    try:
        J.JoinResult(WORK, "j9")
        raise AssertionError("未终验不应可读")
    except ResultNotReady as exc:
        check(True, f"结果可读前被杀：读取显式失败（{exc}），已写出前缀不被当作完成")

    proc = run_join_cli("j9")
    check(proc.returncode == 0, "恢复后运行到完成")
    st = J.join_status(WORK, "j9")
    total = st["receive"]["L"]["segments_total"] + st["receive"]["R"]["segments_total"]
    check(st["counters"]["src_segments_read"] == total,
          f"恢复前后源段读取计数合计 {st['counters']['src_segments_read']} == 源段总数 {total}"
          "（已接收范围绝不重读）")
    rows = result_rows("j9")
    exp = naive_join(load_source_rows(inL, "A"), load_source_rows(inR, "B"), "full")
    check(comparable(rows) == expected_comparable(exp),
          f"多次杀进程恢复后结果仍与朴素参考一致（{len(rows)} 条）")


def gate_j10(layL, layR) -> None:
    banner("J10 结果损坏：内容字节翻转/截断 → 读取显式失败")
    rows = result_rows("j1-full")
    result_path = os.path.join(WORK, "j1-full", "out", "result.rows")
    size = os.path.getsize(result_path)
    with open(result_path, "r+b") as fh:
        fh.seek(size // 2)
        b = fh.read(1)
        fh.seek(size // 2)
        fh.write(bytes([b[0] ^ 0xFF]))
    try:
        with J.JoinResult(WORK, "j1-full") as res:
            list(res.iter())
        raise AssertionError("内容损坏不应读出成功流")
    except JoinFailed as exc:
        check(True, f"内容字节翻转：读取显式失败（{exc}）")
    with open(result_path, "r+b") as fh:  # 还原，供后续关卡使用
        fh.seek(size // 2)
        fh.write(b)

    make_join("j10-trunc", (layL, "j1L"), (layR, "j1R"), "inner")
    J.run_join(WORK, "j10-trunc")
    tp = os.path.join(WORK, "j10-trunc", "out", "result.rows")
    with open(tp, "r+b") as fh:
        fh.truncate(os.path.getsize(tp) - 5)
    try:
        J.JoinResult(WORK, "j10-trunc")
        raise AssertionError("截断结果不应可读")
    except JoinFailed as exc:
        check(True, f"结果截断：打开即显式失败（{exc}）")


def gate_j11(layL, layR) -> None:
    banner("J11 单行超过预算：明确报错")
    inL = os.path.join(ROOT, "in", "j11L.jsonl")
    inR = os.path.join(ROOT, "in", "j11R.jsonl")
    write_input(inL, [("big", "x" * 4096)])
    write_input(inR, [("big", "y")])
    build_job(layL, "j11L", [("S", inL)], 4, 13)
    build_job(layR, "j11R", [("S", inR)], 4, 13)
    make_join("j11", (layL, "j11L"), (layR, "j11R"), "inner", mem_bytes=4096)
    try:
        J.run_join(WORK, "j11")
        raise AssertionError("超预算单行不应静默通过")
    except BudgetExceeded as exc:
        check(True, f"单行 4KB 超过单行预算（4096/16=256B）：显式报错")
    st = J.join_status(WORK, "j11")
    check(st["state"] == "running" and "BudgetExceeded" in (st["last_error"] or ""),
          "失败可诊断：状态与 last_error 记录预算失败原因")


def gate_j12(layL, layR) -> None:
    banner("J12 上游分区数/版本改变，关联身份、重数与次序不变")
    inL = os.path.join(ROOT, "in", "j12L.jsonl")
    inR = os.path.join(ROOT, "in", "j12R.jsonl")
    rowsL = [("k", "a"), ("k", "a"), (None, "n"), ("m", "b"), ("", "e")]
    rowsR = [("k", "p"), ("k", "q"), ("m", "r"), ("", "e2")]
    write_input(inL, rowsL)
    write_input(inR, rowsR)
    build_job(layL, "j12L1", [("S", inL)], 4, 13)
    build_job(layR, "j12R1", [("S", inR)], 3, 7)
    build_job(layL, "j12L2", [("S", inL)], 6, 0)
    build_job(layR, "j12R2", [("S", inR)], 2, 20)
    make_join("j12-a", (layL, "j12L1"), (layR, "j12R1"), "full")
    make_join("j12-b", (layL, "j12L2"), (layR, "j12R2"), "full")
    J.run_join(WORK, "j12-a")
    J.run_join(WORK, "j12-b")
    ra, rb = result_rows("j12-a"), result_rows("j12-b")
    check(comparable(ra) == comparable(rb),
          "分区数 4/3 与 6/2、版本 13/7 与 0/20 的结果完全一致")
    exp = naive_join(load_source_rows(inL, "S"), load_source_rows(inR, "S"), "full")
    check(comparable(ra) == expected_comparable(exp),
          "且与朴素参考一致（重复内容行各自参与，重数正确）")


def gate_j13(layL, layR) -> None:
    banner("J13 CLI 端到端：create / run / status / read / delete")
    def cli(*args):
        proc = subprocess.run(
            [sys.executable, "-m", "shuffle.join_cli", *args],
            capture_output=True, text=True, cwd=PROJECT)
        if proc.returncode != 0:
            raise RuntimeError(f"CLI 失败 {args}: {proc.stderr}")
        return json.loads(proc.stdout)

    created = cli("create", "--workdir", WORK, "--name", "j13",
                  "--left-root", layL.root, "--left-job", "j1L",
                  "--right-root", layR.root, "--right-job", "j1R",
                  "--mode", "full", "--mem-bytes", str(MEM),
                  "--max-open-files", str(FILES))
    check(created["state"] == "created", "CLI create：关联建成")
    st = cli("status", "--workdir", WORK, "--name", "j13")
    check(st["receive"]["L"]["segments_total"] >= 1 and st["state"] == "created",
          "CLI status：阶段进度与已持久接收范围可查")
    run = cli("run", "--workdir", WORK, "--name", "j13")
    check(run["state"] == "done" and run["result"]["rows"] == 8,
          "CLI run：运行到完成（8 条结果）")
    page = cli("read", "--workdir", WORK, "--name", "j13", "--start", "5", "--count", "3")
    check(page["count"] == 3 and page["rows"][0]["seq"] == 5
          and page["rows"][0]["left"]["value"] == "L4",
          "CLI read：按起始序号与条数读取结果")
    deleted = cli("delete", "--workdir", WORK, "--name", "j13")
    check(deleted["deleted"] is True and not os.path.exists(os.path.join(WORK, "j13")),
          "CLI delete：关联删除且工作目录移除")


def main() -> None:
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(WORK, exist_ok=True)
    layL = Layout(os.path.join(ROOT, "rootL"))
    layR = Layout(os.path.join(ROOT, "rootR"))
    gate_j1(layL, layR)
    gate_j2(layL, layR)
    gate_j3(layL, layR)
    gate_j4(layL, layR)
    gate_j5(layL, layR)
    gate_j6(layL)
    gate_j7(layL, layR)
    gate_j8(layL, layR)
    gate_j9(layL, layR)
    gate_j10(layL, layR)
    gate_j11(layL, layR)
    gate_j12(layL, layR)
    gate_j13(layL, layR)
    print("\n全部关联功能关卡通过。")


if __name__ == "__main__":
    main()
