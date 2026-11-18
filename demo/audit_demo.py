"""时态引用完整性审计功能关卡：真实上游工人建输入，独立参考逐项对照。

  A1  规格手算：目标 [0,10) 存在、[10,20) 缺失时源引用 [5,15) 只报 [10,15)；
      目标 payload 为 null 的 [15,20) 仍存在；知识版本 2 撤回目标后，针对
      版本 1 的既有审计结果不变；创建时固定 head 的审计在 v2 提交后运行仍
      反映 v1。
  A2  源引用替换（v2 新断言换引用目标）与局部覆盖后撤回（v3 撤回上层，
      下层重新露出）：三个版本各一份审计，全部对照独立参考。
  A3  缺字段/空字符串/null 引用/不存在的目标是四类不同缺失原因；非字符串
      目标键与不支持的 JSON 形态明确报错（审计 failed，不静默跳过）；
      非法字段路径/非法规则在建审计时明确报错。
  A4  目标与源边界重合、负时间、int64 极值、无穷端点（end=null）。
  A5  分页跨页拼接一致；游标跨审计/破坏/改参数均拒绝；跨进程游标续读；
      未完成审计拒绝读取（不返回成功前缀）。
  A6  翻页途中提交新知识版本：游标后续页仍与已固定结果逐条一致。
  A7  进程终止后恢复：扫描途中/物化后/置 done 前三个同步点杀进程，
      恢复后结果与参考一致，已提交 (实体,规则) 对绝不重算。
  A8  审计文件损坏：结果记录字节翻转/索引损坏/文件截断全部显式报错；
      删除后重建可恢复。
  A9  两个审计并行清理：互不影响，不影响历史表与第三个审计。
  A10 CLI 端到端。
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shuffle import coordinator as coord
from shuffle.cli import _spawn_worker
from shuffle.common import Layout
from shuffle import history as H
from shuffle import audit as A
from demo.audit_ref import RefAudit, load_batch_rows

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.path.join(PROJECT, ".run", "audit-demo")
WORK = os.path.join(ROOT, "work")
AROOT = os.path.join(ROOT, "audits")
MEM = 1 << 20
FILES = 24
INT64_MIN = -(1 << 63)


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


def build_job(layout: Layout, job: str, splits: list, num_p: int = 4,
              version: int = 13, mem: int = 65536) -> None:
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


def make_table(name: str) -> None:
    H.create_table(WORK, name, mem_bytes=MEM, max_open_files=FILES)


def ingest(name: str, layout: Layout, job: str, batch_id: str) -> dict:
    H.submit_batch(WORK, name, src_root=layout.root, src_job=job, batch_id=batch_id)
    return H.run_batch(WORK, name, batch_id)


def commit_rows(name: str, layout: Layout, tag: str, rows: list,
                ref: RefAudit, split_name: str = "S") -> int:
    """写输入 → 真实工人建作业 → 入仓；同批行喂给独立参考。返回新 head。"""
    path = os.path.join(ROOT, "in", f"{name}-{tag}.jsonl")
    write_input(path, rows)
    job = f"{name}-{tag}"
    build_job(layout, job, [(split_name, path)])
    rec = ingest(name, layout, job, f"{job}-b")
    ref.add_batch(load_batch_rows([(path, 0)]))
    return int(rec["kversion"])


def read_all(name: str, limit: int = 1000) -> list:
    recs, cur = [], None
    while True:
        page = A.read_audit(AROOT, name, limit=limit, cursor=cur)
        recs.extend(page["records"])
        cur = page["cursor"]
        if not cur:
            break
    return recs


def run_and_check(audit_name: str, ref: RefAudit, rules: list, t0, t1, K,
                  note: str) -> list:
    A.create_audit(AROOT, audit_name, table_workdir=WORK, table_name=TABLE,
                   rules=rules, t0=t0, t1=t1, kversion=K,
                   mem_bytes=MEM, max_open_files=FILES)
    st = A.run_audit(AROOT, audit_name)
    check(st["state"] == "done", f"{note}：审计完成（state=done）")
    got = read_all(audit_name)
    want = ref.run(rules, t0, t1, K)
    check(got == want, f"{note}：{len(got)} 条缺失区间与独立参考一致")
    return got


TABLE = ""   # 当前关卡的历史表名（run_and_check 用）


# --------------------------------------------------------------------------- #
# A1 规格手算 + 版本推进不变性
# --------------------------------------------------------------------------- #
def gate_a1(lay: Layout) -> None:
    global TABLE
    banner("A1 规格手算：[0,10)存在/[10,20)缺失 × 源[5,15)；null payload 仍存在；"
           "v2 撤回后 v1 审计不变；创建时固定 head")
    TABLE = "a1"
    make_table(TABLE)
    ref = RefAudit()
    v1 = commit_rows(TABLE, lay, "v1", [
        ("acct", assertion("T1", 0, 10, "x")),
        ("acct", assertion("T2", 15, 20, None)),          # null payload 仍算存在
        ("cust", assertion("S1", 5, 15, {"owner": "acct"})),
        ("cust", assertion("S2", 15, 25, {"owner": "acct"})),
    ], ref)
    check(v1 == 1, "v1 入仓")
    rules = [{"rule_id": "r-owner",
              "source": {"kind": "entities", "entities": ["cust"]},
              "path": ["owner"]}]

    got = run_and_check("a1v1", ref, rules, 0, 30, 1, "A1 v1 审计")
    check([(r["start"], r["end"], r["event_id"]) for r in got]
          == [(10, 15, "S1"), (20, 25, "S2")]
          and all(r["target"] == "acct" and r["reason"] == "target_absent"
                  and r["kversion"] == 1 and r["entity"] == "cust"
                  and r["rule_id"] == "r-owner" for r in got),
          "源引用 [5,15) 只报告 [10,15)；目标 null payload 的 [15,20) 仍存在"
          f"（S2 只报 [20,25)）: {[(r['start'], r['end']) for r in got]}")

    # 创建时固定 head 的审计（不指定 kversion），先不运行
    A.create_audit(AROOT, "a1pin", table_workdir=WORK, table_name=TABLE,
                   rules=rules, t0=0, t1=30, mem_bytes=MEM, max_open_files=FILES)
    st = A.audit_status(AROOT, "a1pin")
    check(st["kversion"] == 1, "未指定知识版本时创建即固定当前 head=1")

    # v2 撤回目标 T1
    v2 = commit_rows(TABLE, lay, "v2", [("acct", retraction("RT1", "T1"))], ref)
    check(v2 == 2 and H.HistoryTable(WORK, TABLE).head() == 2, "v2 撤回 T1，head=2")

    got1 = read_all("a1v1")
    check(got1 == got, "知识版本 2 撤回目标后，针对版本 1 的既有审计结果不变")

    st = A.run_audit(AROOT, "a1pin")
    check(st["state"] == "done" and st["kversion"] == 1,
          "创建时固定 head 的审计在 v2 提交后运行，仍反映 v1")
    check(read_all("a1pin") == got, "a1pin 结果 == a1v1（固定知识版本 1）")

    got2 = run_and_check("a1v2", ref, rules, 0, 30, 2, "A1 v2 审计")
    check([(r["start"], r["end"], r["event_id"]) for r in got2]
          == [(5, 15, "S1"), (20, 25, "S2")],
          f"v2 撤回 T1 后：[5,15) 整段缺失 + S2 尾部 [20,25) 缺失: "
          f"{[(r['start'], r['end']) for r in got2]}")


# --------------------------------------------------------------------------- #
# A2 源引用替换 + 局部覆盖后撤回
# --------------------------------------------------------------------------- #
def gate_a2(lay: Layout) -> None:
    global TABLE
    banner("A2 源引用替换与局部覆盖后撤回（下层重新露出），三版本对照参考")
    TABLE = "a2"
    make_table(TABLE)
    ref = RefAudit()
    commit_rows(TABLE, lay, "v1", [
        ("cust", assertion("S1", 0, 30, {"owner": "a"})),
        ("a", assertion("A1", 0, 30, "A")),
        ("b", assertion("B1", 10, 15, "B")),
    ], ref)
    commit_rows(TABLE, lay, "v2", [
        ("cust", assertion("S2", 10, 20, {"owner": "b"})),   # 局部覆盖：引用 a→b
    ], ref)
    commit_rows(TABLE, lay, "v3", [
        ("cust", retraction("RS2", "S2")),                   # 撤回上层，S1 重新露出
    ], ref)
    rules = [{"rule_id": "r-owner", "source": {"kind": "prefix", "prefix": "cu"},
              "path": ["owner"]}]

    got1 = run_and_check("a2v1", ref, rules, 0, 30, 1, "A2 v1")
    check(got1 == [], "v1：a 全程存在，空结果（done 且 0 条）")
    page = A.read_audit(AROOT, "a2v1", limit=10)
    check(page["records"] == [] and page["cursor"] is None and page["total"] == 0,
          "空结果读取：成功返回 0 条且无游标")

    got2 = run_and_check("a2v2", ref, rules, 0, 30, 2, "A2 v2")
    check(len(got2) == 1 and got2[0]["start"] == 15 and got2[0]["end"] == 20
          and got2[0]["event_id"] == "S2" and got2[0]["target"] == "b",
          f"v2：源引用在 [10,20) 换成 b，b 只存在 [10,15) → 只报 [15,20): {got2}")

    got3 = run_and_check("a2v3", ref, rules, 0, 30, 3, "A2 v3")
    check(got3 == [], "v3：撤回 S2 后 S1 重新露出，a 全程存在，空结果")


# --------------------------------------------------------------------------- #
#  A3 缺失原因分类 + 明确报错
# --------------------------------------------------------------------------- #
def gate_a3(lay: Layout) -> None:
    global TABLE
    banner("A3 缺字段/空串/null/不存在 = 四类缺失；非字符串目标键与不支持的 "
           "JSON 形态明确报错；非法字段路径建审计时报错")
    TABLE = "a3"
    make_table(TABLE)
    ref = RefAudit()
    commit_rows(TABLE, lay, "v1", [
        ("cust", assertion("S1", 0, 10, {"owner": "ghost"})),   # 目标不存在
        ("cust", assertion("S2", 10, 20, {"other": "x"})),      # 缺字段
        ("cust", assertion("S3", 20, 30, {"owner": None})),     # null 引用
        ("cust", assertion("S4", 30, 40, {"owner": ""})),       # 空字符串
        ("cust", assertion("S5", 40, 50, {"owner": 123})),      # 非字符串目标键
        ("cust", assertion("S6", 50, 60, "str-payload")),       # payload 非对象
        ("cust", assertion("S7", 60, 70, None)),                # payload null
    ], ref)
    rules = [{"rule_id": "r-owner", "source": {"kind": "all"}, "path": ["owner"]}]

    got = run_and_check("a3ok", ref, rules, 0, 40, 1, "A3 四类缺失")
    kinds = [(r["event_id"], r["reason"], r["target"]) for r in got]
    check(kinds == [("S2", "missing_field", None),
                    ("S3", "null_reference", None),
                    ("S4", "empty_target", ""),
                    ("S1", "target_absent", "ghost")],
          "缺字段/空串/null/不存在严格区分（不混为一类）；结果按目标键字节序"
          f"（null 目标最前）排列: {kinds}")

    # 非字符串目标键 → 明确报错（审计 failed），不静默跳过源事实
    A.create_audit(AROOT, "a3bad", table_workdir=WORK, table_name=TABLE,
                   rules=rules, t0=40, t1=70, kversion=1,
                   mem_bytes=MEM, max_open_files=FILES)
    try:
        A.run_audit(AROOT, "a3bad")
        raise AssertionError("非字符串目标键不应静默通过")
    except A.AuditRuleError as exc:
        check("S5" in str(exc) and "r-owner" in str(exc),
              f"非字符串目标键：审计明确失败并指出规则与事件（{exc}）")
    st = A.audit_status(AROOT, "a3bad")
    check(st["state"] == "failed" and "S5" in st["error"],
          "失败状态与原因可查（state=failed）")
    try:
        A.read_audit(AROOT, "a3bad")
        raise AssertionError("失败审计不应可读")
    except A.AuditNotReady:
        check(True, "失败审计拒绝读取（不返回成功前缀）")
    try:
        ref.run(rules, 40, 70, 1)
        raise AssertionError("独立参考也应判定非法")
    except Exception as exc:
        check(type(exc).__name__ == "RefError", "独立参考同样明确报错（链路一致）")

    # payload 非对象（不支持的 JSON 形态）→ 明确报错
    A.create_audit(AROOT, "a3shape", table_workdir=WORK, table_name=TABLE,
                   rules=rules, t0=50, t1=70, kversion=1,
                   mem_bytes=MEM, max_open_files=FILES)
    try:
        A.run_audit(AROOT, "a3shape")
        raise AssertionError("payload 非对象不应静默通过")
    except A.AuditRuleError as exc:
        check("S6" in str(exc), f"payload 非对象：明确报错（{exc}）")

    # 非法字段路径/非法规则：建审计时明确报错
    bad_rules = [
        [{"rule_id": "r", "source": {"kind": "all"}, "path": []}],
        [{"rule_id": "r", "source": {"kind": "all"}, "path": "owner"}],
        [{"rule_id": "r", "source": {"kind": "all"}, "path": ["a", 3]}],
        [{"rule_id": "r", "source": {"kind": "all"}, "path": [""]}],
        [{"rule_id": "r", "path": ["a"]}],
        [{"rule_id": "r", "source": {"kind": "bogus"}, "path": ["a"]}],
        [{"rule_id": "r", "source": {"kind": "all"}, "path": ["a"]},
         {"rule_id": "r", "source": {"kind": "all"}, "path": ["b"]}],
        [{"rule_id": "r", "source": {"kind": "entities", "entities": ["x", None]},
          "path": ["a"]}],
    ]
    for i, br in enumerate(bad_rules):
        try:
            A.create_audit(AROOT, f"a3ill{i}", table_workdir=WORK, table_name=TABLE,
                           rules=br, t0=0, t1=10, mem_bytes=MEM, max_open_files=FILES)
            raise AssertionError(f"非法规则 {i} 不应建成")
        except A.AuditRuleError:
            pass
    check(True, "空路径/非数组路径/非字符串元素/空元素/缺 source/非法范围/"
                "重复 rule_id/null 实体：建审计时全部明确报错")


# --------------------------------------------------------------------------- #
# A4 边界重合 + 负时间 + int64 极值 + 无穷端点
# --------------------------------------------------------------------------- #
def gate_a4(lay: Layout) -> None:
    global TABLE
    banner("A4 目标与源边界重合、负时间、int64 极值、无穷端点")
    TABLE = "a4"
    make_table(TABLE)
    ref = RefAudit()
    commit_rows(TABLE, lay, "v1", [
        ("cust", assertion("S1", 0, 10, {"owner": "a"})),
        ("a", assertion("A1", 0, 10, "A")),                 # 与源区间精确重合
        ("cust", assertion("S2", 10, 20, {"owner": "b"})),
        ("b", assertion("B1", 20, 30, "B")),                # 源结束时目标才出现
        ("cust", assertion("S3", 100, None, {"owner": "c"})),
        ("c", assertion("C1", 0, 50, "C")),                 # 源尾部到正无穷缺失
        ("cust", assertion("S4", -50, 0, {"owner": "d"})),
        ("d", assertion("D1", 0, None, "D")),               # 负时段缺失
        ("cust", assertion("S5", INT64_MIN, INT64_MIN + 10, {"owner": "e"})),
    ], ref)
    rules = [{"rule_id": "r-owner", "source": {"kind": "prefix", "prefix": "cu"},
              "path": ["owner"]}]

    got = run_and_check("a4", ref, rules, INT64_MIN, None, 1, "A4 全轴审计")
    got4 = [(r["event_id"], r["start"], r["end"]) for r in got]
    check(got4 == [("S2", 10, 20),
                   ("S3", 100, None),
                   ("S4", -50, 0),
                   ("S5", INT64_MIN, INT64_MIN + 10)],
          "边界重合不误报；负时间/int64 极值/无穷端点（end=null）精确；"
          f"结果按目标键字节序排列: {got4}")


# --------------------------------------------------------------------------- #
# A5 分页跨页 + 游标 + 未完成拒读
# --------------------------------------------------------------------------- #
def gate_a5(lay: Layout) -> None:
    global TABLE
    banner("A5 分页跨页拼接一致；游标跨审计/破坏/改参数拒绝；跨进程续读；未完成拒读")
    TABLE = "a5"
    make_table(TABLE)
    ref = RefAudit()
    rows = [("e%02d" % i, assertion("E%02d" % i, 0, 10, {"owner": "ghost-%02d" % i}))
            for i in range(45)]
    commit_rows(TABLE, lay, "v1", rows, ref)
    rules = [{"rule_id": "r-owner", "source": {"kind": "all"}, "path": ["owner"]}]
    full = run_and_check("a5", ref, rules, 0, 10, 1, "A5 45 条缺失")
    check(len(full) == 45, "45 个源实体各报 1 条")

    for page_w in (1, 7, 1000):
        got = read_all("a5", limit=page_w)
        check(got == full, f"页宽 {page_w} 拼接 == 整次读取")

    # 游标误用
    A.create_audit(AROOT, "a5other", table_workdir=WORK, table_name=TABLE,
                   rules=rules, t0=0, t1=10, kversion=1,
                   mem_bytes=MEM, max_open_files=FILES)
    A.run_audit(AROOT, "a5other")
    cur1 = A.read_audit(AROOT, "a5", limit=3)["cursor"]
    try:
        A.read_audit(AROOT, "a5other", cursor=cur1)
        raise AssertionError("跨审计游标不应接受")
    except A.AuditCursorError:
        check(True, "游标用于另一个审计：显式拒绝")
    bad = cur1[:-3] + ("AAA" if cur1[-3:] != "AAA" else "BBB")
    try:
        A.read_audit(AROOT, "a5", cursor=bad)
        raise AssertionError("破坏的游标不应接受")
    except A.AuditCursorError:
        check(True, "破坏游标内容：显式拒绝")
    obj = json.loads(cur1)
    obj["kv"] = 99
    forged = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    try:
        A.read_audit(AROOT, "a5", cursor=forged)
        raise AssertionError("改参数的游标不应接受")
    except A.AuditCursorError:
        check(True, "改换绑定参数的游标：显式拒绝（完整性校验失败）")
    # 历史表游标不能用于审计，审计游标不能用于历史表
    t = H.HistoryTable(WORK, TABLE)
    hcur = t.export(kversion=1, limit=1)["cursor"]
    check(hcur is not None, "历史表导出游标已生成")
    try:
        A.read_audit(AROOT, "a5", cursor=hcur)
        raise AssertionError("历史表游标不应被审计接受")
    except A.AuditCursorError:
        check(True, "历史表游标用于审计：显式拒绝")
    try:
        t.timeline(cursor=cur1, limit=1)
        raise AssertionError("审计游标不应被历史表接受")
    except H.CursorError:
        check(True, "审计游标用于历史表：显式拒绝")

    # 跨进程游标续读（CLI）
    proc = subprocess.run(
        [sys.executable, "-m", "shuffle.audit_cli", "read",
         "--root", AROOT, "--name", "a5", "--limit", "10"],
        capture_output=True, text=True, cwd=PROJECT)
    page1 = json.loads(proc.stdout)
    proc = subprocess.run(
        [sys.executable, "-m", "shuffle.audit_cli", "read",
         "--root", AROOT, "--name", "a5", "--limit", "100",
         "--cursor", page1["cursor"]],
        capture_output=True, text=True, cwd=PROJECT)
    page2 = json.loads(proc.stdout)
    check(page1["records"] + page2["records"] == full,
          "进程退出再打开：游标续读拼接完整")

    # 未完成审计拒绝读取
    A.create_audit(AROOT, "a5pend", table_workdir=WORK, table_name=TABLE,
                   rules=rules, t0=0, t1=10, kversion=1,
                   mem_bytes=MEM, max_open_files=FILES)
    try:
        A.read_audit(AROOT, "a5pend")
        raise AssertionError("未完成审计不应可读")
    except A.AuditNotReady:
        check(True, "结果未完成：拒绝读取（不返回成功前缀）")


# --------------------------------------------------------------------------- #
# A6 翻页途中提交新知识版本
# --------------------------------------------------------------------------- #
def gate_a6(lay: Layout) -> None:
    global TABLE
    banner("A6 翻页途中提交新知识版本：游标后续页仍与已固定结果逐条一致")
    TABLE = "a1"   # 复用 A1 的表（head=2）
    page1 = A.read_audit(AROOT, "a1v2", limit=1)
    check(page1["cursor"] is not None and len(page1["records"]) == 1,
          "a1v2 第 1 页 1 条，游标继续")
    # 提交 v3（空批次）推进 head
    path = os.path.join(ROOT, "in", "a1-v3.jsonl")
    write_input(path, [])
    build_job(lay, "a1-v3", [("S", path)])
    rec = ingest(TABLE, lay, "a1-v3", "a1-v3-b")
    check(rec["kversion"] == 3 and H.HistoryTable(WORK, TABLE).head() == 3,
          "翻页途中提交 v3（空批次），head=3")
    page2 = A.read_audit(AROOT, "a1v2", limit=100, cursor=page1["cursor"])
    full = read_all("a1v2")
    check(page1["records"] + page2["records"] == full,
          "head 推进后，游标后续页仍与已固定（v2）结果逐条一致")
    check(full[0]["kversion"] == 2, "结果记录携带固定知识版本 2")


# --------------------------------------------------------------------------- #
# A7 进程终止后恢复
# --------------------------------------------------------------------------- #
def gate_a7(lay: Layout) -> None:
    global TABLE
    banner("A7 同步点杀进程：扫描途中/物化后/置 done 前；恢复不重算已提交对")
    TABLE = "a7"
    make_table(TABLE)
    ref = RefAudit()
    rows = [("e%02d" % i,
             assertion("E%02d" % i, 0, 10,
                       {"owner": "g%02d" % i, "other": "h%02d" % i}))
            for i in range(12)]
    commit_rows(TABLE, lay, "v1", rows, ref)
    rules = [{"rule_id": "r-owner", "source": {"kind": "all"}, "path": ["owner"]},
             {"rule_id": "r-other", "source": {"kind": "all"}, "path": ["other"]}]
    A.create_audit(AROOT, "a7", table_workdir=WORK, table_name=TABLE,
                   rules=rules, t0=0, t1=10, kversion=1,
                   mem_bytes=MEM, max_open_files=FILES)

    def cli_run(hook: str | None) -> subprocess.CompletedProcess:
        cmd = [sys.executable, "-m", "shuffle.audit_cli", "run",
               "--root", AROOT, "--name", "a7"]
        if hook:
            cmd += ["--hook", hook]
        return subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT)

    proc = cli_run("kill-after-audit-pair=5")
    check(proc.returncode == 42, f"第 5 个 (实体,规则) 对后受控终止（rc={proc.returncode}）")
    st = A.audit_status(AROOT, "a7")
    check(st["state"] == "scanning" and st["progress"]["pairs_done"] == 5
          and st["counters"]["findings_staged"] == 5,
          f"已提交 5 对（状态/进度/暂存计数可查）: pairs={st['progress']['pairs_done']}")

    proc = cli_run("kill-after-materialize")
    check(proc.returncode == 42, "物化完成后受控终止")
    st = A.audit_status(AROOT, "a7")
    check(st["state"] == "finalizing", "物化检查点已落，待终验")
    try:
        A.read_audit(AROOT, "a7")
        raise AssertionError("终验未完成不应可读")
    except A.AuditNotReady:
        check(True, "物化后未终验：拒绝读取（不返回成功前缀）")

    proc = cli_run("kill-before-done")
    check(proc.returncode == 42, "终验通过后置 done 前受控终止")
    check(A.audit_status(AROOT, "a7")["state"] == "finalizing",
          "未落 done：状态仍是 finalizing")

    proc = cli_run(None)
    check(proc.returncode == 0, "恢复运行完成")
    st = A.audit_status(AROOT, "a7")
    c = st["counters"]
    check(st["state"] == "done" and c["source_pairs"] == 24
          and c["source_segments"] == 24 and c["target_queries"] == 24
          and c["findings_staged"] == 24 and c["results_emitted"] == 24
          and c["src_file_opens"] == 0,
          f"恢复后 24 对恰好各算一次（已提交对绝不重算），源段打开 0 次: {c}")
    got = read_all("a7")
    want = ref.run(rules, 0, 10, 1)
    check(got == want, "杀进程恢复后的结果与独立参考逐条一致")


# --------------------------------------------------------------------------- #
# A8 审计文件损坏
# --------------------------------------------------------------------------- #
def gate_a8(lay: Layout) -> None:
    global TABLE
    banner("A8 审计文件损坏：记录字节翻转/索引损坏/文件截断全部显式报错")
    TABLE = "a8"
    make_table(TABLE)
    ref = RefAudit()
    rows = [("e%d" % i, assertion("E%d" % i, 0, 10, {"owner": "ghost-%d" % i}))
            for i in range(3)]
    commit_rows(TABLE, lay, "v1", rows, ref)
    rules = [{"rule_id": "r-owner", "source": {"kind": "all"}, "path": ["owner"]}]

    def fresh() -> None:
        A.create_audit(AROOT, "a8", table_workdir=WORK, table_name=TABLE,
                       rules=rules, t0=0, t1=10, kversion=1,
                       mem_bytes=MEM, max_open_files=FILES)
        A.run_audit(AROOT, "a8")

    fresh()
    results = os.path.join(AROOT, "a8", "results.frm")
    with open(results, "rb") as fh:
        data = fh.read()
    off = data.find(b"ghost-1")
    check(off > 0, "在结果文件中定位记录内容字节")
    with open(results, "r+b") as fh:   # 记录内容字节翻转
        fh.seek(off + 3)
        b = fh.read(1)
        fh.seek(off + 3)
        fh.write(bytes([b[0] ^ 0xFF]))
    try:
        read_all("a8")
        raise AssertionError("记录损坏不应读出成功结果")
    except A.AuditCorrupt as exc:
        check(True, f"结果记录字节翻转：读取显式 AuditCorrupt（{exc}）")

    out = A.delete_audit(AROOT, "a8")
    check(out["deleted"], "损坏的审计可删除（只移除自己的目录）")
    fresh()
    check(len(read_all("a8")) == 3, "删除后重建重跑：结果恢复可读")

    idx = os.path.join(AROOT, "a8", "results.idx")
    with open(idx, "r+b") as fh:   # 索引内容字节翻转
        fh.seek(40)
        b = fh.read(1)
        fh.seek(40)
        fh.write(bytes([b[0] ^ 0xFF]))
    page1 = A.read_audit(AROOT, "a8", limit=1)
    check(len(page1["records"]) == 1, "第 1 页（pos=0，不查索引）仍可读")
    try:
        A.read_audit(AROOT, "a8", limit=10, cursor=page1["cursor"])
        raise AssertionError("索引损坏不应读出成功结果")
    except A.AuditCorrupt:
        check(True, "结果索引损坏：续页读取显式 AuditCorrupt")

    out = A.delete_audit(AROOT, "a8")
    fresh()
    results = os.path.join(AROOT, "a8", "results.frm")
    with open(results, "r+b") as fh:   # 截断（footer 丢失）
        fh.truncate(os.path.getsize(results) - 10)
    try:
        read_all("a8")
        raise AssertionError("截断的结果不应读出成功结果")
    except A.AuditCorrupt:
        check(True, "结果文件截断（footer 丢失）：读取显式 AuditCorrupt")
    check(H.HistoryTable(WORK, TABLE).head() == 1,
          "审计文件损坏不影响历史表（head 不变，查询正常）")


# --------------------------------------------------------------------------- #
# A9 两个审计并行清理
# --------------------------------------------------------------------------- #
def gate_a9(lay: Layout) -> None:
    global TABLE
    banner("A9 两个审计并行清理：互不影响，不影响历史表与第三个审计")
    TABLE = "a9"
    make_table(TABLE)
    ref = RefAudit()
    commit_rows(TABLE, lay, "v1", [
        ("x", assertion("X1", 0, 10, {"owner": "ghost"}))], ref)
    rules = [{"rule_id": "r-owner", "source": {"kind": "all"}, "path": ["owner"]}]
    for nm in ("a9a", "a9b", "a9c"):
        A.create_audit(AROOT, nm, table_workdir=WORK, table_name=TABLE,
                       rules=rules, t0=0, t1=10, kversion=1,
                       mem_bytes=MEM, max_open_files=FILES)
        A.run_audit(AROOT, nm)
    procs = [subprocess.Popen(
        [sys.executable, "-m", "shuffle.audit_cli", "delete",
         "--root", AROOT, "--name", nm],
        cwd=PROJECT, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for nm in ("a9a", "a9b")]
    rcs = [p.wait() for p in procs]
    check(all(rc == 0 for rc in rcs), "两个审计并行删除：都成功")
    check(not os.path.exists(os.path.join(AROOT, "a9a"))
          and not os.path.exists(os.path.join(AROOT, "a9b"))
          and os.path.exists(os.path.join(AROOT, "a9c")),
          "只移除各自目录，第三个审计完好")
    got = read_all("a9c")
    check(len(got) == 1 and got[0]["event_id"] == "X1",
          "第三个审计结果不受并行清理影响")
    t = H.HistoryTable(WORK, TABLE)
    check(t.point("x", 5, kversion=1)["event_id"] == "X1",
          "历史表不受审计删除影响（查询正常）")
    proc = subprocess.run([sys.executable, "-m", "shuffle.audit_cli", "delete",
                           "--root", AROOT, "--name", "a9a"],
                          capture_output=True, text=True, cwd=PROJECT)
    check(proc.returncode == 2, "重复删除不存在的审计：显式失败")


# --------------------------------------------------------------------------- #
# A10 CLI 端到端
# --------------------------------------------------------------------------- #
def gate_a10(lay: Layout) -> None:
    global TABLE
    banner("A10 CLI 端到端：create/run/status/read/delete")
    TABLE = "a10"
    make_table(TABLE)
    path = os.path.join(ROOT, "in", "a10-v1.jsonl")
    write_input(path, [("c1", assertion("C1", 0, 10, {"owner": "ghost"}))])
    build_job(lay, "a10j", [("S", path)])
    ingest(TABLE, lay, "a10j", "a10-b1")

    def cli(*args, ok=True):
        proc = subprocess.run([sys.executable, "-m", "shuffle.audit_cli", *args],
                              capture_output=True, text=True, cwd=PROJECT)
        if ok and proc.returncode != 0:
            raise RuntimeError(f"CLI 失败 {args}: {proc.stderr}")
        return proc

    rules_path = os.path.join(ROOT, "in", "a10-rules.json")
    with open(rules_path, "w", encoding="utf-8") as fh:
        json.dump({"rules": [{"rule_id": "r-owner",
                              "source": {"kind": "all"}, "path": ["owner"]}]}, fh)
    out = json.loads(cli("create", "--root", AROOT, "--name", "a10c",
                         "--table-workdir", WORK, "--table", TABLE,
                         "--t0", "0", "--t1", "10", "--kversion", "1",
                         "--rules-file", rules_path,
                         "--mem-bytes", str(MEM), "--max-open-files", str(FILES)).stdout)
    check(out["state"] == "created" and out["kversion"] == 1 and out["audit_token"],
          "CLI create：规格与版本已固定")
    out = json.loads(cli("run", "--root", AROOT, "--name", "a10c").stdout)
    check(out["state"] == "done" and out["result"]["rows"] == 1, "CLI run：审计完成")
    out = json.loads(cli("status", "--root", AROOT, "--name", "a10c").stdout)
    check(out["counters"]["source_segments"] == 1
          and out["counters"]["target_queries"] == 1
          and out["counters"]["src_file_opens"] == 0,
          "CLI status：真实读取量计数器可查")
    out = json.loads(cli("read", "--root", AROOT, "--name", "a10c",
                         "--limit", "10").stdout)
    check(len(out["records"]) == 1 and out["records"][0]["event_id"] == "C1"
          and out["records"][0]["reason"] == "target_absent"
          and out["records"][0]["kversion"] == 1,
          "CLI read：缺失记录含源事件/原因/固定知识版本")
    out = json.loads(cli("delete", "--root", AROOT, "--name", "a10c").stdout)
    check(out["deleted"] and not os.path.exists(os.path.join(AROOT, "a10c")),
          "CLI delete：只移除审计自己的目录")
    proc = cli("status", "--root", AROOT, "--name", "a10c", ok=False)
    check(proc.returncode == 2, "删除后 status 显式失败")


def main() -> None:
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(WORK, exist_ok=True)
    os.makedirs(AROOT, exist_ok=True)
    lay = Layout(os.path.join(ROOT, "root"))
    gate_a1(lay)
    gate_a2(lay)
    gate_a3(lay)
    gate_a4(lay)
    gate_a5(lay)
    gate_a6(lay)
    gate_a7(lay)
    gate_a8(lay)
    gate_a9(lay)
    gate_a10(lay)
    print("\n全部时态引用审计功能关卡通过。")


if __name__ == "__main__":
    main()
