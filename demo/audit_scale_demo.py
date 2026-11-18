"""时态引用审计规模关卡：运行时生成大量实体与交错长历史，分别超过工作内存。

  AS1  规模审计：5,000 源实体 + 2,500 目标实体 + 1 个热点源实体（3,000 事件），
       6 批交错长历史（覆盖/撤回/无穷终点），审计预算 1MiB（结果物化走外排）。
       短业务区间 [-30000, 30000)（事件轴 ±1,000,000）：验证目标历史读取被
       持久桶索引收窄（实际检查的目标事件 ≪ 目标事件总数）。
       报告：实际扫描区间数、目标查询/源打开次数、缺失输出数、RSS、临时
       磁盘与文件句柄峰值；独立参考抽样核对（逐实体重放原始输入）。

所有数据落在 .run/audit-scale 下，不进 Git；重复运行先清空。
"""

from __future__ import annotations

import json
import os
import random
import shutil
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shuffle import coordinator as coord
from shuffle.cli import _spawn_worker
from shuffle.common import Layout
from shuffle import history as H
from shuffle import audit as A
from demo.demo_data import directory_size
from demo.audit_ref import RefAudit

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.path.join(PROJECT, ".run", "audit-scale")
WORK = os.path.join(ROOT, "work")
AROOT = os.path.join(ROOT, "audits")
IN = os.path.join(ROOT, "in")
TABLE = "as1"
AUDIT = "as1"
N_SRC = 5000
N_TGT = 2500
N_BATCHES = 6
HOT = "s-hot"
HOT_PER_BATCH = 500
T0, T1 = -30000, 30000
RULES = [{"rule_id": "r-ref", "source": {"kind": "prefix", "prefix": "s"},
          "path": ["ref"]}]


def banner(t: str) -> None:
    print(f"\n{'=' * 72}\n{t}\n{'=' * 72}")


def check(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def rss_kb(pid: int) -> int:
    try:
        with open(f"/proc/{pid}/status", "r", encoding="ascii") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except FileNotFoundError:
        return 0
    return 0


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
        out, err = proc.communicate(timeout=300)
        if proc.returncode != 0:
            raise RuntimeError(f"工人 {job}/{sid} 失败: {err.decode()}")
        coord.claim_winner(layout, job, sid, info["attempt_no"])


def assertion(eid, vf, vt, payload):
    return {"event_id": eid, "op": "assert", "valid_from": vf,
            "valid_to": vt, "payload": payload}


def retraction(eid, target):
    return {"event_id": eid, "op": "retract", "target_event_id": target}


class Meter:
    """子进程计量：峰值 RSS、目录磁盘峰值、耗时。"""

    def __init__(self, name: str, watch_dir: str) -> None:
        self.name = name
        self.watch_dir = watch_dir
        self.peak_rss = 0
        self.peak_disk = 0
        self.seconds = 0.0

    def run(self, cmd: list, env: dict | None = None) -> dict:
        t0 = time.time()
        proc = subprocess.Popen(cmd, cwd=PROJECT, env=env,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        while proc.poll() is None:
            self.peak_rss = max(self.peak_rss, rss_kb(proc.pid))
            if os.path.isdir(self.watch_dir):
                self.peak_disk = max(self.peak_disk, directory_size(self.watch_dir))
            time.sleep(0.02)
        self.peak_rss = max(self.peak_rss, rss_kb(proc.pid))
        if os.path.isdir(self.watch_dir):
            self.peak_disk = max(self.peak_disk, directory_size(self.watch_dir))
        out, err = proc.communicate()
        self.seconds = round(time.time() - t0, 2)
        if proc.returncode != 0:
            raise RuntimeError(
                f"{self.name} 失败 rc={proc.returncode}: {err.decode()[-400:]}")
        text = out.decode("utf-8", "replace").strip()
        try:
            return json.loads(text)
        except ValueError:
            pass
        result = None
        for line in reversed(text.splitlines()):
            line = line.strip()
            if line.startswith("{"):
                result = json.loads(line)
                break
        return result or {}


def interpreter_baseline_kb() -> int:
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(0.4)"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    peak = 0
    while proc.poll() is None:
        peak = max(peak, rss_kb(proc.pid))
        time.sleep(0.02)
    proc.wait()
    return peak


# --------------------------------------------------------------------------- #
# 数据生成：6 批交错长历史
# --------------------------------------------------------------------------- #
def generate(lay: Layout) -> dict:
    rng = random.Random(20261118)
    alive: dict[str, list[str]] = {}          # 更早批次已提交、尚未撤回
    stats = {"asserts": 0, "retracts": 0, "target_asserts": 0,
             "source_asserts": 0, "hot_asserts": 0}
    jobs_meta = []
    for b in range(1, N_BATCHES + 1):
        rows = []
        new_alive: dict[str, list[str]] = {}
        # 源实体：~55% 每批 1-2 条断言，60% 落在稠密区 ±40000
        for i in range(N_SRC):
            if rng.random() >= 0.55:
                continue
            ent = "s%06d" % i
            for k in range(1 + (1 if rng.random() < 0.5 else 0)):
                if rng.random() < 0.6:
                    center = rng.randint(-40000, 40000)
                else:
                    center = rng.randint(-1_000_000, 1_000_000)
                span = None if rng.random() < 0.02 else rng.randint(200, 5000)
                if rng.random() < 0.08:
                    tgt = "ghost-%06d" % rng.randint(0, 999)   # 永不存在的目标
                else:
                    tgt = "t%06d" % ((i * 7 + b * 13 + rng.randint(0, 3)) % N_TGT)
                eid = f"as1-s-{b}-{i}-{k}"
                rows.append((ent, assertion(
                    eid, center, None if span is None else center + span,
                    {"ref": tgt})))
                new_alive.setdefault(ent, []).append(eid)
                stats["asserts"] += 1
                stats["source_asserts"] += 1
        # 热点源实体：密集量化端点，深层堆叠
        for j in range(HOT_PER_BATCH):
            center = (rng.randint(-50000, 50000) // 50) * 50
            span = rng.choice((50, 100, 250, 500, 1000, 2000))
            eid = f"as1-h-{b}-{j}"
            rows.append((HOT, assertion(eid, center, center + span,
                                        {"ref": "t%06d" % rng.randint(0, N_TGT - 1)})))
            new_alive.setdefault(HOT, []).append(eid)
            stats["asserts"] += 1
            stats["hot_asserts"] += 1
        # 目标实体：~40% 每批 1 条断言（存在区间稀疏）
        for i in range(N_TGT):
            if rng.random() >= 0.40:
                continue
            ent = "t%06d" % i
            center = rng.randint(-1_000_000, 1_000_000)
            span = rng.randint(500, 8000)
            eid = f"as1-t-{b}-{i}"
            rows.append((ent, assertion(eid, center, center + span, {"t": b})))
            new_alive.setdefault(ent, []).append(eid)
            stats["asserts"] += 1
            stats["target_asserts"] += 1
        # 覆盖后再撤回：只撤回更早批次的断言
        if b >= 4:
            cands = [(ent, eid) for ent, ids in alive.items() for eid in ids]
            for ent, eid in rng.sample(cands, min(30, len(cands))):
                rows.append((ent, retraction(f"as1-r-{b}-{eid}", eid)))
                alive[ent].remove(eid)
                stats["retracts"] += 1
        for ent, ids in new_alive.items():
            alive.setdefault(ent, []).extend(ids)
        path = os.path.join(IN, TABLE, f"b{b:02d}.jsonl")
        write_input(path, rows)
        build_job(lay, f"as1j{b}", [("S", path)])
        jobs_meta.append({"job": f"as1j{b}", "batch_id": f"as1-b{b}"})
    meta_file = os.path.join(ROOT, "as1-jobs.json")
    with open(meta_file, "w", encoding="utf-8") as fh:
        json.dump(jobs_meta, fh)
    stats["jobs_meta"] = meta_file
    return stats


# --------------------------------------------------------------------------- #
# 独立抽样核对：逐实体重放原始输入（带原 row_number），端点枚举参考
# --------------------------------------------------------------------------- #
def ref_check_sample(sample_ents: list[str], K: int) -> int:
    checked = 0
    for ent in sample_ents:
        # 该实体引用过的全部目标（从原始输入解析）
        targets: set[str] = set()
        for b in range(1, N_BATCHES + 1):
            path = os.path.join(IN, TABLE, f"b{b:02d}.jsonl")
            with open(path, "r", encoding="utf-8") as fh:
                for line in fh:
                    obj = json.loads(line)
                    if obj["key"] == ent and obj["value"].get("op") == "assert":
                        targets.add(obj["value"]["payload"]["ref"])
        # 重放该实体与其全部目标的事件（原 row_number 决定批内优先级）
        mini = RefAudit()
        for b in range(1, N_BATCHES + 1):
            path = os.path.join(IN, TABLE, f"b{b:02d}.jsonl")
            rows = []
            with open(path, "r", encoding="utf-8") as fh:
                for rn, line in enumerate(fh, start=1):
                    obj = json.loads(line)
                    if obj["key"] == ent or obj["key"] in targets:
                        rows.append((obj["key"], obj["value"], 0, rn))
            mini.add_batch(rows)
        want = [f for f in mini.run(RULES, T0, T1, K) if f["entity"] == ent]
        # 产品结果：流式分页读取，只留该实体的记录（结果按实体字节序）；
        # seq 为全局序号，逐实体参考从 0 编号——比对内容时去掉 seq
        got: list[dict] = []
        cur = None
        done = False
        while not done:
            page = A.read_audit(AROOT, AUDIT, limit=2000, cursor=cur)
            for rec in page["records"]:
                if rec["entity"] == ent:
                    got.append(rec)
                elif rec["entity"].encode("utf-8") > ent.encode("utf-8"):
                    done = True
                    break
            if done or not page["cursor"]:
                break
            cur = page["cursor"]
        got_c = [{k: v for k, v in r.items() if k != "seq"} for r in got]
        want_c = [{k: v for k, v in r.items() if k != "seq"} for r in want]
        if got_c != want_c:
            raise AssertionError(
                f"实体 {ent} 审计结果与独立参考不符:\n产品 {got[:4]}\n参考 {want[:4]}")
        checked += 1
    return checked


def main() -> None:
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(WORK, exist_ok=True)
    lay = Layout(os.path.join(ROOT, "root"))
    t_start = time.time()
    baseline = interpreter_baseline_kb()
    print(f"解释器固定开销（空载子进程 RSS）：{baseline} KiB（单独报告，不计入预算）")

    banner("AS1 数据生成：5,000 源实体 + 2,500 目标实体 + 热点实体 × 6 批")
    stats = generate(lay)
    input_bytes = directory_size(os.path.join(IN, TABLE))
    print(f"  事件 {stats['asserts']:,} 条断言（源 {stats['source_asserts']:,} + "
          f"热点 {stats['hot_asserts']:,} + 目标 {stats['target_asserts']:,}）"
          f"+ {stats['retracts']} 条撤回；输入 {input_bytes:,} 字节")

    banner("AS1 入仓（历史表预算 4MiB）")
    H.create_table(WORK, TABLE, mem_bytes=4 * 1024 * 1024, max_open_files=24)
    for item in json.load(open(stats["jobs_meta"])):
        H.submit_batch(WORK, TABLE, src_root=lay.root, src_job=item["job"],
                       batch_id=item["batch_id"])
        H.run_batch(WORK, TABLE, item["batch_id"])
    st = H.table_status(WORK, TABLE)
    check(st["head"] == N_BATCHES, f"全部 {N_BATCHES} 批入仓（head={st['head']}）")
    hist_events_total = st["events_total"]
    print(f"  入仓事件 {st['events_total']:,} 条，桶索引 {st['buckets_total']:,} 行")

    banner("AS1 审计（预算 1MiB，短业务区间 [-30000, 30000)）")
    A.create_audit(AROOT, AUDIT, table_workdir=WORK, table_name=TABLE,
                   rules=RULES, t0=T0, t1=T1, kversion=N_BATCHES,
                   mem_bytes=1 * 1024 * 1024, max_open_files=24)
    meter = Meter("as1-audit", os.path.join(AROOT, AUDIT))
    meter.run([sys.executable, "-m", "shuffle.audit_cli", "run",
               "--root", AROOT, "--name", AUDIT], env=dict(os.environ))
    st = A.audit_status(AROOT, AUDIT)
    c = st["counters"]
    check(st["state"] == "done", "审计完成")
    check(c["src_file_opens"] == 0, "源段打开次数 = 0（只读已提交历史）")
    check(c["target_queries"] == c["source_segments"],
          "每段源获胜区间恰好一次目标时间线查询")
    check(c["tgt_events_examined"] < stats["target_asserts"],
          f"短区间目标读取被持久索引收窄：实际检查目标事件 "
          f"{c['tgt_events_examined']:,} ≪ 目标事件总数 {stats['target_asserts']:,}")
    print(f"  扫描源区间 {c['source_segments']:,} 段；目标查询 {c['target_queries']:,} 次；"
          f"目标事件检查 {c['tgt_events_examined']:,} 条（总数 {stats['target_asserts']:,}）")
    print(f"  缺失输出 {c['results_emitted']:,} 条（暂存 {c['findings_staged']:,}）")

    banner("AS1 结果整流核验（流式，受计量子进程）")
    meter2 = Meter("as1-read", os.path.join(AROOT, AUDIT))
    res = meter2.run([sys.executable, "-m", "demo.audit_child", "read-count",
                      AROOT, AUDIT, "997"], env=dict(os.environ))
    check(res["records"] == c["results_emitted"],
          f"流式读完 {res['records']:,} 条 == 终验记录数，次序严格递增")

    banner("AS1 独立抽样核对（25 个源实体逐条对照端点枚举参考）")
    rng = random.Random(777)
    sample = sorted(rng.sample(["s%06d" % i for i in range(N_SRC)], 25))
    n_checked = ref_check_sample(sample, N_BATCHES)
    check(n_checked == 25, f"抽样 {n_checked} 个源实体的全部缺失区间与独立参考一致")

    report = {
        "scenario": "AS1 temporal-reference-audit",
        "audit_mem_budget_bytes": 1 * 1024 * 1024,
        "hist_mem_budget_bytes": 4 * 1024 * 1024,
        "entities": N_SRC + N_TGT + 1, "batches": N_BATCHES,
        "events_total": hist_events_total,
        "asserts": stats["asserts"], "retracts": stats["retracts"],
        "target_asserts": stats["target_asserts"],
        "input_bytes": input_bytes,
        "business_window": [T0, T1],
        "source_entities_scanned": c["source_entities"],
        "source_segments_scanned": c["source_segments"],
        "src_events_examined": c["src_events_examined"],
        "target_queries": c["target_queries"],
        "tgt_events_examined": c["tgt_events_examined"],
        "src_file_opens": c["src_file_opens"],
        "findings_staged": c["findings_staged"],
        "results_emitted": c["results_emitted"],
        "peak_open_files": c.get("peak_open_files"),
        "audit_peak_rss_kb": meter.peak_rss,
        "read_peak_rss_kb": meter2.peak_rss,
        "peak_temp_disk_bytes": max(meter.peak_disk, meter2.peak_disk),
        "audit_seconds": meter.seconds,
        "read_seconds": meter2.seconds,
        "sampled_entities_checked": n_checked,
        "interpreter_baseline_kb": baseline,
        "total_seconds": round(time.time() - t_start, 1),
    }
    print("\nAS1 指标报告：")
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    print(f"\n时态引用审计规模关卡通过（总耗时 {time.time() - t_start:.1f}s）。")


if __name__ == "__main__":
    main()
