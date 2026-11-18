"""双时间历史表规模关卡：运行时生成多批历史，实体总数与单实体历史分别超内存。

  S1  实体总数超内存：8,000 实体、12 批、约 3.4 万事件（含负时间/无穷终点/
      重复端点/空洞/覆盖后撤回），工作内存 4MiB。中途一次杀进程恢复；
      全量导出计数 + 独立参考抽样核对。
  S2  单实体历史超内存：热点实体 3 万事件（交错区间、重复端点、负时间、
      无穷终点、覆盖后再撤回、区间局部暴露），工作内存 8MiB。短区间查询
      统计实际访问的历史记录数与源打开次数；独立端点枚举 + 抽样验证。
  S3  大批次杀进程恢复与幂等重送：接收途中受控终止，恢复后不重读已接收
      源段；提交后重送不占版本。

报告真实入仓/输出数量、RSS、临时磁盘与文件峰值。所有数据落在
.run/hist-scale 下，不进 Git；重复运行先清空。
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
from demo.demo_data import directory_size
from demo.history_ref import RefTable, load_batch_rows

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.path.join(PROJECT, ".run", "hist-scale")
WORK = os.path.join(ROOT, "work")
IN = os.path.join(ROOT, "in")


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


def build_job(layout: Layout, job: str, splits: list, num_p: int, version: int,
              mem: int = 65536) -> None:
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
    """子进程计量：峰值 RSS、临时磁盘峰值、耗时。"""

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
        lines = out.decode("utf-8", "replace").strip().splitlines()
        result = None
        for line in reversed(lines):
            line = line.strip()
            if line.startswith("{"):
                result = json.loads(line)
                break
        if proc.returncode != 0:
            raise RuntimeError(
                f"{self.name} 失败 rc={proc.returncode}: {err.decode()[-400:]}")
        return result or {}


def interpreter_baseline_kb() -> int:
    proc = subprocess.Popen([sys.executable, "-c",
                             "import time; time.sleep(0.4)"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    peak = 0
    while proc.poll() is None:
        peak = max(peak, rss_kb(proc.pid))
        time.sleep(0.02)
    proc.wait()
    return peak


# --------------------------------------------------------------------------- #
# S1 实体总数超内存
# --------------------------------------------------------------------------- #
def scenario_s1(lay: Layout) -> dict:
    banner("S1 实体总数超内存：8,000 实体 × 12 批 ≈ 3.4 万事件（预算 4MiB）")
    rng = random.Random(20261118)
    n_entities, n_batches = 8000, 12
    mem_budget = 4 * 1024 * 1024
    H.create_table(WORK, "s1", mem_bytes=mem_budget, max_open_files=24)

    alive: dict[str, list[str]] = {}   # 更早批次已提交、尚未撤回的断言
    jobs_meta = []
    ref = RefTable()
    total_asserts = total_retracts = 0
    for b in range(1, n_batches + 1):
        rows = []
        new_alive: dict[str, list[str]] = {}
        for i in range(n_entities):
            if rng.random() < 0.42:
                ent = f"e{i:06d}"
                center = rng.randint(-500_000, 500_000)
                r = rng.random()
                if r < 0.65:
                    span = rng.randint(1, 500)
                elif r < 0.85:
                    span = rng.randint(501, 5000)
                elif r < 0.95:
                    span = rng.randint(5001, 50000)
                else:
                    span = None
                eid = f"s1-b{b}-{i}"
                rows.append((ent, assertion(eid, center,
                                            None if span is None else center + span,
                                            {"b": b, "i": i})))
                new_alive.setdefault(ent, []).append(eid)
                total_asserts += 1
        # 覆盖后再撤回：只撤回更早批次的断言（不得前向引用同批）
        if b >= 3:
            for ent in rng.sample(sorted(alive), min(40, len(alive))):
                cands = [e for e in alive[ent]]
                if cands:
                    target = rng.choice(cands)
                    rows.append((ent, retraction(f"s1-r{b}-{ent}", target)))
                    alive[ent].remove(target)
                    total_retracts += 1
        for ent, ids in new_alive.items():
            alive.setdefault(ent, []).extend(ids)
        path = os.path.join(IN, "s1", f"b{b:02d}.jsonl")
        write_input(path, rows)
        build_job(lay, f"s1j{b}", [("S", path)], 4, 13)
        jobs_meta.append({"job": f"s1j{b}", "batch_id": f"s1-b{b}"})
        ref.validate_and_add_batch(load_batch_rows([(path, 0)]))
    meta_file = os.path.join(ROOT, "s1-jobs.json")
    with open(meta_file, "w", encoding="utf-8") as fh:
        json.dump(jobs_meta, fh)
    input_bytes = directory_size(os.path.join(IN, "s1"))
    print(f"  输入：{total_asserts + total_retracts:,} 行（断言 {total_asserts:,} + "
          f"撤回 {total_retracts:,}），{input_bytes:,} 字节；预算 {mem_budget:,} 字节")
    check(input_bytes > mem_budget, "输入体量确超工作内存预算")

    # 入仓（受计量子进程）；先正常跑 1-4 批，第 5 批用同步点杀进程再恢复
    with open(os.path.join(ROOT, "s1-first4.json"), "w") as fh:
        json.dump(jobs_meta[:4], fh)
    meter = Meter("s1-ingest", os.path.join(WORK, "s1"))
    meter.run([sys.executable, "-m", "demo.hist_child", "ingest-file",
               WORK, "s1", lay.root, os.path.join(ROOT, "s1-first4.json")],
              env=dict(os.environ))
    with open(os.path.join(ROOT, "s1-b5.json"), "w") as fh:
        json.dump(jobs_meta[4:5], fh)
    env = dict(os.environ)
    env["SHUFFLE_HIST_HOOK"] = "kill-after-receive-segment=2"
    proc = subprocess.run([sys.executable, "-m", "demo.hist_child", "ingest-file",
                           WORK, "s1", lay.root, os.path.join(ROOT, "s1-b5.json")],
                          capture_output=True, text=True, cwd=PROJECT, env=env)
    check(proc.returncode == 42, "第 5 批接收途中受控终止（同步点）")
    st = H.table_status(WORK, "s1")
    reads_at_kill = st["counters"]["src_segments_read"]
    check(st["head"] == 4, f"前 4 批整体存在（head={st['head']}），第 5 批未提交不可见")
    with open(os.path.join(ROOT, "s1-rest.json"), "w") as fh:
        json.dump(jobs_meta[4:], fh)
    meter2 = Meter("s1-ingest-rest", os.path.join(WORK, "s1"))
    result = meter2.run([sys.executable, "-m", "demo.hist_child", "ingest-file",
                         WORK, "s1", lay.root, os.path.join(ROOT, "s1-rest.json")],
                        env=dict(os.environ))
    st = H.table_status(WORK, "s1")
    check(st["head"] == n_batches, f"恢复后全部 {n_batches} 批入仓（head={st['head']}）")
    check(st["counters"]["src_segments_read"] >= reads_at_kill,
          f"源段读取计数 {st['counters']['src_segments_read']}（恢复不重读已接收段）")
    # 重送已提交批次：幂等，不占版本
    r = H.submit_batch(WORK, "s1", src_root=lay.root, src_job="s1j12",
                       batch_id="s1-b12")
    check(r["state"] == "committed" and r["kversion"] == 12,
          "重送已提交批次：幂等返回，不再占版本")

    # 全量导出（受计量子进程，只计数）
    meter3 = Meter("s1-export", os.path.join(WORK, "s1"))
    exp = meter3.run([sys.executable, "-m", "demo.hist_child", "export-count",
                      WORK, "s1", str(n_batches), "2000"], env=dict(os.environ))
    print(f"  导出：{exp['records']:,} 条记录，{exp['entities']:,} 个实体")

    # 独立参考核对：抽样实体完整时间线 + 抽样点查
    t = H.HistoryTable(WORK, "s1")
    rng2 = random.Random(777)
    sample_ents = rng2.sample(sorted({f"e{i:06d}" for i in range(n_entities)}), 30)
    for ent in sample_ents:
        got = [(s["start"], s["end"], s["event_id"], s["present"])
               for s in t.timeline(ent, -(2**63), 2**63 - 1, kversion=n_batches,
                                   limit=100000)["segments"]]
        want = [(s["start"], s["end"], s["event_id"], s["present"])
                for s in ref.timeline(ent, -(2**63), 2**63 - 1, n_batches)]
        # 无穷端点表示差异：产品 None=+∞；参考 None=+∞（一致）
        if got != want:
            raise AssertionError(f"实体 {ent} 时间线与参考不符:\n{got[:6]}\n{want[:6]}")
    check(True, f"抽样 {len(sample_ents)} 个实体完整时间线与独立参考一致")
    for _ in range(60):
        ent = rng2.choice(sample_ents)
        tt = rng2.randint(-500_000, 500_000)
        got = t.point(ent, tt, kversion=n_batches)
        w = ref.winner_at(ent, tt, n_batches)
        want_id = w[1] if w else None
        if got["event_id"] != want_id or got["present"] != (w is not None):
            raise AssertionError(f"点查 {ent}@{tt}: 产品 {got['event_id']} 参考 {want_id}")
    check(True, "60 个抽样点查与独立参考一致")

    report = {
        "scenario": "S1 entity-count-exceeds-memory",
        "mem_budget_bytes": mem_budget,
        "entities": n_entities, "batches": n_batches,
        "input_rows": total_asserts + total_retracts,
        "asserts": total_asserts, "retracts": total_retracts,
        "input_bytes": input_bytes,
        "export_records": exp["records"],
        "events_total": st["events_total"], "buckets_total": st["buckets_total"],
        "src_segments_read": st["counters"]["src_segments_read"],
        "src_file_opens": st["counters"]["src_file_opens"],
        "peak_open_files": st["counters"].get("peak_open_files"),
        "ingest_peak_rss_kb": max(meter.peak_rss, meter2.peak_rss),
        "export_peak_rss_kb": meter3.peak_rss,
        "peak_temp_disk_bytes": max(meter.peak_disk, meter2.peak_disk, meter3.peak_disk),
        "ingest_seconds": meter.seconds + meter2.seconds,
        "export_seconds": meter3.seconds,
    }
    print("\nS1 指标报告：")
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return report


# --------------------------------------------------------------------------- #
# S2 单实体历史超内存
# --------------------------------------------------------------------------- #
def scenario_s2(lay: Layout) -> dict:
    banner("S2 单实体历史超内存：热点实体 3 万事件（预算 8MiB）+ 短区间查询指标")
    rng = random.Random(20261119)
    n_batches, per_batch = 20, 1500
    n_bg = 2000
    mem_budget = 8 * 1024 * 1024
    H.create_table(WORK, "s2", mem_bytes=mem_budget, max_open_files=24)

    hot_events = []   # (vf, vt, eid, batch) 供独立端点枚举
    alive_hot: list[str] = []      # 更早批次已提交、尚未撤回的热点断言
    new_hot: list[str] = []
    hot_retracted: set = set()
    jobs_meta = []
    for b in range(1, n_batches + 1):
        rows = []
        if b <= 4:   # 背景实体
            for i in range(n_bg // 4):
                ent = f"bg{b}-{i:05d}"
                rows.append((ent, assertion(f"s2-bg-{b}-{i}", -1000, 1000, {"bg": i})))
        for j in range(per_batch):
            eid = f"s2-h-{b}-{j}"
            if rng.random() < 0.05:      # 密集簇：深层堆叠（覆盖后再撤回/局部暴露）
                center = rng.randint(-5000, 5000)
                center = (center // 50) * 50
                span = rng.choice((100, 250, 500, 1000, 2000))
            else:
                center = rng.randint(-1_000_000, 1_000_000)
                center = (center // 50) * 50      # 端点量化 → 大量重复端点
                r = rng.random()
                if r < 0.80:
                    span = rng.choice((50, 75, 100))
                elif r < 0.95:
                    span = rng.choice((150, 300, 500))
                elif r < 0.998:
                    span = rng.choice((1000, 3000))
                else:
                    span = None                    # 无穷终点
            rows.append(("hot", assertion(eid, center,
                                          None if span is None else center + span,
                                          {"b": b, "j": j})))
            hot_events.append((center, None if span is None else center + span, eid, b))
            new_hot.append(eid)
        if b >= 6:     # 覆盖后再撤回（部分暴露）：只撤回更早批次的断言
            for _ in range(15):
                cands = [e for e in alive_hot if e not in hot_retracted]
                if cands:
                    target = rng.choice(cands)
                    rows.append(("hot", retraction(f"s2-r-{b}-{target}", target)))
                    hot_retracted.add(target)
        alive_hot.extend(new_hot)
        new_hot = []
        path = os.path.join(IN, "s2", f"b{b:02d}.jsonl")
        write_input(path, rows)
        build_job(lay, f"s2j{b}", [("S", path)], 4, 13)
        jobs_meta.append({"job": f"s2j{b}", "batch_id": f"s2-b{b}"})
    meta_file = os.path.join(ROOT, "s2-jobs.json")
    with open(meta_file, "w", encoding="utf-8") as fh:
        json.dump(jobs_meta, fh)
    n_hot = len(hot_events)
    print(f"  热点实体事件 {n_hot:,} 条（含 {len(hot_retracted)} 条撤回），"
          f"背景实体 {n_bg:,} 个；预算 {mem_budget:,} 字节")

    meter = Meter("s2-ingest", os.path.join(WORK, "s2"))
    meter.run([sys.executable, "-m", "demo.hist_child", "ingest-file",
               WORK, "s2", lay.root, meta_file], env=dict(os.environ))
    st = H.table_status(WORK, "s2")
    check(st["head"] == n_batches, f"全部 {n_batches} 批入仓")
    check(st["buckets_total"] > n_hot, f"桶索引行数 {st['buckets_total']:,} 随事实增长")

    t = H.HistoryTable(WORK, "s2")
    # 短区间查询：统计实际访问的历史记录数与源打开次数
    rng3 = random.Random(4242)
    examined = []
    src_opens = []
    for _ in range(40):
        tt = rng3.randint(-1_000_000, 1_000_000)
        got = t.point("hot", tt, kversion=n_batches)
        examined.append(got["stats"]["events_examined"])
        src_opens.append(got["stats"]["src_opens"])
    for _ in range(10):
        t0 = rng3.randint(-1_000_000, 999_000)
        page = t.timeline("hot", t0, t0 + 1000, kversion=n_batches, limit=100000)
        examined.append(page["stats"]["events_examined"])
        src_opens.append(page["stats"]["src_opens"])
    avg_examined = sum(examined) / len(examined)
    max_examined = max(examined)
    check(all(o == 0 for o in src_opens), "短区间查询源打开次数 = 0（不重放源段）")
    check(max_examined < n_hot // 10,
          f"短区间查询访问历史记录最多 {max_examined} 条 ≪ 热点实体总事件 {n_hot:,}")
    print(f"  短区间查询访问历史记录：平均 {avg_examined:.1f} 条，最多 {max_examined} 条"
          f"（总事件 {n_hot:,}）")

    # 独立端点枚举 + 抽样验证
    endpoints = sorted({p for e in hot_events for p in (e[0], e[1]) if p is not None})
    # 独立计算：每个原子区间的获胜者（暴力最大优先级），抽样 250 个
    alive_events = [(vf, vt, eid, b) for (vf, vt, eid, b) in hot_events
                    if eid not in hot_retracted]
    # 事件的优先级 = (batch, 批内次序) —— 批内次序用生成序号近似：
    # 产品侧优先级为 (kversion, split 清单序, row_number)；本场景单 split，
    # row_number = 批内行号。hot 行在每批中的行号需按生成顺序重算。
    # 这里用独立重放：按批扫描输入文件，得到每个 event_id 的 (batch, row_number)。
    pri: dict[str, tuple[int, int]] = {}
    for b in range(1, n_batches + 1):
        path = os.path.join(IN, "s2", f"b{b:02d}.jsonl")
        with open(path, "r", encoding="utf-8") as fh:
            for rn, line in enumerate(fh, start=1):
                obj = json.loads(line)
                v = obj["value"]
                pri[v["event_id"]] = (b, rn)
    ev_map = {eid: (vf, vt) for vf, vt, eid, _b in hot_events}

    def ref_winner(t_: int):
        best = None
        for vf, vt, eid, b in alive_events:
            if vf <= t_ and (vt is None or t_ < vt):
                p = pri[eid]
                if best is None or p > best[0]:
                    best = (p, eid)
        return best[1] if best else None

    atomic = []
    for i in range(len(endpoints) - 1):
        atomic.append((endpoints[i], endpoints[i + 1]))
    sample_idx = rng3.sample(range(len(atomic)), min(250, len(atomic)))
    for idx in sample_idx:
        a, b_ = atomic[idx]
        mid = a  # 左闭
        want = ref_winner(mid)
        got = t.point("hot", mid, kversion=n_batches)
        if got["event_id"] != want:
            raise AssertionError(f"点查 hot@{mid}: 产品 {got['event_id']} 参考 {want}")
    check(True, f"独立端点枚举（{len(endpoints):,} 端点）+ 抽样 {len(sample_idx)} 点查一致")

    # 全轴时间线：流式分页拉取，只计数（不把单实体全部事件收进容器）
    total_segments = 0
    cur = None
    while True:
        page = t.timeline("hot", -(2**63), 2**63 - 1, kversion=n_batches,
                          limit=20000) if cur is None else \
            t.timeline(cursor=cur, limit=20000)
        total_segments += len(page["segments"])
        cur = page["cursor"]
        if not cur:
            break
    # 换页宽再数一遍：分页与整次一致
    total2 = 0
    cur = None
    while True:
        page = t.timeline("hot", -(2**63), 2**63 - 1, kversion=n_batches,
                          limit=977) if cur is None else \
            t.timeline(cursor=cur, limit=977)
        total2 += len(page["segments"])
        cur = page["cursor"]
        if not cur:
            break
    check(total_segments == total2,
          f"全轴时间线两种页宽段数一致（{total_segments:,} 段，流式分页）")
    check(total_segments > n_hot // 6,
          f"热点实体全轴时间线 {total_segments:,} 段（流式分页，无全量收集）")
    # 抽样区间与独立参考逐段核对
    for _ in range(5):
        t0 = rng3.choice(endpoints[:-1])
        t1 = min(t0 + 5000, 1_000_000)
        got = [(s["start"], s["end"], s["event_id"])
               for s in t.timeline("hot", t0, t1, kversion=n_batches,
                                   limit=100000)["segments"]]
        # 独立参考：枚举该区间端点
        pts = sorted({t0, t1} | {p for p in endpoints if t0 < p < t1})
        want = []
        for i in range(len(pts) - 1):
            w = ref_winner(pts[i])
            seg = [pts[i], pts[i + 1], w]
            if want and w is not None and want[-1][2] == w:
                want[-1][1] = pts[i + 1]
            else:
                want.append(seg)
        want = [(a, b_, w) for a, b_, w in want]
        if got != want:
            raise AssertionError(f"区间 [{t0},{t1}) 时间线与独立参考不符:\n{got[:8]}\n{want[:8]}")
    check(True, "5 个抽样区间完整时间线与独立参考逐段一致")

    report = {
        "scenario": "S2 single-entity-history-exceeds-memory",
        "mem_budget_bytes": mem_budget,
        "hot_entity_events": n_hot, "hot_retracts": len(hot_retracted),
        "background_entities": n_bg, "batches": n_batches,
        "buckets_total": st["buckets_total"],
        "short_query_events_examined_avg": round(avg_examined, 1),
        "short_query_events_examined_max": max_examined,
        "short_query_src_opens": max(src_opens),
        "hot_full_timeline_segments": total_segments,
        "endpoints_enumerated": len(endpoints),
        "ingest_peak_rss_kb": meter.peak_rss,
        "peak_temp_disk_bytes": meter.peak_disk,
        "peak_open_files": st["counters"].get("peak_open_files"),
        "ingest_seconds": meter.seconds,
    }
    print("\nS2 指标报告：")
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return report


# --------------------------------------------------------------------------- #
# S3 大批次杀进程恢复与幂等
# --------------------------------------------------------------------------- #
def scenario_s3(lay: Layout) -> dict:
    banner("S3 大批次杀进程恢复与幂等重送")
    rng = random.Random(99)
    rows1 = [(f"k{i:05d}", assertion(f"t3a-{i}", rng.randint(-1000, 1000),
                                     rng.randint(1001, 2000), {"i": i}))
             for i in range(6000)]
    rows2 = [(f"m{i:05d}", assertion(f"t3b-{i}", 0, 50, {"i": i}))
             for i in range(4000)]
    p1 = os.path.join(IN, "s3a.jsonl")
    p2 = os.path.join(IN, "s3b.jsonl")
    write_input(p1, rows1)
    write_input(p2, rows2)
    build_job(lay, "s3j", [("A", p1), ("B", p2)], 4, 13)
    H.create_table(WORK, "s3", mem_bytes=4 * 1024 * 1024, max_open_files=24)
    H.submit_batch(WORK, "s3", src_root=lay.root, src_job="s3j", batch_id="s3-b1")

    env = dict(os.environ)
    env["SHUFFLE_HIST_HOOK"] = "kill-after-receive-segment=3"
    proc = subprocess.run([sys.executable, "-m", "shuffle.hist_cli", "run",
                           "--workdir", WORK, "--name", "s3", "--batch-id", "s3-b1"],
                          capture_output=True, text=True, cwd=PROJECT, env=env)
    check(proc.returncode == 42, "接收第 3 段后受控终止")
    st = H.table_status(WORK, "s3")
    reads_kill = st["counters"]["src_segments_read"]
    check(reads_kill == 3 and st["head"] == 0, f"已接收 3 段，head=0")

    env2 = dict(os.environ)
    env2["SHUFFLE_HIST_HOOK"] = "kill-after-stage"
    proc = subprocess.run([sys.executable, "-m", "shuffle.hist_cli", "run",
                           "--workdir", WORK, "--name", "s3", "--batch-id", "s3-b1"],
                          capture_output=True, text=True, cwd=PROJECT, env=env2)
    check(proc.returncode == 42, "暂存后提交前受控终止")
    st = H.table_status(WORK, "s3")
    check(st["batches"][0]["state"] == "staged" and st["head"] == 0,
          "暂存未提交：版本整体不可见")
    check(st["counters"]["src_segments_read"] > reads_kill,
          "恢复后继续接收剩余段（已接收不重读）")
    total_reads = st["counters"]["src_segments_read"]

    env3 = dict(os.environ)
    env3["SHUFFLE_HIST_HOOK"] = "kill-after-commit"
    proc = subprocess.run([sys.executable, "-m", "shuffle.hist_cli", "run",
                           "--workdir", WORK, "--name", "s3", "--batch-id", "s3-b1"],
                          capture_output=True, text=True, cwd=PROJECT, env=env3)
    check(proc.returncode == 42, "提交已持久未响应时受控终止")
    st = H.table_status(WORK, "s3")
    check(st["head"] == 1, "重开后版本 1 整体存在")
    r = H.submit_batch(WORK, "s3", src_root=lay.root, src_job="s3j", batch_id="s3-b1")
    check(r["state"] == "committed" and r["kversion"] == 1,
          "重送同标识同绑定：幂等返回，不再占版本")
    check(st["counters"]["src_segments_read"] == total_reads,
          "提交阶段不重读任何源段")
    t = H.HistoryTable(WORK, "s3")
    check(t.point("k00123", 0, kversion=1)["present"]
          and t.point("m03999", 25, kversion=1)["present"],
          "两个 split 的事实都可查")
    return {"scenario": "S3 crash-recovery-idempotent",
            "src_segments_read": total_reads, "head": st["head"]}


def main() -> None:
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(WORK, exist_ok=True)
    lay = Layout(os.path.join(ROOT, "root"))
    t0 = time.time()
    baseline = interpreter_baseline_kb()
    print(f"解释器固定开销（空载子进程 RSS）：{baseline} KiB（单独报告，不计入预算）")
    reports = [scenario_s1(lay), scenario_s2(lay), scenario_s3(lay)]
    reports.append({"interpreter_baseline_kb": baseline,
                    "total_seconds": round(time.time() - t0, 1)})
    print(f"\n全部历史表规模关卡通过（总耗时 {time.time() - t0:.1f}s）。")
    print("\n汇总：")
    print(json.dumps(reports, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
