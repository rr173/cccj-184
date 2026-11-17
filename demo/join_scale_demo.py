"""关联算子规模关卡：运行时生成数据，验证超内存输入与热点键，报告真实指标。

  SA  两边输入都超过工作内存（256KiB 预算 vs 双侧各约 3.5MB 输入）：
      真实子进程运行，中途两次受控终止（接收段途中 / emit 途中）并恢复；
      报告输入/输出行数、峰值 RSS、临时磁盘峰值、文件打开峰值、
      恢复前后源段读取计数；结果与朴素参考逐条一致。
  SB  一个键的两侧组都超过工作内存（16KiB 预算 vs 双侧各约 24KB 热键组）：
      identity 视图控制结果体积；用独立的乘法/身份抽样核对热点输出。
  SC  小规模 full 视图热键关联：证明完整结果接口保留原 value。

所有数据落在 .run/join-scale 下，不进 Git；重复运行先清空。
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
from shuffle.common import Layout
from shuffle import join as J
from demo.demo_data import directory_size
from demo.join_ref import (
    comparable,
    expected_comparable,
    load_source_rows,
    naive_join,
    write_input,
)

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.path.join(PROJECT, ".run", "join-scale")
WORK = os.path.join(ROOT, "work")


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


def join_dir_bytes(name: str) -> int:
    """关联工作目录中的临时数据体量（recv/norm/keys/tmp，不含结果与库）。"""
    total = 0
    for sub in ("recv", "norm", "keys", "tmp"):
        d = os.path.join(WORK, name, sub)
        if os.path.isdir(d):
            total += directory_size(d)
    return total


def run_join_metered(name: str, hook: str | None = None) -> dict:
    """以真实子进程运行关联，采样峰值 RSS 与临时磁盘峰值。"""
    env = dict(os.environ)
    env.pop("SHUFFLE_JOIN_HOOK", None)
    cmd = [sys.executable, "-m", "shuffle.join_cli", "run",
           "--workdir", WORK, "--name", name]
    if hook:
        cmd += ["--hook", hook]
    t0 = time.time()
    proc = subprocess.Popen(cmd, cwd=PROJECT, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    peak_rss = 0
    peak_disk = 0
    while proc.poll() is None:
        peak_rss = max(peak_rss, rss_kb(proc.pid))
        peak_disk = max(peak_disk, join_dir_bytes(name))
        time.sleep(0.02)
    peak_rss = max(peak_rss, rss_kb(proc.pid))
    peak_disk = max(peak_disk, join_dir_bytes(name))
    out, err = proc.communicate()
    return {"returncode": proc.returncode, "seconds": round(time.time() - t0, 2),
            "peak_rss_kb": peak_rss, "peak_temp_disk_bytes": peak_disk,
            "stderr_tail": err.decode("utf-8", "replace").strip().splitlines()[-3:]}


def build_job(layout: Layout, job: str, splits: list, num_p: int, version: int,
              mem: int) -> None:
    coord.create_job(layout, job, splits, num_p, version)
    for sid, _path in splits:
        info = coord.launch_attempt(layout, job, sid, mem)
        proc = _spawn_worker(layout, job, sid, info["attempt_no"], mem, None)
        out, err = proc.communicate(timeout=300)
        if proc.returncode != 0:
            raise RuntimeError(f"工人 {job}/{sid} 失败: {err.decode()}")
        coord.claim_winner(layout, job, sid, info["attempt_no"])


def gen_rows(path: str, n: int, key_fn, tag: str) -> None:
    """流式生成输入文件，不在驱动进程保留行列表。"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for i in range(n):
            key = key_fn(i)
            fh.write(json.dumps({"key": key,
                                 "value": {"seq": i, "tag": tag,
                                           "blob": "v" * (20 + i % 30)}},
                                ensure_ascii=False) + "\n")


# --------------------------------------------------------------------------- #
# SA：两边输入都超过工作内存
# --------------------------------------------------------------------------- #
def scenario_a(layL: Layout, layR: Layout) -> None:
    banner("SA 两边输入都超过工作内存（256KiB 预算 vs 双侧各约 3.5MB 输入）")
    mem_budget = 256 * 1024
    n_la, n_lb, n_ra, n_rb = 25_000, 15_000, 25_000, 15_000
    shared = 15_000

    in_dir = os.path.join(ROOT, "in")
    p_la = os.path.join(in_dir, "saLA.jsonl")
    p_lb = os.path.join(in_dir, "saLB.jsonl")
    p_ra = os.path.join(in_dir, "saRA.jsonl")
    p_rb = os.path.join(in_dir, "saRB.jsonl")
    # 共享键 k* + 单侧键 Lonly*/Ronly*；重复键天然存在（键空间小于行数）
    gen_rows(p_la, n_la, lambda i: f"k{i % shared}" if i % 41 else f"Lonly{i % 500}", "LA")
    gen_rows(p_lb, n_lb, lambda i: f"k{(i * 7) % shared}", "LB")
    gen_rows(p_ra, n_ra, lambda i: f"k{i % shared}" if i % 43 else f"Ronly{i % 700}", "RA")
    gen_rows(p_rb, n_rb, lambda i: f"k{(i * 11) % shared}", "RB")
    input_bytes = sum(directory_size(os.path.dirname(p)) for p in ())  # placeholder
    input_bytes = directory_size(in_dir)
    print(f"  输入：左 {n_la + n_lb:,} 行、右 {n_ra + n_rb:,} 行，"
          f"共 {input_bytes:,} 字节（预算 {mem_budget:,} 字节）")
    check(input_bytes > 4 * mem_budget, "输入体量确超工作内存预算")

    build_job(layL, "saL", [("LA", p_la), ("LB", p_lb)], 4, 13, mem=65536)
    build_job(layR, "saR", [("RA", p_ra), ("RB", p_rb)], 3, 7, mem=65536)

    J.create_join(WORK, "sa", left_root=layL.root, left_job="saL",
                  right_root=layR.root, right_job="saR", mode="full",
                  mem_bytes=mem_budget, max_open_files=32, checkpoint_rows=20000)

    st0 = J.join_status(WORK, "sa")
    total_segs = st0["receive"]["L"]["segments_total"] + st0["receive"]["R"]["segments_total"]
    print(f"  源段总数（两侧 winner 段，含溢写）: {total_segs}")

    m1 = run_join_metered("sa", hook="kill-after-receive-segment=20")
    check(m1["returncode"] == 42, "接收第 20 段后受控终止")
    st = J.join_status(WORK, "sa")
    reads_after_kill = st["counters"]["src_segments_read"]
    check(reads_after_kill == 20, f"已持久接收 20 段，源段读取计数 = {reads_after_kill}")

    m2 = run_join_metered("sa", hook="kill-at-emit=60000")
    check(m2["returncode"] == 42, "emit 途中受控终止")
    st = J.join_status(WORK, "sa")
    check(st["checkpoints"]["emit"]["out_rows"] <= 60000,
          f"emit 检查点停在 {st['checkpoints']['emit']['out_rows']} 行")

    m3 = run_join_metered("sa")
    check(m3["returncode"] == 0, "恢复后运行到完成")
    st = J.join_status(WORK, "sa")
    reads_final = st["counters"]["src_segments_read"]
    check(reads_final == total_segs,
          f"恢复前后源段读取计数 {reads_final} == 源段总数 {total_segs}（已接收不重读）")

    # 独立核对：朴素参考在驱动进程内存中计算（产品侧全程受限）
    left_rows = load_source_rows(p_la, "LA") + load_source_rows(p_lb, "LB")
    right_rows = load_source_rows(p_ra, "RA") + load_source_rows(p_rb, "RB")
    expected = naive_join(left_rows, right_rows, "full")
    with J.JoinResult(WORK, "sa") as res:
        check(res.row_count == len(expected),
              f"输出行数 {res.row_count:,} == 朴素参考 {len(expected):,}")
        got = list(res.iter())
    check(comparable(got) == expected_comparable(expected),
          "40,000+40,000 行 full 关联结果与朴素参考逐条一致")

    report = {
        "scenario": "SA both-inputs-exceed-memory",
        "mem_budget_bytes": mem_budget,
        "input_rows_left": n_la + n_lb,
        "input_rows_right": n_ra + n_rb,
        "input_bytes": input_bytes,
        "output_rows": len(expected),
        "result_bytes": st["result"]["bytes"],
        "peak_rss_kb": max(m1["peak_rss_kb"], m2["peak_rss_kb"], m3["peak_rss_kb"]),
        "peak_temp_disk_bytes_sampled": max(m1["peak_temp_disk_bytes"],
                                            m2["peak_temp_disk_bytes"],
                                            m3["peak_temp_disk_bytes"]),
        "peak_open_files": st["counters"].get("peak_open_files"),
        "src_segments_total": total_segs,
        "src_segments_read_after_kill": reads_after_kill,
        "src_segments_read_final": reads_final,
        "run_seconds": [m1["seconds"], m2["seconds"], m3["seconds"]],
    }
    print("\nSA 指标报告：")
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))


# --------------------------------------------------------------------------- #
# SB：一个键的两侧组都超过工作内存（identity 视图 + 独立抽样）
# --------------------------------------------------------------------------- #
def scenario_b(layL: Layout, layR: Layout) -> None:
    banner("SB 热点键两侧组都超过工作内存（16KiB 预算 vs 双侧各约 24KB 热键组）")
    mem_budget = 16 * 1024
    hot_l, hot_r, background = 400, 400, 5_000
    in_dir = os.path.join(ROOT, "in")
    p_l = os.path.join(in_dir, "sbL.jsonl")
    p_r = os.path.join(in_dir, "sbR.jsonl")
    os.makedirs(in_dir, exist_ok=True)
    hot_value_l = lambda i: "HL" + str(i) + "x" * 40   # 热键行约 60B
    hot_value_r = lambda i: "HR" + str(i) + "y" * 40
    with open(p_l, "w", encoding="utf-8") as fh:
        for i in range(hot_l):
            fh.write(json.dumps({"key": "hot", "value": hot_value_l(i)}) + "\n")
        for i in range(background):
            fh.write(json.dumps({"key": f"bk{i % 2000}", "value": f"bl{i}"}) + "\n")
    with open(p_r, "w", encoding="utf-8") as fh:
        for i in range(hot_r):
            fh.write(json.dumps({"key": "hot", "value": hot_value_r(i)}) + "\n")
        for i in range(background):
            fh.write(json.dumps({"key": f"bk{i % 2000}", "value": f"br{i}"}) + "\n")
    # 精确计算热键组字节（规范行字节口径）
    from shuffle.common import canonical_row_bytes
    group_bytes_l = sum(len(canonical_row_bytes("H", i + 1, "hot", hot_value_l(i)))
                        for i in range(hot_l))
    group_bytes_r = sum(len(canonical_row_bytes("H", i + 1, "hot", hot_value_r(i)))
                        for i in range(hot_r))
    group_bytes = min(group_bytes_l, group_bytes_r)
    print(f"  热键组体量：左 {group_bytes_l:,} 字节、右 {group_bytes_r:,} 字节 "
          f"> 预算 {mem_budget:,} 字节")
    check(group_bytes > mem_budget, "热键两侧组确超工作内存预算")

    build_job(layL, "sbL", [("H", p_l)], 4, 13, mem=65536)
    build_job(layR, "sbR", [("H", p_r)], 3, 7, mem=65536)
    J.create_join(WORK, "sb", left_root=layL.root, left_job="sbL",
                  right_root=layR.root, right_job="sbR", mode="inner",
                  mem_bytes=mem_budget, max_open_files=24,
                  result_kind="identity", checkpoint_rows=20000)
    m = run_join_metered("sb")
    check(m["returncode"] == 0, "热点关联运行完成")
    st = J.join_status(WORK, "sb")

    # 独立的乘法/身份抽样：期望序列 = 热键左行源序 × 右行源序 的嵌套循环
    left_rows = load_source_rows(p_l, "H")
    right_rows = load_source_rows(p_r, "H")
    l_hot = [(r["split_id"], r["row_number"]) for r in left_rows if r["key"] == "hot"]
    r_hot = [(r["split_id"], r["row_number"]) for r in right_rows if r["key"] == "hot"]
    expected_total = len(l_hot) * len(r_hot)
    print(f"  期望热键乘积 {len(l_hot)}×{len(r_hot)} = {expected_total:,} 对")
    hot_seen = 0
    samples_checked = 0
    with J.JoinResult(WORK, "sb") as res:
        for row in res.iter():
            l, r = row["left"], row["right"]
            if l["key"] != "hot":
                continue
            if hot_seen % 401 == 0:  # 抽样：每 401 对核一对身份
                exp_l = l_hot[hot_seen // len(r_hot)]
                exp_r = r_hot[hot_seen % len(r_hot)]
                got_l = (l["split_id"], l["row_number"])
                got_r = (r["split_id"], r["row_number"])
                if got_l != exp_l or got_r != exp_r:
                    raise AssertionError(
                        f"热键第 {hot_seen} 对：期望 {exp_l}×{exp_r}，实际 {got_l}×{got_r}"
                    )
                samples_checked += 1
            hot_seen += 1
        total_rows = res.row_count
    check(hot_seen == expected_total,
          f"热键输出对数 {hot_seen:,} == 乘法期望 {expected_total:,}")
    check(samples_checked >= expected_total // 401,
          f"独立身份抽样核对 {samples_checked} 对全部一致")
    exp_background = naive_join(left_rows, right_rows, "inner")
    check(total_rows == len(exp_background),
          f"总输出行数 {total_rows:,} 与朴素参考一致")

    report = {
        "scenario": "SB hot-key-groups-exceed-memory (identity view)",
        "mem_budget_bytes": mem_budget,
        "hot_group_rows_per_side": [len(l_hot), len(r_hot)],
        "hot_group_bytes_estimate": group_bytes,
        "input_rows_left": len(left_rows),
        "input_rows_right": len(right_rows),
        "output_rows": total_rows,
        "hot_pairs": hot_seen,
        "identity_samples_checked": samples_checked,
        "peak_rss_kb": m["peak_rss_kb"],
        "peak_temp_disk_bytes_sampled": m["peak_temp_disk_bytes"],
        "peak_open_files": st["counters"].get("peak_open_files"),
        "src_segments_read": st["counters"]["src_segments_read"],
        "run_seconds": m["seconds"],
    }
    print("\nSB 指标报告：")
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))


# --------------------------------------------------------------------------- #
# SC：小规模 full 视图热键关联——完整结果接口保留原 value
# --------------------------------------------------------------------------- #
def scenario_c(layL: Layout, layR: Layout) -> None:
    banner("SC full 视图热键关联：完整结果接口保留原 value（4KiB 预算）")
    mem_budget = 4 * 1024
    hot = 100
    in_dir = os.path.join(ROOT, "in")
    p_l = os.path.join(in_dir, "scL.jsonl")
    p_r = os.path.join(in_dir, "scR.jsonl")
    write_input(p_l, [("hot", f"VL{i}") for i in range(hot)])
    write_input(p_r, [("hot", f"VR{i}") for i in range(hot)])
    build_job(layL, "scL", [("H", p_l)], 4, 13, mem=65536)
    build_job(layR, "scR", [("H", p_r)], 3, 7, mem=65536)
    J.create_join(WORK, "sc", left_root=layL.root, left_job="scL",
                  right_root=layR.root, right_job="scR", mode="inner",
                  mem_bytes=mem_budget, max_open_files=24,
                  result_kind="full", checkpoint_rows=1000)
    J.run_join(WORK, "sc")
    with J.JoinResult(WORK, "sc") as res:
        check(res.row_count == hot * hot, f"full 视图热键输出 {res.row_count:,} 对")
        idx = 1234  # 第 idx 对 = VL{idx//100}×VR{idx%100}（嵌套次序）
        sample = res.read(idx, 3)
        exp = [(f"VL{idx // hot}", f"VR{idx % hot}"),
               (f"VL{(idx + 1) // hot}", f"VR{(idx + 1) % hot}"),
               (f"VL{(idx + 2) // hot}", f"VR{(idx + 2) % hot}")]
        got = [(r["left"]["value"], r["right"]["value"]) for r in sample]
        check(got == exp, f"第 {idx} 起三对 = {exp}（嵌套次序正确）")
        check(all("value" in r["left"] and "value" in r["right"] for r in sample),
              "完整结果接口保留原 value 字段")
    print("  ✓ full 视图在热键乘积中保留完整原行（含 value）")


def main() -> None:
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(WORK, exist_ok=True)
    layL = Layout(os.path.join(ROOT, "rootL"))
    layR = Layout(os.path.join(ROOT, "rootR"))
    t0 = time.time()
    scenario_a(layL, layR)
    scenario_b(layL, layR)
    scenario_c(layL, layR)
    print(f"\n全部关联规模关卡通过（总耗时 {time.time() - t0:.1f}s）。")


if __name__ == "__main__":
    main()
