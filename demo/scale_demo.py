"""规模关卡：运行时生成大数据，真实多进程流式加工，报告溢写/RSS/磁盘。

不向仓库提交数据：所有输入生成在 .run/scale 下，脚本结束保留现场路径报告，
可重复运行时先清空。工人按每分区极小内存上限运行，强制产生多个溢写段，
验证“不按分区攒完整 Python 列表”的流式路径。
"""

from __future__ import annotations

import json
import os
import resource
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shuffle import coordinator as coord
from shuffle.cli import _spawn_worker
from shuffle.common import Layout
from shuffle import reader as reader_mod
from demo.demo_data import count_files, directory_size

ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".run", "scale")
JOB = "job-scale"
NUM_P = 8
VERSION = 13
SPLITS = 4
ROWS_PER_SPLIT = 75_000          # 合计 30 万行
MEM_PER_PARTITION = 64 * 1024    # 64KiB：强制频繁溢写
KEY_SPACE = 2_000                # 必然出现重复键/重复内容，验证不合并


def rss_kb(pid: int) -> int:
    """读取 /proc/<pid>/status 的 VmRSS（kB）。进程退出后返回 0。"""
    try:
        with open(f"/proc/{pid}/status", "r", encoding="ascii") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except FileNotFoundError:
        return 0
    return 0


def generate_inputs(in_dir: str) -> list[str]:
    """流式生成全部输入文件并落盘，不在驱动进程保留行列表。"""
    os.makedirs(in_dir, exist_ok=True)
    sids = []
    for s in range(SPLITS):
        sid = f"S{s}"
        sids.append(sid)
        path = os.path.join(in_dir, f"{sid}.jsonl")
        with open(path, "w", encoding="utf-8") as fh:
            for rn in range(1, ROWS_PER_SPLIT + 1):
                # 键在小空间内循环 -> 大量重复键；value 带循环 -> 重复内容行也存在
                key = f"k{rn % KEY_SPACE}"
                value = {"seq": rn, "blob": "v" * (24 + (rn % 17))}
                fh.write(json.dumps({"key": key, "value": value}, ensure_ascii=False))
                fh.write("\n")
    return sids


def main() -> None:
    import shutil

    if os.path.isdir(ROOT):
        shutil.rmtree(ROOT)
    os.makedirs(ROOT, exist_ok=True)
    layout = Layout(ROOT)
    in_dir = os.path.join(ROOT, "inputs")

    t0 = time.time()
    print(f"生成规模输入：{SPLITS} split × {ROWS_PER_SPLIT:,} 行 = "
          f"{SPLITS * ROWS_PER_SPLIT:,} 行，{NUM_P} 目标分区")
    sids = generate_inputs(in_dir)
    input_bytes = directory_size(in_dir)
    print(f"  输入磁盘占用: {input_bytes:,} 字节，生成耗时 {time.time() - t0:.2f}s")

    coord.create_job(
        layout, JOB,
        [(f"S{s}", os.path.join(in_dir, f"S{s}.jsonl")) for s in range(SPLITS)],
        NUM_P, VERSION,
    )

    # 四个真实工人进程并行；S0 再启动一个投机 attempt，验证同 split 双 attempt
    t1 = time.time()
    procs = []
    attempts = {}
    for s in range(SPLITS):
        sid = f"S{s}"
        info = coord.launch_attempt(layout, JOB, sid, MEM_PER_PARTITION)
        attempts[(sid, info["attempt_no"])] = info
        procs.append((sid, info["attempt_no"],
                      _spawn_worker(layout, JOB, sid, info["attempt_no"],
                                    MEM_PER_PARTITION, None)))
    # S0 的第二个 attempt（并行重做），稍晚启动以模拟“慢分片投机备份”
    spec = coord.launch_attempt(layout, JOB, "S0", MEM_PER_PARTITION)
    spec_proc = _spawn_worker(layout, JOB, "S0", spec["attempt_no"],
                              MEM_PER_PARTITION, None)

    peak_rss: dict[int, int] = {}
    alive = list(procs) + [("S0", spec["attempt_no"], spec_proc)]
    while alive:
        for sid, ano, p in alive:
            peak_rss[p.pid] = max(peak_rss.get(p.pid, 0), rss_kb(p.pid))
        alive = [(sid, ano, p) for sid, ano, p in alive if p.poll() is None]
        time.sleep(0.02)
    for sid, ano, p in procs + [("S0", spec["attempt_no"], spec_proc)]:
        out, err = p.communicate()
        if p.returncode != 0:
            raise RuntimeError(f"工人 {sid}#{ano} 失败: {err.decode()}")
    worker_seconds = time.time() - t1
    print(f"  {len(procs) + 1} 个真实工人进程并行加工完成，耗时 {worker_seconds:.2f}s")

    # 确认：S0 两个 attempt 竞争，其余各自确认
    for s in range(1, SPLITS):
        coord.claim_winner(layout, JOB, f"S{s}", 0)
    # S0 先到者赢（两个都已 completed）；另一个必须成为 lost
    s0_state = coord.inspect(layout, JOB)["splits"][0]
    first_complete = sorted(
        (a for a in s0_state["attempts"]), key=lambda a: a["attempt_no"]
    )
    coord.claim_winner(layout, JOB, "S0", first_complete[0]["attempt_no"])
    try:
        coord.claim_winner(layout, JOB, "S0", first_complete[1]["attempt_no"])
        raise AssertionError("S0 第二个 attempt 不应成为 winner")
    except coord.AlreadyWinner:
        pass

    st = coord.inspect(layout, JOB)
    total_seg = sum(
        sum(p["segments"] for p in a["partitions"])
        for sp in st["splits"] for a in sp["attempts"]
    )
    spill_files = count_files(os.path.join(ROOT, JOB, "segments"), ".seg")
    tmp_left = count_files(os.path.join(ROOT, JOB, "segments"), ".tmp")
    print(f"  段文件总数（含输者）: {spill_files}，残留 .tmp: {tmp_left}（应为 0）")
    assert tmp_left == 0

    # 读取并独立核对全部 8 个分区：顺序、重数、每行恰好一次。
    # 核对本身也流式：重扫输入按 (split 清单序, row_number) 生成期望流，
    # 与 reader 的输出流逐行配对，任何一侧都不攒分区列表。
    t2 = time.time()

    def expected_stream(partition: int):
        import json as _json

        from shuffle.common import partition_of

        for s in range(SPLITS):
            sid = f"S{s}"
            with open(os.path.join(in_dir, f"{sid}.jsonl"), "rb") as fh:
                for rn, line in enumerate(fh, start=1):
                    obj = _json.loads(line)
                    if partition_of(obj["key"], NUM_P, VERSION) == partition:
                        yield (sid, rn)

    total_read = 0
    winner_attempts = {}
    with reader_mod.open_reader(layout, JOB) as rdr:
        winner_attempts = dict(rdr.winners)
        for p in range(NUM_P):
            exp = expected_stream(p)
            got = ((r["split_id"], r["row_number"]) for r in rdr.read_partition(p))
            # zip 两侧都是生成器；用标记检测期望/实际长度不一致
            sentinel = object()
            matched = 0
            for e, g in zip(exp, got):
                assert e == g, f"分区 {p} 第 {matched} 行: 期望 {e} 实际 {g}"
                matched += 1
            if next(exp, sentinel) is not sentinel:
                raise AssertionError(f"分区 {p} 期望流仍有未交付行（漏行）")
            if next(got, sentinel) is not sentinel:
                raise AssertionError(f"分区 {p} 实际流存在多余行（重行/混拼）")
            total_read += matched
    read_seconds = time.time() - t2
    expected_total = SPLITS * ROWS_PER_SPLIT
    assert total_read == expected_total
    print(f"  流式读取+独立核对 {NUM_P} 分区完成，{total_read:,} 行恰好各一次，"
          f"耗时 {read_seconds:.2f}s")

    # 清理输者段，确认 winner 段保留且仍可读
    clean = coord.cleanup(layout, JOB)
    with reader_mod.open_reader(layout, JOB) as rdr:
        again = sum(len(list(rdr.read_partition(p))) for p in range(NUM_P))
    assert again == expected_total
    seg_bytes = directory_size(os.path.join(ROOT, JOB, "segments"))
    db_bytes = os.path.getsize(layout.db_path)

    # 驱动自身峰值 RSS（ru_maxrss，Linux 单位 kB）
    self_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    report = {
        "job": JOB,
        "root": ROOT,
        "rows_total": expected_total,
        "partitions": NUM_P,
        "worker_processes": len(procs) + 1,
        "worker_pids": sorted(peak_rss),
        "worker_peak_rss_kb": {str(pid): kb for pid, kb in sorted(peak_rss.items())},
        "worker_peak_rss_max_kb": max(peak_rss.values()),
        "driver_peak_rss_kb": self_rss,
        "mem_limit_per_partition_bytes": MEM_PER_PARTITION,
        "spill_segment_files_total": spill_files,
        "spill_records_in_db": total_seg,
        "segments_after_cleanup": count_files(
            os.path.join(ROOT, JOB, "segments"), ".seg"
        ),
        "reaped_files": len(clean["reaped"]),
        "input_bytes": input_bytes,
        "winner_segment_bytes": seg_bytes,
        "sqlite_bytes": db_bytes,
        "winner_attempts": winner_attempts,
        "seconds": {
            "generate": round(time.time() - t0, 2),
            "workers": round(worker_seconds, 2),
            "read_verify": round(read_seconds, 2),
        },
    }
    print("\n规模关卡报告：")
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    # 溢写确实发生：平均每分区段数应显著大于 1
    winner_seg = report["segments_after_cleanup"]
    assert winner_seg >= NUM_P * SPLITS
    assert spill_files > winner_seg, "输者 S0 投机 attempt 的段应已被回收"
    print("\n规模关卡通过：全程流式、多段溢写、双 attempt 竞争、独立核对无缺行/重行。")


if __name__ == "__main__":
    main()
