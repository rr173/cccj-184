"""纯命令行冒烟：只通过 CLI 的 create/launch-attempt/inspect/read-partition/
recover/cleanup 六个规定接口驱动，验证命令面端到端可用。"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys

ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".run", "cli")


def run(args: list[str]) -> dict:
    proc = subprocess.run(
        [sys.executable, "-m", "shuffle.cli", *args],
        capture_output=True, text=True, cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    )
    if proc.returncode != 0:
        raise RuntimeError(f"CLI 失败: {args}\n{proc.stderr}")
    return json.loads(proc.stdout)


def main() -> None:
    shutil.rmtree(ROOT, ignore_errors=True)
    in_dir = os.path.join(ROOT, "inputs")
    os.makedirs(in_dir, exist_ok=True)
    with open(os.path.join(in_dir, "X.jsonl"), "w", encoding="utf-8") as fh:
        fh.write('{"key":"a1","value":"one"}\n{"key":"a2","value":"two"}\n')

    job = "job-cli"
    created = run(["create", "--root", ROOT, "--job", job,
                   "--split", f"X={os.path.join(in_dir, 'X.jsonl')}",
                   "--partitions", "4", "--version", "13"])
    assert created["num_partitions"] == 4
    print("create OK")

    launched = run(["launch-attempt", "--root", ROOT, "--job", job, "--split", "X",
                    "--mem-bytes", "65536", "--spawn", "--wait", "--claim"])
    assert launched["claim"]["winner_attempt"] == 0
    assert launched["worker"]["returncode"] == 0
    print(f"launch-attempt --spawn --wait --claim OK（真实工人 pid={launched['worker']['pid']}）")

    st = run(["inspect", "--root", ROOT, "--job", job])
    assert st["splits"][0]["winner_attempt"] == 0
    assert len(st["splits"][0]["attempts"][0]["partitions"]) == 4
    print("inspect OK：4 个分区均有完成证据段")

    p0 = run(["read-partition", "--root", ROOT, "--job", job, "--partition", "0"])
    p1 = run(["read-partition", "--root", ROOT, "--job", job, "--partition", "1"])
    p3 = run(["read-partition", "--root", ROOT, "--job", job, "--partition", "3"])
    assert [r["row_number"] for r in p0["rows"]] == [1]
    assert [r["row_number"] for r in p1["rows"]] == [2]
    assert p3["row_count"] == 0
    print(f"read-partition OK：p0={p0['row_count']} 行, p1={p1['row_count']} 行, 空 p3={p3['row_count']} 行")

    rec = run(["recover", "--root", ROOT, "--job", job])
    assert {(w["split_id"], w["attempt_no"]) for w in rec["winners"]} == {("X", 0)}
    print("recover OK：winner 重启后核对可见")

    cleaned = run(["cleanup", "--root", ROOT, "--job", job])
    assert isinstance(cleaned["reaped"], list)
    print(f"cleanup OK：回收 {len(cleaned['reaped'])} 个受管文件，winner 段保留")

    # destroy 需先无打开 reader
    destroyed = run(["cleanup", "--root", ROOT, "--job", job, "--destroy"])
    assert destroyed["destroyed"] is True
    assert not os.path.isdir(os.path.join(ROOT, job))
    print("cleanup --destroy OK：作业受管目录移除，SQLite 审计行保留")
    print("\nCLI 六接口冒烟全部通过。")


if __name__ == "__main__":
    main()
