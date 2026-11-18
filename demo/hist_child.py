"""历史表演示的子进程助手（真实独立进程，只经 SQLite/文件与父进程交互）。

用法：python -m demo.hist_child <action> ...
  commit-race WORKDIR NAME BATCH_ID READY_FILE GO_FILE
      接收+暂存后写 READY_FILE，等 GO_FILE 出现再提交；输出 JSON 结果。
"""

from __future__ import annotations

import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shuffle import history as H


def commit_race(workdir: str, name: str, batch_id: str,
                ready_file: str, go_file: str) -> int:
    H.receive_batch(workdir, name, batch_id)
    H.stage_batch(workdir, name, batch_id)
    with open(ready_file, "w", encoding="ascii") as fh:
        fh.write("ready")
    deadline = time.time() + 60
    while not os.path.exists(go_file):
        if time.time() > deadline:
            print(json.dumps({"ok": False, "error": "等待放行超时"}))
            return 3
        time.sleep(0.02)
    try:
        result = H.commit_batch(workdir, name, batch_id)
        print(json.dumps({"ok": True, "committed": result["kversion"]},
                         ensure_ascii=False))
        return 0
    except H.CommitConflict as exc:
        print(json.dumps({"ok": False, "conflict": str(exc)}, ensure_ascii=False))
        return 24


def ingest_file(workdir: str, name: str, src_root: str, jobs_file: str) -> int:
    """按 JSON 清单依次提交并入仓多个批次（规模演示的受计量子进程）。"""
    with open(jobs_file, "r", encoding="utf-8") as fh:
        jobs = json.load(fh)
    done = 0
    for item in jobs:
        H.submit_batch(workdir, name, src_root=src_root, src_job=item["job"],
                       batch_id=item["batch_id"])
        H.run_batch(workdir, name, item["batch_id"])
        done += 1
        if done % 2 == 0 or done == len(jobs):
            st = H.table_status(workdir, name)
            print(f"[ingest] {done}/{len(jobs)} head={st['head']} "
                  f"events={st['events_total']}", flush=True)
    st = H.table_status(workdir, name)
    print(json.dumps({"ok": True, "done": done, "head": st["head"],
                      "events_total": st["events_total"],
                      "counters": st["counters"]}, ensure_ascii=False))
    return 0


def export_count(workdir: str, name: str, kversion: int, limit: int) -> int:
    """流式导出全部并只计数（规模演示的受计量子进程）。"""
    t = H.HistoryTable(workdir, name)
    total = 0
    entities = 0
    last_ent = None
    cur = None
    stats_sum = {"candidates": 0, "events_examined": 0}
    while True:
        page = t.export(kversion=kversion, limit=limit) if cur is None else \
            t.export(limit=limit, cursor=cur)
        for r in page["records"]:
            total += 1
            if r["entity"] != last_ent:
                entities += 1
                last_ent = r["entity"]
        stats_sum["candidates"] += page["stats"]["candidates"]
        stats_sum["events_examined"] += page["stats"]["events_examined"]
        cur = page["cursor"]
        if not cur:
            break
    print(json.dumps({"ok": True, "records": total, "entities": entities,
                      "stats": stats_sum}, ensure_ascii=False))
    return 0


def main() -> int:
    action = sys.argv[1]
    if action == "commit-race":
        return commit_race(*sys.argv[2:7])
    if action == "ingest-file":
        return ingest_file(*sys.argv[2:6])
    if action == "export-count":
        return export_count(sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5]))
    raise SystemExit(f"未知动作: {action}")


if __name__ == "__main__":
    raise SystemExit(main())
