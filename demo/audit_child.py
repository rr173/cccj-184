"""审计演示子进程助手（真实独立进程，只经 SQLite/文件与父进程交互）。

用法：python -m demo.audit_child <action> ...
  read-count ROOT NAME LIMIT
      流式分页读完审计结果，校验固定次序严格递增，输出计数 JSON。
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shuffle import audit as A


def _sort_key(rec: dict) -> tuple:
    tsort = b"\x00" if rec["target"] is None else b"\x01" + rec["target"].encode("utf-8")
    return (rec["entity"].encode("utf-8"), rec["rule_id"].encode("utf-8"),
            tsort, rec["start"], rec["event_id"].encode("utf-8"))


def read_count(root: str, name: str, limit: int) -> int:
    total = 0
    prev_key = None
    prev_end = None
    prev_merge_key = None
    cur = None
    while True:
        page = A.read_audit(root, name, limit=limit, cursor=cur)
        for rec in page["records"]:
            key = _sort_key(rec)
            if prev_key is not None:
                if key <= prev_key:
                    raise SystemExit(f"结果次序损坏: {rec} 不在 {prev_key} 之后")
                mk = (rec["entity"], rec["rule_id"], rec["target"],
                      rec["event_id"], rec["reason"])
                if mk == prev_merge_key and prev_end is not None \
                        and prev_end == rec["start"]:
                    raise SystemExit(f"存在应合并而未合并的相邻记录: {rec}")
                prev_merge_key = mk
            else:
                prev_merge_key = (rec["entity"], rec["rule_id"], rec["target"],
                                  rec["event_id"], rec["reason"])
            prev_key = key
            prev_end = rec["end"]
            if rec["seq"] != total:
                raise SystemExit(f"结果序号不连续: {rec['seq']} != {total}")
            total += 1
        cur = page["cursor"]
        if not cur:
            break
    print(json.dumps({"ok": True, "records": total}, ensure_ascii=False))
    return 0


def main() -> int:
    action = sys.argv[1]
    if action == "read-count":
        return read_count(sys.argv[2], sys.argv[3], int(sys.argv[4]))
    raise SystemExit(f"未知动作: {action}")


if __name__ == "__main__":
    raise SystemExit(main())
