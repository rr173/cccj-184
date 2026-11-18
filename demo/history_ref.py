"""双时间历史表的独立小规模参考实现。

只用于演示/测试核对：从原始输入文件重建事实，用朴素内存算法实现规格语义，
**不调用 shuffle.history 的任何产品区间逻辑**（分区、桶索引、扫描线都与产品
无关），保证参考链路独立。

语义（与规格文字一一对应）：
  * 知识版本 K = 批次序号（1 起）；批次 = 有序行清单，行位置 = (split 清单序,
    row_number)。
  * 断言 {event_id, valid_from, valid_to(None=+∞), payload}；撤回 {event_id,
    target_event_id}。
  * 时点 T 在版本 K 的获胜者：版本 ≤ K、未被 ≤ K 的撤回命中、覆盖 T 的断言中，
    (kversion, split 序, row_number) 最大者。
  * 时间线：把区间按所有相关端点切开，逐段求获胜者；只有获胜 event_id 相同
    的相邻片段才合并。
"""

from __future__ import annotations

import json
from typing import Any, Optional

INF = None  # valid_to 为 None 表示正无穷


def canon(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


class RefInvalid(Exception):
    """参考实现判定批次非法。"""


class RefTable:
    def __init__(self) -> None:
        # (entity, event_id, op, vf, vt, payload_canon, target, kversion, sord, srow)
        self.events: list[tuple] = []
        self.head = 0

    # ------------------------------------------------------------------ #
    def validate_and_add_batch(self, rows: list[tuple[str, dict, int, int]]) -> None:
        """rows: (entity, value, split_ord, row_number)。整批原子：非法即抛。"""
        kversion = self.head + 1
        staged = []
        seen_ids = set()
        batch_retracted: set = set()
        committed_ids = {e[1] for e in self.events}
        retracted = {e[6] for e in self.events if e[2] == "retract"}
        by_id = {e[1]: e for e in self.events}
        for entity, value, sord, srow in rows:
            if entity is None or not isinstance(entity, str):
                raise RefInvalid("key 必须是 text（null 非法）")
            if not isinstance(value, dict):
                raise RefInvalid("value 必须是对象")
            eid = value.get("event_id")
            op = value.get("op")
            if not isinstance(eid, str):
                raise RefInvalid("event_id 必须是 text")
            if eid in seen_ids or eid in committed_ids:
                raise RefInvalid(f"event_id 重复: {eid!r}")
            if op == "assert":
                vf, vt = value["valid_from"], value["valid_to"]
                if not isinstance(vf, int) or isinstance(vf, bool):
                    raise RefInvalid("valid_from 必须是整数")
                if vt is not None and (not isinstance(vt, int) or isinstance(vt, bool)):
                    raise RefInvalid("valid_to 必须是整数或 null")
                if vt is not None and not vf < vt:
                    raise RefInvalid("区间为空或反向")
                payload_c = canon(value["payload"])
                staged.append((entity, eid, "assert", vf, vt, payload_c, None,
                               kversion, sord, srow))
            elif op == "retract":
                target = value.get("target_event_id")
                if not isinstance(target, str):
                    raise RefInvalid("target_event_id 必须是 text")
                if target not in by_id:
                    raise RefInvalid(f"撤回目标不存在: {target!r}")
                te = by_id[target]
                if te[2] != "assert":
                    raise RefInvalid("撤回目标不是断言")
                if te[0] != entity:
                    raise RefInvalid("错实体撤回")
                if te[7] >= kversion:
                    raise RefInvalid("前向引用")
                if target in retracted or target in batch_retracted:
                    raise RefInvalid("再次撤回")
                staged.append((entity, eid, "retract", None, None, None, target,
                               kversion, sord, srow))
                batch_retracted.add(target)
            else:
                raise RefInvalid(f"非法 op: {op!r}")
            seen_ids.add(eid)
        self.events.extend(staged)
        self.head = kversion

    # ------------------------------------------------------------------ #
    def _alive_asserts(self, entity: str, K: int) -> list[tuple]:
        retracted = {e[6] for e in self.events
                     if e[2] == "retract" and e[7] <= K}
        return [e for e in self.events
                if e[0] == entity and e[2] == "assert" and e[7] <= K
                and e[1] not in retracted]

    def winner_at(self, entity: str, t: int, K: int) -> Optional[tuple]:
        best = None
        for e in self._alive_asserts(entity, K):
            vf, vt = e[3], e[4]
            if vf <= t and (vt is None or t < vt):
                pri = (e[7], e[8], e[9])
                if best is None or pri > (best[7], best[8], best[9]):
                    best = e
        return best

    def timeline(self, entity: str, t0: int, t1, K: int) -> list[dict]:
        """完整切分 [t0, t1)；t1 为 None 表示正无穷。空洞 present=False。"""
        asserts = self._alive_asserts(entity, K)
        points = {t0}
        if t1 is not None:
            points.add(t1)
        for e in asserts:
            vf, vt = e[3], e[4]
            lo = max(vf, t0)
            finite_ups = [x for x in (vt, t1) if x is not None]
            up = min(finite_ups) if finite_ups else None   # None = +∞
            if up is None or lo < up:
                points.add(lo)
                if up is not None:
                    points.add(up)
        ordered = sorted(p for p in points if p is not None)
        if t1 is None:
            ordered.append(None)  # 末尾正无穷
        segments = []
        for i in range(len(ordered) - 1):
            s = ordered[i]
            e = ordered[i + 1]
            w = self.winner_at(entity, s, K)
            segments.append({"start": s, "end": e,
                             "event_id": w[1] if w else None,
                             "payload": json.loads(w[5]) if w else None,
                             "present": w is not None,
                             "kversion": w[7] if w else None})
        # 只有获胜 event_id 相同的相邻片段才合并
        merged: list[dict] = []
        for seg in segments:
            if merged and seg["present"] and merged[-1]["present"] \
                    and merged[-1]["event_id"] == seg["event_id"]:
                merged[-1]["end"] = seg["end"]
            else:
                merged.append(dict(seg))
        return merged

    def diff(self, entity: str, t0: int, t1, kold: int, knew: int) -> list[dict]:
        old = self.timeline(entity, t0, t1, kold)
        new = self.timeline(entity, t0, t1, knew)
        points = sorted({s["start"] for s in old + new if s["start"] is not None}
                        | {s["end"] for s in old + new if s["end"] is not None})
        if t1 is None:
            points.append(None)

        def at(tl, t):
            for s in tl:
                if s["start"] <= t and (s["end"] is None or t < s["end"]):
                    return s
            raise AssertionError("参考时间线未覆盖区间")

        changes = []
        for i in range(len(points) - 1):
            a, b = points[i], points[i + 1]
            so, sn = at(old, a), at(new, a)
            ko = (so["present"], canon(so["payload"]) if so["present"] else None,
                  so["event_id"])
            kn = (sn["present"], canon(sn["payload"]) if sn["present"] else None,
                  sn["event_id"])
            if ko != kn:
                changes.append({"start": a, "end": b, "old": so, "new": sn})
        return changes


def load_batch_rows(path_splits: list[tuple[str, int]], ) -> list[tuple]:
    """从原始 JSONL 输入重建批次行：(entity, value, split_ord, row_number)。

    path_splits: [(path, split_ord), ...]，按 split 清单序。
    """
    rows = []
    for path, sord in path_splits:
        with open(path, "r", encoding="utf-8") as fh:
            for rn, line in enumerate(fh, start=1):
                obj = json.loads(line)
                rows.append((obj["key"], obj["value"], sord, rn))
    return rows
