"""时态引用审计的独立小规模参考实现（端点枚举）。

只用于演示/测试核对：从原始 JSONL 输入重建事实，用朴素内存算法实现规格语义，
**不调用 shuffle.history 或 shuffle.audit 的任何产品逻辑**（不读桶索引、不调
产品 timeline/扫描线/审计），保证参考链路独立。

语义与规格文字一一对应：
  * 知识版本 K = 批次序号（1 起）；断言/撤回与历史表同一模型。
  * 时点 T 在版本 K 的获胜者：版本 ≤ K、未被 ≤ K 撤回命中、覆盖 T 的断言中
    (kversion, split 清单序, row_number) 最大者（暴力比较）。
  * 源引用区间：把 [t0,t1) 按源实体全部相关端点切成原子区间，逐段求获胜者，
    相邻同 event_id 合并。
  * 目标存在：同一目标在源区间内按目标端点再切原子区间，逐段判 present
    （payload 为 null 仍算存在）；缺失的极大连通子区间即缺失输出。
  * 输出排序/合并与产品规格一致：源实体 UTF-8 字节序 → rule_id → 目标键
    UTF-8 字节序（null 最前）→ 区间起点 → 源 event_id；相邻且来源字段完全
    相同的区间才合并。
"""

from __future__ import annotations

import json
from typing import Any, Optional

INF = None  # valid_to / 区间终点为 None 表示正无穷


class RefError(Exception):
    """参考实现判定规则/数据非法（非字符串目标键、不支持的 JSON 形态等）。"""


def _canon(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


class RefAudit:
    """从原始行重建的事实集 + 端点枚举审计。"""

    def __init__(self) -> None:
        # (entity, event_id, op, vf, vt, payload, target_event_id, kv, sord, srow)
        self.events: list[tuple] = []
        self.head = 0

    # ------------------------------------------------------------------ #
    def add_batch(self, rows: list[tuple[str, dict, int, int]]) -> None:
        """rows: (entity, value, split_ord, row_number)。输入须为合法批次。"""
        self.head += 1
        for entity, value, sord, srow in rows:
            op = value.get("op")
            if op == "assert":
                self.events.append((entity, value["event_id"], "assert",
                                    value["valid_from"], value["valid_to"],
                                    value["payload"], None,
                                    self.head, sord, srow))
            elif op == "retract":
                self.events.append((entity, value["event_id"], "retract",
                                    None, None, None, value["target_event_id"],
                                    self.head, sord, srow))
            else:
                raise RefError(f"非法 op: {op!r}")

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

    # ------------------------------------------------------------------ #
    def _segments(self, entity: str, t0: int, t1, K: int) -> list[list]:
        """端点枚举：把 [t0,t1) 切成原子区间，逐段求获胜者；相邻同 event_id
        合并，相邻空洞合并。t1 为 None 表示正无穷。"""
        asserts = self._alive_asserts(entity, K)
        points = {t0}
        if t1 is not None:
            points.add(t1)
        for e in asserts:
            vf, vt = e[3], e[4]
            lo = max(vf, t0)
            ups = [x for x in (vt, t1) if x is not None]
            up = min(ups) if ups else None
            if up is None or lo < up:
                points.add(lo)
                if up is not None:
                    points.add(up)
        ordered = sorted(p for p in points if p is not None)
        if t1 is None:
            ordered.append(None)
        merged: list[list] = []
        for i in range(len(ordered) - 1):
            a, b = ordered[i], ordered[i + 1]
            w = self.winner_at(entity, a, K)
            if merged:
                pw = merged[-1][2]
                if w is None and pw is None:
                    merged[-1][1] = b
                    continue
                if w is not None and pw is not None and pw[1] == w[1]:
                    merged[-1][1] = b
                    continue
            merged.append([a, b, w])
        return merged

    def _gaps(self, entity: str, s: int, e, K: int) -> list[tuple]:
        """目标实体在 [s,e) 内的缺失子区间（空洞；e 为 None 表示正无穷）。"""
        return [(a, b) for a, b, w in self._segments(entity, s, e, K) if w is None]

    # ------------------------------------------------------------------ #
    @staticmethod
    def _extract(payload: Any, path: list, rule_id: str, entity: str,
                 event_id: str) -> tuple[str, Optional[str]]:
        node = payload
        for i, step in enumerate(path):
            if not isinstance(node, dict):
                raise RefError(
                    f"规则 {rule_id!r} 源实体 {entity!r} 事件 {event_id!r}: "
                    f"payload 在路径第 {i} 步不是对象（不支持的 JSON 形态）")
            if step not in node:
                return "missing_field", None
            node = node[step]
        if node is None:
            return "null_reference", None
        if isinstance(node, str):
            if node == "":
                return "empty_target", ""
            return "ok", node
        raise RefError(
            f"规则 {rule_id!r} 源实体 {entity!r} 事件 {event_id!r}: "
            f"目标键不是字符串（得到 {type(node).__name__}）")

    @staticmethod
    def _scope_matches(rule: dict, entity: str) -> bool:
        src = rule["source"]
        kind = src["kind"]
        if kind == "all":
            return True
        if kind == "prefix":
            return entity.startswith(src["prefix"])
        if kind == "entities":
            return entity in set(src["entities"])
        raise RefError(f"非法 source.kind: {kind!r}")

    # ------------------------------------------------------------------ #
    def run(self, rules: list[dict], t0: int, t1, K: int) -> list[dict]:
        """端点枚举审计：输出与产品规格一致的缺失区间记录（含 seq/kversion）。"""
        findings: list[dict] = []
        entities = sorted({e[0] for e in self.events},
                          key=lambda s: s.encode("utf-8"))
        for ent in entities:
            for rule in sorted(rules, key=lambda r: r["rule_id"].encode("utf-8")):
                if not self._scope_matches(rule, ent):
                    continue
                for s, e, w in self._segments(ent, t0, t1, K):
                    if w is None:
                        continue
                    kind, target = self._extract(w[5], rule["path"],
                                                 rule["rule_id"], ent, w[1])
                    if kind == "ok":
                        for gs, ge in self._gaps(target, s, e, K):
                            findings.append({"entity": ent, "rule_id": rule["rule_id"],
                                             "target": target, "start": gs, "end": ge,
                                             "event_id": w[1],
                                             "reason": "target_absent"})
                    elif kind == "empty_target":
                        findings.append({"entity": ent, "rule_id": rule["rule_id"],
                                         "target": "", "start": s, "end": e,
                                         "event_id": w[1], "reason": "empty_target"})
                    else:
                        findings.append({"entity": ent, "rule_id": rule["rule_id"],
                                         "target": None, "start": s, "end": e,
                                         "event_id": w[1], "reason": kind})

        def sort_key(f: dict) -> tuple:
            tsort = b"\x00" if f["target"] is None else b"\x01" + f["target"].encode("utf-8")
            return (f["entity"].encode("utf-8"), f["rule_id"].encode("utf-8"),
                    tsort, f["start"], f["event_id"].encode("utf-8"))

        findings.sort(key=sort_key)
        merged: list[dict] = []
        for f in findings:
            if merged:
                p = merged[-1]
                if (p["entity"] == f["entity"] and p["rule_id"] == f["rule_id"]
                        and p["target"] == f["target"]
                        and p["event_id"] == f["event_id"]
                        and p["reason"] == f["reason"]
                        and p["end"] is not None and p["end"] == f["start"]):
                    p["end"] = f["end"]
                    continue
            merged.append(dict(f))
        for i, f in enumerate(merged):
            f2 = {"seq": i, "entity": f["entity"], "rule_id": f["rule_id"],
                  "target": f["target"], "start": f["start"], "end": f["end"],
                  "kversion": K, "event_id": f["event_id"], "reason": f["reason"]}
            merged[i] = f2
        return merged


def load_batch_rows(path_splits: list[tuple[str, int]]) -> list[tuple]:
    """从原始 JSONL 输入重建批次行：(entity, value, split_ord, row_number)。"""
    rows = []
    for path, sord in path_splits:
        with open(path, "r", encoding="utf-8") as fh:
            for rn, line in enumerate(fh, start=1):
                obj = json.loads(line)
                rows.append((obj["key"], obj["value"], sord, rn))
    return rows
