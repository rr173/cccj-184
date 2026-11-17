"""朴素参考实现与测试工具：直接用 Python 字典/列表在内存中算关联。

只用于小规模核对——不调用 shuffle.join 的任何产品逻辑，数据来自原始输入
文件（而不是关联算子的中间产物），保证参考链路独立。
"""

from __future__ import annotations

import json
import os
from typing import Optional


def load_source_rows(path: str, split_id: str) -> list[dict]:
    """从原始 JSONL 输入重建带身份的行（split_id/row_number/key/value）。"""
    rows = []
    with open(path, "r", encoding="utf-8") as fh:
        for rn, line in enumerate(fh, start=1):
            obj = json.loads(line)
            rows.append({"split_id": split_id, "row_number": rn,
                         "key": obj["key"], "value": obj["value"]})
    return rows


def naive_join(left_rows: list[dict], right_rows: list[dict],
               mode: str) -> list[tuple[Optional[dict], Optional[dict]]]:
    """内存朴素关联：精确 text 相等；null 永不匹配（含 null 对 null）。

    输出次序与产品规定一致：左源序驱动，匹配右行按右源序；
    full 最后按右源序追加未匹配右行。重复行各自参与（m×n）。
    """
    right_by_key: dict[str, list[dict]] = {}
    for r in right_rows:
        if r["key"] is not None:
            right_by_key.setdefault(r["key"], []).append(r)
    left_keys = {l["key"] for l in left_rows if l["key"] is not None}
    out: list[tuple[Optional[dict], Optional[dict]]] = []
    for l in left_rows:
        matches = right_by_key.get(l["key"], []) if l["key"] is not None else []
        if matches:
            for r in matches:
                out.append((l, r))
        elif mode in ("left", "full"):
            out.append((l, None))
    if mode == "full":
        for r in right_rows:
            if r["key"] is None or r["key"] not in left_keys:
                out.append((None, r))
    return out


def comparable(result_rows: list[dict]) -> list[tuple]:
    """把产品结果行转成可比较元组序列（含次序与重数）。"""
    out = []
    for row in result_rows:
        left = tuple(sorted(row["left"].items())) if row["left"] is not None else None
        right = tuple(sorted(row["right"].items())) if row["right"] is not None else None
        out.append((row["seq"], left, right))
    return out


def expected_comparable(pairs: list[tuple]) -> list[tuple]:
    out = []
    for i, (l, r) in enumerate(pairs):
        left = tuple(sorted(l.items())) if l is not None else None
        right = tuple(sorted(r.items())) if r is not None else None
        out.append((i, left, right))
    return out


def write_input(path: str, rows: list[tuple]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for key, value in rows:
            fh.write(json.dumps({"key": key, "value": value}, ensure_ascii=False) + "\n")
