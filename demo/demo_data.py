"""演示/测试数据生成与独立校验工具（不依赖协调器状态，按公开规则复算）。"""

from __future__ import annotations

import json
import os
from typing import Optional

from shuffle.common import canonical_row_bytes, partition_of, sha256_hex


def write_jsonl(path: str, rows: list[tuple[Optional[str], object]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for key, value in rows:
            fh.write(json.dumps({"key": key, "value": value}, ensure_ascii=False) + "\n")


def expected_identities(rows: list[tuple[Optional[str], object]], split_id: str,
                        num_partitions: int, version: int) -> dict[int, list[tuple[str, int]]]:
    """独立按公开分区函数计算每个分区应有的行身份与顺序。

    返回 partition -> [(split_id, row_number), ...]，分区内按 row_number 排序。
    """
    out: dict[int, list[tuple[str, int]]] = {p: [] for p in range(num_partitions)}
    for rn, (key, _value) in enumerate(rows, start=1):
        out[partition_of(key, num_partitions, version)].append((split_id, rn))
    for p in out:
        out[p].sort(key=lambda t: t[1])
    return out


def expected_row_digest(split_id: str, row_number: int, key, value) -> str:
    return sha256_hex(canonical_row_bytes(split_id, row_number, key, value))


def directory_size(path: str) -> int:
    total = 0
    for dirpath, _dirs, files in os.walk(path):
        for name in files:
            fp = os.path.join(dirpath, name)
            if os.path.isfile(fp):
                total += os.path.getsize(fp)
    return total


def count_files(path: str, suffix: Optional[str] = None) -> int:
    n = 0
    for dirpath, _dirs, files in os.walk(path):
        for name in files:
            if suffix is None or name.endswith(suffix):
                n += 1
    return n
