"""共享协议：确定性分区规则、行/段摘要、段文件格式、SQLite schema 与路径布局。

所有摘要均为 SHA-256，分区完全由 ``(key, partition_version, num_partitions)``
决定，不依赖 Python 进程内随机 hash。

段文件布局（小端整数）::

    MAGIC(8) | header_len(4) | header_json(bytes)
    重复 record: row_len(4) | row_payload(32B digest + utf8 canonical json)
    FOOTER_MAGIC(8) | footer_len(4) | footer_json
    FOOTER_MAGIC = b"SHUFF1F\n"

header/footer 都是 UTF-8 JSON。读取器按 frame 流式读，不在内存攒整个分区。
"""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from typing import Any, Optional

MAGIC = b"SHUFF1SE"
FOOTER_MAGIC = b"SHUFF1F\n"
NULL_KEY_TOKEN = b"\x00NULL"
SCHEMA_VERSION = 1
BUSY_TIMEOUT_MS = 30_000

# 作业/attempt 生命周期状态
JOB_OPEN = "open"
JOB_CANCELLED = "cancelled"
JOB_DESTROYED = "destroyed"
ATT_RUNNING = "running"
ATT_COMPLETED = "completed"      # 段齐备但尚未（或未通过）确认
ATT_WINNER = "winner"
ATT_FAILED = "failed"
ATT_LOST = "lost"                # 另一 attempt 先确认，本 attempt 为输者


class ShuffleError(Exception):
    """本模块所有可预期失败的基类。"""


class JobCancelled(ShuffleError):
    """作业已取消：拒绝新读取，未开始读取不得返回缺行成功流。"""


class NotReady(ShuffleError):
    """固定获胜清单中仍有 split 未确认 winner（非阻塞读取时）。"""


class SegmentCorrupt(ShuffleError):
    """段文件损坏、截断或 frame 不合法。"""


class DigestMismatch(ShuffleError):
    """行/段/源 split 摘要核对失败。"""


class IdentityChanged(ShuffleError):
    """源 split 摘要与建作业时绑定不一致——作业输入绑定不可修改。"""


class NoAttemptRunning(ShuffleError):
    """没有处于 running 状态的 attempt，不能重新派工以外的操作。"""


# --------------------------------------------------------------------------- #
# 确定性、公开的摘要与分区规则
# --------------------------------------------------------------------------- #
def sha256_hex(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


def canonical_key_bytes(key: Optional[str]) -> bytes:
    """text 键：UTF-8 字节；null 键：固定哨兵 ``b"\\x00NULL"``。"""
    if key is None:
        return NULL_KEY_TOKEN
    if not isinstance(key, str):
        raise ShuffleError(f"分区键必须是 text 或 null，得到 {type(key).__name__}")
    return key.encode("utf-8")


def partition_of(key: Optional[str], num_partitions: int, version: int) -> int:
    """公开确定性分区函数。

    规则（与语言无关，可在读取器侧独立复算）：
      1. text 键取 UTF-8 字节；null 键取固定哨兵 ``0x00 + b"NULL"``；
      2. 前缀 ``b"shuffle-pv" + u8(version)`` 后做 SHA-256；
      3. 取摘要前 8 字节按大端解释成无符号整数，对目标分区数取模。
    """
    if num_partitions <= 0:
        raise ShuffleError("目标分区数必须为正整数")
    if version < 0:
        raise ShuffleError("分区函数版本不能为负")
    import hashlib

    h = hashlib.sha256()
    h.update(b"shuffle-pv")
    h.update(bytes([version & 0xFF]))
    h.update(canonical_key_bytes(key))
    bucket = int.from_bytes(h.digest()[:8], "big")
    return bucket % num_partitions


def canonical_row_bytes(split_id: str, row_number: int, key: Optional[str], value: Any) -> bytes:
    """单行的规范化字节序列：行摘要、段内 digest 链都基于它。

    重复内容只要 ``(split_id, row_number)`` 不同就是不同的行，不做合并。
    """
    if not isinstance(row_number, int) or row_number <= 0:
        raise ShuffleError("row_number 必须是从 1 开始的正整数")
    payload = json.dumps(
        {"split_id": split_id, "row_number": row_number, "key": key, "value": value},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return payload


def row_digest(payload: bytes) -> str:
    return sha256_hex(payload)


# --------------------------------------------------------------------------- #
# 路径布局
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Layout:
    root: str

    @property
    def db_path(self) -> str:
        return os.path.join(self.root, "coordinator.sqlite3")

    def input_dir(self, job_id: str) -> str:
        return os.path.join(self.root, job_id, "inputs")

    def seg_dir(self, job_id: str, split_id: str, attempt_no: int) -> str:
        return os.path.join(self.root, job_id, "segments", _safe(split_id), f"attempt-{attempt_no}")

    def seg_path(self, job_id: str, split_id: str, attempt_no: int, partition: int, seq: int) -> str:
        return os.path.join(
            self.seg_dir(job_id, split_id, attempt_no), f"p{partition}-s{seq}.seg"
        )

    def tmp_path(self, job_id: str, split_id: str, attempt_no: int, partition: int, seq: int) -> str:
        return self.seg_path(job_id, split_id, attempt_no, partition, seq) + ".tmp"

    def job_dir(self, job_id: str) -> str:
        return os.path.join(self.root, job_id)


def _safe(name: str) -> str:
    """split_id 只允许文件系统安全字符，杜绝路径穿越。"""
    if not name or any(c in name for c in "/\\\x00") or name in (".", ".."):
        raise ShuffleError(f"非法 split_id: {name!r}")
    return name


# --------------------------------------------------------------------------- #
# SQLite
# --------------------------------------------------------------------------- #
SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS jobs(
    job_id            TEXT PRIMARY KEY,
    num_partitions    INTEGER NOT NULL CHECK (num_partitions > 0),
    partition_version INTEGER NOT NULL CHECK (partition_version >= 0),
    state             TEXT NOT NULL,
    created_at        REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS job_splits(
    job_id        TEXT NOT NULL REFERENCES jobs(job_id),
    ord           INTEGER NOT NULL,           -- 固定清单次序，从 0 开始
    split_id      TEXT NOT NULL,
    input_path    TEXT NOT NULL,
    input_digest  TEXT NOT NULL,              -- 建作业时绑定的源摘要
    row_count     INTEGER NOT NULL,
    PRIMARY KEY (job_id, split_id),
    UNIQUE (job_id, ord)
);

CREATE TABLE IF NOT EXISTS attempts(
    job_id        TEXT NOT NULL REFERENCES jobs(job_id),
    split_id      TEXT NOT NULL,
    attempt_no    INTEGER NOT NULL,
    state         TEXT NOT NULL,              -- running/completed/winner/failed/lost
    pid           INTEGER,
    mem_bytes     INTEGER NOT NULL,           -- 每分区内存上限（字节）
    launched_at   REAL NOT NULL,
    finished_at   REAL,
    claimed_at    REAL,
    PRIMARY KEY (job_id, split_id, attempt_no),
    FOREIGN KEY (job_id, split_id) REFERENCES job_splits(job_id, split_id)
);

CREATE TABLE IF NOT EXISTS segments(
    job_id        TEXT NOT NULL,
    split_id      TEXT NOT NULL,
    attempt_no    INTEGER NOT NULL,
    partition_id  INTEGER NOT NULL,
    seq           INTEGER NOT NULL,           -- 段内次序，从 0 开始
    relpath       TEXT NOT NULL,
    row_count     INTEGER NOT NULL,
    byte_length   INTEGER NOT NULL,
    seg_digest    TEXT NOT NULL,              -- 对所有规范行字节流式摘要
    state         TEXT NOT NULL,              -- tmp/done/reaped
    PRIMARY KEY (job_id, split_id, attempt_no, partition_id, seq),
    FOREIGN KEY (job_id, split_id, attempt_no)
        REFERENCES attempts(job_id, split_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS winners(
    job_id       TEXT NOT NULL,
    split_id     TEXT NOT NULL,
    attempt_no   INTEGER NOT NULL,
    confirmed_at REAL NOT NULL,
    PRIMARY KEY (job_id, split_id),
    FOREIGN KEY (job_id, split_id, attempt_no)
        REFERENCES attempts(job_id, split_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS readers(
    reader_id    TEXT PRIMARY KEY,
    job_id       TEXT NOT NULL REFERENCES jobs(job_id),
    opened_at    REAL NOT NULL,
    closed_at    REAL,
    state        TEXT NOT NULL               -- open/closed/failed
);

CREATE TABLE IF NOT EXISTS events(
    ts        REAL NOT NULL,
    job_id    TEXT NOT NULL,
    split_id  TEXT,
    attempt_no INTEGER,
    kind      TEXT NOT NULL,
    detail    TEXT NOT NULL
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=BUSY_TIMEOUT_MS / 1000.0)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


def log_event(conn: sqlite3.Connection, job_id: str, kind: str, detail: str = "",
              split_id: Optional[str] = None, attempt_no: Optional[int] = None) -> None:
    import time

    conn.execute(
        "INSERT INTO events(ts, job_id, split_id, attempt_no, kind, detail) VALUES (?,?,?,?,?,?)",
        (time.time(), job_id, split_id, attempt_no, kind, detail),
    )


def json_dumps(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def json_loads(data: bytes) -> Any:
    # 拒绝重复键，避免被构造的段用重复字段掩盖身份问题
    def _hook(pairs):
        seen = set()
        for k, _ in pairs:
            if k in seen:
                raise SegmentCorrupt(f"段 JSON 出现重复键: {k}")
            seen.add(k)
        return dict(pairs)

    return json.loads(data.decode("utf-8"), object_pairs_hook=_hook)
