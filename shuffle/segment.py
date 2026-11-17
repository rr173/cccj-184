"""分区段文件的流式写入器与流式校验读取器。

写入器按目标分区各持一个受限内存缓冲；缓冲达到上限就溢写为一个新的段文件，
因此同一分区允许出现多个段，且任何时刻内存里都不保存整个分区。
"""

from __future__ import annotations

import hashlib
import os
import struct
from dataclasses import dataclass
from typing import BinaryIO, Iterator, Optional

from .common import (
    FOOTER_MAGIC,
    MAGIC,
    SegmentCorrupt,
    canonical_row_bytes,
    json_dumps,
    json_loads,
    partition_of,
)


@dataclass
class SegmentRecord:
    """段登记信息（与 SQLite ``segments`` 表一一对应）。"""

    partition_id: int
    seq: int
    relpath: str
    row_count: int
    byte_length: int
    seg_digest: str


class _PartitionSpiller:
    """单个目标分区的有界内存写入器，可溢写多个段。"""

    def __init__(self, job_id: str, split_id: str, attempt_no: int, partition_id: int,
                 mem_bytes: int, layout) -> None:
        self.job_id = job_id
        self.split_id = split_id
        self.attempt_no = attempt_no
        self.partition_id = partition_id
        self.mem_bytes = max(64, mem_bytes)
        self.layout = layout
        self._buf: list[bytes] = []
        self._buf_size = 0
        self._seq = 0
        self._seg_hasher = hashlib.sha256()
        self._seg_rows = 0
        self._fh: Optional[BinaryIO] = None
        self._tmp_path: Optional[str] = None
        self._final_path: Optional[str] = None
        self.records: list[SegmentRecord] = []

    # ------------------------------------------------------------------ #
    def _open_segment(self) -> None:
        assert self._fh is None
        path = self.layout.tmp_path(self.job_id, self.split_id, self.attempt_no,
                                    self.partition_id, self._seq)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._tmp_path = path
        self._final_path = self.layout.seg_path(self.job_id, self.split_id, self.attempt_no,
                                                self.partition_id, self._seq)
        fh = open(path, "wb")
        header = json_dumps({
            "magic": "shuffle-segment/1",
            "job_id": self.job_id,
            "split_id": self.split_id,
            "attempt_no": self.attempt_no,
            "partition_id": self.partition_id,
            "seq": self._seq,
        })
        fh.write(MAGIC)
        fh.write(struct.pack("<I", len(header)))
        fh.write(header)
        self._fh = fh
        self._seg_hasher = hashlib.sha256()
        self._seg_rows = 0

    def add_row(self, payload: bytes) -> None:
        if self._fh is None:
            self._open_segment()
        assert self._fh is not None
        # frame = row_len(4) | 32B 原始 SHA-256 | 规范行字节
        digest_raw = hashlib.sha256(payload).digest()
        frame = struct.pack("<I", len(payload)) + digest_raw + payload
        self._fh.write(frame)
        # 故障演示需要：确保“写一半被杀”时字节已落盘成为真实半截 .tmp
        if os.environ.get("SHUFFLE_HOOK") == "kill-mid-segment":
            self._fh.flush()
            os.fsync(self._fh.fileno())
        self._seg_hasher.update(payload)
        self._seg_rows += 1
        self._buf_size += len(frame)
        # 缓冲只用于判断溢写阈值；frame 已直接落盘，这里记账即“受限内存”
        self._buf.append(frame)
        if self._buf_size >= self.mem_bytes:
            self.flush_spill()

    def flush_spill(self) -> None:
        """把当前段封尾并原子改名，开启下一个 seq（若还有行）。"""
        if self._fh is None:
            return
        self._close_segment()
        self._seq += 1
        self._buf = []
        self._buf_size = 0

    def _close_segment(self) -> None:
        assert self._fh is not None and self._tmp_path and self._final_path
        footer = json_dumps({
            "row_count": self._seg_rows,
            "seg_digest": self._seg_hasher.hexdigest(),
        })
        self._fh.write(FOOTER_MAGIC)
        self._fh.write(struct.pack("<I", len(footer)))
        self._fh.write(footer)
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._fh.close()
        self._fh = None
        size = os.path.getsize(self._tmp_path)
        os.replace(self._tmp_path, self._final_path)
        relpath = os.path.relpath(self._final_path, self.layout.job_dir(self.job_id))
        self.records.append(SegmentRecord(
            partition_id=self.partition_id,
            seq=self._seq,
            relpath=relpath,
            row_count=self._seg_rows,
            byte_length=size,
            seg_digest=self._seg_hasher.hexdigest(),
        ))

    def finish(self) -> list[SegmentRecord]:
        """封尾最后一个段；空分区也会产生一个 0 行的明确完成证据段。"""
        if self._fh is None:
            self._open_segment()
        self._close_segment()
        return list(self.records)

    def abort(self) -> None:
        if self._fh is not None:
            try:
                self._fh.close()
            except OSError:
                pass
        # .tmp 留给清理器按状态回收，不在这里吞掉异常掩盖问题


class SpillingShuffleWriter:
    """工人侧：一次 attempt 的全部分区写入器。"""

    def __init__(self, job_id: str, split_id: str, attempt_no: int,
                 num_partitions: int, partition_version: int, mem_bytes: int, layout) -> None:
        self.partition_version = partition_version
        self.spillers = [
            _PartitionSpiller(job_id, split_id, attempt_no, p, mem_bytes, layout)
            for p in range(num_partitions)
        ]

    def write_row(self, split_id: str, row_number: int, key, value) -> None:
        payload = canonical_row_bytes(split_id, row_number, key, value)
        # 行自带 split/row_number 身份；分区只由 key 决定
        p = partition_of(key, len(self.spillers), self.partition_version)
        self.spillers[p].add_row(payload)

    def close(self) -> list[SegmentRecord]:
        out: list[SegmentRecord] = []
        for sp in self.spillers:
            out.extend(sp.finish())
        return out

    def abort(self) -> None:
        for sp in self.spillers:
            sp.abort()


# --------------------------------------------------------------------------- #
# 流式校验读取
# --------------------------------------------------------------------------- #
def _read_exact(fh: BinaryIO, n: int, what: str) -> bytes:
    data = fh.read(n)
    if len(data) != n:
        raise SegmentCorrupt(f"段被截断：读取 {what} 需要 {n} 字节，实际 {len(data)}")
    return data


def open_segment(path: str, expected: dict) -> tuple[BinaryIO, dict]:
    """打开段并核对 header 身份。返回 (文件句柄, header)。调用方负责关闭。"""
    fh = open(path, "rb")
    try:
        magic = fh.read(len(MAGIC))
        if magic != MAGIC:
            raise SegmentCorrupt(f"{path}: 魔数不符")
        (hlen,) = struct.unpack("<I", _read_exact(fh, 4, "header length"))
        if not 0 < hlen <= 1 << 20:
            raise SegmentCorrupt(f"{path}: 非法 header 长度 {hlen}")
        try:
            header = json_loads(_read_exact(fh, hlen, "header"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise SegmentCorrupt(f"{path}: header 损坏无法解析: {exc}") from exc
        for field in ("job_id", "split_id", "attempt_no", "partition_id", "seq"):
            if header.get(field) != expected[field]:
                raise SegmentCorrupt(
                    f"{path}: header 字段 {field}={header.get(field)!r} 与登记 {expected[field]!r} 不符"
                )
        return fh, header
    except Exception:
        fh.close()
        raise


def iter_segment_rows(path: str, expected: dict) -> Iterator[tuple[bytes, str]]:
    """逐行产出 ``(规范行字节, 行摘要)``，同时流式核对行摘要与段摘要/行数。

    整个函数不缓存分区，只保留固定大小的读缓冲。
    """
    fh, _header = open_segment(path, expected)
    seg_hasher = hashlib.sha256()
    rows = 0
    try:
        footer_size = len(FOOTER_MAGIC) + 4
        while True:
            peek = fh.read(4)
            if len(peek) < 4:
                raise SegmentCorrupt(f"{path}: 记录区截断")
            # 区分 record 与 footer：footer 以 FOOTER_MAGIC 开头
            if peek == FOOTER_MAGIC[:4]:
                rest = _read_exact(fh, len(FOOTER_MAGIC) - 4, "footer magic")
                if peek + rest != FOOTER_MAGIC:
                    raise SegmentCorrupt(f"{path}: footer 魔数损坏")
                (flen,) = struct.unpack("<I", _read_exact(fh, 4, "footer length"))
                if not 0 < flen <= 1 << 20:
                    raise SegmentCorrupt(f"{path}: 非法 footer 长度 {flen}")
                try:
                    footer = json_loads(_read_exact(fh, flen, "footer"))
                except (UnicodeDecodeError, ValueError) as exc:
                    raise SegmentCorrupt(f"{path}: footer 损坏无法解析: {exc}") from exc
                tail = fh.read(1)
                if tail:
                    raise SegmentCorrupt(f"{path}: footer 之后存在多余字节")
                break
            (plen,) = struct.unpack("<I", peek)
            if plen <= 0 or plen > 64 * 1024 * 1024:
                raise SegmentCorrupt(f"{path}: 非法行长度 {plen}")
            digest_raw = _read_exact(fh, 32, "row digest")
            payload = _read_exact(fh, plen, "row payload")
            actual_raw = hashlib.sha256(payload).digest()
            if actual_raw != digest_raw:
                raise DigestMismatchLocal(f"{path}: 第 {rows + 1} 行行摘要不符")
            digest_hex = digest_raw.hex()
            seg_hasher.update(payload)
            rows += 1
            yield payload, digest_hex
        if footer.get("row_count") != rows:
            raise SegmentCorrupt(
                f"{path}: footer 行数 {footer.get('row_count')} 与实际 {rows} 不符"
            )
        if not isinstance(footer.get("seg_digest"), str) or len(footer["seg_digest"]) != 64:
            raise SegmentCorrupt(f"{path}: footer seg_digest 结构非法")
        if footer.get("seg_digest") != seg_hasher.hexdigest():
            raise DigestMismatchLocal(f"{path}: 段摘要不符")
    finally:
        fh.close()


class DigestMismatchLocal(SegmentCorrupt):
    """摘要不符属于段损坏的一种，读取器统一按显式失败处理。"""


def segment_byte_length(path: str) -> int:
    return os.path.getsize(path)
