"""双输入关联算子的共享底座：错误类型、框式落盘流、文件句柄账本、预算核算。

关联自有文件统一使用带逐记录摘要的框式流（小端）::

    MAGIC(8) | header_len(4) | header_json
    重复: rec_len(4) | rec_payload | rec_digest(32，对 payload 的 SHA-256)
    FOOTER_MAGIC(8) | footer_len(4) | footer_json      # 一次性文件封尾时写

两类文件：

* **一次性文件**（norm/keys/索引/排序 run）：先写 ``.tmp``，fsync 后原子改名，
  footer 记录行数与流摘要；恢复复用前必须整流校验；
* **增量文件**（recv/result.rows/result.index）：按检查点追加。崩溃恢复时
  截断到 SQLite 检查点记录的字节数，只允许重做未落定的部分；已落定的
  输入范围绝不重读源段。

预算模型（字节记账，解释器自身开销不计）：

* ``mem_bytes``：总工作内存。单行上限 = mem_bytes/16（归并堆最多 16 条
  当前记录，最坏情形仍不超总预算）；排序块 = mem_bytes/4；
  接收/结果写缓冲各 = mem_bytes/8；
* ``max_open_files``：关联自管数据文件的同时打开上限（SQLite 连接为固定
  常数个，不占额度）。归并扇入 = min(max_open_files - 保留, 16)。
"""

from __future__ import annotations

import hashlib
import os
import struct
from dataclasses import dataclass
from typing import BinaryIO, Iterator, Optional

from .common import ShuffleError, json_dumps, json_loads

J_MAGIC = b"SHJRN1F\n"
J_FOOTER_MAGIC = b"SHJRF1F\n"
J_FOOTER_PREFIX = J_FOOTER_MAGIC[:4]
FRAME_OVERHEAD = 4 + 32  # rec_len + digest
RESERVED_FILES = 10      # db/wal、输入流、输出流、索引、位图等常数句柄
HOOK_EXIT_CODE = 42      # 同步关卡受控终止的退出码


class JoinError(ShuffleError):
    """关联算子所有可预期失败的基类。"""


class JoinNotReady(JoinError):
    """某一侧源作业未就绪（缺 winner）、已取消或绑定非法。"""


class JoinFailed(JoinError):
    """关联执行或结果内容核验失败。"""


class ResultNotReady(JoinError):
    """结果尚未完整核验落盘，拒绝把已写出的前缀当作完成。"""


class BudgetExceeded(JoinError):
    """超出资源预算（单行过大、同时打开文件过多等）。"""


# --------------------------------------------------------------------------- #
# 预算
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Budget:
    mem_bytes: int
    max_open_files: int

    def __post_init__(self) -> None:
        if self.mem_bytes < 4096:
            raise JoinError("工作内存预算至少 4096 字节")
        if self.max_open_files < RESERVED_FILES + 2:
            raise JoinError(f"同时打开文件预算至少 {RESERVED_FILES + 2}")

    @property
    def single_row_limit(self) -> int:
        return self.mem_bytes // 16

    @property
    def sort_chunk(self) -> int:
        return max(self.mem_bytes // 4, self.single_row_limit)

    @property
    def recv_buffer(self) -> int:
        return self.mem_bytes // 8

    @property
    def emit_buffer(self) -> int:
        return self.mem_bytes // 8

    @property
    def fan_in(self) -> int:
        return max(2, min(self.max_open_files - RESERVED_FILES, 16))

    @property
    def index_max_entries(self) -> int:
        return max(256, min(self.mem_bytes // 64, 1 << 20))


# --------------------------------------------------------------------------- #
# 文件句柄账本：限制同时打开数并记录峰值
# --------------------------------------------------------------------------- #
class FileLedger:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.open_count = 0
        self.peak = 0

    def open(self, path: str, mode: str = "rb") -> "TrackedFile":
        if self.open_count >= self.limit:
            raise BudgetExceeded(
                f"同时打开文件数达到预算上限 {self.limit}（打开 {path} 时）"
            )
        fh = open(path, mode)
        self.open_count += 1
        self.peak = max(self.peak, self.open_count)
        return TrackedFile(self, fh, path)

    def _dec(self) -> None:
        self.open_count -= 1


class TrackedFile:
    def __init__(self, ledger: FileLedger, fh: BinaryIO, path: str) -> None:
        self._ledger = ledger
        self._fh = fh
        self.path = path
        self._closed = False

    def __getattr__(self, item: str):
        return getattr(self._fh, item)

    def fileno(self) -> int:
        return self._fh.fileno()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            try:
                self._fh.close()
            finally:
                self._ledger._dec()

    def __enter__(self) -> "TrackedFile":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


# --------------------------------------------------------------------------- #
# 框式流写入器
# --------------------------------------------------------------------------- #
class FramedWriter:
    """框式记录流写入器。

    tmp_rename=True：写入 ``final_path + '.tmp'``，finish 时 fsync 并原子改名
    （一次性文件）。resume_bytes 不为 None：打开既有文件并截断到该字节数
    （增量文件的崩溃恢复），调用方保证该字节边界来自已提交的 SQLite 检查点。
    """

    def __init__(self, ledger: FileLedger, final_path: str, header: Optional[dict],
                 *, tmp_rename: bool = False, resume_bytes: Optional[int] = None) -> None:
        self.ledger = ledger
        self.final_path = final_path
        self.tmp_rename = tmp_rename
        self.rows = 0
        self._hasher = hashlib.sha256() if resume_bytes is None else None
        self._finished = False
        os.makedirs(os.path.dirname(final_path), exist_ok=True)
        if resume_bytes is not None:
            if not os.path.exists(final_path):
                raise JoinFailed(f"增量文件丢失，无法按检查点恢复: {final_path}")
            actual = os.path.getsize(final_path)
            if actual < resume_bytes:
                raise JoinFailed(
                    f"增量文件 {final_path} 长度 {actual} 小于检查点 {resume_bytes}，已损坏"
                )
            self._fh = ledger.open(final_path, "r+b")
            self._fh.truncate(resume_bytes)  # 未落定的尾部一律截掉重做
            self._fh.seek(resume_bytes)
            self._tmp_path = None
        else:
            self._tmp_path = final_path + ".tmp" if tmp_rename else None
            self._fh = ledger.open(self._tmp_path or final_path, "wb")
            if header is not None:
                hb = json_dumps(header)
                self._fh.write(J_MAGIC)
                self._fh.write(struct.pack("<I", len(hb)))
                self._fh.write(hb)

    def tell(self) -> int:
        return self._fh.tell()

    def write(self, payload: bytes) -> int:
        if self._finished:
            raise JoinError("写入器已封尾")
        off = self._fh.tell()
        self._fh.write(struct.pack("<I", len(payload)))
        self._fh.write(payload)
        self._fh.write(hashlib.sha256(payload).digest())
        if self._hasher is not None:
            self._hasher.update(payload)
        self.rows += 1
        return off

    @property
    def digest_hex(self) -> str:
        if self._hasher is None:
            raise JoinError("增量写入器不维护全程摘要（见检查点/终验）")
        return self._hasher.hexdigest()

    def flush_fsync(self) -> None:
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def finish(self, footer: Optional[dict] = None) -> None:
        if self._finished:
            return
        if footer is not None:
            fb = json_dumps(footer)
            self._fh.write(J_FOOTER_MAGIC)
            self._fh.write(struct.pack("<I", len(fb)))
            self._fh.write(fb)
        self.flush_fsync()
        self._fh.close()
        if self._tmp_path is not None:
            os.replace(self._tmp_path, self.final_path)
        self._finished = True

    def abort(self) -> None:
        try:
            self._fh.close()
        except OSError:
            pass


# --------------------------------------------------------------------------- #
# 框式流读取器（全部逐条校验摘要，损坏即显式失败）
# --------------------------------------------------------------------------- #
def _read_exact(fh: BinaryIO, n: int, what: str, path: str) -> bytes:
    data = fh.read(n)
    if len(data) != n:
        raise JoinFailed(f"{path}: 读取 {what} 需要 {n} 字节，实际 {len(data)}（截断）")
    return data


def read_header(fh: BinaryIO, path: str) -> tuple[dict, int]:
    magic = fh.read(len(J_MAGIC))
    if magic != J_MAGIC:
        raise JoinFailed(f"{path}: 魔数不符")
    (hlen,) = struct.unpack("<I", _read_exact(fh, 4, "header length", path))
    if not 0 < hlen <= 1 << 20:
        raise JoinFailed(f"{path}: 非法 header 长度 {hlen}")
    try:
        header = json_loads(_read_exact(fh, hlen, "header", path))
    except (UnicodeDecodeError, ValueError) as exc:
        raise JoinFailed(f"{path}: header 损坏: {exc}") from exc
    return header, len(J_MAGIC) + 4 + hlen


def _read_frame_at(fh: BinaryIO, path: str) -> Optional[bytes]:
    """读一条记录；遇到 footer 返回 None（footer 由调用方进一步解析）。"""
    peek = fh.read(4)
    if not peek:
        raise JoinFailed(f"{path}: 记录区截断（缺 footer）")
    if peek == J_FOOTER_MAGIC[:4]:
        rest = _read_exact(fh, len(J_FOOTER_MAGIC) - 4, "footer magic", path)
        if peek + rest != J_FOOTER_MAGIC:
            raise JoinFailed(f"{path}: footer 魔数损坏")
        return None
    (plen,) = struct.unpack("<I", peek)
    if plen <= 0 or plen > 256 * 1024 * 1024:
        raise JoinFailed(f"{path}: 非法记录长度 {plen}")
    payload = _read_exact(fh, plen, "record payload", path)
    digest = _read_exact(fh, 32, "record digest", path)
    if hashlib.sha256(payload).digest() != digest:
        raise JoinFailed(f"{path}: 记录摘要不符（内容损坏）")
    return payload


def _read_footer(fh: BinaryIO, path: str) -> dict:
    (flen,) = struct.unpack("<I", _read_exact(fh, 4, "footer length", path))
    if not 0 < flen <= 1 << 20:
        raise JoinFailed(f"{path}: 非法 footer 长度 {flen}")
    try:
        footer = json_loads(_read_exact(fh, flen, "footer", path))
    except (UnicodeDecodeError, ValueError) as exc:
        raise JoinFailed(f"{path}: footer 损坏: {exc}") from exc
    if fh.read(1):
        raise JoinFailed(f"{path}: footer 之后存在多余字节")
    return footer


def iter_frames(path: str, ledger: FileLedger,
                expected_header: Optional[dict] = None) -> Iterator[bytes]:
    """整流读取一次性文件：校验 header、逐条摘要、footer 行数与流摘要。"""
    with ledger.open(path, "rb") as fh:
        header, _ = read_header(fh, path)
        if expected_header:
            for k, v in expected_header.items():
                if header.get(k) != v:
                    raise JoinFailed(
                        f"{path}: header 字段 {k}={header.get(k)!r} 与期望 {v!r} 不符"
                    )
        hasher = hashlib.sha256()
        rows = 0
        while True:
            payload = _read_frame_at(fh, path)
            if payload is None:
                break
            hasher.update(payload)
            rows += 1
            yield payload
        footer = _read_footer(fh, path)
        if footer.get("row_count") != rows:
            raise JoinFailed(
                f"{path}: footer 行数 {footer.get('row_count')} 与实际 {rows} 不符"
            )
        if footer.get("digest") != hasher.hexdigest():
            raise JoinFailed(f"{path}: 流摘要不符（内容损坏）")


def iter_frames_from(path: str, offset: int, ledger: FileLedger) -> Iterator[bytes]:
    """从字节偏移继续读增量/一次性文件（恢复用）：逐条摘要 + footer 结构校验。

    全程行数/流摘要由阶段完成时的整流校验负责；这里保证每条记录自身完整。
    """
    with ledger.open(path, "rb") as fh:
        _, data_start = read_header(fh, path)
        if offset < data_start:
            raise JoinFailed(f"{path}: 恢复偏移 {offset} 越出数据区起点 {data_start}")
        fh.seek(offset)
        while True:
            payload = _read_frame_at(fh, path)
            if payload is None:
                break
            yield payload
        _read_footer(fh, path)


def iter_frames_range(path: str, offset: int, nbytes: int, ledger: FileLedger,
                      *, verify_header: bool = True) -> Iterator[bytes]:
    """读取 [offset, offset+nbytes) 内的完整记录（recv 文件的分区块）。"""
    with ledger.open(path, "rb") as fh:
        if verify_header:
            read_header(fh, path)
        fh.seek(offset)
        consumed = 0
        while consumed < nbytes:
            payload = _read_frame_at(fh, path)
            if payload is None:
                raise JoinFailed(f"{path}: 范围内出现 footer，块边界损坏")
            consumed += FRAME_OVERHEAD + len(payload)
            if consumed > nbytes:
                raise JoinFailed(f"{path}: 记录跨越块边界，范围登记损坏")
            yield payload


def verify_file(path: str, ledger: FileLedger, expected_header: Optional[dict],
                expected_rows: int, expected_digest: str) -> None:
    """恢复时整流校验一次性文件，通过才允许复用（先校验后复用）。"""
    hasher = hashlib.sha256()
    rows = 0
    for payload in iter_frames(path, ledger, expected_header):
        hasher.update(payload)
        rows += 1
    if rows != expected_rows:
        raise JoinFailed(f"{path}: 行数 {rows} 与检查点 {expected_rows} 不符")
    if hasher.hexdigest() != expected_digest:
        raise JoinFailed(f"{path}: 流摘要与检查点不符")


# --------------------------------------------------------------------------- #
# 记录编解码（关联内部二进制帧）
# --------------------------------------------------------------------------- #
KTAG_NULL = 0
KTAG_TEXT = 1


def key_to_bytes(key) -> tuple[int, bytes]:
    """null 与 text 严格区分：null 永不匹配；text 按 UTF-8 字节精确相等。"""
    if key is None:
        return KTAG_NULL, b""
    if not isinstance(key, str):
        raise JoinError(f"关联键必须是 text 或 null，得到 {type(key).__name__}")
    return KTAG_TEXT, key.encode("utf-8")


def pack_recv(row_number: int, ktag: int, key: bytes, orig: bytes) -> bytes:
    return struct.pack("<qBI", row_number, ktag, len(key)) + key + orig


def unpack_recv(pl: bytes) -> tuple[int, int, bytes, bytes]:
    row_number, ktag, klen = struct.unpack_from("<qBI", pl, 0)
    return row_number, ktag, pl[13:13 + klen], pl[13 + klen:]


def pack_norm(seq: int, ktag: int, key: bytes, orig: bytes) -> bytes:
    return struct.pack("<qBI", seq, ktag, len(key)) + key + orig


def unpack_norm(pl: bytes) -> tuple[int, int, bytes, bytes]:
    seq, ktag, klen = struct.unpack_from("<qBI", pl, 0)
    return seq, ktag, pl[13:13 + klen], pl[13 + klen:]


def pack_keys(key: bytes, seq: int, orig: bytes) -> bytes:
    return struct.pack("<Iq", len(key), seq) + key + orig


def unpack_keys(pl: bytes) -> tuple[bytes, int, bytes]:
    klen, seq = struct.unpack_from("<Iq", pl, 0)
    return pl[12:12 + klen], seq, pl[12 + klen:]


def unpack_keys_head(pl: bytes) -> tuple[bytes, int]:
    klen, seq = struct.unpack_from("<Iq", pl, 0)
    return pl[12:12 + klen], seq


def pack_result(seq: int, left: Optional[bytes], right: Optional[bytes]) -> bytes:
    flags = (1 if left is not None else 0) | (2 if right is not None else 0)
    lb = left or b""
    rb = right or b""
    return struct.pack("<qBII", seq, flags, len(lb), len(rb)) + lb + rb


def unpack_result(pl: bytes) -> tuple[int, Optional[bytes], Optional[bytes]]:
    seq, flags, llen, rlen = struct.unpack_from("<qBII", pl, 0)
    off = 17
    left = pl[off:off + llen] if flags & 1 else None
    off += llen
    right = pl[off:off + rlen] if flags & 2 else None
    return seq, left, right


def pack_row_index(seq: int, offset: int) -> bytes:
    return struct.pack("<qq", seq, offset)


def unpack_row_index(pl: bytes) -> tuple[int, int]:
    return struct.unpack("<qq", pl)


def pack_key_index(key: bytes, offset: int) -> bytes:
    return struct.pack("<Iq", len(key), offset) + key


def unpack_key_index(pl: bytes) -> tuple[bytes, int]:
    klen, offset = struct.unpack_from("<Iq", pl, 0)
    return pl[12:], offset


# --------------------------------------------------------------------------- #
# 同步关卡钩子（仅演示/测试；经环境变量或显式参数传入，不参与正常协议）
# --------------------------------------------------------------------------- #
def parse_hook(spec: Optional[str]) -> Optional[tuple[str, int]]:
    """``kill-after-receive`` / ``kill-after-receive-segment=3`` /
    ``kill-after-normalize`` / ``kill-after-keysort`` / ``kill-at-emit=1000`` /
    ``kill-before-finalize`` → (名称, 参数)。"""
    if not spec:
        return None
    if "=" in spec:
        name, _, arg = spec.partition("=")
        return name.strip(), int(arg)
    return spec.strip(), 0


def hook_fire(hook: Optional[tuple[str, int]], name: str, counter: int = 0) -> None:
    """到达同步关卡即 os._exit（模拟进程被杀），不执行任何清理。"""
    if hook is None or hook[0] != name:
        return
    if name in ("kill-after-receive-segment", "kill-at-emit") and counter != hook[1]:
        return
    import sys

    sys.stderr.write(f"[join-hook] 受控终止: {name} (counter={counter}, pid={os.getpid()})\n")
    sys.stderr.flush()
    os._exit(HOOK_EXIT_CODE)
