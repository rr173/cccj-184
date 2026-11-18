"""双时间历史表的时态引用完整性审计（cross-entity reference integrity audit）。

历史表能回答"某实体在某段业务时间、某个知识版本里是什么"，本模块回答其上的
引用完整性问题：**源实体每条获胜事实在其整个生效区间内，payload 引用字段指向
的目标实体是否始终存在**。

审计规格（用户在建审计时固定）：
  * 历史表（workdir + name，绑定其持久身份 table_token）；
  * 知识版本 K：显式给定，或在建审计时固定当前 head——只处理已提交历史，
    不修改事件、知识版本或 head；head 之后推进不改变已开始审计的结果；
  * 业务区间 [t0, t1)，t1 可缺省为正无穷；
  * 规则集合：每条规则 = 唯一 rule_id + 源实体范围（all/prefix/entities）
    + payload 中的 JSON 字段路径；字段值（必须是字符串）即目标实体键。

语义（与实现路线无关的判定定义）：
  * 源引用区间 = 源实体在 [t0,t1)、版本 K 下按历史表裁定规则的获胜片段
    （相邻同 event_id 片段已合并）；空洞（present=false）没有引用，不产生结果。
  * 对每段源引用区间提取目标键，用**两侧时间线的区间交集**判定整个区间：
    目标实体在该区间内的缺失子区间（目标时间线空洞 ∩ 源区间）逐段输出——
    不是只查区间起点。目标 payload 为 JSON null 仍算存在；没有事实才是缺失。
  * 源引用值变化、源事实被覆盖或撤回、目标实体出现或消失，都在相应边界
    切开结果。
  * 缺失原因（严格区分，不混为一类）：
      - ``missing_field``   payload 缺少字段路径中的键（目标键记为 null）；
      - ``null_reference``  字段值为 JSON null（目标键记为 null）；
      - ``empty_target``    字段值为空字符串（目标键记为 ""）；
      - ``target_absent``   目标实体在子区间内无任何事实。
  * 非法字段路径（建审计时报错）、非字符串目标键、payload 不是对象等不支持
    的 JSON 形态（运行时报错）：**明确报错，不静默跳过源事实**——运行期遇到
    即把审计置为 failed 并记录源实体/规则/事件，结果不可读。

结果次序（固定）：源实体 UTF-8 字节序 → rule_id（UTF-8 字节序）→ 目标键
UTF-8 字节序（null 目标排最前）→ 区间起点 → 源 event_id。相邻区间只有在
（源实体、rule_id、目标键、源 event_id、缺失原因）完全相同且首尾相接时才
合并。

实现路线（路线 a：逐源区间读目标时间线 + 流式求交；排序用有界外排）
------------------------------------------------------------------
* 扫描：按实体 UTF-8 字节序流式遍历（历史表 events 索引，不收进容器），
  对每个 (源实体, 规则) 对：用历史表持久桶索引取源实体在 [t0,t1) 的获胜
  片段；对每段获胜片段，再用桶索引只读**与该片段相交**的目标实体历史
  （真阳性候选），流式求交得缺失子区间。短业务区间因此只读覆盖该区间的
  目标事件，不重放目标全部历史。读取代价 = O(源获胜片段数 × (65 次桶探测
  + 相交目标事件数))；源/目标各一条只读连接（各自独立的临时候选表，互不
  干扰）。扫描源引用时的真实读取量（候选数、检查事件数、目标查询次数）
  全部落 counters 可查。
* 暂存：缺失发现先落审计自有 SQLite 的 findings 表（每个 (实体,规则) 对
  一个事务，与进度检查点同事务提交）——恢复边界 = (源实体, 规则) 对，
  已提交的对绝不重算。暂存空间 = O(缺失区间数) 条 findings 行，全部在
  审计自有目录内。
* 物化：findings 流式读出 → 受限内存分块排序 → 溢写 run 文件（审计 tmp
  目录）→ 受限扇入归并 → 相邻合并 → 框式结果流 + 稀疏索引（tmp+原子改名）。
  全部缺失区间不进入 Python 容器；排序/归并内存受预算约束。
* 终验（完成前）：整流重读结果，校验逐条摘要、序号连续、次序严格递增、
  无应合并而未合并的相邻记录、总数与摘要等于物化检查点，然后才置 done。
  完成后每次读取打开时复核 footer 与 meta 的记录数/摘要，读到的每条记录
  再验摘要——结果未完成或已损坏时绝不返回成功前缀。

预算：``mem_bytes`` 工作内存（单行上限 = mem_bytes/16，排序块 = mem_bytes/4，
SQLite 页缓存 ≤ mem_bytes/4）；``max_open_files`` 审计自管数据文件句柄上限
（SQLite 连接为固定常数个，不占额度）。解释器固定开销不计入预算。历史表查询
侧的资源上限（扫描线开放集合等）由历史表自身预算约束。

所有审计文件限于自身受管目录（<root>/<name>/）；删除或失败只影响该目录，
不影响历史表、其他审计或正在读取的结果（POSIX 下已打开的读取句柄继续有效）。
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import struct
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Any, Optional

from . import history as H
from .common import ShuffleError, connect, json_dumps, json_loads
from .join_common import (
    J_FOOTER_MAGIC,
    BudgetExceeded,
    FileLedger,
    FramedWriter,
    JoinFailed,
    _read_frame_at,
    iter_frames,
    pack_row_index,
    parse_hook,
    read_header,
    unpack_row_index,
    verify_file,
)

# --------------------------------------------------------------------------- #
# 常量
# --------------------------------------------------------------------------- #
A_CREATED = "created"             # 已建审计（规格与版本已固定），未开始扫描
A_SCANNING = "scanning"           # 扫描进行中（按 (实体,规则) 对检查点恢复）
A_MATERIALIZING = "materializing" # 扫描完成，待物化（排序/归并/写结果）
A_FINALIZING = "finalizing"       # 物化检查点已落，待终验
A_DONE = "done"
A_FAILED = "failed"

REASON_MISSING_FIELD = "missing_field"
REASON_NULL_REFERENCE = "null_reference"
REASON_EMPTY_TARGET = "empty_target"
REASON_TARGET_ABSENT = "target_absent"
REASONS = (REASON_MISSING_FIELD, REASON_NULL_REFERENCE,
           REASON_EMPTY_TARGET, REASON_TARGET_ABSENT)

HOOK_EXIT_CODE = 42
RESULT_INDEX_EVERY = 1024     # 结果稀疏索引间隔（分页定位后顺序扫描）
RESERVED_FILES = 8            # db/wal、结果流、索引流等常数句柄
MAX_RULES = 64                # 规则集合规模上限（规格即元数据，必须有限）
MAX_PATH_STEPS = 8            # 字段路径最大深度
MAX_ENTITIES_PER_RULE = 4096  # 显式源实体清单上限

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;

CREATE TABLE IF NOT EXISTS meta(
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS findings(
    seq          INTEGER PRIMARY KEY,      -- 生成次序（插入序）
    entity_bytes BLOB NOT NULL,
    entity_key   TEXT NOT NULL,
    rule_id      TEXT NOT NULL,
    target_sort  BLOB NOT NULL,            -- 0x00=null 目标 | 0x01||utf8
    target_key   TEXT,                     -- null：missing_field/null_reference
    start_b      INTEGER NOT NULL,         -- 业务时间起点（int64）
    end_b        INTEGER,                  -- NULL = 正无穷
    event_id     TEXT NOT NULL,            -- 获胜源事件
    reason       TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS counters(
    name  TEXT PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS audit_events(
    ts     REAL NOT NULL,
    kind   TEXT NOT NULL,
    detail TEXT NOT NULL
);
"""


# --------------------------------------------------------------------------- #
# 错误类型
# --------------------------------------------------------------------------- #
class AuditError(ShuffleError):
    """时态引用审计所有可预期失败的基类。"""


class AuditNotReady(AuditError):
    """审计不存在，或结果尚未完成（拒绝把未完成/未核验的前缀当作成功）。"""


class AuditRuleError(AuditError):
    """规则非法（非法字段路径/源范围）或求值遇非字符串目标键、不支持的
    JSON 形态——明确报错，不静默跳过源事实。"""


class AuditCorrupt(AuditError):
    """审计自身存储（findings/结果流/索引/元数据）损坏。"""


class AuditCursorError(AuditError):
    """游标被用于另一个审计/另一组参数，或内容被破坏。"""


# --------------------------------------------------------------------------- #
# 布局与预算
# --------------------------------------------------------------------------- #
class AuditLayout:
    def __init__(self, root: str, name: str) -> None:
        if not name or any(c in name for c in "/\\\x00") or name in (".", ".."):
            raise AuditError(f"非法审计名: {name!r}")
        self.root = os.path.abspath(root)
        self.name = name
        self.dir = os.path.join(self.root, name)

    @property
    def db_path(self) -> str:
        return os.path.join(self.dir, "audit.sqlite3")

    @property
    def tmp_dir(self) -> str:
        return os.path.join(self.dir, "tmp")

    @property
    def results_path(self) -> str:
        return os.path.join(self.dir, "results.frm")

    @property
    def results_index_path(self) -> str:
        return os.path.join(self.dir, "results.idx")


@dataclass(frozen=True)
class AuditBudget:
    mem_bytes: int
    max_open_files: int

    def __post_init__(self) -> None:
        if self.mem_bytes < 16384:
            raise AuditError("工作内存预算至少 16384 字节")
        if self.max_open_files < RESERVED_FILES + 2:
            raise AuditError(f"同时打开文件预算至少 {RESERVED_FILES + 2}")

    @property
    def single_row_limit(self) -> int:
        return self.mem_bytes // 16

    @property
    def sort_chunk(self) -> int:
        return max(self.mem_bytes // 4, self.single_row_limit)

    @property
    def sqlite_cache_kb(self) -> int:
        return max(16, self.mem_bytes // 4 // 1024)

    @property
    def fan_in(self) -> int:
        return max(2, min(self.max_open_files - RESERVED_FILES, 16))


# --------------------------------------------------------------------------- #
# 小工具
# --------------------------------------------------------------------------- #
def _connect_audit(al: AuditLayout, cache_kb: int = 256) -> sqlite3.Connection:
    conn = connect(al.db_path)
    conn.executescript(SCHEMA)
    conn.execute(f"PRAGMA cache_size={-cache_kb}")
    conn.execute("PRAGMA temp_store=1")
    conn.commit()
    return conn


def _meta_get(conn: sqlite3.Connection, key: str, default=None):
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return json_loads(row["value"].encode("utf-8")) if row else default


def _meta_set(conn: sqlite3.Connection, key: str, value) -> None:
    conn.execute(
        "INSERT INTO meta(key,value) VALUES (?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, json_dumps(value).decode("utf-8")),
    )


def _counter_add(conn: sqlite3.Connection, name: str, delta: int) -> None:
    conn.execute(
        "INSERT INTO counters(name,value) VALUES (?,?) "
        "ON CONFLICT(name) DO UPDATE SET value=value+excluded.value",
        (name, delta),
    )


def _counter_get(conn: sqlite3.Connection, name: str) -> int:
    row = conn.execute("SELECT value FROM counters WHERE name=?", (name,)).fetchone()
    return int(row["value"]) if row else 0


def _aevent(conn: sqlite3.Connection, kind: str, detail: str = "") -> None:
    conn.execute("INSERT INTO audit_events(ts,kind,detail) VALUES (?,?,?)",
                 (time.time(), kind, detail))


def _check_int64(v: Any, what: str) -> int:
    if not isinstance(v, int) or isinstance(v, bool) \
            or not (H.INT64_MIN <= v <= H.INT64_MAX):
        raise AuditError(f"{what} 必须是有符号 64 位整数，得到 {v!r}")
    return int(v)


def _target_sort(target_key: Optional[str]) -> bytes:
    """null 目标（缺字段/null 引用）排在所有字符串目标之前。"""
    if target_key is None:
        return b"\x00"
    return b"\x01" + target_key.encode("utf-8")


def _hook_fire(hook: Optional[tuple[str, int]], name: str, counter: int = 0) -> None:
    """审计同步关卡（仅演示/测试）：到达即 os._exit，模拟进程被杀。"""
    if hook is None or hook[0] != name:
        return
    if name == "kill-after-audit-pair" and counter != hook[1]:
        return
    sys.stderr.write(f"[audit-hook] 受控终止: {name} (counter={counter}, "
                     f"pid={os.getpid()})\n")
    sys.stderr.flush()
    os._exit(HOOK_EXIT_CODE)


# --------------------------------------------------------------------------- #
# 规则校验（非法字段路径/源范围在建审计时明确报错）
# --------------------------------------------------------------------------- #
def _validate_source(src: Any, idx: int) -> dict:
    if not isinstance(src, dict):
        raise AuditRuleError(f"规则 #{idx} 的 source 必须是对象，得到 {type(src).__name__}")
    kind = src.get("kind")
    if kind == "all":
        if set(src) != {"kind"}:
            raise AuditRuleError(f"规则 #{idx} 的 all 源范围不接受额外字段: {sorted(set(src) - {'kind'})}")
        return {"kind": "all"}
    if kind == "prefix":
        if set(src) != {"kind", "prefix"}:
            raise AuditRuleError(f"规则 #{idx} 的 prefix 源范围字段非法: {sorted(set(src))}")
        prefix = src["prefix"]
        if not isinstance(prefix, str):
            raise AuditRuleError(f"规则 #{idx} 的 prefix 必须是 text，得到 {type(prefix).__name__}")
        return {"kind": "prefix", "prefix": prefix}
    if kind == "entities":
        if set(src) != {"kind", "entities"}:
            raise AuditRuleError(f"规则 #{idx} 的 entities 源范围字段非法: {sorted(set(src))}")
        ents = src["entities"]
        if not isinstance(ents, list):
            raise AuditRuleError(f"规则 #{idx} 的 entities 必须是数组")
        if len(ents) > MAX_ENTITIES_PER_RULE:
            raise AuditRuleError(
                f"规则 #{idx} 的显式源实体清单超过上限 {MAX_ENTITIES_PER_RULE}")
        seen: set[str] = set()
        for e in ents:
            if not isinstance(e, str):
                raise AuditRuleError(
                    f"规则 #{idx} 的源实体必须是 text（null 非法），得到 {type(e).__name__}")
            if e in seen:
                raise AuditRuleError(f"规则 #{idx} 的源实体 {e!r} 重复（不静默去重）")
            seen.add(e)
        return {"kind": "entities",
                "entities": sorted(seen, key=lambda s: s.encode("utf-8"))}
    raise AuditRuleError(f"规则 #{idx} 的 source.kind 非法: {kind!r}"
                         "（应为 all/prefix/entities）")


def _validate_path(path: Any, idx: int) -> list:
    if not isinstance(path, list) or not path:
        raise AuditRuleError(
            f"规则 #{idx} 的字段路径必须是非空数组（非法字段路径），得到 {path!r}")
    if len(path) > MAX_PATH_STEPS:
        raise AuditRuleError(f"规则 #{idx} 的字段路径深度超过上限 {MAX_PATH_STEPS}")
    for step in path:
        if not isinstance(step, str) or not step:
            raise AuditRuleError(
                f"规则 #{idx} 的字段路径元素必须是非空 text（非法字段路径）: {step!r}")
    return list(path)


def _validate_rules(rules: Any) -> list:
    """校验并规范化规则集合：rule_id 唯一，按 rule_id UTF-8 字节序排序。"""
    if not isinstance(rules, list) or not rules:
        raise AuditRuleError("规则集合必须是非空数组")
    if len(rules) > MAX_RULES:
        raise AuditRuleError(f"规则集合规模 {len(rules)} 超过上限 {MAX_RULES}")
    norm = []
    seen: set[str] = set()
    for i, r in enumerate(rules):
        if not isinstance(r, dict):
            raise AuditRuleError(f"规则 #{i} 必须是对象，得到 {type(r).__name__}")
        extra = set(r) - {"rule_id", "source", "path"}
        if extra:
            raise AuditRuleError(f"规则 #{i} 含未知字段: {sorted(extra)}")
        rid = r.get("rule_id")
        if not isinstance(rid, str) or not rid:
            raise AuditRuleError(f"规则 #{i} 的 rule_id 必须是非空 text")
        if rid in seen:
            raise AuditRuleError(f"rule_id {rid!r} 重复（规则集合内必须唯一）")
        seen.add(rid)
        norm.append({"rule_id": rid,
                     "source": _validate_source(r.get("source"), i),
                     "path": _validate_path(r.get("path"), i)})
    norm.sort(key=lambda r: r["rule_id"].encode("utf-8"))
    return norm


def _scope_matches(rule: dict, entity_bytes: bytes) -> bool:
    src = rule["source"]
    kind = src["kind"]
    if kind == "all":
        return True
    if kind == "prefix":
        return entity_bytes.startswith(src["prefix"].encode("utf-8"))
    return entity_bytes in rule["_entity_set"]


def _attach_scope_sets(rules: list) -> None:
    for r in rules:
        r["_entity_set"] = (frozenset(e.encode("utf-8") for e in r["source"]["entities"])
                            if r["source"]["kind"] == "entities" else frozenset())


# --------------------------------------------------------------------------- #
# 引用提取：缺字段/null/空串是缺失原因；非字符串目标键与不支持的 JSON 形态
# 明确报错（不静默跳过源事实）
# --------------------------------------------------------------------------- #
def _extract_ref(payload: Any, path: list, rule_id: str, entity: str,
                 event_id: str) -> tuple[str, Optional[str]]:
    """返回 (kind, target)：kind ∈ ok/missing_field/null_reference/empty_target。"""
    node = payload
    for i, step in enumerate(path):
        if not isinstance(node, dict):
            raise AuditRuleError(
                f"规则 {rule_id!r} 源实体 {entity!r} 事件 {event_id!r}: payload "
                f"在字段路径第 {i} 步（{step!r}）不是对象（不支持的 JSON 形态），"
                "审计明确失败而不静默跳过源事实"
            )
        if step not in node:
            return REASON_MISSING_FIELD, None
        node = node[step]
    if node is None:
        return REASON_NULL_REFERENCE, None
    if isinstance(node, str):
        if node == "":
            return REASON_EMPTY_TARGET, ""
        return "ok", node
    raise AuditRuleError(
        f"规则 {rule_id!r} 源实体 {entity!r} 事件 {event_id!r}: 目标键不是字符串"
        f"（得到 {type(node).__name__}，非字符串目标键），审计明确失败而不静默跳过源事实"
    )


# --------------------------------------------------------------------------- #
# 游标：绑定审计身份、表持久身份、知识版本、业务区间、规则集合与当前位置
# --------------------------------------------------------------------------- #
def _cursor_encode(audit_token: str, kind: str, fields: dict) -> str:
    body = {"v": 1, "tok": audit_token, "kind": kind}
    body.update(fields)
    sig = hashlib.sha256(b"audit-cursor:" + json_dumps(body)).hexdigest()
    body["sig"] = sig
    return json.dumps(body, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def _cursor_decode(audit_token: str, kind: str, raw: str) -> dict:
    try:
        body = json.loads(raw)
    except (ValueError, TypeError) as exc:
        raise AuditCursorError(f"游标无法解析（内容被破坏）: {exc}") from exc
    if not isinstance(body, dict):
        raise AuditCursorError("游标结构非法（内容被破坏）")
    sig = body.pop("sig", None)
    if not isinstance(sig, str):
        raise AuditCursorError("游标缺少完整性签名（内容被破坏）")
    expect = hashlib.sha256(b"audit-cursor:" + json_dumps(body)).hexdigest()
    if sig != expect:
        raise AuditCursorError("游标完整性校验失败（内容被破坏）")
    if body.get("tok") != audit_token:
        raise AuditCursorError("游标属于另一个审计（持久身份不符）")
    if body.get("kind") != kind:
        raise AuditCursorError(f"游标属于另一种接口（{body.get('kind')!r}，应为 {kind!r}）")
    if body.get("v") != 1:
        raise AuditCursorError("游标版本不受支持")
    return body


# --------------------------------------------------------------------------- #
# 建审计
# --------------------------------------------------------------------------- #
def create_audit(root: str, name: str, *, table_workdir: str, table_name: str,
                 rules: Any, t0: Any, t1: Any = None,
                 kversion: Optional[int] = None,
                 mem_bytes: int, max_open_files: int) -> dict:
    """创建审计：固定表持久身份、知识版本（缺省 = 当前 head）、业务区间与
    规则集合。只登记规格，不触碰历史数据。"""
    budget = AuditBudget(mem_bytes, max_open_files)
    al = AuditLayout(root, name)
    if os.path.exists(al.dir):
        raise AuditError(f"审计 {name!r} 已存在: {al.dir}")
    t0 = _check_int64(t0, "业务区间起点")
    if t1 is not None:
        t1 = _check_int64(t1, "业务区间终点")
        if not t0 < t1:
            raise AuditError(f"业务区间为空或反向: [{t0}, {t1})")
    norm_rules = _validate_rules(rules)
    # 固定知识版本：显式值必须已提交（未来/未提交版本显式拒绝）；缺省固定当前 head
    tbl = H.HistoryTable(table_workdir, table_name)
    conn = tbl._query_conn()
    try:
        K = tbl._pin_version(conn, kversion)
    finally:
        conn.rollback()
        conn.close()
    rules_digest = hashlib.sha256(json_dumps(norm_rules)).hexdigest()
    spec_digest = hashlib.sha256(b"audit-spec:" + json_dumps({
        "table_token": tbl.token, "kversion": K, "t0": t0, "t1": t1,
        "rules_digest": rules_digest,
    })).hexdigest()
    token = uuid.uuid4().hex
    try:
        os.makedirs(al.tmp_dir, exist_ok=False)
    except FileExistsError as exc:
        raise AuditError(f"审计 {name!r} 已存在（并发创建）: {al.dir}") from exc
    conn = _connect_audit(al, budget.sqlite_cache_kb)
    try:
        _meta_set(conn, "name", name)
        _meta_set(conn, "audit_token", token)
        _meta_set(conn, "state", A_CREATED)
        _meta_set(conn, "error", None)
        _meta_set(conn, "table_workdir", os.path.abspath(table_workdir))
        _meta_set(conn, "table_name", table_name)
        _meta_set(conn, "table_token", tbl.token)
        _meta_set(conn, "kversion", K)
        _meta_set(conn, "t0", t0)
        _meta_set(conn, "t1", t1)
        _meta_set(conn, "rules", norm_rules)
        _meta_set(conn, "rules_digest", rules_digest)
        _meta_set(conn, "spec_digest", spec_digest)
        _meta_set(conn, "mem_bytes", mem_bytes)
        _meta_set(conn, "max_open_files", max_open_files)
        _meta_set(conn, "created_at", time.time())
        _meta_set(conn, "scan_entity", None)     # 最近完成的 (实体,规则) 对
        _meta_set(conn, "scan_rule", None)
        _meta_set(conn, "pairs_done", 0)
        _meta_set(conn, "materialize_ckpt", None)
        _meta_set(conn, "result_rows", None)
        _meta_set(conn, "result_digest", None)
        _meta_set(conn, "done_at", None)
        _counter_add(conn, "src_file_opens", 0)   # 审计只读已提交历史，从不打开源段
        _aevent(conn, "audit_created",
                f"table={table_name} K={K} t0={t0} t1={t1} rules={len(norm_rules)}")
        conn.commit()
    finally:
        conn.close()
    return audit_status(root, name)


def _open_audit(root: str, name: str) -> tuple[AuditLayout, sqlite3.Connection, dict, AuditBudget]:
    al = AuditLayout(root, name)
    if not os.path.exists(al.db_path):
        raise AuditNotReady(f"审计不存在: {al.dir}")
    conn = _connect_audit(al)
    meta = {k: _meta_get(conn, k) for k in (
        "name", "audit_token", "state", "error", "table_workdir", "table_name",
        "table_token", "kversion", "t0", "t1", "rules", "rules_digest",
        "spec_digest", "mem_bytes", "max_open_files", "created_at",
        "scan_entity", "scan_rule", "pairs_done", "materialize_ckpt",
        "result_rows", "result_digest", "done_at")}
    if meta["audit_token"] is None or meta["state"] is None:
        conn.close()
        raise AuditCorrupt("审计元数据缺失（目录损坏或不是审计）")
    budget = AuditBudget(int(meta["mem_bytes"]), int(meta["max_open_files"]))
    conn.execute(f"PRAGMA cache_size={-budget.sqlite_cache_kb}")
    return al, conn, meta, budget


def audit_status(root: str, name: str) -> dict:
    """审计状态：状态机位置、扫描进度、真实读取量计数器、结果摘要。"""
    al, conn, meta, budget = _open_audit(root, name)
    try:
        counters = {r["name"]: int(r["value"]) for r in
                    conn.execute("SELECT name, value FROM counters").fetchall()}
        return {
            "name": name, "dir": al.dir, "state": meta["state"],
            "error": meta["error"],
            "audit_token": meta["audit_token"],
            "table": {"workdir": meta["table_workdir"], "name": meta["table_name"],
                      "table_token": meta["table_token"]},
            "kversion": meta["kversion"], "t0": meta["t0"], "t1": meta["t1"],
            "rules": meta["rules"], "rules_digest": meta["rules_digest"],
            "spec_digest": meta["spec_digest"],
            "progress": {"pairs_done": meta["pairs_done"],
                         "scan_entity": meta["scan_entity"],
                         "scan_rule": meta["scan_rule"]},
            "counters": counters,
            "budgets": {"mem_bytes": meta["mem_bytes"],
                        "max_open_files": meta["max_open_files"],
                        "single_row_limit": budget.single_row_limit,
                        "sort_chunk": budget.sort_chunk,
                        "fan_in": budget.fan_in},
            "result": ({"rows": meta["result_rows"], "digest": meta["result_digest"]}
                       if meta["state"] == A_DONE else None),
            "created_at": meta["created_at"], "done_at": meta["done_at"],
        }
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 扫描阶段：逐 (源实体, 规则) 对，源获胜片段 ∩ 目标存在区间 → findings
# --------------------------------------------------------------------------- #
def _insert_finding(conn: sqlite3.Connection, entity_bytes: bytes, entity_key: str,
                    rule_id: str, target_key: Optional[str],
                    s_u: int, e_u: int, event_id: str, reason: str) -> None:
    conn.execute(
        "INSERT INTO findings(entity_bytes,entity_key,rule_id,target_sort,"
        "target_key,start_b,end_b,event_id,reason) VALUES (?,?,?,?,?,?,?,?,?)",
        (entity_bytes, entity_key, rule_id, _target_sort(target_key), target_key,
         H._unshift(s_u), H._unshift(e_u), event_id, reason),
    )


def _scan_pair(tbl: H.HistoryTable, src: sqlite3.Connection, tgt: sqlite3.Connection,
               audit_conn: sqlite3.Connection, entity_bytes: bytes, entity_key: str,
               rule: dict, K: int, u0: int, u1: int, stats: dict) -> None:
    """处理一个 (源实体, 规则) 对：源实体在 [u0,u1) 的每段获胜片段，

    对有效引用用历史表持久桶索引读取目标实体在该片段内的存在区间并流式求交；
    只把目标缺失的子区间写入 findings（与进度检查点同事务，由调用方提交）。
    """
    stats["source_pairs"] += 1
    full = (u0 == 0 and u1 == H.U_INF)
    sstats: dict = {"candidates": 0, "events_examined": 0}
    tbl._collect_candidates(src, entity_bytes, u0, u1, full, K, sstats)
    stats["src_candidates"] += sstats["candidates"]
    for s, e, winner in tbl._merged_segments(src, entity_bytes, u0, u1, K, sstats):
        if winner is None:
            continue                      # 空洞没有引用，不产生结果
        stats["source_segments"] += 1
        payload = json_loads(winner["payload_json"].encode("utf-8"))
        kind, target = _extract_ref(payload, rule["path"], rule["rule_id"],
                                    entity_key, winner["event_id"])
        if kind == "ok":
            target_bytes = target.encode("utf-8")
            stats["target_queries"] += 1
            tstats: dict = {"candidates": 0, "events_examined": 0}
            # 短区间：桶索引只取与该源片段相交的目标事件（真阳性），
            # 用两侧时间线的区间交集判定整个片段，而不是只查片段起点
            tbl._collect_candidates(tgt, target_bytes, s, e,
                                    s == 0 and e == H.U_INF, K, tstats)
            stats["tgt_candidates"] += tstats["candidates"]
            for gs, ge, gw in tbl._merged_segments(tgt, target_bytes, s, e, K, tstats):
                if gw is None:
                    _insert_finding(audit_conn, entity_bytes, entity_key,
                                    rule["rule_id"], target, gs, ge,
                                    winner["event_id"], REASON_TARGET_ABSENT)
                    stats["findings_staged"] += 1
            stats["tgt_events_examined"] += tstats["events_examined"]
        elif kind == REASON_EMPTY_TARGET:
            _insert_finding(audit_conn, entity_bytes, entity_key, rule["rule_id"],
                            "", s, e, winner["event_id"], REASON_EMPTY_TARGET)
            stats["findings_staged"] += 1
        else:   # missing_field / null_reference：整段即缺失，目标键为 null
            _insert_finding(audit_conn, entity_bytes, entity_key, rule["rule_id"],
                            None, s, e, winner["event_id"], kind)
            stats["findings_staged"] += 1
    stats["src_events_examined"] += sstats["events_examined"]


def _flush_stats(conn: sqlite3.Connection, stats: dict) -> None:
    for key, val in stats.items():
        if val:
            _counter_add(conn, key, val)
            stats[key] = 0


def _scan_phase(al: AuditLayout, conn: sqlite3.Connection, meta: dict,
                budget: AuditBudget, hook: Optional[tuple[str, int]]) -> None:
    tbl = H.HistoryTable(meta["table_workdir"], meta["table_name"])
    if tbl.token != meta["table_token"]:
        raise AuditError("历史表持久身份与审计绑定不符（表被删除后重建），审计拒绝继续")
    K = int(meta["kversion"])
    u0 = H._shift(int(meta["t0"]))
    u1 = H.U_INF if meta["t1"] is None else H._shift(int(meta["t1"]))
    rules = meta["rules"]
    _attach_scope_sets(rules)
    src = tbl._query_conn()      # 源扫描专用连接（独立临时候选表）
    tgt = tbl._query_conn()      # 目标时间线专用连接（与源扫描互不干扰）
    stats = {"source_entities": 0, "source_pairs": 0, "source_segments": 0,
             "src_candidates": 0, "src_events_examined": 0,
             "target_queries": 0, "tgt_candidates": 0, "tgt_events_examined": 0,
             "findings_staged": 0}
    pairs_done = int(meta["pairs_done"])
    try:
        tbl._pin_version(src, K)   # 固定版本必须仍合法（版本只增不改，恒成立）
        if meta["state"] == A_CREATED:
            conn.execute("BEGIN IMMEDIATE")
            _meta_set(conn, "state", A_SCANNING)
            _aevent(conn, "scan_started", f"K={K}")
            conn.commit()

        def process_pair(entity_bytes: bytes, entity_key: str, rule: dict,
                         first_of_entity: bool) -> None:
            nonlocal pairs_done
            if first_of_entity:
                stats["source_entities"] += 1
            conn.execute("BEGIN IMMEDIATE")
            try:
                _scan_pair(tbl, src, tgt, conn, entity_bytes, entity_key,
                           rule, K, u0, u1, stats)
                pairs_done += 1
                _meta_set(conn, "scan_entity", entity_bytes.hex())
                _meta_set(conn, "scan_rule", rule["rule_id"])
                _meta_set(conn, "pairs_done", pairs_done)
                _flush_stats(conn, stats)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            _hook_fire(hook, "kill-after-audit-pair", pairs_done)

        scan_entity = meta["scan_entity"]
        scan_rule = meta["scan_rule"]
        resume_after: Optional[str] = None
        if scan_entity is not None:
            # 先补完检查点实体剩余的规则（该实体之前的对已提交，绝不重算；
            # 实体计数已随其首个已提交对落库，这里不再重复计数）
            entity_bytes = bytes.fromhex(scan_entity)
            entity_key = entity_bytes.decode("utf-8")
            for rule in rules:
                if rule["rule_id"].encode("utf-8") <= scan_rule.encode("utf-8"):
                    continue
                if _scope_matches(rule, entity_bytes):
                    process_pair(entity_bytes, entity_key, rule, False)
            resume_after = entity_key
        for entity_bytes, entity_key in H.HistoryTable._entity_stream(
                src, K, resume_after, exhausted=True):
            first = True
            for rule in rules:
                if not _scope_matches(rule, entity_bytes):
                    continue
                process_pair(entity_bytes, entity_key, rule, first)
                first = False
        conn.execute("BEGIN IMMEDIATE")
        _flush_stats(conn, stats)
        _meta_set(conn, "state", A_MATERIALIZING)
        _aevent(conn, "scan_done",
                f"pairs={pairs_done} findings={_counter_get(conn, 'findings_staged')}")
        conn.commit()
    except AuditRuleError as exc:
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        try:
            conn.execute("BEGIN IMMEDIATE")
            _meta_set(conn, "state", A_FAILED)
            _meta_set(conn, "error", f"AuditRuleError: {exc}")
            _aevent(conn, "audit_failed", str(exc))
            conn.commit()
        except sqlite3.Error:
            pass
        raise
    finally:
        src.rollback()
        src.close()
        tgt.rollback()
        tgt.close()


# --------------------------------------------------------------------------- #
# 物化阶段：findings → 有界外排 → 相邻合并 → 框式结果流 + 稀疏索引
# --------------------------------------------------------------------------- #
def _sort_key_list(rec: list) -> tuple:
    """结果固定次序：源实体字节序 → rule_id → 目标键字节序 → 起点 → event_id。"""
    return (rec[0].encode("utf-8"), rec[1].encode("utf-8"),
            _target_sort(rec[2]), rec[3], rec[5].encode("utf-8"))


def _sort_key_dict(rec: dict) -> tuple:
    return (rec["entity"].encode("utf-8"), rec["rule_id"].encode("utf-8"),
            _target_sort(rec["target"]), rec["start"],
            rec["event_id"].encode("utf-8"))


def _mergeable_list(prev: list, cur: list) -> bool:
    """相邻区间只有来源与结果字段完全相同（实体/规则/目标/事件/原因）且
    首尾相接才可合并。"""
    return (prev[0] == cur[0] and prev[1] == cur[1] and prev[2] == cur[2]
            and prev[5] == cur[5] and prev[6] == cur[6]
            and prev[4] is not None and prev[4] == cur[3])


def _mergeable_dict(prev: dict, cur: dict) -> bool:
    return (prev["entity"] == cur["entity"] and prev["rule_id"] == cur["rule_id"]
            and prev["target"] == cur["target"] and prev["event_id"] == cur["event_id"]
            and prev["reason"] == cur["reason"]
            and prev["end"] is not None and prev["end"] == cur["start"])


def _clean_tmp(al: AuditLayout) -> None:
    """物化开始前清理上一（被终止的）运行遗留的中间文件与未落定结果。"""
    if os.path.isdir(al.tmp_dir):
        for nm in os.listdir(al.tmp_dir):
            if nm.endswith(".run") or nm.endswith(".run.tmp"):
                try:
                    os.remove(os.path.join(al.tmp_dir, nm))
                except OSError:
                    pass
    for p in (al.results_path, al.results_index_path,
              al.results_path + ".tmp", al.results_index_path + ".tmp"):
        if os.path.exists(p):
            os.remove(p)


def _merge_run_streams(specs: list, key_of, ledger: FileLedger,
                       budget: AuditBudget, al: AuditLayout, emit) -> None:
    """多轮受限扇入归并（中间 run 落审计 tmp 目录，随用随清）。"""
    import heapq

    def merge_group(group: list, emit_fn) -> None:
        readers = [iter_frames(p, ledger, None) for p in group]
        heap: list[tuple] = []
        try:
            for i, it in enumerate(readers):
                try:
                    pl = next(it)
                except StopIteration:
                    continue
                heapq.heappush(heap, (key_of(pl), i, pl))
            alive = [True] * len(readers)
            while heap:
                _key, i, pl = heapq.heappop(heap)
                emit_fn(pl)
                if alive[i]:
                    try:
                        nxt = next(readers[i])
                        heapq.heappush(heap, (key_of(nxt), i, nxt))
                    except StopIteration:
                        alive[i] = False
        finally:
            for it in readers:
                close = getattr(it, "close", None)
                if close:
                    close()

    round_no = 0
    cur = list(specs)
    intermediates: list[str] = []
    try:
        while len(cur) > budget.fan_in:
            nxt = []
            for gi in range(0, len(cur), budget.fan_in):
                group = cur[gi:gi + budget.fan_in]
                tmp_path = os.path.join(al.tmp_dir, f"merge-{round_no}-{gi}.run")
                writer = FramedWriter(ledger, tmp_path,
                                      {"type": "audit-merge-run", "audit": al.name},
                                      tmp_rename=True)
                hasher = hashlib.sha256()
                rows = 0

                def emit_mid(pl: bytes, _w=writer, _h=hasher) -> None:
                    nonlocal rows
                    _w.write(pl)
                    _h.update(pl)
                    rows += 1

                merge_group(group, emit_mid)
                writer.finish({"row_count": rows, "digest": hasher.hexdigest()})
                intermediates.append(tmp_path)
                nxt.append(tmp_path)
            cur = nxt
            round_no += 1
        merge_group(cur, emit)
    finally:
        for p in intermediates:
            try:
                os.remove(p)
            except OSError:
                pass


def _materialize_phase(al: AuditLayout, conn: sqlite3.Connection, meta: dict,
                       budget: AuditBudget, hook: Optional[tuple[str, int]]) -> None:
    if meta["materialize_ckpt"] is not None:
        raise AuditCorrupt("物化检查点已存在但状态未推进（审计存储损坏）")
    _clean_tmp(al)
    os.makedirs(al.tmp_dir, exist_ok=True)
    ledger = FileLedger(budget.max_open_files)
    K = int(meta["kversion"])
    # pass 1：findings 流式读出（生成次序），分块内存排序 → 溢写 run 文件
    runs: list[str] = []
    chunk: list[tuple[tuple, bytes]] = []
    chunk_bytes = 0
    run_no = 0
    staged_total = 0

    def flush_chunk() -> None:
        nonlocal chunk, chunk_bytes, run_no
        if not chunk:
            return
        chunk.sort(key=lambda t: t[0])
        path = os.path.join(al.tmp_dir, f"sort-{run_no}.run")
        writer = FramedWriter(ledger, path,
                              {"type": "audit-sort-run", "audit": al.name},
                              tmp_rename=True)
        hasher = hashlib.sha256()
        rows = 0
        for _key, pl in chunk:
            writer.write(pl)
            hasher.update(pl)
            rows += 1
        writer.finish({"row_count": rows, "digest": hasher.hexdigest()})
        runs.append(path)
        run_no += 1
        chunk = []
        chunk_bytes = 0

    for row in conn.execute(
            "SELECT entity_key, rule_id, target_key, start_b, end_b, event_id,"
            " reason FROM findings ORDER BY seq"):
        rec = [row["entity_key"], row["rule_id"], row["target_key"],
               int(row["start_b"]), row["end_b"] if row["end_b"] is None
               else int(row["end_b"]), row["event_id"], row["reason"]]
        payload = json_dumps(rec)
        if len(payload) > budget.single_row_limit:
            raise BudgetExceeded(
                f"单条结果 {len(payload)} 字节超过单行预算 "
                f"{budget.single_row_limit}（工作内存 {budget.mem_bytes}）")
        chunk.append((_sort_key_list(rec), payload))
        chunk_bytes += len(payload) + 64
        staged_total += 1
        if chunk_bytes >= budget.sort_chunk:
            flush_chunk()
    flush_chunk()

    # pass 2：归并 → 相邻合并 → 框式结果流 + 稀疏索引（tmp + 原子改名）
    rw = FramedWriter(ledger, al.results_path,
                      {"type": "audit-results", "audit": al.name,
                       "spec_digest": meta["spec_digest"]}, tmp_rename=True)
    iw = FramedWriter(ledger, al.results_index_path,
                      {"type": "audit-results-index", "audit": al.name},
                      tmp_rename=True)
    prev: Optional[list] = None
    out_rows = 0
    index_entries = 0
    hasher = hashlib.sha256()
    ihasher = hashlib.sha256()

    def flush_prev() -> None:
        nonlocal out_rows, index_entries
        record = {"seq": out_rows, "entity": prev[0], "rule_id": prev[1],
                  "target": prev[2], "start": prev[3], "end": prev[4],
                  "kversion": K, "event_id": prev[5], "reason": prev[6]}
        pl = json_dumps(record)
        if len(pl) > budget.single_row_limit:
            raise BudgetExceeded(
                f"单条结果 {len(pl)} 字节超过单行预算 {budget.single_row_limit}")
        if out_rows % RESULT_INDEX_EVERY == 0:
            ipl = pack_row_index(out_rows, rw.tell())
            iw.write(ipl)
            ihasher.update(ipl)
            index_entries += 1
        rw.write(pl)
        hasher.update(pl)
        out_rows += 1

    def emit_sorted(pl: bytes) -> None:
        nonlocal prev
        rec = json_loads(pl)
        if prev is not None and _mergeable_list(prev, rec):
            prev[4] = rec[4]          # 首尾相接且来源字段全同：合并
            return
        if prev is not None:
            flush_prev()
        prev = list(rec)

    try:
        if runs:
            _merge_run_streams(runs, lambda pl: _sort_key_list(json_loads(pl)),
                               ledger, budget, al, emit_sorted)
        if prev is not None:
            flush_prev()
        frames_bytes = rw.tell()
        rw.finish({"row_count": out_rows, "digest": hasher.hexdigest()})
        i_frames = iw.tell()
        iw.finish({"row_count": index_entries, "digest": ihasher.hexdigest()})
    except Exception:
        rw.abort()
        iw.abort()
        raise
    finally:
        for p in runs:
            try:
                os.remove(p)
            except OSError:
                pass
    ckpt = {"rows": out_rows, "digest": hasher.hexdigest(),
            "frames_bytes": frames_bytes, "index_rows": index_entries,
            "index_digest": ihasher.hexdigest(), "index_frames_bytes": i_frames}
    conn.execute("BEGIN IMMEDIATE")
    _meta_set(conn, "materialize_ckpt", ckpt)
    _meta_set(conn, "state", A_FINALIZING)
    _counter_add(conn, "results_emitted", out_rows)
    if ledger.peak > _counter_get(conn, "peak_open_files"):
        _counter_add(conn, "peak_open_files",
                     ledger.peak - _counter_get(conn, "peak_open_files"))
    _aevent(conn, "materialize_done",
            f"staged={staged_total} rows={out_rows} runs={run_no}")
    conn.commit()
    _hook_fire(hook, "kill-after-materialize")


# --------------------------------------------------------------------------- #
# 终验阶段（完成前校验）：整流重读，校验记录数、次序与摘要后才置 done
# --------------------------------------------------------------------------- #
def _finalize_phase(al: AuditLayout, conn: sqlite3.Connection, meta: dict,
                    budget: AuditBudget, hook: Optional[tuple[str, int]]) -> None:
    ckpt = meta["materialize_ckpt"]
    if ckpt is None:
        raise AuditCorrupt("缺少物化检查点（审计存储损坏）")
    ledger = FileLedger(budget.max_open_files)
    prev_key: Optional[tuple] = None
    prev_rec: Optional[dict] = None
    n = 0
    hasher = hashlib.sha256()
    try:
        for payload in iter_frames(
                al.results_path, ledger,
                {"type": "audit-results", "audit": al.name,
                 "spec_digest": meta["spec_digest"]}):
            rec = json_loads(payload)
            if not isinstance(rec, dict) or rec.get("seq") != n:
                raise AuditCorrupt(f"结果序号不连续（第 {n} 条处损坏）")
            key = _sort_key_dict(rec)
            if prev_key is not None:
                if key <= prev_key:
                    raise AuditCorrupt("结果次序损坏（未按固定次序排列）")
                if _mergeable_dict(prev_rec, rec):
                    raise AuditCorrupt("存在应合并而未合并的相邻记录（物化损坏）")
            prev_key, prev_rec = key, rec
            hasher.update(payload)
            n += 1
    except JoinFailed as exc:
        raise AuditCorrupt(f"结果流损坏: {exc}") from exc
    if n != int(ckpt["rows"]):
        raise AuditCorrupt(f"结果记录数 {n} 与物化检查点 {ckpt['rows']} 不符")
    if hasher.hexdigest() != ckpt["digest"]:
        raise AuditCorrupt("结果流摘要与物化检查点不符")
    try:
        verify_file(al.results_index_path, ledger,
                    {"type": "audit-results-index", "audit": al.name},
                    int(ckpt["index_rows"]), ckpt["index_digest"])
    except JoinFailed as exc:
        raise AuditCorrupt(f"结果索引损坏: {exc}") from exc
    _hook_fire(hook, "kill-before-done")
    conn.execute("BEGIN IMMEDIATE")
    _meta_set(conn, "state", A_DONE)
    _meta_set(conn, "result_rows", n)
    _meta_set(conn, "result_digest", hasher.hexdigest())
    _meta_set(conn, "done_at", time.time())
    _aevent(conn, "audit_done", f"rows={n}")
    conn.commit()


# --------------------------------------------------------------------------- #
# 运行（可恢复）：扫描 → 物化 → 终验；任何中断后再调即从持久检查点恢复
# --------------------------------------------------------------------------- #
def run_audit(root: str, name: str, *,
              hook: Optional[tuple[str, int]] = None) -> dict:
    """继续完成审计。中断（kill、Ctrl-C、同步关卡）后再调即从检查点恢复。"""
    if hook is None:
        hook = parse_hook(os.environ.get("SHUFFLE_AUDIT_HOOK"))
    al, conn, meta, budget = _open_audit(root, name)
    try:
        state = meta["state"]
        if state == A_DONE:
            return audit_status(root, name)
        if state == A_FAILED:
            raise AuditError(f"审计 {name!r} 已失败: {meta['error']}")
        if state in (A_CREATED, A_SCANNING):
            _scan_phase(al, conn, meta, budget, hook)
            meta["state"] = _meta_get(conn, "state")
        if meta["state"] == A_MATERIALIZING:
            _materialize_phase(al, conn, meta, budget, hook)
            meta["state"] = _meta_get(conn, "state")
            meta["materialize_ckpt"] = _meta_get(conn, "materialize_ckpt")
        if meta["state"] == A_FINALIZING:
            _finalize_phase(al, conn, meta, budget, hook)
        return audit_status(root, name)
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 分页读取（稳定游标）：只有 done 才可读；损坏显式失败，不返回成功前缀
# --------------------------------------------------------------------------- #
def _open_results_verified(al: AuditLayout, meta: dict,
                           ledger: FileLedger) -> tuple[Any, int, int]:
    """打开结果流并复核 header/footer 与 meta 的记录数、摘要（完成后校验）。

    返回 (文件句柄, 数据区起点偏移, 记录区结束偏移)。
    """
    ckpt = meta["materialize_ckpt"]
    if ckpt is None or meta["result_rows"] is None:
        raise AuditCorrupt("结果元数据不完整（审计存储损坏）")
    try:
        fh = ledger.open(al.results_path, "rb")
    except OSError as exc:
        raise AuditCorrupt(f"结果文件不可读（丢失或损坏）: {exc}") from exc
    try:
        header, data_start = read_header(fh, al.results_path)
        if header.get("type") != "audit-results" or header.get("audit") != al.name \
                or header.get("spec_digest") != meta["spec_digest"]:
            raise AuditCorrupt("结果 header 与审计规格不符（文件被替换或损坏）")
        fh.seek(int(ckpt["frames_bytes"]))
        magic = fh.read(len(J_FOOTER_MAGIC))
        if magic != J_FOOTER_MAGIC:
            raise AuditCorrupt("结果 footer 缺失（文件被截断或改写）")
        (flen,) = struct.unpack("<I", fh.read(4))
        fbytes = fh.read(flen)
        if len(fbytes) != flen:
            raise AuditCorrupt("结果 footer 截断（文件被截断或改写）")
        try:
            footer = json_loads(fbytes)
        except (ValueError, UnicodeDecodeError) as exc:
            raise AuditCorrupt(f"结果 footer 损坏: {exc}") from exc
        if footer.get("row_count") != int(meta["result_rows"]):
            raise AuditCorrupt("结果 footer 记录数与终验元数据不符")
        if footer.get("digest") != meta["result_digest"]:
            raise AuditCorrupt("结果 footer 摘要与终验元数据不符")
        if os.path.getsize(al.results_path) != fh.tell():
            raise AuditCorrupt("结果文件长度与终验记录不符（尾部被改写）")
    except JoinFailed as exc:
        fh.close()
        raise AuditCorrupt(f"结果流损坏: {exc}") from exc
    except Exception:
        fh.close()
        raise
    return fh, data_start, int(ckpt["frames_bytes"])


def read_audit(root: str, name: str, *, limit: int = 1024,
               cursor: Optional[str] = None) -> dict:
    """按稳定游标分页读取结果。游标绑定审计身份、表持久身份、知识版本、
    业务区间、规则集合与当前位置；换审计/换参数/损坏都显式拒绝。"""
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise AuditError("limit 必须是正整数")
    al, conn, meta, budget = _open_audit(root, name)
    try:
        if meta["state"] != A_DONE:
            raise AuditNotReady(
                f"审计状态为 {meta['state']}，结果尚未完成核验，"
                "拒绝把未完成的前缀当作成功结果返回")
        pos = 0
        if cursor is not None:
            body = _cursor_decode(meta["audit_token"], "read", cursor)
            for key, expect in (("tt", meta["table_token"]), ("kv", meta["kversion"]),
                                ("t0", meta["t0"]), ("t1", meta["t1"]),
                                ("rd", meta["rules_digest"])):
                if body.get(key) != expect:
                    raise AuditCursorError(
                        f"游标绑定的参数 {key}={body.get(key)!r} 与审计 {expect!r} "
                        "不符（不得把游标用于另一组参数）")
            pos = body.get("pos")
            if not isinstance(pos, int) or isinstance(pos, bool) or pos < 0:
                raise AuditCursorError("游标位置非法（内容被破坏）")
        total = int(meta["result_rows"])
        if pos > total:
            raise AuditCursorError("游标位置越出结果总数（内容被破坏）")
        ledger = FileLedger(budget.max_open_files)
        fh, data_start, _frames_end = _open_results_verified(al, meta, ledger)
        try:
            # 稀疏索引定位（流式扫描索引，不把索引收进容器）
            seek_off = None
            seek_seq = 0
            if pos > 0:
                try:
                    for pl in iter_frames(al.results_index_path, ledger,
                                          {"type": "audit-results-index",
                                           "audit": al.name}):
                        s, o = unpack_row_index(pl)
                        if s <= pos:
                            seek_seq, seek_off = s, o
                        else:
                            break
                except (JoinFailed, OSError) as exc:
                    raise AuditCorrupt(f"结果索引损坏: {exc}") from exc
            fh.seek(seek_off if seek_off is not None else data_start)
            # 顺序扫描到 pos（逐条验摘要；跳过的记录同样校验，不静默放过损坏）
            seq = seek_seq
            try:
                while seq < pos:
                    pl = _read_frame_at(fh, al.results_path)
                    if pl is None:
                        raise AuditCorrupt("结果流在游标位置前意外结束（损坏）")
                    seq += 1
                records: list[dict] = []
                while len(records) < limit and seq < total:
                    pl = _read_frame_at(fh, al.results_path)
                    if pl is None:
                        raise AuditCorrupt("结果流在终验记录数前意外结束（损坏）")
                    rec = json_loads(pl)
                    if not isinstance(rec, dict) or rec.get("seq") != seq:
                        raise AuditCorrupt(f"结果序号不连续（第 {seq} 条处损坏）")
                    records.append(rec)
                    seq += 1
            except JoinFailed as exc:
                raise AuditCorrupt(f"结果流损坏: {exc}") from exc
        finally:
            fh.close()
        next_cursor = None
        if seq < total:
            next_cursor = _cursor_encode(meta["audit_token"], "read", {
                "tt": meta["table_token"], "kv": meta["kversion"],
                "t0": meta["t0"], "t1": meta["t1"], "rd": meta["rules_digest"],
                "pos": seq})
        return {"name": name, "kversion": meta["kversion"], "t0": meta["t0"],
                "t1": meta["t1"], "rules_digest": meta["rules_digest"],
                "total": total, "records": records, "cursor": next_cursor}
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 删除：只移除审计自己的目录，不影响历史表、其他审计或正在读取的结果
# --------------------------------------------------------------------------- #
def delete_audit(root: str, name: str) -> dict:
    al = AuditLayout(root, name)
    if not os.path.exists(al.db_path):
        raise AuditNotReady(f"审计不存在: {al.dir}")
    shutil.rmtree(al.dir, ignore_errors=True)
    return {"name": name, "deleted": True}
