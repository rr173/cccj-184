"""时态引用完整性审计的命令行接口。

子命令：create / run / status / read / delete。运行中断（kill、Ctrl-C、同步
关卡）后再次 ``run`` 即从持久检查点恢复。所有输出为 JSON；同步关卡经环境
变量 SHUFFLE_AUDIT_HOOK 或 --hook 传入。

规则集合用 JSON 文件（--rules-file）或内联 JSON（--rules）给出，形如::

    [{"rule_id": "owner-ref",
      "source": {"kind": "prefix", "prefix": "cust:"},
      "path": ["owner", "id"]}]

source.kind ∈ all / prefix / entities（entities 另带 "entities": [...]）。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Optional

from . import audit as A
from .common import ShuffleError
from .join_common import parse_hook


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True))


def _load_rules(args) -> list:
    if args.rules_file:
        with open(args.rules_file, "r", encoding="utf-8") as fh:
            spec = json.load(fh)
    elif args.rules:
        spec = json.loads(args.rules)
    else:
        raise A.AuditError("必须用 --rules-file 或 --rules 给出规则集合")
    if isinstance(spec, dict) and "rules" in spec:
        spec = spec["rules"]
    return spec


def _cmd_create(args) -> int:
    _print(A.create_audit(
        args.root, args.name, table_workdir=args.table_workdir,
        table_name=args.table, rules=_load_rules(args),
        t0=args.t0, t1=args.t1, kversion=args.kversion,
        mem_bytes=args.mem_bytes, max_open_files=args.max_open_files))
    return 0


def _cmd_run(args) -> int:
    hook = parse_hook(args.hook or os.environ.get("SHUFFLE_AUDIT_HOOK"))
    _print(A.run_audit(args.root, args.name, hook=hook))
    return 0


def _cmd_status(args) -> int:
    _print(A.audit_status(args.root, args.name))
    return 0


def _cmd_read(args) -> int:
    _print(A.read_audit(args.root, args.name, limit=args.limit,
                        cursor=args.cursor))
    return 0


def _cmd_delete(args) -> int:
    _print(A.delete_audit(args.root, args.name))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m shuffle.audit_cli",
        description="双时间历史表的时态引用完整性审计")
    sub = p.add_subparsers(dest="command", required=True)

    c = sub.add_parser("create", help="创建审计：固定表/知识版本/业务区间/规则集合")
    c.add_argument("--root", required=True, help="审计根目录（审计自有受管目录的父目录）")
    c.add_argument("--name", required=True)
    c.add_argument("--table-workdir", required=True, help="历史表 workdir")
    c.add_argument("--table", required=True, help="历史表名")
    c.add_argument("--t0", type=int, required=True, help="业务区间起点（int64）")
    c.add_argument("--t1", type=int, default=None, help="业务区间终点（缺省=正无穷）")
    c.add_argument("--kversion", type=int, default=None,
                   help="知识版本（缺省=创建时固定当前 head）")
    c.add_argument("--rules-file", default=None, help="规则集合 JSON 文件")
    c.add_argument("--rules", default=None, help="规则集合内联 JSON")
    c.add_argument("--mem-bytes", type=int, required=True, help="工作内存预算（字节）")
    c.add_argument("--max-open-files", type=int, required=True, help="自管文件句柄预算")
    c.set_defaults(func=_cmd_create)

    r = sub.add_parser("run", help="继续完成审计：扫描→物化→终验（可恢复）")
    r.add_argument("--root", required=True)
    r.add_argument("--name", required=True)
    r.add_argument("--hook", default=os.environ.get("SHUFFLE_AUDIT_HOOK"),
                   help="同步关卡（测试用）：kill-after-audit-pair=N / "
                        "kill-after-materialize / kill-before-done")
    r.set_defaults(func=_cmd_run)

    s = sub.add_parser("status", help="审计状态：状态机/进度/真实读取量/结果摘要")
    s.add_argument("--root", required=True)
    s.add_argument("--name", required=True)
    s.set_defaults(func=_cmd_status)

    rd = sub.add_parser("read", help="按稳定游标分页读取结果（只有 done 才可读）")
    rd.add_argument("--root", required=True)
    rd.add_argument("--name", required=True)
    rd.add_argument("--limit", type=int, default=1024)
    rd.add_argument("--cursor", default=None)
    rd.set_defaults(func=_cmd_read)

    d = sub.add_parser("delete", help="删除审计（只移除它自己的目录）")
    d.add_argument("--root", required=True)
    d.add_argument("--name", required=True)
    d.set_defaults(func=_cmd_delete)
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except ShuffleError as exc:
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"},
                         ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
