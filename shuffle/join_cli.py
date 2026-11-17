"""双输入关联算子的命令行接口。

子命令：create / run / status / read / delete。
运行中断（kill、Ctrl-C、同步关卡）后再次 ``run`` 即从持久检查点恢复。
所有输出为 JSON；同步关卡经环境变量 SHUFFLE_JOIN_HOOK 或 --hook 传入。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Optional

from . import join as join_mod
from .common import ShuffleError


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True))


def _cmd_create(args) -> int:
    result = join_mod.create_join(
        args.workdir, args.name,
        left_root=args.left_root, left_job=args.left_job,
        right_root=args.right_root, right_job=args.right_job,
        mode=args.mode, mem_bytes=args.mem_bytes,
        max_open_files=args.max_open_files,
        result_kind=args.result_kind,
        checkpoint_rows=args.checkpoint_rows,
    )
    _print(result)
    return 0


def _cmd_run(args) -> int:
    status = join_mod.run_join(args.workdir, args.name, hook=args.hook)
    _print(status)
    return 0


def _cmd_status(args) -> int:
    _print(join_mod.join_status(args.workdir, args.name))
    return 0


def _cmd_read(args) -> int:
    with join_mod.JoinResult(args.workdir, args.name) as result:
        rows = result.read(args.start, args.count)
        _print({"name": args.name, "row_count": result.row_count,
                "start": args.start, "count": len(rows), "rows": rows})
    return 0


def _cmd_delete(args) -> int:
    _print(join_mod.delete_join(args.workdir, args.name))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m shuffle.join_cli",
        description="双输入关联算子：消费两个 shuffle 作业的 winner 段并落盘关联结果")
    sub = p.add_subparsers(dest="command", required=True)

    c = sub.add_parser("create", help="建关联：固定两侧 winner 清单并登记持久段保护")
    c.add_argument("--workdir", required=True, help="关联工作目录（关联名称为其子目录）")
    c.add_argument("--name", required=True, help="关联名称")
    c.add_argument("--left-root", required=True)
    c.add_argument("--left-job", required=True)
    c.add_argument("--right-root", required=True)
    c.add_argument("--right-job", required=True)
    c.add_argument("--mode", choices=list(join_mod.MODES), required=True)
    c.add_argument("--mem-bytes", type=int, required=True, help="总工作内存预算（字节）")
    c.add_argument("--max-open-files", type=int, required=True, help="同时打开文件数预算")
    c.add_argument("--result-kind", choices=list(join_mod.KINDS),
                   default=join_mod.KIND_FULL,
                   help="full 保留完整原行（含 value）；identity 只存身份字段")
    c.add_argument("--checkpoint-rows", type=int, default=4096,
                   help="emit 阶段每多少结果行落一次检查点")
    c.set_defaults(func=_cmd_create)

    r = sub.add_parser("run", help="运行关联到完成；中断后再跑即从检查点恢复")
    r.add_argument("--workdir", required=True)
    r.add_argument("--name", required=True)
    r.add_argument("--hook", default=os.environ.get("SHUFFLE_JOIN_HOOK"),
                   help="同步关卡（测试用）：kill-after-receive / "
                        "kill-after-receive-segment=k / kill-after-normalize / "
                        "kill-after-keysort / kill-at-emit=n / kill-before-finalize")
    r.set_defaults(func=_cmd_run)

    s = sub.add_parser("status", help="查看阶段进度、已持久接收范围、源段读取计数")
    s.add_argument("--workdir", required=True)
    s.add_argument("--name", required=True)
    s.set_defaults(func=_cmd_status)

    rd = sub.add_parser("read", help="按起始序号与条数读取结果（只有终验通过才可读）")
    rd.add_argument("--workdir", required=True)
    rd.add_argument("--name", required=True)
    rd.add_argument("--start", type=int, default=0)
    rd.add_argument("--count", type=int, default=100)
    rd.set_defaults(func=_cmd_read)

    d = sub.add_parser("delete", help="删除关联：释放它自己的源段保护并移除工作目录")
    d.add_argument("--workdir", required=True)
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
