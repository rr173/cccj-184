"""命令行接口。

规定接口：create / launch-attempt / inspect / read-partition / recover / cleanup
另含工人进程入口与 confirm/cancel 辅助命令。所有输出为 JSON。
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from typing import Optional

from . import coordinator as coord
from . import reader as reader_mod
from .common import Layout, ShuffleError


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True))


def _cmd_create(args) -> int:
    layout = Layout(args.root)
    splits = []
    for item in args.split:
        if "=" not in item:
            raise ShuffleError("--split 格式应为 split_id=/path/to/input.jsonl")
        sid, path = item.split("=", 1)
        splits.append((sid, path))
    result = coord.create_job(layout, args.job, splits, args.partitions, args.version)
    _print(result)
    return 0


def _spawn_worker(layout: Layout, job: str, split: str, attempt: int,
                  mem_bytes: int, hook: Optional[str]) -> subprocess.Popen:
    env = dict(os.environ)
    if hook:
        env["SHUFFLE_HOOK"] = hook
    cmd = [sys.executable, "-m", "shuffle.worker",
           "--root", layout.root, "--job", job, "--split", split,
           "--attempt", str(attempt), "--mem-bytes", str(mem_bytes)]
    if hook:
        cmd += ["--hook", hook]
    return subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            cwd=_project_root())


def _project_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _cmd_launch_attempt(args) -> int:
    layout = Layout(args.root)
    info = coord.launch_attempt(layout, args.job, args.split, args.mem_bytes)
    result = {"attempt": info, "worker": None}
    if args.spawn:
        proc = _spawn_worker(layout, args.job, args.split, info["attempt_no"],
                             args.mem_bytes, args.hook)
        result["worker"] = {"pid": proc.pid}
        if args.wait:
            out, err = proc.communicate(timeout=args.timeout)
            result["worker"]["returncode"] = proc.returncode
            if out:
                result["worker"]["stdout"] = out.decode("utf-8", "replace").strip()
            if err:
                result["worker"]["stderr"] = err.decode("utf-8", "replace").strip()
            if proc.returncode != 0:
                result["worker"]["ok"] = False
                _print(result)
                return 1
        if args.claim:
            claim = coord.claim_winner(layout, args.job, args.split, info["attempt_no"])
            result["claim"] = claim
    _print(result)
    return 0


def _cmd_confirm(args) -> int:
    layout = Layout(args.root)
    result = coord.claim_winner(layout, args.job, args.split, args.attempt)
    _print(result)
    return 0


def _cmd_inspect(args) -> int:
    layout = Layout(args.root)
    _print(coord.inspect(layout, args.job))
    return 0


def _cmd_read_partition(args) -> int:
    layout = Layout(args.root)
    with reader_mod.open_reader(layout, args.job, timeout=args.timeout) as r:
        rows = list(r.read_partition(args.partition))
    _print({"partition": args.partition, "row_count": len(rows), "rows": rows})
    return 0


def _cmd_recover(args) -> int:
    layout = Layout(args.root)
    _print(coord.recover(layout, args.job))
    return 0


def _cmd_cleanup(args) -> int:
    layout = Layout(args.root)
    _print(coord.cleanup(layout, args.job, destroy=args.destroy))
    return 0


def _cmd_cancel(args) -> int:
    layout = Layout(args.root)
    coord.cancel_job(layout, args.job)
    _print({"job_id": args.job, "state": "cancelled"})
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m shuffle.cli",
                                description="本机多进程 shuffle 交换层")
    sub = p.add_subparsers(dest="command", required=True)

    c = sub.add_parser("create", help="建作业并固定 split 清单/摘要/分区数/分区版本")
    c.add_argument("--root", required=True)
    c.add_argument("--job", required=True)
    c.add_argument("--split", action="append", required=True,
                   help="split_id=/path/input.jsonl，可重复，顺序即固定清单次序")
    c.add_argument("--partitions", type=int, required=True)
    c.add_argument("--version", type=int, default=0)
    c.set_defaults(func=_cmd_create)

    l = sub.add_parser("launch-attempt", help="登记 attempt，可直接拉起真实工人进程")
    l.add_argument("--root", required=True)
    l.add_argument("--job", required=True)
    l.add_argument("--split", required=True)
    l.add_argument("--mem-bytes", type=int, default=1 << 20)
    l.add_argument("--spawn", action="store_true", help="拉起独立工人进程")
    l.add_argument("--wait", action="store_true", help="等待工人进程结束")
    l.add_argument("--claim", action="store_true", help="工人完成后立即确认 winner")
    l.add_argument("--hook", choices=["pause-after-first", "kill-mid-segment"])
    l.add_argument("--timeout", type=float, default=120.0)
    l.set_defaults(func=_cmd_launch_attempt)

    cf = sub.add_parser("confirm", help="确认某完整 attempt 为 winner")
    cf.add_argument("--root", required=True)
    cf.add_argument("--job", required=True)
    cf.add_argument("--split", required=True)
    cf.add_argument("--attempt", type=int, required=True)
    cf.set_defaults(func=_cmd_confirm)

    i = sub.add_parser("inspect", help="查看作业/attempt/段/reader/事件全状态")
    i.add_argument("--root", required=True)
    i.add_argument("--job", required=True)
    i.set_defaults(func=_cmd_inspect)

    r = sub.add_parser("read-partition", help="只按 winner 流式读取目标分区")
    r.add_argument("--root", required=True)
    r.add_argument("--job", required=True)
    r.add_argument("--partition", type=int, required=True)
    r.add_argument("--timeout", type=float, default=30.0)
    r.set_defaults(func=_cmd_read_partition)

    rc = sub.add_parser("recover", help="协调器重启恢复：核对 winner、清理死 attempt")
    rc.add_argument("--root", required=True)
    rc.add_argument("--job", required=True)
    rc.set_defaults(func=_cmd_recover)

    cl = sub.add_parser("cleanup", help="回收本作业受管文件（保护打开中的 winner 段）")
    cl.add_argument("--root", required=True)
    cl.add_argument("--job", required=True)
    cl.add_argument("--destroy", action="store_true", help="无打开 reader 时销毁作业目录")
    cl.set_defaults(func=_cmd_cleanup)

    x = sub.add_parser("cancel", help="取消作业：拒绝新读取")
    x.add_argument("--root", required=True)
    x.add_argument("--job", required=True)
    x.set_defaults(func=_cmd_cancel)
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except ShuffleError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False),
              file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
