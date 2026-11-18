"""双时间历史表的命令行接口。

子命令：create / submit / run / status / abandon / point / timeline / diff /
export / drop。运行中断（kill、Ctrl-C、同步关卡）后再次 ``run`` 即从持久
检查点恢复；重送同标识同绑定幂等返回原结果。所有输出为 JSON；同步关卡
经环境变量 SHUFFLE_HIST_HOOK 或 --hook 传入。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Optional

from . import history as H
from .common import ShuffleError
from .join_common import parse_hook


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True))


def _cmd_create(args) -> int:
    _print(H.create_table(args.workdir, args.name, mem_bytes=args.mem_bytes,
                          max_open_files=args.max_open_files))
    return 0


def _cmd_submit(args) -> int:
    _print(H.submit_batch(args.workdir, args.name, src_root=args.root,
                          src_job=args.job, batch_id=args.batch_id))
    return 0


def _cmd_run(args) -> int:
    hook = parse_hook(args.hook or os.environ.get("SHUFFLE_HIST_HOOK"))
    _print(H.run_batch(args.workdir, args.name, args.batch_id, hook=hook))
    return 0


def _cmd_status(args) -> int:
    _print(H.table_status(args.workdir, args.name))
    return 0


def _cmd_abandon(args) -> int:
    _print(H.abandon_batch(args.workdir, args.name, args.batch_id))
    return 0


def _table(args) -> H.HistoryTable:
    return H.HistoryTable(args.workdir, args.name)


def _cmd_point(args) -> int:
    t = _table(args)
    _print(t.point(args.entity, args.time, kversion=args.kversion))
    return 0


def _cmd_timeline(args) -> int:
    t = _table(args)
    _print(t.timeline(args.entity, args.t0, args.t1, kversion=args.kversion,
                      limit=args.limit, cursor=args.cursor))
    return 0


def _cmd_diff(args) -> int:
    t = _table(args)
    _print(t.diff(args.entity, args.t0, args.t1, kold=args.kold, knew=args.knew,
                  limit=args.limit, cursor=args.cursor))
    return 0


def _cmd_export(args) -> int:
    t = _table(args)
    _print(t.export(kversion=args.kversion, t0=args.t0, t1=args.t1,
                    limit=args.limit, cursor=args.cursor))
    return 0


def _cmd_drop(args) -> int:
    _print(H.drop_table(args.workdir, args.name))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m shuffle.hist_cli",
        description="双时间历史表：知识版本 × 业务时间的入仓与查询")
    sub = p.add_subparsers(dest="command", required=True)

    c = sub.add_parser("create", help="创建历史表（独立目录 + 持久身份）")
    c.add_argument("--workdir", required=True)
    c.add_argument("--name", required=True)
    c.add_argument("--mem-bytes", type=int, required=True, help="工作内存预算（字节）")
    c.add_argument("--max-open-files", type=int, required=True, help="自管文件句柄预算")
    c.set_defaults(func=_cmd_create)

    s = sub.add_parser("submit", help="登记批次：固定完整 winner 清单与批次标识")
    s.add_argument("--workdir", required=True)
    s.add_argument("--name", required=True)
    s.add_argument("--root", required=True, help="源作业 root")
    s.add_argument("--job", required=True, help="源作业 id")
    s.add_argument("--batch-id", required=True, help="批次标识（幂等键）")
    s.set_defaults(func=_cmd_submit)

    r = sub.add_parser("run", help="继续完成入仓：接收→暂存→提交（可恢复）")
    r.add_argument("--workdir", required=True)
    r.add_argument("--name", required=True)
    r.add_argument("--batch-id", required=True)
    r.add_argument("--hook", default=os.environ.get("SHUFFLE_HIST_HOOK"),
                   help="同步关卡（测试用）：kill-after-receive-segment=k / "
                        "kill-after-receive / kill-after-stage / kill-after-commit")
    r.set_defaults(func=_cmd_run)

    st = sub.add_parser("status", help="表状态：head/批次进度/接收范围/计数器")
    st.add_argument("--workdir", required=True)
    st.add_argument("--name", required=True)
    st.set_defaults(func=_cmd_status)

    a = sub.add_parser("abandon", help="放弃未提交批次（只释放自己的引用与暂存）")
    a.add_argument("--workdir", required=True)
    a.add_argument("--name", required=True)
    a.add_argument("--batch-id", required=True)
    a.set_defaults(func=_cmd_abandon)

    pt = sub.add_parser("point", help="点查：知识版本 K 下业务时点 T 的归属")
    pt.add_argument("--workdir", required=True)
    pt.add_argument("--name", required=True)
    pt.add_argument("--entity", required=True)
    pt.add_argument("--time", type=int, required=True)
    pt.add_argument("--kversion", type=int, default=None)
    pt.set_defaults(func=_cmd_point)

    tl = sub.add_parser("timeline", help="有限业务区间的时间线（分页 + 游标）")
    tl.add_argument("--workdir", required=True)
    tl.add_argument("--name", required=True)
    tl.add_argument("--entity")
    tl.add_argument("--t0", type=int)
    tl.add_argument("--t1", type=int)
    tl.add_argument("--kversion", type=int, default=None)
    tl.add_argument("--limit", type=int, default=1024)
    tl.add_argument("--cursor", default=None)
    tl.set_defaults(func=_cmd_timeline)

    df = sub.add_parser("diff", help="跨知识版本差异读取（分页 + 游标）")
    df.add_argument("--workdir", required=True)
    df.add_argument("--name", required=True)
    df.add_argument("--entity")
    df.add_argument("--t0", type=int)
    df.add_argument("--t1", type=int)
    df.add_argument("--kold", type=int)
    df.add_argument("--knew", type=int)
    df.add_argument("--limit", type=int, default=1024)
    df.add_argument("--cursor", default=None)
    df.set_defaults(func=_cmd_diff)

    ex = sub.add_parser("export", help="流式全实体导出（实体 UTF-8 字节序）")
    ex.add_argument("--workdir", required=True)
    ex.add_argument("--name", required=True)
    ex.add_argument("--kversion", type=int, default=None)
    ex.add_argument("--t0", type=int, default=None)
    ex.add_argument("--t1", type=int, default=None)
    ex.add_argument("--limit", type=int, default=4096)
    ex.add_argument("--cursor", default=None)
    ex.set_defaults(func=_cmd_export)

    d = sub.add_parser("drop", help="删除历史表（只移除它自己的目录）")
    d.add_argument("--workdir", required=True)
    d.add_argument("--name", required=True)
    d.set_defaults(func=_cmd_drop)
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
