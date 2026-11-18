"""本机多进程 shuffle 交换层（SQLite 协调 + 文件分区段）。

公开入口：
    shuffle.coordinator  作业/attempt/确认/恢复/清理 的协调逻辑
    shuffle.worker       真实工人进程入口
    shuffle.reader       分区读取器（仅消费 winner 段）
    shuffle.cli          create / launch-attempt / inspect / read-partition /
                         recover / cleanup 命令行接口
    shuffle.join         双输入关联算子：消费两个作业的 winner 段，落盘关联结果
    shuffle.join_cli     关联算子命令行接口 create/run/status/read/delete
    shuffle.history      双时间历史表：知识版本 × 业务时间的入仓与查询
    shuffle.hist_cli     历史表命令行接口 create/submit/run/status/point/...
"""

__all__ = ["common", "coordinator", "worker", "reader", "cli",
           "join_common", "join", "join_cli", "history", "hist_cli"]
