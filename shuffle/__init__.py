"""本机多进程 shuffle 交换层（SQLite 协调 + 文件分区段）。

公开入口：
    shuffle.coordinator  作业/attempt/确认/恢复/清理 的协调逻辑
    shuffle.worker       真实工人进程入口
    shuffle.reader       分区读取器（仅消费 winner 段）
    shuffle.cli          create / launch-attempt / inspect / read-partition /
                         recover / cleanup 命令行接口
"""

__all__ = ["common", "coordinator", "worker", "reader", "cli"]
