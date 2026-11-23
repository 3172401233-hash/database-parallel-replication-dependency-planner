# database-parallel-replication-dependency-planner
规划并行复制的依赖、冲突和提交边界

## 概述

针对逻辑复制中"多个 worker 并行应用事务"的场景：事务之间存在行级、
唯一键和 DDL 依赖，两个事务单独看可以并行，但按提交顺序应用却可能与
源库最终状态不一致。本仓库在**纯本地内存模型**中构建依赖图并规划并发
应用批次——所有事务、依赖、worker 均为 Python 对象，不连接任何真实
数据库。

## 模型

`parallel_apply.model.Transaction` 描述一条事务：

- `txid` / `commit_lsn`：标识与源库提交顺序；
- `read_set` / `write_set`：行键集合（`"table:pk"` 形式）；
- `unique_keys`：触及的唯一约束标识（如 `"users.email"`）；
- `ddl_targets`：非空表示 DDL，作用于这些表；
- `prepared`：`False` 表示部分提交（源端尚未完整提交）。

## 依赖规则（`parallel_apply.graph`）

冲突边总是从较小 LSN 指向较大 LSN：

- 行级冲突：同一行键上的 RW / WR / WW 重叠；
- 唯一键竞争：两事务触及同一唯一约束，即使行键不同也串行；
- DDL 依赖：DDL 与其目标表上的任何 DML/DDL 互相串行。

## 调度（`parallel_apply.scheduler.plan_batches`）

输出 `ApplyPlan`：

- `batches`：可并行应用的批次，批内事务互相独立且不超过 worker 容量；
- `serial_ranges`：必须串行的 LSN 范围（依赖环、部分提交）；
- `cycles`：检测到的依赖环（输入不一致时显式报告，而不是静默调度）；
- `partial_commits`：因未 prepare 事务而被阻断的 txid（含其传递依赖）；
- `orphan_workers`：从未分到任务的孤立 worker。

## 崩溃模拟（`parallel_apply.simulator.simulate_apply`）

可指定某个 worker 在应用 N 条事务后崩溃：崩溃 worker 上未应用的事务
及其依赖者会被跳过，运行被标记为部分提交；安全不变式为"依赖未全部
应用的事务绝不应用"。

## 测试

```bash
python -m unittest
```

覆盖行级冲突、唯一键竞争、DDL 与 DML 顺序、依赖环、孤立 worker、
部分提交与 worker 崩溃。代码兼容 Python 3.8.2+。
