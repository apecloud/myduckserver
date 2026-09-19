# Superset SQL Lab 的 PostgreSQL 会话标识兼容

## 问题与证据

2026-09-19 在源码 `b045a60a` 的不可变镜像 `9bf2c7c4…` 上，用 Superset 6.1.0 和 psycopg2-binary 2.9.13 查询 `shop_us`。建连和五张业务表的发现都成功，但 SQL Lab 返回 HTTP 500。完整错误来自前置语句 `SELECT pg_backend_pid()`：函数不存在。对同一连接直接执行销售查询可返回 143163，说明尚未进入业务 SQL 就失败了。

Superset 的 PostgreSQL 驱动会在每次 SQL Lab 查询前取得后端标识，把它记作后续取消请求的定位信息。它不是可跳过的可选探针。测试不能通过删除这条调用或绕过 SQL Lab 获得通过。

## 设计与主要改动

MyDuck 在一个进程中处理多个连接，所以不能把操作系统进程号当作每个会话的唯一标识。使用现有 PG 连接 ID 的低 31 位作为正整数会话标识：高位是 MyDuck 内部用于区分 MySQL/PG 连接的标记，不属于 PostgreSQL 的 int4 标识。

在现有 PG 查询转换入口中识别独立的 `SELECT pg_backend_pid()` 和 `SELECT pg_catalog.pg_backend_pid()`，返回该连接的整数标识。握手 `BackendKeyData.ProcessID` 使用同一标识。只匹配完整语句，避免改写字符串常量或不相关的 SQL。预备语句仅在当前连接内缓存，标识在连接生命周期内稳定。

主要文件：`pgserver/in_place_handler.go` 处理查询；`connection_handler.go` 统一标识及握手；`listener.go` 删除不再使用的共享进程号；`backend_pid_test.go` 验证实际协议行为。

## 兼容边界

该改动解除 SQL Lab 在正常查询前的失败，并使并发 PG 会话可区分。它不实现 `pg_stat_activity`、`pg_terminate_backend`、取消请求认证/调度，也不声称 Superset 的“停止查询”功能已验收。带别名或嵌入其他表达式的调用不在本次独立语句兼容范围内。

本次不改变数据、复制起点、复制过滤或稳定镜像标签。新镜像必须重新执行两款应用的真实业务查询和看板验收，不能把本地候选结果写成旧镜像通过。

## 验证计划

1. 原镜像保存 Superset HTTP 500 请求/响应和同连接的最小 SQL 对照；新增集成测试先在原源码复现两个协议模式的失败。
2. 简单查询与缓存预备语句都验证：返回正整数、与握手标识相同、重复调用不变、两个并发连接不同；字符串常量保持原样，后续业务查询可执行。
3. 运行已有 Metabase/Superset PG 兼容回归和 PG 包测试。
4. 用修复二进制驱动真实 Superset 的 SQL Lab、数据集和看板，逐项排查后续缺口。独立审查、合入及不可变镜像构建后，再以同一新摘要重跑 Metabase/Superset 的完整业务看板与截图。
