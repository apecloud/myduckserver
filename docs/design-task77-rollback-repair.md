# task #77 回滚与孤儿清理修复设计

## 目标

在冻结提交 `5cfbe01d79b601c353fbecd9d0270a6489de6ef8` 之上，闭合对象表回滚的产品行为：

1. MySQL 和 PostgreSQL 的显式 `BEGIN` 在默认 autocommit 下都创建实际的 DuckDB `*sql.Tx`。
2. 对象表的 INSERT、UPDATE、DELETE、TRUNCATE、RETURNING 以及必要的 catalog 元数据操作复用当前 session transaction；回滚后未提交行不可见。
3. 前端协议 `ROLLBACK` 成功后，先释放/确认底层事务已 inactive，再在同一物理连接上调用产品拥有的 DuckLake orphan cleanup。不能通过第二条池连接，也不能与活动 `*sql.Tx` 并发使用同一连接。

## 非目标与边界

- 不修改 `591bdd2b`、`5cfbe01d` 或其父提交，不 amend 旧 OID，不合并 PR #489，不构建或发布镜像/标签。
- 不迁移旧表，不改变复制路径，不宣称 S3/MinIO 或全量对象存储 readiness；本轮产品验收限定为 fresh local bind-mount lake。
- extension-only 或缺少 metadata/data 完整配置时 cleanup 是 no-op；不直接删除文件，删除只能由 `ducklake_delete_orphaned_files('__myduck_ducklake', cleanup_all => true)` 完成。
- replication、recovery、maintenance 等非前端 origin 不触发用户回滚清理。

## 实现策略

- 在事务计划构建阶段先设置 `IgnoreAutoCommit`，让 GMS 的 `StartTransaction` 回调看到显式 BEGIN，而不把它误判为空的 autocommit wrapper。
- 由 session/connection-pool 维护一条逻辑 session 到物理连接及底层 transaction 的绑定；所有对象表执行器在取 executor 时优先使用当前 transaction。
- 回滚顺序固定为：回滚底层 DuckDB transaction -> 使 session transaction 映射失效 -> 回滚 GMS 内存 transaction -> 复用原物理连接执行 DuckLake cleanup。每一步错误都保留，cleanup 错误不能伪装成回滚成功。
- PostgreSQL 的事务控制走 handler 直接执行，因此在 `ROLLBACK` 返回成功后调用同一连接 cleanup；MySQL 走 `backend.Session.Rollback` 的统一 hook。cleanup 上下文保留 `*sql.Context`，但切换到 service-owned maintenance origin 并去除请求取消。
- 连接/事务收尾使用 identity-safe 的映射删除和串行化，避免旧 transaction 的 defer 清除后来建立的新 transaction；cleanup/临时表清理不得在活动 transaction 上执行。
- 事务完成回调注册和完成使用同一把锁。完成先于注册时先写入带结果的完成标记，迟到的注册立即收到 `success=false`（或真实成功结果），不能把 operation lease 留在一个永远不会再被触发的状态。池关闭、连接淘汰和意外回滚也都算物理事务的终端事件。
- pool finalizer 只在锁内更新事务/连接映射并收集回调，然后按“session 锁 -> lifecycle 锁 -> callback”的顺序退出。因此 callback 可以同步重入 pool 的释放和只读接口，并且依然在原 public finalizer 返回前完成。provider 代际切换的外层锁还可能持有，callback 不得重入 provider DDL、cleanup、`Close` 或 `Restart`。
- callback 属于外部回调，可能 panic。一个 callback panic 时，pool 仍要执行同一事务的其余 callback、provider finished hook 和同一批的其余完成事件，最后把第一个 panic 原样抛回。这样既不吞异常，也不会因计数未释放而把代际切换永久卡住。
- 完成标记只保留一个有限窗口：按完成时间懒清理过期 tombstone，并设置固定数量上限；只有首次终态派发的 owner 在 callback 和 provider finished hook 都结束后才能记录 tombstone，迟到注册只能消费结果，不能提前把仍在派发的状态变成可清理。清理只允许删除已记录的 tombstone，活动事务和仍等待 callback/finished hook 的条目不能被驱逐。窗口外的旧物理事务注册必须 fail-closed，由调用方立即走 fallback 释放路径，不能创建新的永久 pending 状态。`Close`/`Reset` 会清掉已完成缓存，但保留待处理或 malformed 状态供收尾。
- 普通连接、execution snapshot、当前 schema/catalog 和连接初始化统一经过 provider 的 connection-admission guard。guard 在进入 pool lifecycle 前检查一次，拿到 lifecycle/session 锁后再检查一次，关闭切换窗口。poison 时所有新连接工作都拒绝；正常代际切换期间只允许已预约的 rollback cleanup 和显式 recovery 上下文使用已知物理连接，普通前台和 maintenance 请求都失败关闭。
- `*sql.Tx` 只表示物理事务身份，不是执行许可。创建或查询事务的方法返回后，调用者如果还要执行 SQL、准备语句、访问 `Conn.Raw` 或消费结果，必须重新取得并一直保留 execution snapshot lease；lease 要覆盖到同步调用结束、`Rows`/`Row` 扫描结束或 iterator 关闭。代际切换可以发生在两次协议请求之间，但不能在已准入的 driver 调用中间回滚或关闭它的物理 owner。
- adapter 的直接 `Exec`/`Query`/事务执行辅助方法也遵守同一规则：同步结果在返回前释放 lease；行结果把 lease 交给包装器，在 `Close`、EOF 或 `Scan` 后释放。复制路径如果已经捕获 `(conn, tx)`，必须把同一 lease 传过整段 flush/write/LSN 更新，不能再用裸事务指针开始下一次 driver 调用。
- FlightSQL 不再直接把 `provider.Storage()` 当作无生命周期的连接池。每个一次性请求先取得 provider operation lease 和 pool direct-connection lease；查询流消费完成后才关闭连接并释放。独立 prepared statement 和 FlightSQL transaction 在 handle 整个存活期保留同一 lease，transaction 内 prepared statement 复用其 owner，不单独关闭连接。服务关闭时先拒绝新 handle，清理遗留 prepared/transaction handle 并释放 lease，再允许 provider 关闭或重启旧代际；不能由 FlightSQL 直接关闭 provider 的共享 `*sql.DB`。
- rollback cleanup 的 provider 屏障必须在物理 finalizer 前预约。预约失败不能跳过已有物理事务的 rollback，但必须跳过 cleanup SQL 并把预约错误返回。对没有物理 `*sql.Tx` 的 GMS-only 回滚，预约失败后不再额外获取连接。
- COPY 的 `discardToSync` 不把网络 EOF/解码错误当成普通返回；它先中止并等待 loader、释放 snapshot/operation lease、清除 pending completion，再把原始接收错误返回给上层。这样断开连接和半截 extended exchange 不会留下 COPY 状态。

## 错误与并发约束

- rollback 返回 driver 的真实错误；只有明确的 `ErrTxDone` 或 DuckDB “no transaction is active” 才视为底层已 inactive。
- 非预期 rollback 错误不得继续复用失效 transaction；应清除绑定并让后续请求重新获取连接/事务。
- 同一 session 的事务建立、获取、提交、回滚和关闭串行；不同 session 的 cleanup 也不得在同一物理连接上交叉执行。
- 代际关闭不能只等待 operation lease 后才进入 pool teardown；已完成但等待 Sync 的 COPY lease 必须能由物理事务 teardown 触发终端回调。活动 loader 仍由 snapshot lease 保持到 child 结束，随后才允许关闭其物理 owner。
- row inserter 必须原子地捕获 `(conn, tx, executor)` 快照；旧 transaction 的临时表清理只能在其 owner 仍匹配时使用 transaction executor，否则在事务已 inactive 后使用同一连接。
- 如果 pool teardown 发现无法解释的 transaction/owner 映射，代际切换进入不可复用的 poisoned 终态：保留原始映射和未知物理 owner，不尝试类型不安全的 rollback/close；释放切换屏障并唤醒等待者，让 `Close`/`Restart` 在有界时间内返回原始错误。后续 DuckLake 事务和操作不再等待或重新进入这个代际，避免把失效状态当作新代际使用。
- 已 poison 的代际没有 cleanup/recovery 豁免；所有新 SQL 和重开都返回同一个可观测错误。cleanup/recovery 豁免只适用于状态仍完整的正常代际切换，不能用来绕过 poison。
- retained handle 的关闭和正在执行的 handle 操作必须串行。关闭只在该 handle 的同步调用或查询流退出后释放 generation lease；并发的 commit/rollback、prepared close 或服务 shutdown 不能让旧连接在仍有 driver 调用时重新进入池 teardown。

## 验收

先运行 focused unit tests、`gofmt`、`git diff --check` 和带 `duckdb_arrow`/ICU 的相关包测试；然后由本负责人启动唯一 fresh local runtime，逐条保存命令、exit、raw SHA 和 before/after inventory：

- MySQL simple wire：`BEGIN; INSERT; SELECT; ROLLBACK; SELECT`，行数应 `1 -> 2 -> 1`，未登记 parquet 应被清理且登记文件保留。
- PostgreSQL simple wire：同一序列，行数应 `1 -> 2 -> 1`，并验证 cleanup。
- PostgreSQL extended wire：Parse/Bind/Execute 的 INSERT/ROLLBACK 路径至少覆盖一次，确认不绕过 active transaction。
- 错误/边界：cleanup SQL 错误可见、非前端 origin no-op、extension-only no-op、rollback 后连接可再次使用。
- 生命周期：正常 generation transition 等待已准入的事务 driver 调用、行消费和 FlightSQL 流/handle；transition 公布后发起的新调用在任何 driver SQL 或 `Conn.Raw` 前失败。FlightSQL 服务关闭后 prepared/transaction handle 清单为空，共享 provider storage 仍只由 provider 关闭。

所有结果只绑定本 child 的 exact OID、parent/tree、binary/runtime 和本地 lake；未覆盖的 restart/recreate、全树 orphan、S3/MinIO 必须明确列为证据缺口。
