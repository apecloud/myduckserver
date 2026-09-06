# task #83：0.2.1 / 0.3 优雅退出设计

## 1. 背景与基线

task #74 的既有记录包含初始化、双协议读写和删除容器后重建读回操作，但 wrapper、初始主容器 bind 和同卷连续 provenance 没有完全证明；其中正常停止也明确不合格。task #83 会在独立目录里重新保存完整命令和路径身份，不把旧记录越界写成已经完成的本次证据：

- 镜像：`apecloud/myduckserver:v0.2.1-dev.20260827.2`
- 不可变索引：`sha256:8285369844a1da259910c2d100d8af7196019d7f9cf0ec72babcacdccdd0a780`
- 源码：`2eb4143b82ca1102d5162079dcc481cfc9093d6d`
- 父提交：`68760b5a7433e45607e9db69fc84412c0ec2989e`
- 源码树：`38c864cbc1e6051558f4faa9097add3b868b1492`

既有运行记录显示，Docker 在 `16:37:37.423557589` 发送 `SIGTERM`，30.007 秒后又发送 `SIGKILL`，容器最终 `ExitCode=137`、`OOMKilled=false`。`docker stop --time 30` 本身返回 0 只代表 Docker 成功执行停止操作，不能代表服务优雅退出。

只读比较还确认：上述 `0.2.1` 源码与比较时的 `0.3/main`（`dd6d79113ca0b3132141c6f7ee7f36025b62f843`，tree `cdfcb7dc831b07a27136ce6cc026dd94d49293dd`）都使用同一套 `docker/entrypoint.sh`，Dockerfile 也都使用 shell 形式的入口。因此两条线具有同一个启动和信号传递缺陷，应使用同一份最小修复，再分别验证。若 @卡特 最终指定更新的 `0.3` 集成基线，移植前必须重新核对启动、信号和关闭链，不能沿用这个比较结论代替复核。

## 2. 根因

当前关闭链同时存在三个问题，缺少其中任一层都不能宣称数据库优雅退出。

### 2.1 Docker 的 1 号进程没有直接运行入口脚本

Dockerfile 使用：

```dockerfile
ENTRYPOINT /home/admin/entrypoint.sh
```

实际容器 1 号进程是 `/bin/sh -c /home/admin/entrypoint.sh`。Docker 把 `SIGTERM` 发给 `/bin/sh`，后面的 Bash trap 没有收到信号。既有日志中也没有 `Received shutdown signal`。

### 2.2 入口脚本记录了错误的 PID

脚本使用：

```bash
nohup myduckserver ... | tee ... &
echo "$!" > "${PID_FILE}"
```

Bash 中 `$!` 是后台管道最后一个进程的 PID，这里是 `tee`，不是 `myduckserver`。即使 Bash 收到信号，现有 `cleanup` 也会杀错进程；函数返回后脚本仍会继续 readiness/init 或永久轮询。

### 2.3 Go 主进程没有关闭协调器

`main.go` 没有处理 `SIGTERM`/`SIGINT`。MySQL `Start()` 阻塞在监听循环，PostgreSQL 在另一个 goroutine 中监听。默认信号终止会直接结束 Go 进程，`myServer.Close()`、`pgServer.Close()`、`engine.Close()` 以及 `defer provider.Close()` 都没有可靠执行；现有证据因此只能证明被强制终止后的恢复能力，不能证明有序关闭完成。

Flight SQL 当前把 `os.Kill` 注册为关闭信号，但 `SIGKILL` 不能被捕获，而且它与 MySQL/PostgreSQL 没有统一关闭顺序。

## 3. 目标与非目标

### 3.1 目标

1. Docker 的 `SIGTERM` 能到达入口脚本。
2. 入口脚本只向真实 `myduckserver` 进程转发一次信号，并等待它退出、回收子进程、删除 PID 文件，然后用服务的退出状态结束容器。
3. Go 进程收到 `SIGTERM`、`SIGINT` 或 `SIGQUIT` 后停止 MySQL/PostgreSQL 新连接，等待两套 serve loop 返回，依次完成 `engine.Close()` 和 `provider.Close()`，留下明确的关闭日志。
4. `SETUP_MODE=SERVER`、无活动客户端/长查询、无 MySQL replica 和 PostgreSQL subscription 的场景在 Docker 30 秒宽限期内正常退出，容器退出码为 0，无 `SIGKILL`、无 OOM。
5. 停止前分别通过 MySQL 和 PostgreSQL 写入的数据，在停止、同盘重启以及删除/重建容器后都能由两套协议读回。
6. 同一逻辑补丁分别落到 `0.2.1` 和 `0.3` 的独立干净工作树，各自产生可追溯的源码、镜像和运行证据。

### 3.2 非目标

- 不修改任何 stable 版本标签、`latest`、promotion workflow 或已冻结镜像；不向 registry push 本任务候选。
- 不在 task #77 的共享 `0.3` 工作树里改代码或混入提交。
- 不为任意长查询承诺固定强制终止时间，也不以 `SIGKILL` 冒充优雅退出。
- 不在本任务重构 replica、事务或存储层生命周期；`REPLICA` 模式和存在 PostgreSQL subscription 的退出仍属未证明范围。
- 不把一次本机结果直接描述成正式发布结果。
- `0.3` 验证关闭 DuckLake/默认对象存储配置，只证明本地表关闭与恢复，不证明 task #77 的对象存储、rollback 或 orphan 行为。
- Flight SQL backend 没有统一 Close，可能仍持有 prepared statement/open transaction；task #83 产品验证保持默认 `flightsql-port=-1`。实现可以停止并 join Flight transport，但不能据此声称 Flight 资源已优雅关闭。

## 4. 共用最小修复

### 4.1 Dockerfile

把入口改成 exec 形式：

```dockerfile
ENTRYPOINT ["/home/admin/entrypoint.sh"]
```

这样 Bash 直接成为容器 1 号进程，能收到 Docker 发出的信号。

### 4.2 入口脚本

保留现有 readiness、初始化 SQL 和 replica setup 行为，只改变进程监督方式：

1. 在容器自己的日志目录创建 FIFO。Bash 在启动窗口临时持有一个读写保护描述符，`myduckserver` 和 `tee` 各自关闭继承的保护描述符后打开自己的 FIFO 端；等两个 PID 都登记后，Bash 再关闭保护描述符。这样任一端都不会在 PID 尚未登记时卡在 FIFO open，同时仍保留 Docker logs 和文件日志。
2. 把真实服务 PID 和 `tee` PID 都保存在内存中，PID 文件只写真实服务 PID。
3. trap 只记录第一个 TERM/INT/QUIT，并立即同时向已经登记的真实服务和 readiness/init 子进程发信号；它不在 trap 内等待。主流程随后等待服务完成关闭、等待 `tee` 排空日志，再回收 setup 子进程。每个子进程都有独立的“已经发过信号”状态，重复 Docker 信号和后续收尾调用都不能把同一信号重复发给服务。
4. 无论正常退出、readiness 超时、启动步骤失败还是信号退出，都终止并回收仍存活的服务进程、等待 `tee`、删除临时/PID/FIFO 文件；入口脚本保留服务或原始启动步骤的退出状态。初始化 SQL 继续保持旧版 best-effort 语义，单条 `mysqlsh`/`psql` 失败不能在本任务中被改成容器启动失败。
5. 若停止发生在 readiness/init 客户端运行期间，必须先同时 TERM 服务和客户端，不能先无界等待客户端。数据库和日志已完成收尾后，仍拒绝 TERM 的 setup 客户端会被单独强制回收；这不是向数据库进程发送 KILL，也不能拿来掩盖数据库未优雅退出。
6. 完成 readiness/init 后直接 `wait` 服务，不再每 10 秒读 PID 文件轮询。这样 Bash 会回收子进程，服务异常退出也能立即反映为容器退出。
7. 信号在服务尚未启动或 PID 文件尚未原子发布时到达也必须安全退出，不能 `kill 0`、误伤别的进程或留下 FIFO reader/writer。

### 4.3 Go 服务生命周期

建立一个小型、可单测的关闭协调器，不改变查询执行路径：

1. 订阅 `SIGTERM`、`SIGINT`、`SIGQUIT`，不再注册不可捕获的 `SIGKILL`。
2. MySQL `Start()`、PostgreSQL `Start()` 和启用时的 Flight SQL `Serve()` 都由协调器持有完成 channel；协调器等待“收到信号”或“任一 serve loop 提前返回”。Flight 不再单独注册信号处理器，但只把它作为 transport 收尾，不扩大产品证明范围。
3. 关闭动作只执行一次，先停止 PostgreSQL/MySQL 和已启用的 Flight transport 新连接，再等待所有已启用的 serve loop 返回。现有 PostgreSQL `Start`/`Close` 没有错误返回，因此只保留当前 API 可观察到的 MySQL/Flight/engine/provider 错误，不声称能捕获 PostgreSQL listener 内部已打印并忽略的错误。
4. serve loop 全部停止后调用 `engine.Close()`，让 GMS 结束 process/background lifecycle；只有它完成后才能调用 `provider.Close()`，不能在仍存活的 engine/handler 下关闭存储。Flight 不再单独 `defer provider.Storage().Close()`，provider 是存储 DB 的唯一关闭 owner。由于 Flight backend 没有 Close，本任务运行门必须保持它禁用。
5. 当前 MySQL 和 PostgreSQL listener close 都不等待每条 client handler；本任务产品证明因此严格要求停止前所有一次性 SQL 客户端已经退出，不能外推为“活动连接已优雅排空”。
6. 把当前 API 可见的 engine 和 provider 关闭结果写入日志。provider 关闭完成加上后续重启读回，只证明有序关闭路径完成且数据可恢复；`provider.Close()` 当前不返回 connector 的 defer 错误，除非扩大修复范围补齐该 API，不能声称 connector 内部错误已被审计。
7. 若可观察的 serve/close/finalize 返回错误，合并并记录错误，不能因为关闭路径调用 `Fatal` 而跳过尚未执行的收尾。

关闭顺序为：

```text
Docker SIGTERM
  -> Bash PID 1 trap
  -> real myduckserver PID
  -> Go signal coordinator
  -> stop PostgreSQL/MySQL admissions (and Flight transport if enabled)
  -> join every enabled protocol serve loop
  -> engine.Close
  -> provider.Close
  -> Go exit 0
  -> Bash wait returns 0
  -> container exit 0
```

## 5. 测试设计

### 5.1 确定性回归

- Dockerfile 断言入口为 JSON/exec 形式。
- 入口脚本测试使用假的 `myduckserver`、`mysqlsh`、`tee` 和临时目录，验证 PID 文件属于服务而不是 `tee`、TERM 只向服务转发一次、PID 文件发布前的 TERM 不会挂住、TERM-responsive 和 TERM-resistant readiness 子进程都会被回收、服务早退状态不丢、日志完全排空、脚本清理临时/PID/FIFO 且服务和 `tee` 都无孤儿进程。测试不得把旧版 best-effort init 擅自改成 fatal init。
- Go 生命周期测试使用假的 serve/close/finalize 函数和信号 channel，验证立即信号、重复信号、任一 serve loop 提前返回、MySQL-only/PG-enabled/Flight-transport-enabled 组合、所有 serve loop join 后才进入 engine/provider 收尾，以及当前 API 可见的 serve/close/finalize 错误不丢失。Flight 组合只证明 transport join，不算 backend 资源关闭证明。
- `gofmt`、`git diff --check`、focused test、race、vet 和全仓编译门均需通过；命令、退出码和原始输出单独保存。

### 5.2 `0.2.1` 产品验证

1. 用不可变旧镜像和独立临时数据目录复现一次 30 秒超时，保存 inspect、process tree、PID 文件、日志和 scoped Docker events。
2. 从精确源码 `2eb4143b...` 构建使用唯一 task-local 标签的本地候选，以 image ID 锁定后续命令；记录父提交、子提交、tree、二进制哈希、镜像 ID、OCI revision 和 `--version`。本地标签可移动，不能称为不可变引用或虚构 RepoDigest。
3. 用唯一容器名、端口和 bind 目录在 `SETUP_MODE=SERVER` 启动，保持 `flightsql-port=-1`，关闭所有 DuckLake/对象存储配置，不配置 MySQL replica 或 PostgreSQL subscription；复用已知 init fixtures，并保存这些禁用条件的检查结果。
4. MySQL 和 PostgreSQL 分别写入来源可区分的 sentinel 行，并保存完整的“2 个协议 × 停止前、原容器重启后、同盘重建后 3 个阶段”读回矩阵。
5. 执行 `docker stop --timeout 30`，要求实际耗时小于 30 秒、host command exit 0、container exit 0、`OOMKilled=false`、events 有 TERM 无 KILL，日志包含应用关闭完成点。
6. 用原容器再次启动并双协议读回，证明正常停止后已落盘。
7. 删除且只删除该已停止容器，使用同一候选镜像和完全相同的 host bind path 重建，不挂 init SQL，再次双协议读回全部值。

### 5.3 `0.3` 产品验证

根因和补丁形态在 `0.2.1` 做实后，由 @卡特 明确一个不与 task #77 冲突的干净 `0.3/main` 集成落点。重新核对所选基线后，只移植同一逻辑修复，并重新执行 5.1 与 5.2 的正常退出、落盘、原容器重启读回和同盘删除/重建读回；证据、提交和镜像身份与 `0.2.1` 分开记录。

`0.3` 的相关 API 保持向后兼容，错误移植即使漏参数也可能编译并通过 DuckLake-off 产品门。因此移植测试还必须静态核对 `configuration.LoadDuckLakeConfig`、`catalog.WithDuckLakeConfig(duckLakeConfig)`、`provider.InitializeConnection` 以及 Dockerfile 的 extension COPY 都仍存在；不能把“可以 cherry-pick/可以编译”当成语义等价证明。

任一条线未通过，都不能对外宣称两条线已解决。task #83 最终只提交一次完整双线证据包给独立验收人。

## 6. 证据与资源隔离

- baseline、`0.2.1` candidate、`0.3` candidate 使用不同的 `mktemp -d` 数据目录、容器名、端口和日志目录。
- 不复用或删除 task #74、task #77 的容器、数据目录、工作树或镜像标签。
- 不执行广泛 Docker cleanup、Compose teardown 或未限定目标的删除命令。
- 生产改动 allowlist 为 `docker/Dockerfile`、`docker/entrypoint.sh`、`main.go` 及其定向测试和本设计文档；超出范围必须先说明原因。
- 两条线分别记录 base/child/tree、逐文件 diff 和 patch-id；共同逻辑必须有稳定 patch-id 或逐行等价证明，不能只说“已经同步”。
- 在验证前后分别记录 stable 版本标签和 `latest` 的远端引用，证明未发生 promotion 或标签移动。
- 每个命令保存命令文本、开始/结束时间、退出码和原始 stdout/stderr；每个文件生成 SHA-256，最终生成 manifest 和 `SHA256SUMS`。
- 证据描述只到实际证明的强度：本地候选通过不等于正式版本已发布。

## 7. 失败判定

出现下列任一情况即为 FAIL：

- 30 秒内未退出，或 Docker 发送了 `SIGKILL`。
- 容器退出码非 0、`OOMKilled=true`，或只证明 wrapper 退出而没有应用/provider 关闭证据。
- 任一协议写入或任一阶段的双协议读回不一致。
- 删除/重建时数据目录或镜像身份发生变化。
- 两条线混用提交、工作树、镜像或证据，导致来源无法复核。
