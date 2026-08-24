# task #52: DoltgreSQL v1.2.0 migration candidate

Date: 2026-08-25

## Verdict

The isolated `github.com/dolthub/doltgresql v1.2.0` candidate now builds and
passes the focused MySQL, PostgreSQL, catalog, Arrow, replication, restart,
and logical-replication gates. All upgrade-only failures found in the full
root suite were either fixed in the compatibility layer or classified by
running the exact same input against the exact parent. This includes the
Python MySQL client's cross-request `autocommit=0` transaction failure. The
final candidate and exact parent have the same remaining root failures: 47
`TestQueriesSimple` leaf cases and `TestAddColumn/add column, no default`.

The candidate is ready for fresh code review but not for merge or release.
Its exact parent is main after the normally merged task #53 restart repair,
so the combined tree now includes the previously independent WAL-replay fix.
The migration patch itself still changes only the original 90 files: the
three restart-repair files match exact parent byte-for-byte. Earlier approval
of the old task #52 head does not transfer to this replacement.

PostgreSQL's direct `COMMENT ON TABLE` also bypasses MyDuck's managed
table-comment encoding and can discard decoded table-level metadata. That
boundary remains explicit and is not counted as passed.

## Locked identity and environment

- Exact parent: `3ec2b33ebce3bdc7844b80aade0aac6b74d98270`.
- Exact parent tree: `30453d90c23f446354ffa77a84dc9dd8b8e94646`.
- Parent worktree: `/private/tmp/myduck-task52-parent-main3ec2`, detached and
  clean after temporary same-source restart probes were removed.
- Evaluation branch: `eval/doltgresql-upgrade`.
- Evaluation-only committed head:
  `64848862121d3dd714d4a7149e3306d588fc2bc5`.
- Candidate worktree: `/private/tmp/myduck-task52-signed-replacement`.
- The final review head is generated as one platform-signed replacement
  directly from exact parent
  `3ec2b33ebce3bdc7844b80aade0aac6b74d98270`. Rejected PR #482 heads
  `8bdca66c8238a8bfa8c7495f2d8fa36a041bc7ea`,
  `bfec1ff2fbd773330bd2bdda3b2fe49f18dfa1c1`, and old approved head
  `18f2f76355446761218d72468e6c616966629d27`, plus the unsigned evaluation
  head, are not in its ancestry.
- Frozen review scope: 57 modified repository files and 33 new repository
  files, including this migration report and `mysql_update_wire_test.go`; 90
  files total. No temporary parent probe is included.
- A fresh `git fetch origin main` on 2026-08-25 resolved both `FETCH_HEAD` and
  `origin/main` to the exact parent above; main had not moved.
- The combined TIME/SSL GMS fork dependency is platform-signed head
  `ce812a3e3e6e8707ef0d1089b591f09059c872d9`, tree
  `1a5b47c6f9b7b79762299464147ed547e52f97be`, approved by counted review
  `5008580345`. MyDuck pins immutable pseudo-version
  `v0.0.0-20260824134702-ce812a3e3e6e`; the fork was not merged here.
- Required local build environment:

  ```text
  CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib
  GOFLAGS=-tags=duckdb_arrow
  ```

The candidate branch is intentionally isolated. Formal `main`, `v0.2.0`,
and `latest` were not modified.

## Selected module graph

| Module | Exact parent | Candidate |
| --- | --- | --- |
| Project Go directive | 1.24.0 | 1.26.2 |
| DoltgreSQL | v0.13.0 | v1.2.0 |
| go-mysql-server | v0.19.1-202412, ApeCloud replacement | upstream selection `v0.20.1-0.20260817180248-8ba7438d98bb`, replaced by ApeCloud `v0.0.0-20260824134702-ce812a3e3e6e` |
| Dolt Vitess | v0.0.0-202412, ApeCloud replacement | v0.0.0-202607 upstream |
| Dolt | old transitive selection | v0.40.5-202608 |
| pgx/v5 | v5.7.1 | v5.9.2 |
| x/crypto | v0.47.0 | v0.52.0 |
| x/net | v0.49.0 | v0.55.0 |
| x/sys | v0.40.0 | v0.45.0 |
| x/text | v0.33.0 | v0.37.0 |

The newer DoltgreSQL graph selects newer upstream GMS and Dolt Vitess
versions, so the exact-version ApeCloud replacement directives no longer
apply. MyDuck's public connection, replication, GTID, error, and SQL-type
paths were migrated to Dolt Vitess. The official `vitess.io/vitess`
requirement remains necessary for the private binary-JSON parser and was not
removed.

`github.com/shopspring/decimal v1.4.0` is now indirect. MyDuck source no
longer imports it directly, but DoltgreSQL v1.2.0 still requires it
transitively through `server/types`.

After a second `go mod tidy`, the module files did not change. Final hashes:

```text
go.mod  04b011e446bb060287bea2749b019667ef9f20720118cfd358fd38f8515de0b3
go.sum  ddb24effb3535a6490a32228bdc3a7011615c9fac9812f0dde3a5612ef9624c9
```

## Compatibility work completed

### GMS and authentication interfaces

- Migrated context-bearing table, index, expression, iterator, and system
  variable interfaces selected by the new GMS graph.
- Added a production once-only DoltgreSQL authentication initializer. Repeated
  server creation no longer resets process-global auth state and the existing
  in-memory MyDuck superuser contract is preserved.
- Added the missing statement lifecycle handling required by the new GMS
  iterator order. `EmptyTableEditor` lifecycle hooks are no-op only for the
  internal DuckDB update path; unsupported `Update` behavior remains
  unsupported rather than being silently accepted.
- Ensured row inserter initialization is once-only when the new iterator calls
  `Insert` or `Close` before the old lifecycle order would have initialized
  the statement.

### Query and protocol behavior

- Restored MySQL session query limits under the new handler/context flow.
- Restored qualified database selection for PostgreSQL `CREATE VIEW`.
- Added `INSERT ... RETURNING` row execution rather than reducing it to an
  `OkResult`. PostgreSQL simple and named prepared flows now emit row
  descriptions and data rows. MySQL text works. MySQL native prepared INSERT
  retains the known `have 0 want 2` binding boundary, returns an explicit
  error, and writes no row; it is not claimed supported.
- Preserved decimal and numeric conversion behavior across MySQL, PostgreSQL,
  and Arrow paths.
- Added exact compatibility handling for the expanded v1.2.0 corpus without
  whole-suite skips. Exact-parent input probes were used before classifying
  missing functions, grouping/HAVING behavior, stale expected types, trigger
  and FK setup, invalid dates, LOAD DATA parsing, view boundaries, enum/SET
  coercion, JSON comparison, and auto-increment sequence behavior.

### MySQL compatibility and direct DML results

- Restored the advertised MySQL compatibility version to the exact-parent
  contract, `8.0.23`. The upgraded GMS default had changed it to `8.0.31`,
  which made MySQL Shell 9.1 probe
  `@@innodb_parallel_read_threads`, a variable MyDuck does not implement.
- Restored the DB-level `CHANGE REPLICATION FILTER` option names removed from
  the selected Vitess grammar. The compatibility parser preserves the
  original option name, parsed SQL, multi-statement remainder, and ParseOne
  offset; six replication-filter tests prove configuration and actual
  allow/deny behavior.
- Restored `types.OkResultSchema` only after `DuckBuilder` has selected direct
  execution and only for UPDATE or DELETE without RETURNING. Fallback paths
  and RETURNING paths are not wrapped. The wrapper is idempotent.
- MySQL wire coverage proves ordinary and repeated UPDATE, affected-row
  counts, final values, and the existing native prepared UPDATE error with no
  write. MySQL and PostgreSQL DELETE both prove affected rows `1`, repeated
  delete `0`, invalid-column errors without additional mutation, and exact
  final row snapshots.

The native MySQL prepared UPDATE error (`incorrect argument count for command:
have 0 want 2`) reproduces on the exact parent. PostgreSQL
`UPDATE ... RETURNING` likewise updates the row but emits zero DataRows and
zero FieldDescriptions on both exact parent and candidate. Both are recorded
baseline boundaries, not newly supported capabilities. Existing MySQL text
and PostgreSQL simple/named prepared `INSERT ... RETURNING` coverage remains
green; native MySQL prepared INSERT RETURNING retains the same explicit error
and no-write boundary.

### MySQL transaction and DDL lifecycle

The upgraded GMS graph made transaction lifetime follow the context of one
protocol request. Python's MySQL client exposed the regression by issuing
`CREATE DATABASE`, `USE`, `SET NAMES`, and `SET autocommit=0` across separate
requests: the next `CREATE TABLE` reached `ConnectionPool.CurrentCatalog`
after DuckDB had already closed the session connection. The rejected
candidate exits 1 on the complete client script while the exact parent exits
0 on the identical `test.data`; the fixed candidate again exits 0.

The replacement keeps the MySQL session contract in four bounded changes:

- A DuckDB transaction uses `context.WithoutCancel`, so ending one request
  does not roll back or close a transaction that belongs to the session.
- Closing a session explicitly rolls back and removes its unfinished DuckDB
  transaction before closing the fixed connection.
- MySQL DDL is classified before GMS analysis and implicitly commits the
  preceding transaction first. DDL then executes without opening a second
  underlying DuckDB transaction. A failed DDL leaves no wrapper transaction
  behind and the same connection remains reusable.
- `SET autocommit=0` is evaluated while the old value is still 1. GMS can
  therefore create an empty autocommit wrapper for the SET statement. MyDuck
  clears only that empty wrapper, so the first following DML enters a real
  DuckDB transaction and can be committed, rolled back, or rolled back on
  disconnect.

These two timing corrections are separate: the DDL implicit commit must occur
before analysis, including when analysis fails, while the first DML after
`SET autocommit=0` must start the underlying transaction only after the SET
statement has installed the new session value.

Permanent gates cover request-context cancellation and disconnect rollback at
the connection-pool layer, the complete Python sequence, and six MySQL wire
boundaries: failed DDL reuse, CREATE/DROP DATABASE/TABLE visibility, explicit
COMMIT/ROLLBACK, autocommit 0/1 transitions, concurrent-session isolation,
and disconnect rollback without affecting another session. The disconnect
case also reuses the rolled-back key, proving rollback completed rather than
merely hiding the old connection.

### Applier-owned query visibility barrier

The rejected candidate let foreground `SHOW` and `SELECT` queries flush the
shared replication delta buffer directly. That could expose data without the
matching replication position. The replacement makes visibility an applier
operation instead:

- A foreground query sends a request to the active applier run. Request and
  completion channels are created per run, so an old run cannot acknowledge a
  new run's request.
- The applier commits only when `ongoingBatchTxn && !dirtyStream`, using its
  existing `commitOngoingTxn` path. Delta data and the pending replication
  position therefore become visible in the same transaction.
- No-run, stopped-run, abnormal-exit, and canceled-request paths terminate
  without waiting forever. Concurrent requesters each receive a completion.
- Replication-origin queries bypass the barrier, preventing recursion.
- Structured replication controller plans also bypass it. `DuckBuilder`
  checks `plan.BinlogReplicaControllerCommand`; it does not match SQL text.
  This lets STOP/RESET/CHANGE/START and SHOW STATUS control a reconnecting
  applier without first waiting for that applier to flush. Ordinary queries
  keep the strict barrier.

Permanent coverage includes request/run lifecycle, cancellation, run
generation isolation, 32 concurrent requests, data-and-position visibility,
half-packet reconnect, file-position and GTID, keyed and keyless rows,
restart consistency, the complete bad-source
START -> STOP -> RESET -> CHANGE -> START sequence, and a direct plan-boundary
test proving controller commands do not change delta/position while an
ordinary plan does. The focused race log is
`/private/tmp/task52-final-query-flush-race.log`; the dual-mode control logs
are `/private/tmp/task52-final-control-sequence-false.log` and
`/private/tmp/task52-final-control-sequence-true.log`.

### Candidate-only reds found and closed

Five failures were unique to intermediate upgrade candidates. They remain in
the record rather than being rewritten as passing first attempts.

1. **TIME wire incompatibility.** The new GMS path emitted prepared-binary
   TIME fields with `Decimals=0`, so clients truncated non-zero fractional
   seconds. The underlying GMS `TimeType` also did not carry declared FSP
   through parsing, stringification, and field metadata. The approved fork
   head `ce812a3e3e6e8707ef0d1089b591f09059c872d9` carries declaration precision
   0..6 end to end, while MyDuck preserves that precision through its
   catalog mapping. The permanent file-position/GTID matrix proves source and
   target metadata `[0,0,3,6]` and identical ordinary/prepared wire values for
   zero and non-zero fractions. Final logs are
   `/private/tmp/task52-final-fork-ssl-time-filepos.log` and
   `/private/tmp/task52-final-fork-ssl-time-gtid.log`.

2. **AutoReconnect duplicate replay.** The candidate returned 1001 rows where
   1000 were expected. A foreground `SHOW REPLICA STATUS` had committed one
   shared delta row while the stored position remained at
   `binlog.000002:1483`; reconnect replayed `pk=1`. The applier-owned barrier
   above restores the invariant that replicated data and position commit
   together. The original red is preserved in
   `/private/tmp/task52-candidate-autoreconnect-filepos-rerun.log`; the final
   Group 1 file-position/GTID runs and QueryFlush race run retain the original
   exact-row assertion and pass.

3. **STOP REPLICA deadlock.** The first barrier also covered controller
   commands. While the applier was retrying a bad source, foreground STOP
   waited for a flush that only the still-running applier could service, so
   STOP never reached the controller. The final plan-interface whitelist
   bypass fixes only structured replication commands. The timed-out first
   Group 2 run is preserved as
   `/private/tmp/task52-final-group2-false-rerun.log`; both final Group 2
   modes, both control-sequence tests, and the QueryFlush race run pass except
   for the separately frozen AutoRestart `1105/0x10` parent boundary.

4. **Sanity first-catalog lookup.** After the dependency upgrade, analysis of
   the first fully-qualified query on a new protocol session asked the
   provider for its current catalog before a DuckDB connection existed.
   `CurrentCatalog` returned an empty name and an existing `db01` was reported
   as error 1049. `ConnectionPool` now returns only its owning provider's
   default catalog before the first connection; closed/broken connections
   still return empty, and a live connection's USE state wins. Coverage locks
   unknown-database errors, two-provider isolation, live USE behavior, and
   closed/broken lifecycle behavior. Sanity passes in both modes. The red is
   `/private/tmp/task52-final-candidate-sanity-false-isolated.log`; focused and
   final logs are `/private/tmp/task52-final-catalog-fallback-focused.log`,
   `/private/tmp/task52-final-sanity-filepos.log`, and
   `/private/tmp/task52-final-sanity-gtid.log`. Frozen MySQL 26.7 row bytes
   separately prove row flag `0x10` was not this failure's cause and remains
   explicitly unsupported.

5. **Index initialization panic.** A provisional migration made `DuckHarness`
   implement GMS `IndexDriverHarness` and installed the memory-only test index
   driver. GMS then invoked it through a base/empty session; the initializer
   entered MyDuck catalog code without `adapter.ConnectionHolder` and
   `TestUpdate` panicked. MyDuck no longer implements that interface or stores
   and registers `memory.TestIndexDriver`. Its CREATE INDEX and CREATE UNIQUE
   INDEX paths use the base row executor and `catalog.Table.CreateIndex`, which
   executes native DuckDB DDL. GMS's own `MemoryHarness` still initializes and
   registers its memory driver. `TestIndexDriverOwnership` proves the legacy
   initializer is never called, first catalog access uses a backend session,
   ordinary/unique index behavior survives inserts, updates, deletes, key
   reuse, and Restart, and the upstream memory harness remains intact. The red
   is `/private/tmp/task52-final-root-candidate.log`; the focused pass is
   `/private/tmp/task52-final-index-ownership-focused.log`.

### Server-test process isolation

`testutil.StartDuckSqlServer` previously called `os.Chdir`, mutating the test
process globally. One server test could therefore make every later
`go run .` start from `/private/tmp`. The helper now leaves the test process
cwd unchanged and sets only the child command's `cmd.Dir` to the resolved
repository root.

- `TestCreateViewDatabaseSelectionWire` asserts that cwd is unchanged across
  server startup and retains a cleanup restore as a defensive boundary.
- The non-CI path starts `go run .` from `goDirPath`.
- An isolated `CI=true` run builds the cached dev binary and starts that
  absolute binary from the same `cmd.Dir`; it exits 0.
- The isolated CI run leaves no child process for its temporary directory.
- The full `./pgserver` package now exits 0 with create-view and all ten
  previously affected later tests executing in the same test process.

### VECTOR storage boundary

The new GMS corpus introduced `VECTOR(N)` columns into fixtures. The initial
candidate panicked because MyDuck had no mapping. The migration now provides
the following bounded storage path:

```text
GMS VECTOR(N) <-> DuckDB BLOB <-> Arrow binary / wire bytes
```

- `VECTOR(N)` maps to a physical DuckDB `BLOB` with a length constraint and
  round-trips to MySQL metadata with its dimension intact.
- `STRING_TO_VECTOR` produces the binary representation used by stored
  generated columns.
- Dimension, generated expression, stored/virtual classification, NOT NULL,
  nullable, and physical CHECK metadata are retained.
- NULL, valid values, wrong dimensions, `VECTOR(0)`, empty input, Arrow binary,
  MySQL text/prepared reads, and PostgreSQL simple/named prepared reads are
  covered. Empty input is rejected before the upstream encoder's out-of-bounds
  behavior and the connection remains usable after the error.
- File-backed provider clean reopen retains schema metadata and binary values,
  and wrong-dimension writes remain rejected after reopen.
- Real child-process restart replays the generated-column/comment WAL, restores
  the `mysql_rand` UDF, and retains VECTOR/BLOB schema metadata and bytes.
- The unfiltered `TestJoinQueries` fixture creates and fills 101 generated
  VECTOR rows and executes two VECTOR INDEX setup statements without panic.

This does not prove distance calculations or real vector-index acceleration.
Neither capability is claimed.

### Replication positions

- GTID=false parsing preserves the original
  `CHANGE REPLICATION SOURCE ... SOURCE_LOG_FILE/SOURCE_LOG_POS` names,
  values, order, original SQL, and multi-query offsets.
- Initial file/position configuration autocommits. Apply-time position updates
  remain transactional with replicated data.
- Stored positions use explicit `FilePos/...` and `MySQL56/...` flavor
  prefixes, with legacy unprefixed reads retained.
- `SHOW REPLICA STATUS` restores the real file and position rather than
  exposing an encoded internal string.
- Synthetic file-position events do not call GTID-only `Bytes()` paths.
- File-position and GTID STOP/START/RESET behavior is covered. `RESET ALL`
  clears file-position configuration while preserving executed GTID history.
- DB and table replication-filter configuration and actual replication
  behavior are covered, including repeated configuration and case
  normalization. Table filters appear in `SHOW REPLICA STATUS`. DB filters do
  not: `Replicate_Do_DB` and `Replicate_Ignore_DB` are NULL on both exact
  parent and candidate because the shared `ReplicaStatus` structure has no DB
  filter fields. The repository test marks this TODO, so DB SHOW is an
  explicit baseline boundary rather than a pass claim.

### Client and copy-instance compatibility

- The full Go MySQL compatibility client passes through CREATE, INSERT,
  ordinary UPDATE, DELETE, SELECT, and DROP. Before the final DML fix the
  candidate failed at DELETE with `types.OkResult is not a valid value type
  for int`; exact parent and the fixed candidate both exit 0.
- A fresh MySQL Shell 8.4.7 `util.copyInstance` run used the same pinned MySQL
  8.4.7 source image. The target was proved empty first, advertised
  `@@version=8.0.23`, and had no `testdb`. Copy completed with 3 tables / 10
  rows (`users=5`, `items=3`, `documents=2`), and sorted source/target full-row
  snapshots had an empty `diff -u`.

### Managed table comments

The new GMS calls `sql.CommentAlterableTable` for `ALTER TABLE ... COMMENT`.
`catalog.Table.ModifyComment` now updates the DuckDB comment atomically while
retaining MyDuck's existing base64-managed `ExtraTableInfo`.

The supported path is covered as follows:

- MySQL performs replace, clear, then replace.
- `SHOW CREATE TABLE`, `information_schema.tables`, and
  `information_schema.columns` retain AUTO_INCREMENT, PK, sequence, CHECK,
  generated expression, stored classification, NOT NULL, and user comments.
- PostgreSQL simple wire reads the same table after every MySQL update.
- Raw table and column comments and their decoded metadata are compared before
  mutation, after every mutation, after PostgreSQL client reconnect, and after
  clean provider reopen.
- Focused logs:
  `/private/tmp/task52-comment-managed-metadata-v2.log` and
  `/private/tmp/task52-comment-wire-v5.log`, both exit 0.

PostgreSQL direct comment mutation is not supported by this contract. The
isolated statement

```sql
COMMENT ON TABLE comment_wire.pg_comment_boundary IS 'postgres direct'
```

returns success but replaces the table-level managed base64 payload with raw
text. Decoded PK ordinal, sequence, and CHECK mappings then become empty.
Column-level generated/NOT NULL comments and the physical PK/sequence remain.
The regression uses a separate boundary table so this destructive path does
not contaminate the supported MySQL-write/PostgreSQL-read path. It is not
counted as passed.

## Final regression evidence

### Final runtime-tree binding

The final runtime gates are bound by content, not only by timestamps. The
sorted SHA256 list for all 89 changed non-report files is
`/private/tmp/task52-final-runtime-tree-files.sha256` with SHA256
`b0d99616e7ea07a1309bc01dd91283ffcef69c99494ac49db5bded0e80a48d66`.
All runtime logs below were produced after that runtime tree's last source or
test edit, and no source/test file changed afterward. This report is the only
90th file changed after the runtime manifest. The runtime-log checksum
manifest is `/private/tmp/task52-final-runtime-gates.sha256`, SHA256
`a41de6afda9c36b8ec55d7a6db584373c6af095de377747ede6f205cbc8f6424`.

| Final-tree gate | Result | Log SHA256 |
| --- | --- | --- |
| Six MySQL transaction/connection boundaries plus read-only/audit | exit 0 | `983366b50e08910cb9d9ef259628522dd92a1237a6b2f884c11020bd703b178f` |
| Python MySQL client | exit 0 | `6748d7ab6ff80a300eb1560d4311b47b3570146a3e22b2dd4e58e2e1dad836cd` |
| Go MySQL client | exit 0 | `6748d7ab6ff80a300eb1560d4311b47b3570146a3e22b2dd4e58e2e1dad836cd` |
| Client server log | intentional interrupt after both clients | `22b2a7040ac87ac643c442533589762e937cf371ed2819888f289c9b843eb55c` |
| Six MySQL replication filters | exit 0 | `572051883442f0b2f3e579d91ef15b9284923bb97ec041f91d212bab631b7640` |
| Final fork SSL/TIME, file-position | exit 0 | `0114af1cfe624224aaef33a1dcb0e11ade4d5646cb97408df212083292554e69` |
| Final fork SSL/TIME, GTID | exit 0 | `446d1f0a0ced6a1d789e6326bc963386f41de1c6ce5e3d61e1c8bb93a9d08cb0` |
| Stopped restart, file-position | exit 0 | `fda102a89ed08a375f89e8e8f796a1f9b3483977c25dcec1b948599111010e40` |
| Stopped restart, GTID | exit 0 | `c98382bc6f36efddfa45928bb28112706a4919b54abc8fd55733550808ce3d69` |
| Running restart, file-position | exact two-row data pass; shared `1105/0x10` health failure | `43615832877cbf4dc153285ea09717b1e0a060a64e74bc3aa79420b019fe47b9` |
| Running restart, GTID | exact two-row data pass; shared `1105/0x10` health failure | `1a6e1c774ee43aa7ac9b0a31ac6a55852791286754eeff6663dea5c0fd98e5eb` |
| 13 PostgreSQL logical-replication scenarios | exit 0 | `7da06e2e7d3a5f901fd9a54f8e73309aa78e9dd8e0453b3d6eed94eef1f638ff` |
| Fresh MySQL Shell 8.4.7 `copyInstance` | exit 0 | `9ae13b75438fe3eee8a3ab1cbfa65e9bb287efbbc64c801de5fe3fc7cba30b40` |
| Copy source snapshot | 3 tables / 10 rows | `a788e395265ff978ae1475d53fd50cae8a2f1db478b9dd8c1263a22eafd4bd65` |
| Copy target snapshot | exact source match | `a788e395265ff978ae1475d53fd50cae8a2f1db478b9dd8c1263a22eafd4bd65` |

The evidence bundle records the corresponding command, environment, exit
code, log path, log hash, and the 89-file runtime manifest for every row.

### Root package candidate/parent comparison

| Worktree | Result | Remaining failures | Log |
| --- | --- | --- | --- |
| Candidate | exit 1 | `TestQueriesSimple`: 47 logical leaf failures; `TestAddColumn/add column, no default` | `/private/tmp/task52-final-root-candidate-after-index-ownership.log` |
| Exact parent | exit 1 | Same 47 logical leaf failures; same AddColumn failure | `/private/tmp/task52-evidence-main3ec2/task52-root-parent.log` |

The selected candidate GMS prints some QueryTests again below nested
FunctionQueryTests wrappers; those are duplicate failure nodes for the same
SQL, not candidate-only product failures. The final logical set is identical
to the 47 parent leaf cases. Each side has 48 immediate failure nodes after
normalization, and the sorted failure-list diff
`task52-root-failure-parity.diff` is empty. The final normalized candidate
list is
`/private/tmp/task52-final-root-candidate-after-index-ownership-failures.txt`.

All earlier candidate-only top-level failures are gone: `TestJoinQueries`,
`TestInsertInto`, `TestInsertIntoErrors`, `TestLoadData`, `TestUpdate`,
`TestOnUpdateExprScripts`, `TestCreateTable`, `TestCreateDatabase`,
`TestViews`, `TestAlterTable`, `TestJsonScripts`, RETURNING, VECTOR, and
managed comments. The final run has no candidate-only panic.

### Replication

| Gate | Result | Coverage | Log |
| --- | --- | --- | --- |
| MySQL replication Group 1, file-position | exit 0 | Sanity, AllTypes, AutoReconnect, stopped restart, TIME, QueryFlush visibility/reconnect | `/private/tmp/task52-final-group1-false-rerun.log` |
| MySQL replication Group 1, GTID | exit 0 | Same Group 1 contracts in GTID mode | `/private/tmp/task52-final-group1-true-rerun.log` |
| MySQL replication Group 2, file-position | only exact-parent AutoRestart `1105/0x10` | remaining lifecycle, STOP and frozen-event coverage | `/private/tmp/task52-final-group2-false-after-control-bypass.log` |
| MySQL replication Group 2, GTID | only exact-parent AutoRestart `1105/0x10` | same Group 2 contracts in GTID mode | `/private/tmp/task52-final-group2-true-after-control-bypass.log` |
| MySQL replication filters | exit 0 | all six DB/table do/ignore configuration and apply tests | `/private/tmp/task52-final-replication-filters-after-queryflush.log` |
| PostgreSQL logical replication | exit 0 | all 13 `ReplicationTest` subscenarios | `/private/tmp/task52-final-pg-logical-replication.log` |
| Stopped process restart | candidate file-position and GTID exit 0 | STOP, frozen state across process restart, explicit START, final convergence | `/private/tmp/task52-final-stopped-restart-filepos.log`; `/private/tmp/task52-final-stopped-restart-gtid.log` |

The 13 PostgreSQL logical-replication subscenarios are:

1. `simple replication, strings and integers`
2. `stale start`
3. `stopping and resuming replication`
4. `extended stop/start`
5. `all supported types`
6. `concurrent writes`
7. `concurrent writes with restarts`
8. `concurrent writes with rollbacks`
9. `concurrent writes, stale commits`
10. `concurrent writes, very stale commits`
11. `all types`
12. `Create table automatically`
13. `Truncate table`

An earlier status update said 12 because its manual summary omitted one
subscenario. The source slice and final verbose log both show 13; 13 is the
final count.

The permanent `TestBinlogReplicationServerRestart` contract was corrected.
Its previous implementation left replication running, then compared the
stored position before restart with the position after the automatically
resumed applier had advanced. Equality was therefore timing-dependent and did
not isolate stopped-state persistence. The replacement is deterministic:

```text
STOP REPLICA -> process restart -> frozen state -> START REPLICA -> convergence
```

For both file-position and GTID modes it freezes and compares source
configuration (host, port, user, UUID, retry values, and auto-position),
position flavor, file/position status, internal stored position, IO/SQL thread
state, count/max, and the ordered full-row snapshot. After explicit START it
requires both threads to be `Yes`, the stored position to advance, and source
and replica count/max/full rows to be identical. Candidate file-position and
GTID runs both exit 0. The same temporary test source was run against clean
exact parent `3ec2b33e`: GTID exits 0; file-position passes every STOP/restart
frozen-state assertion and only then hits the existing 1105/row-flag `0x10`
baseline while applying the two post-START rows. The temporary parent edit was
removed and that worktree is clean. Parent logs are
`task52-stopped-restart-filepos-parent.log` and
`task52-stopped-restart-gtid-parent.log`.

`TestAutoRestartReplica` remains a separate running-restart contract. Four
runs (candidate and exact parent, file-position and GTID) all prove the
replica automatically starts after process restart and contains the exact
ordered values `100, 200` before the health assertion. All four then report
the same status: `Last_IO_Errno=0`, empty `Last_IO_Error`,
`Last_SQL_Errno=1105`, `Last_SQL_Error="unsupported binlog protocol message:
row event with unsupported flags '10'"`, IO/SQL threads both `Yes`, and server
row flags `0x10`. Thus post-restart data consistency passes, while healthy
replication status remains an exact-main baseline; automatic recovery is not
claimed fully green and the flag handling is not claimed fixed. Evidence is
`/private/tmp/task52-final-running-restart-filepos.log` and
`/private/tmp/task52-final-running-restart-gtid.log`; exact-parent comparison
logs remain in the evidence bundle.

### DML, client, filter, and copy-instance gates

All of the following exited 0:

- complete Python MySQL client after the session-transaction fix:
  `/private/tmp/task52-final-python-client.log`
- complete Go MySQL client on the same fixed binary:
  `/private/tmp/task52-final-go-client.log`
- six transaction/connection boundaries:
  `/private/tmp/task52-final-user-facing-gates.log`
- OkResult wrapper schema/idempotence:
  `/private/tmp/task52-ok-result-wrapper-final.log`
- MySQL/PostgreSQL UPDATE and DELETE wire coverage:
  `/private/tmp/task52-dml-wire-final.log`
- existing INSERT RETURNING wire coverage:
  `/private/tmp/task52-pg-insert-returning-after-delete.log`
- autocommit, explicit transaction, and RETURNING error lifecycle:
  `/private/tmp/task52-transaction-error-after-delete.log`
- full Go MySQL client after the earlier DELETE fix:
  `/private/tmp/task52-client-go-delete-final.log`
- fresh MySQL Shell `util.copyInstance` on the transaction-lifetime fix:
  `/private/tmp/task52-final-copy-instance.log`

The DELETE regression comparison is candidate-before-fix exit 1
(`/private/tmp/task52-client-go-final.log`), exact parent exit 0
(`/private/tmp/task52-client-go-parent-repro.log`), and fixed candidate exit 0.
Fresh copy-instance full-row snapshots are `task52-copy-source.tsv` and
`task52-copy-target.tsv`; `task52-copy-snapshot.diff` is empty. The target was
empty before copy and advertised `8.0.23`. The source and MySQL Shell were
pinned to exact MySQL 8.4.7 image
`sha256:6c6706ec58d3ac9e925d5f0435c6a923117f68232551b9c8271a294ea72ee331`.
Copy completed with 3 tables and 10 rows.

The Python session-lifetime comparison is rejected candidate exit 1
(`/private/tmp/task52-python-candidate-before.log`), exact parent exit 0
(`/private/tmp/task52-python-parent-before.log`), and fixed candidate exit 0.
The candidate and parent before-fix logs remain unchanged.

Baseline-boundary evidence is retained separately:

- DB filter SHOW output, exact parent and candidate:
  `/private/tmp/task52-filter-show-parent-boundary.log` and
  `/private/tmp/task52-filter-show-candidate-boundary.log`
- PostgreSQL UPDATE RETURNING, exact parent and candidate:
  `/private/tmp/task52-pg-update-returning-parent-probe.log` and
  `/private/tmp/task52-pg-returning-update-final.log`

### Focused PostgreSQL/server gates

The following all exited 0 as independent processes with the ICU environment
shown above:

- `TestInsertReturningWireProtocols`
- `TestDecimalWireProtocols`
- `TestVectorStorageWireProtocols`
- managed comment wire coverage
- `TestQueryRowLimitOnMySQLAndPostgresSessions`
- `TestPostgresDuckDBOnlyCreateOrReplaceTable`
- `TestFuncReplacement`
- `TestIssue280RepeatedConnectMySQLDatabase`
- `TestIssue341PgAuthRejectsWrongPassword`
- `TestSchemaSummaryWireProtocols`
- `TestCreateViewDatabaseSelectionWire`
- `TestSessParam`
- `TestNewServerInitializesAuthOnce`

Fresh combined-tree wire evidence is
`/private/tmp/task52-evidence-main3ec2/task52-comment-vector-wire.log`;
the earlier focused migration logs remain unchanged.

The final full `./pgserver` package exits 0 with the locked ICU
environment. `TestCreateViewDatabaseSelectionWire` and all ten tests that had
previously been affected by cwd pollution execute and pass in the same test
process. The final-tree log is
`/private/tmp/task52-final-pgserver-full-after-index.log`.

The CI precompiled-binary path independently exits 0. Its log records the
isolated dev build, absolute binary invocation, successful server startup, and
test pass at
`/private/tmp/task52-evidence-main3ec2/task52-testutil-ci-prebuild.log`. An earlier
focused run without the local ICU include/library variables produced compile
failures; rerunning with the locked environment passed. The environment
failure is retained and is not treated as a product result.

### Package, build, and source gates

All of these exited 0:

- `backend`, `myarrow`, `delta`, `pgtest`, `catalog`, and `pgtypes`:
  `/private/tmp/task52-final-core-packages-after-index.log`
- read-only and audit regression:
  `/private/tmp/task52-final-user-facing-gates.log`
- compile-only all packages with `duckdb_arrow`:
  `/private/tmp/task52-final-compile-all.log` (SHA256
  `482e6608de543a529f32217e497d654899f45debc53f267ca5c9300f72b4caee`)
- `go build ./...` with `duckdb_arrow`:
  `/private/tmp/task52-final-build-all.log` (SHA256
  `5f91d44ea878a9bc4970440add8052ebc417e32c50709c456386e72a52c01c88`)
- two successive `go mod tidy` runs: both exit 0, with no `go.mod` or
  `go.sum` drift; logs are
  `/private/tmp/task52-final-go-mod-tidy-pass1.log` and
  `/private/tmp/task52-final-go-mod-tidy-pass2.log`; both are empty success
  logs with SHA256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`,
  and the before/pass1/pass2 module hashes are identical
- `git -c core.whitespace=cr-at-eol diff --check`: clean. The
  `cr-at-eol` policy is required because upstream's
  `load_defaults_null.csv` query explicitly uses `LINES TERMINATED BY
  '\r\n'`; default `git diff --check` reports that required CR byte as
  trailing whitespace. Final log:
  `/private/tmp/task52-final-diff-check.log`, empty SHA256
  `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
- `gofmt -l` across all changed and new Go files: no output. Final log:
  `/private/tmp/task52-final-gofmt-l.log`, empty SHA256
  `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
  The only
  repository-wide `gofmt -l` output is the unchanged exact-parent file
  `binlogreplication/ignorable_event_test.go`.

## Integrated process restart

Exact parent `3ec2b33e` is the normally merged task #53 repair. It opens an
in-memory default database, registers MyDuck UDFs, then attaches and selects
the persistent database so generated-column/comment WAL replay has a valid
binding context. The candidate leaves its three files unchanged from parent:
`catalog/provider.go`, `catalog/provider_restart_test.go`, and
`catalog/provider_wal_replay_test.go`.

Fresh combined-tree catalog evidence exits 0 and covers process-internal and
real child-process restart; ordinary/generated rows with and without comments;
`mysql_rand`; VECTOR/BLOB data and metadata; file-position and GTID snapshots;
read-only to writable reopen; concurrent connections; same-name provider
isolation; and no user tables in the in-memory default database. Logs are
`task52-catalog-restart-focused.log`,
`task52-generated-comment-vector-focused.log`, and
`task52-comment-vector-wire.log` in the fresh evidence directory.

The earlier failure was WAL replay calling
`DatabaseManager::GetDefaultDatabase` before any default database existed.
That blocker is now repaired in ancestry and real process restart is claimed
for the tested catalog, generated-expression, comment, UDF, VECTOR/BLOB, and
replication-position state. Running-restart health is still bounded by the
exact-main 1105/row-flag `0x10` result documented above; it is not hidden as a
pass.

## Non-actions and remaining decision

- The final review commit is one platform-signed replacement directly on the
  exact parent `3ec2b33ebce3bdc7844b80aade0aac6b74d98270`; it does not
  descend from rejected PR #482 head
  `8bdca66c8238a8bfa8c7495f2d8fa36a041bc7ea`, rejected replacement
  `bfec1ff2fbd773330bd2bdda3b2fe49f18dfa1c1`, old approved head
  `18f2f76355446761218d72468e6c616966629d27`, or unsigned evaluation
  commit `64848862121d3dd714d4a7149e3306d588fc2bc5`.
- No merge, branch-protection bypass, or admin action is authorized.
- No image was built or published.
- `main`, `v0.2.0`, and `latest` were not changed.
- Final cleanup found no task #52 child process, MyDuck server, MySQL source,
  Toxiproxy container, or copy-instance container still running. Containers
  owned by other tasks and long-lived local services were left untouched.
- Stopped process restart and post-START data consistency are claimed for the
  tested file-position and GTID contracts. Running-restart data consistency is
  claimed, but error-free health is not: the exact-main 1105/row-flag `0x10`
  baseline remains.
- Vector distance calculation and real vector-index acceleration are not
  claimed.
- Direct PostgreSQL `COMMENT ON TABLE` is not claimed safe for managed MyDuck
  metadata.

This PR is for fresh code review only. Direct PostgreSQL comments, MySQL native
prepared UPDATE, PostgreSQL UPDATE RETURNING rows, MySQL native prepared INSERT
RETURNING, error-free running-restart health, distance calculation, and real
vector-index acceleration remain outside its accepted capability statement.
