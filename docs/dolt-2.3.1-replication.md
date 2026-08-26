# Dolt 2.3.1 Replication Compatibility

Date: 2026-08-26

## Scope and source identity

- Exact post-#484 source parent: `ba805622a2b0c855e904e44e158112b216c8724b`
- Parent ancestry: `ba805622` is the signed #484 Dockerfile fix on `cbd566f9`; this PR does not include the Dockerfile change.
- Dolt source: `dolthub/dolt-sql-server@sha256:dbd13efe2e19e02c079efd2152bca5032071a3cd6f79a9b2a7b404d19ff3ee3f`
- Dolt SQL identity: `@@VERSION_COMMENT = Dolt`, `DOLT_VERSION() = 2.3.1`, `VERSION() = 8.0.31`
- Candidate image: `myduckserver:task61-post484`, digest `sha256:cd15802b761da1f4ee6763c0bbcc7c094f7f07375cc6a2803ad91dca677405c9`
- Exact-parent image: `myduckserver:task61-parent-ba805`, digest `sha256:518653912d43d5bd9aab96abce9c8606f54f319093da6d80403b6569a107c5ee`

The effective change is the six-file Dolt replication patch replayed onto the
post-#484 main. It does not change `go.mod`, the product version, release tags,
or image registry tags. PR #480 remains review-only; no merge, auto-merge,
release, or `latest` update is implied by this evidence.

## Conflict composition

The old PR #480 applier patch was originally based on `cbd566f9` and had seven
overlapping hunks. The replay preserves the current-main behavior introduced
by the DoltgreSQL work:

- `RequestQueryFlush` and the applier-owned flush channel
- `discardOngoingTxn` before reconnect and after the run exits
- `positionStore.Load` / `LoadEncoded` and the current `mysql.Position`
- two-argument `SendBinlogDumpCommand(serverId, position)` for non-Dolt paths
- current reconnect and transaction ordering, including `pendingPosition`

It adds only the Dolt 2.3.1 capability check and the `BinlogThroughGTID`
(`0x04`) flag. `@@VERSION_COMMENT = 'Dolt'` and `DOLT_VERSION() = '2.3.1'`
must both match. Only a MySQL56 GTID position with that confirmed identity
uses `WriteComBinlogDumpGTID`; all other sources and probe failures retain the
main two-argument dump path and flags `0`. GTID mode parsing remains the
main-tree `ToString()` plus `ON` / `1` logic.

The replication script now derives source and MyDuck container names from its
`RESOURCE_SUFFIX`. All `run`, `inspect`, `exec`, `logs`, `rm`, and DSN uses share
those names, so concurrent runs cannot collide and cleanup cannot remove a
pre-existing fixed-name container.

## Runtime matrix

All official script runs used the same pinned Dolt digest and the candidate or
exact-parent image named above. The script compares ordered `id:name` rows.

| Source | Candidate | Exact parent | Result |
| --- | --- | --- | --- |
| Dolt 2.3.1 | exit 0; initial/incremental/STOP-START/process restart rows 2 -> 3 -> 4 -> 5; GTID advances through `:1-8`; IO/SQL Yes, errno 0 | exit 1 at STOP-START: expected 4 rows, got 7; initial/incremental errno 0 | Candidate fixes the duplicate replay; parent mismatch is the locked baseline |
| MySQL | exit 0; initial and incremental rows/GTID/position pass | exit 0 | No candidate-only red |
| PostgreSQL | exit 1; incremental phase remains at 2 rows | exit 1; same 2-row incremental boundary | Shared replication baseline, not candidate-only |
| MariaDB | exit 0; known `Last_SQL_Errno=1105` unknown-event baseline | exit 0; same `1105` baseline | No candidate-only red |

Log paths and SHA-256:

- candidate Dolt: `/private/tmp/task61-post484-dolt.log` - `8044f44ee0949ed59018f13c85b247db84acfe2c0cc00f4c5b2d02faedc15cea`
- candidate MySQL: `/private/tmp/task61-post484-mysql.log` - `8eba72edc20dbade655dda450866af645dc2755027ba3552b64578d356037024`
- candidate PostgreSQL: `/private/tmp/task61-post484-postgres.log` - `3ab4fa592c91381c8290fbc3a4dc322c50592f24498a8a752e1a74aad4a8ec3e`
- candidate MariaDB: `/private/tmp/task61-post484-mariadb.log` - `d745a7fe83114e03996e82efb4dfe5118cdd1b832f63a7fb6bdb0a1c3a2f681d`
- parent Dolt: `/private/tmp/task61-post484-parent-dolt.log` - `095371a931aff3f7ed99ab4f40a31806269ed34b8b421ca83496172af7a05c7f`
- parent MySQL: `/private/tmp/task61-post484-parent-mysql.log` - `f34183373515cdf534fb5a2d8e07fed1d4c5ae53bb5d906142ec8512efcd06ff`
- parent PostgreSQL: `/private/tmp/task61-post484-parent-postgres.log` - `86880ecfd51fb9eb4fa88eb9a24f7c9ff5778ee7346a82853c9ba3e711899319`
- parent MariaDB: `/private/tmp/task61-post484-parent-mariadb-ba805.log` - `209c0c3371739cdcf8e89520219ea3fb3b15b485b851e82985e551b1f3ac3d7c`

## Focused and shared gates

The focused capability, dump-packet, QueryFlush/reconnect, position-load, and
transaction-boundary tests passed. Candidate and parent precompiled
file-position, GTID-restart, and QueryFlush integration probes passed. The
full precompiled binlog package has only the shared `TestAutoRestartReplica`
baseline (`Last_SQL_Errno=1105`, row-event flags `0x10`) when compared with the
exact parent.

The host-only command
`go test -tags=duckdb_arrow ./binlogreplication -count=1` was allowed to run
to completion and timed out after 601 seconds because the macOS `go run .`
harness service never listened on its ephemeral port. Every fixture showed
connection refused before a product assertion; this is retained as harness
environment evidence, not a candidate product regression.

Focused log: `/private/tmp/task61-post484-binlog-candidate.log` -
`c57cee9d1404d73dd2ea454ba5acfb05cd762d2634a1af8d122700fb60e9a82a`.

## Static and cleanup gates

The candidate tree passed `gofmt`, `go mod tidy`/module-graph checks,
compile-only tests, `go build ./...`, `bash -n`, ShellCheck, and
`git diff --check`. Candidate and parent focused binlog tests both passed:

- candidate focused binlog package: `ok .../binlogreplication 0.302s`
- exact-parent focused binlog package: `ok .../binlogreplication 0.289s`

The two-round static evidence was generated before the report-only update;
the report is the only post-runtime file change. Final cleanup removed the
post-#484 `task61-fresh` and MySQL fixture containers. Historical
`task51-dolt-source`, `myduck-repro-pg`, `myduck-debug-pg`, and MinIO resources
were not touched.

Static evidence hashes:

- round 1: `/private/tmp/task61-post484-static1.log` - `0b5159a7b0495d1e2df1c470d55c370877247438a9a7357365b114cccbafb802`
- round 2: `/private/tmp/task61-post484-static2.log` - `8529f7135ceb56aaa7adc7dfb9e9ce12894ff344eb5b0720d2967cf2d8597ef9`

The final tree contains exactly these six paths relative to `ba805622`:

```text
A .github/scripts/replication-test.sh
M .github/workflows/replication-test.yml
A binlogreplication/binlog_dump_test.go
A binlogreplication/binlog_position_atomicity_test.go
M binlogreplication/binlog_replica_applier.go
A docs/dolt-2.3.1-replication.md
```

## Reproduction

```text
MYDUCK_IMAGE=<image-built-from-this-tree> bash .github/scripts/replication-test.sh dolt
MYDUCK_IMAGE=<image-built-from-this-tree> bash .github/scripts/replication-test.sh mysql
MYDUCK_IMAGE=<image-built-from-this-tree> bash .github/scripts/replication-test.sh postgres
MYDUCK_IMAGE=<image-built-from-this-tree> bash .github/scripts/replication-test.sh mariadb
CI=true go test -tags=duckdb_arrow ./binlogreplication -count=1
```

The next review object must be one platform-signed commit whose unique parent
is `ba805622`, with this report, the six-file manifest, and the evidence
hashes recomputed from the final tree. The new head requires a fresh
non-author review; the old PR #480 approvals do not transfer.
