# Dolt 2.3.1 Replication Compatibility

Date: 2026-08-24

## Scope and source identity

- Exact source parent: `749b6164b105dafc08d627fa7ab522f4805c654b`
- Candidate branch: `feature/dolt-2.3.1-support`
- Dolt source: `dolthub/dolt-sql-server@sha256:dbd13efe2e19e02c079efd2152bca5032071a3cd6f79a9b2a7b404d19ff3ee3f`
- Dolt SQL identity: `@@VERSION_COMMENT = Dolt`, `DOLT_VERSION() = 2.3.1`, `VERSION() = 8.0.31`

The production change is deliberately limited to the MySQL GTID handshake for this
identity. File-position replication, MariaDB replication, and ordinary MySQL keep
their existing paths. No `go.mod`, product version, release tag, or image registry
tag was changed.

## Root cause and fix

Vitess sends the processed MySQL 5.6 GTID set as the COM_BINLOG_DUMP_GTID SID block.
The Dolt 2.3.1 fork only decodes that SID block when `BinlogThroughGTID` (`0x04`) is
present. MyDuck previously sent flags `0`, so Dolt treated the processed set as
empty and replayed old GTIDs after STOP/START or a process restart. The replay
caused duplicate DDL/DML and advanced local metadata incorrectly for the source
stream.

The applier now probes `@@VERSION_COMMENT` and `DOLT_VERSION()` during connection
capability detection. Only the exact pair `Dolt` / `2.3.1` selects flags `0x04`.
Any probe error, missing variable, or other version falls back to flags `0` without
blocking replication. The packet construction is centralized and tested to keep
server ID, filename, position (`4` for GTID requests), and SID block unchanged.

## Transaction boundary probe

The existing `commitOngoingTxn` order is retained. A focused probe calls
`positionStore.Save`, then rolls back the same SQL transaction and closes/reopens
the provider: both data rows and binlog-position rows remain `0`. A second probe
commits after the save and reopens with exactly `1` data row and `1` position row.
This establishes the observed rollback/commit boundary for one SQL transaction; it
does not claim atomicity for every possible process crash timing.

## Regression evidence

All runs below used the current checkout's arm64 image (`myduckserver:task51-final`)
and the fixed Dolt digest above. The GTID UUIDs are source-run identifiers; the
intervals and row counts are the assertions.

### Dolt 2.3.1

| Phase | Executed GTID | `items` rows / distinct IDs | Result |
| --- | --- | --- | --- |
| Initial | `226eb363-e0ca-4c50-91ec-c0babd230366:1-5` | `2 / 2` | PASS |
| Incremental insert | `...:1-6` | `3 / 3` | PASS |
| STOP/START | `...:1-7` | `4 / 4` | PASS |
| Process restart/autostart | `...:1-8` | `5 / 5` | PASS |

The filtered `test.skip` table was present but empty, which is the existing workflow
contract. Replica IO/SQL status was `Yes/Yes` and both error codes were `0` in each
formal Dolt phase. The STOP/START and restart phases reached only the next GTID;
there was no duplicate DDL, duplicate row, or applier-side event skipping.

The harness compares the source and replica's ordered `id:name` rows exactly at the
initial and incremental checkpoints, and again after Dolt STOP/START and process
restart. The intentional Dolt STOP checkpoint uses a count-only assertion while the
replica is stopped before it receives the source's next row. Final checks also verify
the MySQL wire row count and the filtered-table contract.

### Comparison sources

- MySQL 8.4 GTID mode: `...:1-11` / `2` rows initially, then `...:1-12` / `3`
  rows; flags remained `0` and IO/SQL errors were `0`.
- PostgreSQL logical replication: no MySQL GTID set; initial and incremental data
  snapshots were `2` and `3` rows.
- MariaDB 11.4: `0-1-6` / `2` rows, then `0-1-7` / `3` rows; filtering and data
  replication passed and IO/SQL were `Yes/Yes`. It continues to expose the
  exact-parent baseline `Last_SQL_Errno=1105` (`received unknown event`), so this
  PR does not claim MariaDB error-free status or change that path.

## First-red classification

1. Exact-parent Dolt with flags `0` reproduced the lifecycle red: after a
   reconnect Dolt resent the processed GTIDs, causing `database exists` / `table
   already exists` errors and duplicate rows. This is the protocol mismatch fixed
   here; it is not hidden by an applier deduplication rule.
2. The old Dolt workflow's `-u/-p` server arguments were rejected by Dolt 2.3.1;
   the harness now starts the pinned image without those removed arguments and
   creates an explicit replication user.
3. The first local run used a stricter-than-original skip-table assertion and
   reported a table-present/empty red. The checker now matches the original
   workflow contract (absent or present-but-empty).
4. An attempted `linux/amd64` build on this arm64 host failed at the compiler's
   `-m64` flag. This is a local cross-architecture invocation error; the required
   `linux/arm64` build completed successfully and is not a product red.

## Commands

```text
MYDUCK_IMAGE=myduckserver:task51-final bash .github/scripts/replication-test.sh dolt
MYDUCK_IMAGE=myduckserver:task51-final bash .github/scripts/replication-test.sh mysql
MYDUCK_IMAGE=myduckserver:task51-final bash .github/scripts/replication-test.sh postgres
MYDUCK_IMAGE=myduckserver:task51-final bash .github/scripts/replication-test.sh mariadb
CI=true go test -tags=duckdb_arrow ./binlogreplication -run 'Test(DetectBinlogDumpGTIDFlags|DetectSourceCapabilitiesToleratesOptionalProbeFailures|MySQL56BinlogDumpRequest|BinlogPositionAndAppliedDataShareCommitBoundary)$' -count=1
CI=true go test -tags=duckdb_arrow ./binlogreplication -count=1
CI=true go test -tags=duckdb_arrow ./... -run '^$' -count=1
bash -n .github/scripts/replication-test.sh
```

The four replication scripts, the focused tests, the complete
`binlogreplication` package, and the repository compile-only pass all returned
exit code `0`. The workflow now builds the current checkout and runs the same
script in a PostgreSQL/MySQL/MariaDB/Dolt matrix with the Dolt digest pinned.
