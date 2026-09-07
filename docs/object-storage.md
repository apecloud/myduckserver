# Object storage (DuckLake) first-cut notes

Accepted candidate `450fe6f0` replayed onto current `main`. Delivery image
must not include diagnostic inject switches.

## Usage boundaries

1. **MySQL CREATE is an implicit commit.** `CREATE TABLE` is not undone by
   `ROLLBACK`, for local tables and object tables alike.
2. **One transaction cannot write both storages.** Mixing a local-table write
   and an object-table write in the same transaction returns 1105. This is
   unsupported, not a bug to paper over.
3. **The local catalog must persist.** DuckLake metadata lives in the local
   catalog file (`MYDUCK_DUCKLAKE_METADATA_PATH`). The object bucket stores
   table data only. Losing the catalog cannot be recovered from the bucket
   alone.

## Scope

New tables may choose object storage. Existing tables are not migrated.
Replication is unchanged. Merge and image publication are separate steps.

FlightSQL prepared-statement close is idempotent after execute.
