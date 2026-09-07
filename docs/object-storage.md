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
4. **Object tables do not support PRIMARY KEY.** `CREATE TABLE ... ENGINE=DUCKLAKE`
   with a primary key returns 1105. The table is not created and no object
   prefix is written.
5. **Data directory ownership.** The image user is `admin` (uid 1000). The
   image ships `/home/admin/data` owned by admin so an empty Docker named
   volume mounted there inherits that ownership. A bind-mounted host directory
   that is `root:root` still needs `chown 1000:1000` before first start.

## Scope

New tables may choose object storage. Existing tables are not migrated.
Replication is unchanged. Merge and image publication are separate steps.

FlightSQL prepared-statement close is idempotent after execute.
