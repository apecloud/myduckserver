package catalog

type InternalView struct {
	Schema string
	Name   string
	DDL    string
}

func (v *InternalView) QualifiedName() string {
	return v.Schema + "." + v.Name
}

var InternalViews = []InternalView{
	{
		Schema: "__sys__",
		Name:   "pg_stat_user_tables",
		DDL: `SELECT
    t.table_schema || '.' || t.table_name AS relid, -- Create a unique ID for the table
    t.table_schema AS schemaname,                  -- Schema name
    t.table_name AS relname,                       -- Table name
    0 AS seq_scan,                                 -- Default to 0 (DuckDB doesn't track this)
    NULL AS last_seq_scan,                         -- Placeholder (DuckDB doesn't track this)
    0 AS seq_tup_read,                             -- Default to 0
    0 AS idx_scan,                                 -- Default to 0
    NULL AS last_idx_scan,                         -- Placeholder
    0 AS idx_tup_fetch,                            -- Default to 0
    0 AS n_tup_ins,                                -- Default to 0 (inserted tuples not tracked)
    0 AS n_tup_upd,                                -- Default to 0 (updated tuples not tracked)
    0 AS n_tup_del,                                -- Default to 0 (deleted tuples not tracked)
    0 AS n_tup_hot_upd,                            -- Default to 0 (HOT updates not tracked)
    0 AS n_tup_newpage_upd,                        -- Default to 0 (new page updates not tracked)
    0 AS n_live_tup,                               -- Default to 0 (live tuples not tracked)
    0 AS n_dead_tup,                               -- Default to 0 (dead tuples not tracked)
    0 AS n_mod_since_analyze,                      -- Default to 0
    0 AS n_ins_since_vacuum,                       -- Default to 0
    NULL AS last_vacuum,                           -- Placeholder
    NULL AS last_autovacuum,                       -- Placeholder
    NULL AS last_analyze,                          -- Placeholder
    NULL AS last_autoanalyze,                      -- Placeholder
    0 AS vacuum_count,                             -- Default to 0
    0 AS autovacuum_count,                         -- Default to 0
    0 AS analyze_count,                            -- Default to 0
    0 AS autoanalyze_count                         -- Default to 0
FROM
    information_schema.tables t
WHERE
    t.table_type = 'BASE TABLE'; -- Include only base tables (not views)`,
	},
	{
		Schema: "__sys__",
		Name:   "pg_index",
		DDL: `SELECT
    (300000 + (hash(k.table_schema || '.' || k.table_name || '.' || c.constraint_name)::UBIGINT % 800000000))::BIGINT AS indexrelid,
    t.table_oid AS indrelid,                          -- OID of the table
    COUNT(k.column_name) AS indnatts,                 -- Number of columns included in the index
    COUNT(k.column_name) AS indnkeyatts,              -- Number of key columns in the index (same as indnatts here)
    CASE
        WHEN c.constraint_type = 'UNIQUE' THEN TRUE
        ELSE FALSE
    END AS indisunique,                               -- Indicates if the index is unique
    CASE
        WHEN c.constraint_type = 'PRIMARY KEY' THEN TRUE
        ELSE FALSE
    END AS indisprimary,                              -- Indicates if the index is a primary key
    ARRAY_AGG(k.ordinal_position ORDER BY k.ordinal_position) AS indkey,  -- Array of column positions
    ARRAY[]::BIGINT[] AS indcollation,                -- DuckDB does not support collation, set to default
    ARRAY[]::BIGINT[] AS indclass,                    -- DuckDB does not support index class, set to default
    ARRAY[]::INTEGER[] AS indoption,                  -- DuckDB does not support index options, set to default
    NULL AS indexprs,                                 -- DuckDB does not support expression indexes, set to NULL
    NULL AS indpred                                   -- DuckDB does not support partial indexes, set to NULL
FROM
    information_schema.key_column_usage k
JOIN
    information_schema.table_constraints c
    ON k.constraint_name = c.constraint_name
    AND k.table_name = c.table_name
    AND k.table_schema = c.table_schema
JOIN
    duckdb_tables() t
    ON k.table_name = t.table_name
    AND k.table_schema = t.schema_name
WHERE
    c.constraint_type IN ('PRIMARY KEY', 'UNIQUE')    -- Only select primary key and unique constraints
GROUP BY
    t.table_oid, c.constraint_type, c.constraint_name, k.table_schema, k.table_name
ORDER BY
    t.table_oid;`,
	},
	{
		Schema: "__sys__",
		Name:   "pg_namespace",
		// Static pg_catalog rows plus live user schemas. Metabase lists schemas
		// from pg_namespace, while CREATE DATABASE only creates a DuckDB schema.
		DDL: `SELECT oid, nspname, nspowner, nspacl FROM __sys__.pg_namespace_catalog
UNION ALL
SELECT
    (200000 + (hash(s.schema_name)::UBIGINT % 800000000))::BIGINT AS oid,
    s.schema_name AS nspname,
    10::BIGINT AS nspowner,
    CAST(NULL AS VARCHAR) AS nspacl
FROM (
    SELECT DISTINCT schema_name
    FROM information_schema.schemata
    WHERE catalog_name NOT IN ('system', 'temp', 'memory')
      AND schema_name NOT IN ('__sys__', 'mysql', 'performance_schema', 'main')
      AND schema_name NOT IN (SELECT nspname FROM __sys__.pg_namespace_catalog)
) s`,
	},
	{
		Schema: "__sys__",
		Name:   "pg_class",
		// Keep the seeded PostgreSQL catalog dump and append live user tables and
		// views so Metabase/JDBC schema sync can join pg_class to pg_namespace.
		DDL: `SELECT * FROM __sys__.pg_class_catalog
UNION ALL
SELECT
    live.oid,
    live.relname,
    live.relnamespace,
    live.reltype,
    live.reloftype,
    live.relowner,
    live.relam,
    live.relfilenode,
    live.reltablespace,
    live.relpages,
    live.reltuples,
    live.relallvisible,
    live.reltoastrelid,
    live.relhasindex,
    live.relisshared,
    live.relpersistence,
    live.relkind,
    live.relnatts,
    live.relchecks,
    live.relhasrules,
    live.relhastriggers,
    live.relhassubclass,
    live.relrowsecurity,
    live.relforcerowsecurity,
    live.relispopulated,
    live.relreplident,
    live.relispartition,
    live.relrewrite,
    live.relfrozenxid,
    live.relminmxid,
    live.relacl,
    live.reloptions,
    live.relpartbound
FROM (
    SELECT
        t.table_oid::BIGINT AS oid,
        t.table_name::VARCHAR AS relname,
        n.oid AS relnamespace,
        0::BIGINT AS reltype,
        0::BIGINT AS reloftype,
        10::BIGINT AS relowner,
        0::BIGINT AS relam,
        0::BIGINT AS relfilenode,
        0::BIGINT AS reltablespace,
        0::INTEGER AS relpages,
        0::FLOAT AS reltuples,
        0::INTEGER AS relallvisible,
        0::BIGINT AS reltoastrelid,
        (t.has_primary_key OR t.index_count > 0) AS relhasindex,
        false AS relisshared,
        'p' AS relpersistence,
        'r' AS relkind,
        t.column_count::SMALLINT AS relnatts,
        t.check_constraint_count::SMALLINT AS relchecks,
        false AS relhasrules,
        false AS relhastriggers,
        false AS relhassubclass,
        false AS relrowsecurity,
        false AS relforcerowsecurity,
        true AS relispopulated,
        'n' AS relreplident,
        false AS relispartition,
        0::BIGINT AS relrewrite,
        0::BIGINT AS relfrozenxid,
        0::BIGINT AS relminmxid,
        CAST(NULL AS VARCHAR) AS relacl,
        CAST(NULL AS VARCHAR) AS reloptions,
        CAST(NULL AS VARCHAR) AS relpartbound,
        ROW_NUMBER() OVER (
            PARTITION BY t.schema_name, t.table_name
            ORDER BY CASE t.database_name WHEN 'myduck' THEN 0 ELSE 1 END, t.database_name
        ) AS rn
    FROM duckdb_tables() t
    INNER JOIN __sys__.pg_namespace n ON n.nspname = t.schema_name
    WHERE NOT t.internal
      AND NOT t.temporary
      AND t.database_name NOT IN ('system', 'temp', 'memory')
      AND t.schema_name NOT IN ('__sys__', 'mysql', 'performance_schema', 'main', 'pg_catalog', 'information_schema', 'pg_toast')
    UNION ALL
    SELECT
        v.view_oid::BIGINT AS oid,
        v.view_name::VARCHAR AS relname,
        n.oid AS relnamespace,
        0::BIGINT AS reltype,
        0::BIGINT AS reloftype,
        10::BIGINT AS relowner,
        0::BIGINT AS relam,
        0::BIGINT AS relfilenode,
        0::BIGINT AS reltablespace,
        0::INTEGER AS relpages,
        0::FLOAT AS reltuples,
        0::INTEGER AS relallvisible,
        0::BIGINT AS reltoastrelid,
        false AS relhasindex,
        false AS relisshared,
        'p' AS relpersistence,
        'v' AS relkind,
        v.column_count::SMALLINT AS relnatts,
        0::SMALLINT AS relchecks,
        false AS relhasrules,
        false AS relhastriggers,
        false AS relhassubclass,
        false AS relrowsecurity,
        false AS relforcerowsecurity,
        true AS relispopulated,
        'n' AS relreplident,
        false AS relispartition,
        0::BIGINT AS relrewrite,
        0::BIGINT AS relfrozenxid,
        0::BIGINT AS relminmxid,
        CAST(NULL AS VARCHAR) AS relacl,
        CAST(NULL AS VARCHAR) AS reloptions,
        CAST(NULL AS VARCHAR) AS relpartbound,
        ROW_NUMBER() OVER (
            PARTITION BY v.schema_name, v.view_name
            ORDER BY CASE v.database_name WHEN 'myduck' THEN 0 ELSE 1 END, v.database_name
        ) AS rn
    FROM duckdb_views() v
    INNER JOIN __sys__.pg_namespace n ON n.nspname = v.schema_name
    WHERE NOT v.internal
      AND NOT v.temporary
      AND v.database_name NOT IN ('system', 'temp', 'memory')
      AND v.schema_name NOT IN ('__sys__', 'mysql', 'performance_schema', 'main', 'pg_catalog', 'information_schema', 'pg_toast')
    UNION ALL
    SELECT
        (300000 + (hash(c.table_schema || '.' || c.table_name || '.' || c.constraint_name)::UBIGINT % 800000000))::BIGINT AS oid,
        c.constraint_name::VARCHAR AS relname,
        n.oid AS relnamespace,
        0::BIGINT AS reltype,
        0::BIGINT AS reloftype,
        10::BIGINT AS relowner,
        0::BIGINT AS relam,
        0::BIGINT AS relfilenode,
        0::BIGINT AS reltablespace,
        0::INTEGER AS relpages,
        0::FLOAT AS reltuples,
        0::INTEGER AS relallvisible,
        0::BIGINT AS reltoastrelid,
        false AS relhasindex,
        false AS relisshared,
        'p' AS relpersistence,
        'i' AS relkind,
        1::SMALLINT AS relnatts,
        0::SMALLINT AS relchecks,
        false AS relhasrules,
        false AS relhastriggers,
        false AS relhassubclass,
        false AS relrowsecurity,
        false AS relforcerowsecurity,
        true AS relispopulated,
        'n' AS relreplident,
        false AS relispartition,
        0::BIGINT AS relrewrite,
        0::BIGINT AS relfrozenxid,
        0::BIGINT AS relminmxid,
        CAST(NULL AS VARCHAR) AS relacl,
        CAST(NULL AS VARCHAR) AS reloptions,
        CAST(NULL AS VARCHAR) AS relpartbound,
        ROW_NUMBER() OVER (
            PARTITION BY c.table_schema, c.table_name, c.constraint_name
            ORDER BY c.constraint_catalog
        ) AS rn
    FROM information_schema.table_constraints c
    INNER JOIN __sys__.pg_namespace n ON n.nspname = c.table_schema
    WHERE c.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
      AND c.table_schema NOT IN ('__sys__', 'mysql', 'performance_schema', 'main', 'pg_catalog', 'information_schema', 'pg_toast')
) live
WHERE live.rn = 1`,
	},
	{
		Schema: "__sys__",
		Name:   "pg_attribute",
		// Live columns for user tables so JDBC getPrimaryKeys can join
		// pg_class.oid to attrelid. Seeded dumps do not contain app tables.
		DDL: `SELECT
    col.attrelid,
    col.attname,
    col.atttypid,
    col.attstattarget,
    col.attlen,
    col.attnum,
    col.attndims,
    col.attcacheoff,
    col.atttypmod,
    col.attbyval,
    col.attalign,
    col.attstorage,
    col.attcompression,
    col.attnotnull,
    col.atthasdef,
    col.atthasmissing,
    col.attidentity,
    col.attgenerated,
    col.attisdropped,
    col.attislocal,
    col.attinhcount,
    col.attcollation,
    col.attacl,
    col.attoptions,
    col.attfdwoptions
FROM (
    SELECT
        t.table_oid::BIGINT AS attrelid,
        c.column_name::VARCHAR AS attname,
        25::BIGINT AS atttypid,
        -1::INTEGER AS attstattarget,
        4::SMALLINT AS attlen,
        c.ordinal_position::SMALLINT AS attnum,
        0::INTEGER AS attndims,
        -1::INTEGER AS attcacheoff,
        -1::INTEGER AS atttypmod,
        true AS attbyval,
        'i' AS attalign,
        'p' AS attstorage,
        '' AS attcompression,
        (c.is_nullable = 'NO') AS attnotnull,
        false AS atthasdef,
        false AS atthasmissing,
        '' AS attidentity,
        '' AS attgenerated,
        false AS attisdropped,
        true AS attislocal,
        0::INTEGER AS attinhcount,
        0::BIGINT AS attcollation,
        CAST(NULL AS VARCHAR) AS attacl,
        CAST(NULL AS VARCHAR) AS attoptions,
        CAST(NULL AS VARCHAR) AS attfdwoptions,
        ROW_NUMBER() OVER (
            PARTITION BY t.schema_name, t.table_name, c.column_name
            ORDER BY CASE t.database_name WHEN 'myduck' THEN 0 ELSE 1 END, t.database_name
        ) AS rn
    FROM information_schema.columns c
    JOIN duckdb_tables() t
      ON c.table_schema = t.schema_name
     AND c.table_name = t.table_name
    WHERE NOT t.internal
      AND NOT t.temporary
      AND t.database_name NOT IN ('system', 'temp', 'memory')
      AND t.schema_name NOT IN ('__sys__', 'mysql', 'performance_schema', 'main', 'pg_catalog', 'information_schema', 'pg_toast')
) col
WHERE col.rn = 1`,
	},
}
