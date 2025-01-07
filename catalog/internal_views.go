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
    ROW_NUMBER() OVER () AS indexrelid,                -- Simulated unique ID for the index
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
JOIN
    duckdb_tables() t
    ON k.table_name = t.table_name
    AND k.table_schema = t.schema_name
WHERE
    c.constraint_type IN ('PRIMARY KEY', 'UNIQUE')    -- Only select primary key and unique constraints
GROUP BY
    t.table_oid, c.constraint_type, c.constraint_name
ORDER BY
    t.table_oid;`,
	},
}
