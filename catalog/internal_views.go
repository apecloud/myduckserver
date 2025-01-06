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
}
