package catalog

// SchemaSummaryQuery is the stable, read-only schema discovery query for
// clients and agent integrations. table_schema is the client-visible database
// (MySQL) or schema (Postgres) name.
//
// Keep the projection and ordering explicit: callers can consume the result
// without depending on information_schema's wider, version-specific schema.
const SchemaSummaryQuery = `
SELECT
    c.table_schema,
    c.table_name,
    c.column_name,
    LOWER(CASE
        WHEN instr(c.data_type, '(') > 0 THEN left(c.data_type, instr(c.data_type, '(') - 1)
        ELSE c.data_type
    END) AS data_type,
    c.ordinal_position,
    t.table_type
FROM information_schema.columns AS c
JOIN information_schema.tables AS t
  ON c.table_catalog = t.table_catalog
 AND c.table_schema = t.table_schema
 AND c.table_name = t.table_name
WHERE c.table_schema NOT IN (
    'information_schema',
    'pg_catalog',
    '__sys__',
    'mysql',
    'performance_schema'
)
ORDER BY c.table_schema, c.table_name, c.ordinal_position`
