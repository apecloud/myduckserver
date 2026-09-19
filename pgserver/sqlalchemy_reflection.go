package pgserver

import (
	"fmt"
	"regexp"
	"strings"
)

// SQLAlchemy 1.4's PostgreSQL 12+ get_columns query. Match the complete client
// query, allowing whitespace changes and only a numeric/bound relation OID.
// This does not pretend that the general PostgreSQL sequence APIs exist.
const sqlAlchemyColumnsQuery = `
SELECT a.attname,
  pg_catalog.format_type(a.atttypid, a.atttypmod),
  (
    SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid)
    FROM pg_catalog.pg_attrdef d
    WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum
    AND a.atthasdef
  ) AS DEFAULT,
  a.attnotnull,
  a.attrelid as table_oid,
  pgd.description as comment,
  a.attgenerated as generated,
  (SELECT json_build_object(
    'always', a.attidentity = 'a',
    'start', s.seqstart,
    'increment', s.seqincrement,
    'minvalue', s.seqmin,
    'maxvalue', s.seqmax,
    'cache', s.seqcache,
    'cycle', s.seqcycle)
    FROM pg_catalog.pg_sequence s
    JOIN pg_catalog.pg_class c on s.seqrelid = c."oid"
    WHERE c.relkind = 'S'
    AND a.attidentity != ''
    AND s.seqrelid = pg_catalog.pg_get_serial_sequence(
      a.attrelid::regclass::text, a.attname
    )::regclass::oid
  ) as identity_options
FROM pg_catalog.pg_attribute a
LEFT JOIN pg_catalog.pg_description pgd ON (
  pgd.objoid = a.attrelid AND pgd.objsubid = a.attnum)
WHERE a.attrelid = $1
AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum`

var sqlAlchemyColumnsPattern = func() *regexp.Regexp {
	parts := strings.Fields(sqlAlchemyColumnsQuery)
	for i, part := range parts {
		if part == "$1" {
			parts[i] = `([0-9]+|'[0-9]+'|\$[1-9][0-9]*)`
		} else {
			parts[i] = regexp.QuoteMeta(part)
		}
	}
	return regexp.MustCompile(`^\s*` + strings.Join(parts, `\s+`) + `\s*;?\s*$`)
}()

func rewriteSQLAlchemyReflection(input string) string {
	if sqlAlchemyDomainsPattern.MatchString(RemoveComments(input)) {
		return sqlAlchemyDomainsReflection
	}
	matches := sqlAlchemyColumnsPattern.FindStringSubmatch(RemoveComments(input))
	if matches == nil {
		return ""
	}
	// Read the live physical types, defaults and nullability. MyDuck does not
	// implement PostgreSQL identity columns; MySQL AUTO_INCREMENT is reflected
	// through its actual nextval default, not invented identity options.
	// Managed comments hold the original length, human comment and generated
	// expression. TRY preserves ordinary/malformed comments as plain text.
	return fmt.Sprintf(`WITH columns AS (
    SELECT *, CASE WHEN starts_with(comment, 'base64:') THEN
      TRY(CAST(decode(from_base64(substr(comment, 8))) AS JSON))
      ELSE NULL END AS metadata
    FROM duckdb_columns()
    WHERE table_oid = %s
)
SELECT column_name AS attname,
    CASE
      WHEN data_type IN ('TIMESTAMP_S', 'TIMESTAMP_MS', 'TIMESTAMP_NS', 'TIMESTAMP')
        THEN 'timestamp without time zone'
      WHEN data_type = 'DOUBLE' THEN 'double precision'
      WHEN data_type = 'FLOAT' THEN 'real'
      WHEN data_type = 'BLOB' THEN 'bytea'
      WHEN data_type IN ('TINYINT', 'UTINYINT') THEN 'smallint'
      WHEN data_type = 'USMALLINT' THEN 'integer'
      WHEN data_type = 'UINTEGER' THEN 'bigint'
      WHEN data_type = 'UBIGINT' THEN 'numeric(20,0)'
      WHEN starts_with(data_type, 'DECIMAL(') THEN replace(lower(data_type), 'decimal', 'numeric')
      WHEN data_type = 'VARCHAR' AND TRY_CAST(json_extract_string(metadata, '$.meta.Length') AS BIGINT) > 0
        THEN 'character varying(' || json_extract_string(metadata, '$.meta.Length') || ')'
      ELSE lower(data_type)
    END AS format_type,
    coalesce(nullif(json_extract_string(metadata, '$.meta.Generated'), ''), column_default) AS "default",
    NOT coalesce(TRY_CAST(json_extract_string(metadata, '$.meta.Nullable') AS BOOLEAN), is_nullable) AS attnotnull,
    table_oid,
    CASE WHEN metadata IS NOT NULL THEN json_extract_string(metadata, '$.text') ELSE comment END AS comment,
    CASE WHEN nullif(json_extract_string(metadata, '$.meta.Generated'), '') IS NOT NULL
      THEN CASE WHEN TRY_CAST(json_extract_string(metadata, '$.meta.Virtual') AS BOOLEAN) THEN 'v' ELSE 's' END
      ELSE NULL::VARCHAR END AS generated,
    NULL::JSON AS identity_options
FROM columns ORDER BY column_index`, matches[1])
}

// The seeded PostgreSQL catalog uses PostgreSQL OIDs, whereas DuckDB's
// format_type expects DuckDB OIDs. Resolve domain base types by catalog name.
const sqlAlchemyDomainsQuery = `
SELECT t.typname as "name",
   pg_catalog.format_type(t.typbasetype, t.typtypmod) as "attype",
   not t.typnotnull as "nullable",
   t.typdefault as "default",
   pg_catalog.pg_type_is_visible(t.oid) as "visible",
   n.nspname as "schema"
FROM pg_catalog.pg_type t
   LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE t.typtype = 'd'`

var sqlAlchemyDomainsPattern = func() *regexp.Regexp {
	parts := strings.Fields(sqlAlchemyDomainsQuery)
	for i, part := range parts {
		parts[i] = regexp.QuoteMeta(part)
	}
	return regexp.MustCompile(`^\s*` + strings.Join(parts, `\s+`) + `\s*;?\s*$`)
}()

const sqlAlchemyDomainsReflection = `SELECT t.typname AS name,
    CASE b.typname
      WHEN 'int2' THEN 'smallint'
      WHEN 'int4' THEN 'integer'
      WHEN 'int8' THEN 'bigint'
      WHEN 'name' THEN 'character varying'
      WHEN 'varchar' THEN 'character varying' || CASE WHEN t.typtypmod >= 4 THEN '(' || (t.typtypmod - 4)::VARCHAR || ')' ELSE '' END
      WHEN 'bpchar' THEN 'character' || CASE WHEN t.typtypmod >= 4 THEN '(' || (t.typtypmod - 4)::VARCHAR || ')' ELSE '' END
      WHEN 'timestamptz' THEN 'timestamp' || CASE WHEN t.typtypmod >= 0 THEN '(' || t.typtypmod::VARCHAR || ')' ELSE '' END || ' with time zone'
      ELSE b.typname
    END AS attype,
    NOT t.typnotnull AS nullable, t.typdefault AS "default",
    list_contains(current_schemas(false), n.nspname) AS visible,
    n.nspname AS "schema"
FROM __sys__.pg_type t
LEFT JOIN __sys__.pg_type b ON b.oid = t.typbasetype
LEFT JOIN __sys__.pg_namespace n ON n.oid = t.typnamespace
WHERE t.typtype = 'd'`
