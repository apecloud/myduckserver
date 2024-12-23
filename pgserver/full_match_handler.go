package pgserver

import (
	"regexp"
)

// queryPatterns maps regular expressions to SQL queries to handle PostgreSQL-specific queries
// that DuckDB does not support. When a query matches a pattern, the corresponding SQL query is returned.
//
// Example:
// SELECT pg_type.oid, enumlabel
// FROM pg_enum
// JOIN pg_type ON pg_type.oid=enumtypid
// ORDER BY pg_type.oid, pg_enum.enumsortorder;
//
// DuckDB produces the following error for the above query:
//   Binder Error: Ambiguous reference to column name "oid" (use: "pg_type.oid" or "pg_enum.oid")
//
// In contrast, PostgreSQL executes the query without error.
// The issue arises because DuckDB cannot resolve the `oid` column unambiguously when referenced
// without specifying the table. This behavior differs from PostgreSQL, which allows the ambiguous reference.
//
// Since handling all such cases is complex, we only handle a limited set of common queries,
// especially those frequently used with PostgreSQL clients.

var queryPatterns = map[*regexp.Regexp]string{
	regexp.MustCompile(`(?is)^SELECT\s+pg_type\.oid,\s*enumlabel\s+FROM\s+pg_enum\s+JOIN\s+pg_type\s+ON\s+pg_type\.oid=enumtypid\s+ORDER\s+BY\s+oid,\s+enumsortorder$`): "SELECT pg_type.oid, pg_enum.enumlabel FROM pg_enum JOIN pg_type ON pg_type.oid=enumtypid ORDER BY pg_type.oid, pg_enum.enumsortorder;",
}

// handleFullMatchQuery checks if the given query matches any known patterns and returns the corresponding SQL query.
func handleFullMatchQuery(inputQuery string) string {
	for pattern, sql := range queryPatterns {
		if pattern.MatchString(inputQuery) {
			return sql
		}
	}
	return ""
}
