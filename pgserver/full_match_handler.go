package pgserver

import (
	"regexp"
	"strings"
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

var pattern = regexp.MustCompile(`[ \n\r\t]+`)

type HardCodedQuery struct {
	original  string
	converted string
}

var HardCodedQueries = struct {
	SelectPgTypeOid HardCodedQuery
}{
	SelectPgTypeOid: HardCodedQuery{
		original:  "SELECT pg_type.oid, enumlabel FROM pg_enum JOIN pg_type ON pg_type.oid=enumtypid ORDER BY oid, enumsortorder",
		converted: "SELECT pg_type.oid, pg_enum.enumlabel FROM pg_enum JOIN pg_type ON pg_type.oid=enumtypid ORDER BY pg_type.oid, pg_enum.enumsortorder;",
	},
}

var queryPatterns = map[string]string{
	preProcessing(HardCodedQueries.SelectPgTypeOid.original): HardCodedQueries.SelectPgTypeOid.converted,
}

// handleFullMatchQuery checks if the given query matches any known patterns and returns the corresponding SQL query.
func handleFullMatchQuery(inputQuery string) string {
	return queryPatterns[preProcessing(inputQuery)]
}

func preProcessing(inputQuery string) string {
	trimmed := pattern.ReplaceAllString(inputQuery, "")
	return strings.ToLower(trimmed)
}
