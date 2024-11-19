package pgserver

import (
	"strings"
	"unicode"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/marcboeker/go-duckdb"
)

var wellKnownStatementTags = map[string]struct{}{
	"SELECT":  {},
	"INSERT":  {},
	"UPDATE":  {},
	"DELETE":  {},
	"CALL":    {},
	"PRAGMA":  {},
	"COPY":    {},
	"ALTER":   {},
	"CREATE":  {},
	"DROP":    {},
	"PREPARE": {},
	"EXECUTE": {},
	"ATTACH":  {},
	"DETACH":  {},
}

func IsWellKnownStatementTag(tag string) bool {
	_, ok := wellKnownStatementTags[tag]
	return ok
}

func GetStatementTag(stmt *duckdb.Stmt) string {
	switch stmt.StatementType() {
	case duckdb.DUCKDB_STATEMENT_TYPE_SELECT:
		return "SELECT"
	case duckdb.DUCKDB_STATEMENT_TYPE_INSERT:
		return "INSERT"
	case duckdb.DUCKDB_STATEMENT_TYPE_UPDATE:
		return "UPDATE"
	case duckdb.DUCKDB_STATEMENT_TYPE_DELETE:
		return "DELETE"
	case duckdb.DUCKDB_STATEMENT_TYPE_CALL:
		return "CALL"
	case duckdb.DUCKDB_STATEMENT_TYPE_PRAGMA:
		return "PRAGMA"
	case duckdb.DUCKDB_STATEMENT_TYPE_COPY:
		return "COPY"
	case duckdb.DUCKDB_STATEMENT_TYPE_ALTER:
		return "ALTER"
	case duckdb.DUCKDB_STATEMENT_TYPE_CREATE:
		return "CREATE"
	case duckdb.DUCKDB_STATEMENT_TYPE_CREATE_FUNC:
		return "CREATE FUNCTION"
	case duckdb.DUCKDB_STATEMENT_TYPE_DROP:
		return "DROP"
	case duckdb.DUCKDB_STATEMENT_TYPE_PREPARE:
		return "PREPARE"
	case duckdb.DUCKDB_STATEMENT_TYPE_EXECUTE:
		return "EXECUTE"
	case duckdb.DUCKDB_STATEMENT_TYPE_ATTACH:
		return "ATTACH"
	case duckdb.DUCKDB_STATEMENT_TYPE_DETACH:
		return "DETACH"
	case duckdb.DUCKDB_STATEMENT_TYPE_TRANSACTION:
		return "TRANSACTION"
	case duckdb.DUCKDB_STATEMENT_TYPE_ANALYZE:
		return "ANALYZE"
	case duckdb.DUCKDB_STATEMENT_TYPE_EXPLAIN:
		return "EXPLAIN"
	case duckdb.DUCKDB_STATEMENT_TYPE_SET:
		return "SET"
	case duckdb.DUCKDB_STATEMENT_TYPE_VARIABLE_SET:
		return "SET VARIABLE"
	case duckdb.DUCKDB_STATEMENT_TYPE_EXPORT:
		return "EXPORT"
	case duckdb.DUCKDB_STATEMENT_TYPE_LOAD:
		return "LOAD"
	default:
		return "UNKNOWN"
	}
}

func GuessStatementTag(query string) string {
	// Remove leading line and block comments
	query = RemoveLeadingComments(query)
	// Remove trailing semicolon
	query = sql.RemoveSpaceAndDelimiter(query, ';')

	// Guess the statement tag by looking for the first space in the query.
	for i, c := range query {
		if unicode.IsSpace(c) {
			return strings.ToUpper(query[:i])
		}
	}
	return ""
}

func RemoveLeadingComments(query string) string {
	i := 0
	n := len(query)

	for i < n {
		if strings.HasPrefix(query[i:], "--") {
			// Skip line comment
			end := strings.Index(query[i:], "\n")
			if end == -1 {
				return ""
			}
			i += end + 1
		} else if strings.HasPrefix(query[i:], "/*") {
			// Skip block comment
			end := strings.Index(query[i+2:], "*/")
			if end == -1 {
				return ""
			}
			i += end + 4
		} else if unicode.IsSpace(rune(query[i])) {
			// Skip whitespace
			i++
		} else {
			break
		}
	}
	return query[i:]
}
