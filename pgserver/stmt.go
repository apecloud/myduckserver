package pgserver

import "github.com/marcboeker/go-duckdb"

func getStatementTag(stmt *duckdb.Stmt) string {
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
