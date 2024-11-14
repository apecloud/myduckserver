package catalog

var SystemViews = map[string]struct{}{
	"duckdb_columns":       {},
	"duckdb_constraints":   {},
	"duckdb_databases":     {},
	"duckdb_indexes":       {},
	"duckdb_schemas":       {},
	"duckdb_tables":        {},
	"duckdb_types":         {},
	"duckdb_views":         {},
	"pragma_database_list": {},
	"sqlite_master":        {},
	"sqlite_schema":        {},
	"sqlite_temp_master":   {},
	"sqlite_temp_schema":   {},
}

func IsSystemView(viewName string) bool {
	_, ok := SystemViews[viewName]
	return ok
}
