package configuration

import (
	stdsql "database/sql"
	"strings"
)

// A static map that contains all available configuration names on DuckDB.
// This list will be initialized later in a static block.
var duckConfigNames map[string]struct{}

// Initialize the duckConfigNames list. we will execute "select name from duckdb_settings()"
// to get all the configuration names and store them in the duckConfigNames map.
func Init(db *stdsql.DB) {
	duckConfigNames = make(map[string]struct{})
	rows, err := db.Query("select name from duckdb_settings()")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			panic(err)
		}
		duckConfigNames[strings.ToLower(name)] = struct{}{}
	}
}

// Check if the given configuration name is valid on DuckDB.
func IsValidConfig(name string) bool {
	_, ok := duckConfigNames[strings.ToLower(name)]
	return ok
}
