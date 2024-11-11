package catalog

import "strings"

type InternalTable struct {
	Schema       string
	Name         string
	KeyColumns   []string
	ValueColumns []string
	DDL          string
	InitialData  [][]any
}

func (it *InternalTable) QualifiedName() string {
	return it.Schema + "." + it.Name
}

func (it *InternalTable) UpsertStmt() string {
	var b strings.Builder
	b.Grow(128)
	b.WriteString("INSERT OR REPLACE INTO ")
	b.WriteString(it.Schema)
	b.WriteByte('.')
	b.WriteString(it.Name)
	b.WriteString(" VALUES (?")
	for range it.KeyColumns[1:] {
		b.WriteString(", ?")
	}
	for range it.ValueColumns {
		b.WriteString(", ?")
	}
	b.WriteString(")")
	return b.String()
}

func (it *InternalTable) DeleteStmt() string {
	var b strings.Builder
	b.Grow(128)
	b.WriteString("DELETE FROM ")
	b.WriteString(it.Schema)
	b.WriteByte('.')
	b.WriteString(it.Name)
	b.WriteString(" WHERE ")
	b.WriteString(it.KeyColumns[0])
	b.WriteString(" = ?")
	for _, c := range it.KeyColumns[1:] {
		b.WriteString(c)
		b.WriteString(" = ?")
	}
	return b.String()
}

func (it *InternalTable) SelectStmt() string {
	var b strings.Builder
	b.Grow(128)
	b.WriteString("SELECT ")
	b.WriteString(it.ValueColumns[0])
	for _, c := range it.ValueColumns[1:] {
		b.WriteString(", ")
		b.WriteString(c)
	}
	b.WriteString(" FROM ")
	b.WriteString(it.Schema)
	b.WriteByte('.')
	b.WriteString(it.Name)
	b.WriteString(" WHERE ")
	b.WriteString(it.KeyColumns[0])
	b.WriteString(" = ?")
	for _, c := range it.KeyColumns[1:] {
		b.WriteString(" AND ")
		b.WriteString(c)
		b.WriteString(" = ?")
	}
	return b.String()
}

var InternalTables = struct {
	PersistentVariable InternalTable
	BinlogPosition     InternalTable
	PgReplicationLSN   InternalTable
	GlobalStatus       InternalTable
}{
	PersistentVariable: InternalTable{
		Schema:       "main",
		Name:         "persistent_variable",
		KeyColumns:   []string{"name"},
		ValueColumns: []string{"value", "vtype"},
		DDL:          "name TEXT PRIMARY KEY, value TEXT, vtype TEXT",
	},
	BinlogPosition: InternalTable{
		Schema:       "main",
		Name:         "binlog_position",
		KeyColumns:   []string{"channel"},
		ValueColumns: []string{"position"},
		DDL:          "channel TEXT PRIMARY KEY, position TEXT",
	},
	PgReplicationLSN: InternalTable{
		Schema:       "main",
		Name:         "pg_replication_lsn",
		KeyColumns:   []string{"slot_name"},
		ValueColumns: []string{"lsn"},
		DDL:          "slot_name TEXT PRIMARY KEY, lsn TEXT",
	},
	GlobalStatus: InternalTable{
		Schema:       "performance_schema",
		Name:         "global_status",
		KeyColumns:   []string{"VARIABLE_NAME"},
		ValueColumns: []string{"VARIABLE_VALUE"},
		DDL:          "VARIABLE_NAME TEXT PRIMARY KEY, VARIABLE_VALUE TEXT",
		InitialData: [][]any{
			{"Innodb_redo_log_enabled", "OFF"}, // Queried by MySQL Shell
		},
	},
}

var internalTables = []InternalTable{
	InternalTables.PersistentVariable,
	InternalTables.BinlogPosition,
	InternalTables.PgReplicationLSN,
	InternalTables.GlobalStatus,
}
