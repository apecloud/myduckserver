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

func (it *InternalTable) UpdateStmt(keyColumns []string, valueColumns []string) string {
	var b strings.Builder
	b.Grow(128)
	b.WriteString("UPDATE ")
	b.WriteString(it.QualifiedName())
	b.WriteString(" SET " + valueColumns[0] + " = ?")

	for _, valueColumn := range valueColumns[1:] {
		b.WriteString(", ")
		b.WriteString(valueColumn)
		b.WriteString(" = ?")
	}

	b.WriteString(" WHERE " + keyColumns[0] + " = ?")
	for _, keyColumn := range keyColumns[1:] {
		b.WriteString(", ")
		b.WriteString(keyColumn)
		b.WriteString(" = ?")
	}

	return b.String()
}

func (it *InternalTable) UpdateAllStmt(valueColumns []string) string {
	var b strings.Builder
	b.Grow(128)
	b.WriteString("UPDATE ")
	b.WriteString(it.QualifiedName())
	b.WriteString(" SET " + valueColumns[0] + " = ?")

	for _, valueColumn := range valueColumns[1:] {
		b.WriteString(", ")
		b.WriteString(valueColumn)
		b.WriteString(" = ?")
	}

	return b.String()
}

func (it *InternalTable) UpsertStmt() string {
	var b strings.Builder
	b.Grow(128)
	b.WriteString("INSERT OR REPLACE INTO ")
	b.WriteString(it.QualifiedName())
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
	b.WriteString(it.QualifiedName())
	b.WriteString(" WHERE ")
	b.WriteString(it.KeyColumns[0])
	b.WriteString(" = ?")
	for _, c := range it.KeyColumns[1:] {
		b.WriteString(c)
		b.WriteString(" = ?")
	}
	return b.String()
}

func (it *InternalTable) DeleteAllStmt() string {
	var b strings.Builder
	b.Grow(128)
	b.WriteString("DELETE FROM ")
	b.WriteString(it.QualifiedName())
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
	b.WriteString(it.QualifiedName())
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

func (it *InternalTable) SelectColumnsStmt(valueColumns []string) string {
	var b strings.Builder
	b.Grow(128)
	b.WriteString("SELECT ")
	b.WriteString(valueColumns[0])
	for _, c := range valueColumns[1:] {
		b.WriteString(", ")
		b.WriteString(c)
	}
	b.WriteString(" FROM ")
	b.WriteString(it.QualifiedName())
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

func (it *InternalTable) SelectAllStmt() string {
	var b strings.Builder
	b.Grow(128)
	b.WriteString("SELECT * FROM ")
	b.WriteString(it.QualifiedName())
	return b.String()
}

func (it *InternalTable) CountAllStmt() string {
	var b strings.Builder
	b.Grow(128)
	b.WriteString("SELECT COUNT(*)")
	b.WriteString(" FROM ")
	b.WriteString(it.QualifiedName())
	return b.String()
}

var InternalTables = struct {
	PersistentVariable InternalTable
	BinlogPosition     InternalTable
	PgSubscription     InternalTable
	GlobalStatus       InternalTable
	// TODO(sean): This is a temporary work around for clients that query the 'pg_catalog.pg_stat_replication'.
	//             Once we add 'pg_catalog' and support views for PG, replace this by a view.
	//             https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-REPLICATION-VIEW
	PGStatReplication InternalTable
	PGRange           InternalTable
}{
	PersistentVariable: InternalTable{
		Schema:       "__sys__",
		Name:         "persistent_variable",
		KeyColumns:   []string{"name"},
		ValueColumns: []string{"value", "vtype"},
		DDL:          "name TEXT PRIMARY KEY, value TEXT, vtype TEXT",
	},
	BinlogPosition: InternalTable{
		Schema:       "__sys__",
		Name:         "binlog_position",
		KeyColumns:   []string{"channel"},
		ValueColumns: []string{"position"},
		DDL:          "channel TEXT PRIMARY KEY, position TEXT",
	},
	PgSubscription: InternalTable{
		Schema:       "__sys__",
		Name:         "pg_subscription",
		KeyColumns:   []string{"subname"},
		ValueColumns: []string{"subconninfo", "subpublication", "subskiplsn", "subenabled"},
		DDL:          "subname TEXT PRIMARY KEY, subconninfo TEXT, subpublication TEXT, subskiplsn TEXT, subenabled BOOLEAN",
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
	//	postgres=# \d+ pg_catalog.pg_stat_replication
	//                                 View "pg_catalog.pg_stat_replication"
	//      Column      |           Type           | Collation | Nullable | Default | Storage  | Description
	//------------------+--------------------------+-----------+----------+---------+----------+-------------
	// pid              | integer                  |           |          |         | plain    |
	// usesysid         | oid                      |           |          |         | plain    |
	// usename          | name                     |           |          |         | plain    |
	// application_name | text                     |           |          |         | extended |
	// client_addr      | inet                     |           |          |         | main     |
	// client_hostname  | text                     |           |          |         | extended |
	// client_port      | integer                  |           |          |         | plain    |
	// backend_start    | timestamp with time zone |           |          |         | plain    |
	// backend_xmin     | xid                      |           |          |         | plain    |
	// state            | text                     |           |          |         | extended |
	// sent_lsn         | pg_lsn                   |           |          |         | plain    |
	// write_lsn        | pg_lsn                   |           |          |         | plain    |
	// flush_lsn        | pg_lsn                   |           |          |         | plain    |
	// replay_lsn       | pg_lsn                   |           |          |         | plain    |
	// write_lag        | interval                 |           |          |         | plain    |
	// flush_lag        | interval                 |           |          |         | plain    |
	// replay_lag       | interval                 |           |          |         | plain    |
	// sync_priority    | integer                  |           |          |         | plain    |
	// sync_state       | text                     |           |          |         | extended |
	// reply_time       | timestamp with time zone |           |          |         | plain    |
	//View definition:
	// SELECT s.pid,
	//    s.usesysid,
	//    u.rolname AS usename,
	//    s.application_name,
	//    s.client_addr,
	//    s.client_hostname,
	//    s.client_port,
	//    s.backend_start,
	//    s.backend_xmin,
	//    w.state,
	//    w.sent_lsn,
	//    w.write_lsn,
	//    w.flush_lsn,
	//    w.replay_lsn,
	//    w.write_lag,
	//    w.flush_lag,
	//    w.replay_lag,
	//    w.sync_priority,
	//    w.sync_state,
	//    w.reply_time
	//   FROM pg_stat_get_activity(NULL::integer) s(datid, pid, usesysid, application_name, state, query, wait_event_type, wait_event, xact_start, query_start, backend_start, state_change, client_addr, client_hostname, client_port, backend_xid, backend_xmin, backend_type, ssl, sslversion, sslcipher, sslbits, ssl_client_dn, ssl_client_serial, ssl_issuer_dn, gss_auth, gss_princ, gss_enc, gss_delegation, leader_pid, query_id)
	//     JOIN pg_stat_get_wal_senders() w(pid, state, sent_lsn, write_lsn, flush_lsn, replay_lsn, write_lag, flush_lag, replay_lag, sync_priority, sync_state, reply_time) ON s.pid = w.pid
	//     LEFT JOIN pg_authid u ON s.usesysid = u.oid;
	PGStatReplication: InternalTable{
		// Since the "pg_catalog" is the system catalog on DuckDB, we use "__sys__" as the schema name.
		Schema: "__sys__",
		Name:   "pg_stat_replication",
		KeyColumns: []string{
			"pid",
		},
		ValueColumns: []string{
			"usesysid",
			"usename",
			"application_name",
			"client_addr",
			"client_hostname",
			"client_port",
			"backend_start",
			"backend_xmin",
			"state",
			"sent_lsn",
			"write_lsn",
			"flush_lsn",
			"replay_lsn",
			"write_lag",
			"flush_lag",
			"replay_lag",
			"sync_priority",
			"sync_state",
			"reply_time",
		},
		DDL: "pid INTEGER PRIMARY KEY, usesysid TEXT, usename TEXT, application_name TEXT, client_addr TEXT, client_hostname TEXT, client_port INTEGER, backend_start TIMESTAMP, backend_xmin INTEGER, state TEXT, sent_lsn TEXT, write_lsn TEXT, flush_lsn TEXT, replay_lsn TEXT, write_lag INTERVAL, flush_lag INTERVAL, replay_lag INTERVAL, sync_priority INTEGER, sync_state TEXT, reply_time TIMESTAMP",
	},
	PGRange: InternalTable{
		Schema:       "__sys__",
		Name:         "pg_range",
		KeyColumns:   []string{"rngtypid"},
		ValueColumns: []string{"rngsubtype", "rngmultitypid", "rngcollation", "rngsubopc", "rngcanonical", "rngsubdiff"},
		DDL:          "rngtypid TEXT PRIMARY KEY, rngsubtype TEXT, rngmultitypid TEXT, rngcollation TEXT, rngsubopc TEXT, rngcanonical TEXT, rngsubdiff TEXT",
		InitialData: [][]any{
			{"3904", "23", "4451", "0", "1978", "int4range_canonical", "int4range_subdiff"},
			{"3906", "1700", "4532", "0", "3125", "-", "numrange_subdiff"},
			{"3908", "1114", "4533", "0", "3128", "-", "tsrange_subdiff"},
			{"3910", "1184", "4534", "0", "3127", "-", "tstzrange_subdiff"},
			{"3912", "1082", "4535", "0", "3122", "daterange_canonical", "daterange_subdiff"},
			{"3926", "20", "4536", "0", "3124", "int8range_canonical", "int8range_subdiff"},
		},
	},
}

var internalTables = []InternalTable{
	InternalTables.PersistentVariable,
	InternalTables.BinlogPosition,
	InternalTables.PgSubscription,
	InternalTables.GlobalStatus,
	InternalTables.PGStatReplication,
	InternalTables.PGRange,
}

func GetInternalTables() []InternalTable {
	return internalTables
}
