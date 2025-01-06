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
	PGType            InternalTable
	PGProc            InternalTable
	PGClass           InternalTable
	PGNamespace       InternalTable
	PGMatViews        InternalTable
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
		DDL:          "rngtypid BIGINT PRIMARY KEY, rngsubtype BIGINT, rngmultitypid BIGINT, rngcollation BIGINT, rngsubopc BIGINT, rngcanonical VARCHAR, rngsubdiff VARCHAR",
		InitialData:  InitialDataTables.PGRange,
	},
	// postgres=# \d+ pg_type
	//                                             Table "pg_catalog.pg_type"
	//       Column      |     Type      | Collation | Nullable | Default |  Storage  | Compression | Stats target | Description
	//-------------------+---------------+-----------+----------+---------+-----------+-------------+--------------+-------------
	// oid               | BIGINT        |           | not null |         | plain     |             |              |
	// typname           | VARCHAR       |           | not null |         | plain     |             |              |
	// typnamespace      | BIGINT        |           | not null |         | plain     |             |              |
	// typowner          | BIGINT        |           | not null |         | plain     |             |              |
	// typlen            | SMALLINT      |           | not null |         | plain     |             |              |
	// typbyval          | BOOLEAN       |           | not null |         | plain     |             |              |
	// typtype           | CHAR          |           | not null |         | plain     |             |              |
	// typcategory       | CHAR          |           | not null |         | plain     |             |              |
	// typispreferred    | BOOLEAN       |           | not null |         | plain     |             |              |
	// typisdefined      | BOOLEAN       |           | not null |         | plain     |             |              |
	// typdelim          | CHAR          |           | not null |         | plain     |             |              |
	// typrelid          | BIGINT        |           | not null |         | plain     |             |              |
	// typsubscript      | BIGINT        |           | not null |         | plain     |             |              |
	// typelem           | BIGINT        |           | not null |         | plain     |             |              |
	// typarray          | BIGINT        |           | not null |         | plain     |             |              |
	// typinput          | BIGINT        |           | not null |         | plain     |             |              |
	// typoutput         | BIGINT        |           | not null |         | plain     |             |              |
	// typreceive        | BIGINT        |           | not null |         | plain     |             |              |
	// typsend           | BIGINT        |           | not null |         | plain     |             |              |
	// typmodin          | BIGINT        |           | not null |         | plain     |             |              |
	// typmodout         | BIGINT        |           | not null |         | plain     |             |              |
	// typanalyze        | BIGINT        |           | not null |         | plain     |             |              |
	// typalign          | CHAR          |           | not null |         | plain     |             |              |
	// typstorage        | CHAR          |           | not null |         | plain     |             |              |
	// typnotnull        | BOOLEAN       |           | not null |         | plain     |             |              |
	// typbasetype       | BIGINT        |           | not null |         | plain     |             |              |
	// typtypmod         | INTEGER       |           | not null |         | plain     |             |              |
	// typndims          | INTEGER       |           | not null |         | plain     |             |              |
	// typcollation      | BIGINT        |           | not null |         | plain     |             |              |
	// typdefaultbin     | VARCHAR       | C         |          |         | extended  |             |              |
	// typdefault        | TEXT          | C         |          |         | extended  |             |              |
	// typacl            | TEXT[]        |           |          |         | extended  |             |              |
	PGType: InternalTable{
		Schema: "__sys__",
		Name:   "pg_type",
		KeyColumns: []string{
			"oid",
		},
		ValueColumns: []string{
			"typname", "typnamespace", "typowner", "typlen", "typbyval",
			"typtype", "typcategory", "typispreferred", "typisdefined", "typdelim",
			"typrelid", "typsubscript", "typelem", "typarray", "typinput",
			"typoutput", "typreceive", "typsend", "typmodin", "typmodout",
			"typanalyze", "typalign", "typstorage", "typnotnull", "typbasetype",
			"typtypmod", "typndims", "typcollation", "typdefaultbin", "typdefault",
			"typacl",
		},
		DDL: "oid BIGINT NOT NULL PRIMARY KEY, " + // Replace oid with BIGINT
			"typname VARCHAR , " + // Replace name with VARCHAR
			"typnamespace BIGINT , " + // Replace oid with BIGINT
			"typowner BIGINT , " + // Replace oid with BIGINT
			"typlen SMALLINT , " + // Supported as-is
			"typbyval BOOLEAN , " + // Supported as-is
			"typtype CHAR , " + // Replace \"char\" with CHAR
			"typcategory CHAR , " + // Replace \"char\" with CHAR
			"typispreferred BOOLEAN , " + // Supported as-is
			"typisdefined BOOLEAN , " + // Supported as-is
			"typdelim CHAR , " + // Replace \"char\" with CHAR
			"typrelid BIGINT , " + // Replace oid with BIGINT
			"typsubscript BIGINT , " + // Replace regproc with VARCHAR
			"typelem BIGINT , " + // Replace oid with BIGINT
			"typarray BIGINT , " + // Replace oid with BIGINT
			"typinput BIGINT , " + // Replace regproc with VARCHAR
			"typoutput BIGINT , " + // Replace regproc with VARCHAR
			"typreceive BIGINT , " + // Replace regproc with VARCHAR
			"typsend BIGINT , " + // Replace regproc with VARCHAR
			"typmodin BIGINT , " + // Replace regproc with VARCHAR
			"typmodout BIGINT , " + // Replace regproc with VARCHAR
			"typanalyze BIGINT , " + // Replace regproc with VARCHAR
			"typalign CHAR , " + // Replace \"char\" with CHAR
			"typstorage CHAR , " + // Replace \"char\" with CHAR
			"typnotnull BOOLEAN , " + // Supported as-is
			"typbasetype BIGINT , " + // Replace oid with BIGINT
			"typtypmod INTEGER , " + // Supported as-is
			"typndims INTEGER , " + // Supported as-is
			"typcollation BIGINT , " + // Replace oid with BIGINT
			"typdefaultbin VARCHAR, " + // Replace pg_node_tree with VARCHAR
			"typdefault TEXT, " + // Supported as-is
			"typacl TEXT",
	},
	//postgres=# \d+ pg_proc;
	//                                              Table "pg_catalog.pg_proc"
	//     Column      |     Type     | Collation | Nullable | Default | Storage  | Compression | Stats target | Description
	//-----------------+--------------+-----------+----------+---------+----------+-------------+--------------+-------------
	// oid             | oid          |           | not null |         | plain    |             |              |
	// proname         | name         |           | not null |         | plain    |             |              |
	// pronamespace    | oid          |           | not null |         | plain    |             |              |
	// proowner        | oid          |           | not null |         | plain    |             |              |
	// prolang         | oid          |           | not null |         | plain    |             |              |
	// procost         | real         |           | not null |         | plain    |             |              |
	// prorows         | real         |           | not null |         | plain    |             |              |
	// provariadic     | oid          |           | not null |         | plain    |             |              |
	// prosupport      | regproc      |           | not null |         | plain    |             |              |
	// prokind         | "char"       |           | not null |         | plain    |             |              |
	// prosecdef       | boolean      |           | not null |         | plain    |             |              |
	// proleakproof    | boolean      |           | not null |         | plain    |             |              |
	// proisstrict     | boolean      |           | not null |         | plain    |             |              |
	// proretset       | boolean      |           | not null |         | plain    |             |              |
	// provolatile     | "char"       |           | not null |         | plain    |             |              |
	// proparallel     | "char"       |           | not null |         | plain    |             |              |
	// pronargs        | smallint     |           | not null |         | plain    |             |              |
	// pronargdefaults | smallint     |           | not null |         | plain    |             |              |
	// prorettype      | oid          |           | not null |         | plain    |             |              |
	// proargtypes     | oidvector    |           | not null |         | plain    |             |              |
	// proallargtypes  | oid[]        |           |          |         | extended |             |              |
	// proargmodes     | "char"[]     |           |          |         | extended |             |              |
	// proargnames     | text[]       | C         |          |         | extended |             |              |
	// proargdefaults  | pg_node_tree | C         |          |         | extended |             |              |
	// protrftypes     | oid[]        |           |          |         | extended |             |              |
	// prosrc          | text         | C         | not null |         | extended |             |              |
	// probin          | text         | C         |          |         | extended |             |              |
	// prosqlbody      | pg_node_tree | C         |          |         | extended |             |              |
	// proconfig       | text[]       | C         |          |         | extended |             |              |
	// proacl          | aclitem[]    |           |          |         | extended |             |              |
	PGProc: InternalTable{
		Schema: "__sys__",
		Name:   "pg_proc",
		KeyColumns: []string{
			"oid",
		},
		ValueColumns: []string{
			"proname",
			"pronamespace",
			"proowner",
			"prolang",
			"procost",
			"prorows",
			"provariadic",
			"prosupport",
			"prokind",
			"prosecdef",
			"proleakproof",
			"proisstrict",
			"proretset",
			"provolatile",
			"proparallel",
			"pronargs",
			"pronargdefaults",
			"prorettype",
			"proargtypes",
			"proallargtypes",
			"proargmodes",
			"proargnames",
			"proargdefaults",
			"protrftypes",
			"prosrc",
			"probin",
			"prosqlbody",
			"proconfig",
			"proacl",
		},
		DDL: "oid BIGINT NOT NULL PRIMARY KEY," +
			"proname VARCHAR ," +
			"pronamespace BIGINT ," +
			"proowner BIGINT ," +
			"prolang BIGINT ," +
			"procost FLOAT ," +
			"prorows FLOAT ," +
			"provariadic BIGINT ," +
			"prosupport BIGINT ," + // Replaced regproc with BIGINT
			"prokind CHAR ," +
			"prosecdef BOOLEAN ," +
			"proleakproof BOOLEAN ," +
			"proisstrict BOOLEAN ," +
			"proretset BOOLEAN ," +
			"provolatile CHAR ," +
			"proparallel CHAR ," +
			"pronargs SMALLINT ," +
			"pronargdefaults SMALLINT ," +
			"prorettype BIGINT ," +
			"proargtypes VARCHAR ," + // Replaced oidvector with VARCHAR
			"proallargtypes VARCHAR," + // Replaced oid[] with VARCHAR
			"proargmodes VARCHAR," + // Replaced 'char'[] with VARCHAR
			"proargnames VARCHAR," + // Replaced text[] with VARCHAR
			"proargdefaults TEXT," + // Replaced pg_node_tree with TEXT
			"protrftypes VARCHAR," + // Replaced oid[] with VARCHAR
			"prosrc TEXT ," +
			"probin TEXT," +
			"prosqlbody TEXT," + // Replaced pg_node_tree with TEXT
			"proconfig VARCHAR," + // Replaced text[] with VARCHAR
			"proacl VARCHAR", // Replaced aclitem[] with VARCHAR
	},

	// postgres=# \d+ pg_class;
	//                                                Table "pg_catalog.pg_class"
	//       Column        |     Type     | Collation | Nullable | Default | Storage  | Compression | Stats target | Description
	//---------------------+--------------+-----------+----------+---------+----------+-------------+--------------+-------------
	// oid                 | oid          |           | not null |         | plain    |             |              |
	// relname             | name         |           | not null |         | plain    |             |              |
	// relnamespace        | oid          |           | not null |         | plain    |             |              |
	// reltype             | oid          |           | not null |         | plain    |             |              |
	// reloftype           | oid          |           | not null |         | plain    |             |              |
	// relowner            | oid          |           | not null |         | plain    |             |              |
	// relam               | oid          |           | not null |         | plain    |             |              |
	// relfilenode         | oid          |           | not null |         | plain    |             |              |
	// reltablespace       | oid          |           | not null |         | plain    |             |              |
	// relpages            | integer      |           | not null |         | plain    |             |              |
	// reltuples           | real         |           | not null |         | plain    |             |              |
	// relallvisible       | integer      |           | not null |         | plain    |             |              |
	// reltoastrelid       | oid          |           | not null |         | plain    |             |              |
	// relhasindex         | boolean      |           | not null |         | plain    |             |              |
	// relisshared         | boolean      |           | not null |         | plain    |             |              |
	// relpersistence      | "char"       |           | not null |         | plain    |             |              |
	// relkind             | "char"       |           | not null |         | plain    |             |              |
	// relnatts            | smallint     |           | not null |         | plain    |             |              |
	// relchecks           | smallint     |           | not null |         | plain    |             |              |
	// relhasrules         | boolean      |           | not null |         | plain    |             |              |
	// relhastriggers      | boolean      |           | not null |         | plain    |             |              |
	// relhassubclass      | boolean      |           | not null |         | plain    |             |              |
	// relrowsecurity      | boolean      |           | not null |         | plain    |             |              |
	// relforcerowsecurity | boolean      |           | not null |         | plain    |             |              |
	// relispopulated      | boolean      |           | not null |         | plain    |             |              |
	// relreplident        | "char"       |           | not null |         | plain    |             |              |
	// relispartition      | boolean      |           | not null |         | plain    |             |              |
	// relrewrite          | oid          |           | not null |         | plain    |             |              |
	// relfrozenxid        | xid          |           | not null |         | plain    |             |              |
	// relminmxid          | xid          |           | not null |         | plain    |             |              |
	// relacl              | aclitem[]    |           |          |         | extended |             |              |
	// reloptions          | text[]       | C         |          |         | extended |             |              |
	// relpartbound        | pg_node_tree | C         |          |         | extended |             |              |
	PGClass: InternalTable{
		Schema: "__sys__",
		Name:   "pg_class",
		KeyColumns: []string{
			"oid",
		},
		ValueColumns: []string{
			"relname",
			"relnamespace",
			"reltype",
			"reloftype",
			"relowner",
			"relam",
			"relfilenode",
			"reltablespace",
			"relpages",
			"reltuples",
			"relallvisible",
			"reltoastrelid",
			"relhasindex",
			"relisshared",
			"relpersistence",
			"relkind",
			"relnatts",
			"relchecks",
			"relhasrules",
			"relhastriggers",
			"relhassubclass",
			"relrowsecurity",
			"relforcerowsecurity",
			"relispopulated",
			"relreplident",
			"relispartition",
			"relrewrite",
			"relfrozenxid",
			"relminmxid",
			"relacl",
			"reloptions",
			"relpartbound",
		},
		DDL: "oid BIGINT NOT NULL PRIMARY KEY," +
			"relname VARCHAR ," +
			"relnamespace BIGINT ," +
			"reltype BIGINT ," +
			"reloftype BIGINT ," +
			"relowner BIGINT ," +
			"relam BIGINT ," +
			"relfilenode BIGINT ," +
			"reltablespace BIGINT ," +
			"relpages INTEGER ," +
			"reltuples FLOAT ," +
			"relallvisible INTEGER ," +
			"reltoastrelid BIGINT ," +
			"relhasindex BOOLEAN ," +
			"relisshared BOOLEAN ," +
			"relpersistence CHAR ," +
			"relkind CHAR ," +
			"relnatts SMALLINT ," +
			"relchecks SMALLINT ," +
			"relhasrules BOOLEAN ," +
			"relhastriggers BOOLEAN ," +
			"relhassubclass BOOLEAN ," +
			"relrowsecurity BOOLEAN ," +
			"relforcerowsecurity BOOLEAN ," +
			"relispopulated BOOLEAN ," +
			"relreplident CHAR ," +
			"relispartition BOOLEAN ," +
			"relrewrite BIGINT ," +
			"relfrozenxid BIGINT ," +
			"relminmxid BIGINT ," +
			"relacl TEXT," +
			"reloptions TEXT," +
			"relpartbound TEXT",
	},
	//  Table "pg_catalog.pg_namespace"
	//  Column  |   Type    | Collation | Nullable | Default | Storage  | Compression | Stats target | Description
	//----------+-----------+-----------+----------+---------+----------+-------------+--------------+-------------
	// oid      | oid       |           | not null |         | plain    |             |              |
	// nspname  | name      |           | not null |         | plain    |             |              |
	// nspowner | oid       |           | not null |         | plain    |             |              |
	// nspacl   | aclitem[] |           |          |         | extended |             |              |
	PGNamespace: InternalTable{
		Schema: "__sys__",
		Name:   "pg_namespace",
		KeyColumns: []string{
			"oid",
		},
		ValueColumns: []string{
			"nspname",
			"nspowner",
			"nspacl",
		},
		DDL: "oid BIGINT NOT NULL PRIMARY KEY," +
			"nspname VARCHAR NOT NULL," +
			"nspowner BIGINT NOT NULL," +
			"nspacl TEXT",
		InitialData: InitialDataTables.PGNamespace,
	},
	// View "pg_catalog.pg_matviews"
	// postgres=# \d+ pg_catalog.pg_matviews
	//                          View "pg_catalog.pg_matviews"
	//    Column    |  Type   | Collation | Nullable | Default | Storage  | Description
	//--------------+---------+-----------+----------+---------+----------+-------------
	// schemaname   | name    |           |          |         | plain    |
	// matviewname  | name    |           |          |         | plain    |
	// matviewowner | name    |           |          |         | plain    |
	// tablespace   | name    |           |          |         | plain    |
	// hasindexes   | boolean |           |          |         | plain    |
	// ispopulated  | boolean |           |          |         | plain    |
	// definition   | text    |           |          |         | extended |
	//View definition:
	// SELECT n.nspname AS schemaname,
	//    c.relname AS matviewname,
	//    pg_get_userbyid(c.relowner) AS matviewowner,
	//    t.spcname AS tablespace,
	//    c.relhasindex AS hasindexes,
	//    c.relispopulated AS ispopulated,
	//    pg_get_viewdef(c.oid) AS definition
	//   FROM pg_class c
	//     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
	//     LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace
	//  WHERE c.relkind = 'm'::"char";
	PGMatViews: InternalTable{
		Schema: "__sys__",
		Name:   "pg_matviews",
		KeyColumns: []string{
			"schemaname",
			"matviewname",
		},
		ValueColumns: []string{
			"matviewowner",
			"tablespace",
			"hasindexes",
			"ispopulated",
			"definition",
		},
		DDL: "schemaname VARCHAR NOT NULL, " +
			"matviewname VARCHAR NOT NULL, " +
			"matviewowner VARCHAR, " +
			"tablespace VARCHAR, " +
			"hasindexes BOOLEAN, " +
			"ispopulated BOOLEAN, " +
			"definition TEXT",
	},
}

var internalTables = []InternalTable{
	InternalTables.PersistentVariable,
	InternalTables.BinlogPosition,
	InternalTables.PgSubscription,
	InternalTables.GlobalStatus,
	InternalTables.PGStatReplication,
	InternalTables.PGRange,
	InternalTables.PGType,
	InternalTables.PGProc,
	InternalTables.PGClass,
	InternalTables.PGNamespace,
	InternalTables.PGMatViews,
}

func GetInternalTables() []InternalTable {
	return internalTables
}
