// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build go1.18
// +build go1.18

// Package example contains a FlightSQL Server implementation using
// sqlite as the backing engine.
//
// In order to ensure portability we'll use modernc.org/sqlite instead
// of github.com/mattn/go-sqlite3 because modernc is a translation of the
// SQLite source into Go, such that it doesn't require CGO to run and
// doesn't need to link against the actual libsqlite3 libraries. This way
// we don't require CGO or libsqlite3 to run this example or the tests.
//
// That said, since both implement in terms of Go's standard database/sql
// package, it's easy to swap them out if desired as the modernc.org/sqlite
// package is slower than go-sqlite3.
//
// One other important note is that modernc.org/sqlite only works
// correctly (specifically pragma_table_info) in go 1.18+ so this
// entire package is given the build constraint to only build when
// using go1.18 or higher
package flightsqlserver

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/duckdb/duckdb-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	_ "modernc.org/sqlite"
)

func genRandomString() []byte {
	const length = 16
	max := int('z')
	// don't include ':' as a valid byte to generate
	// because we use it as a separator for the transactions
	min := int('<')

	out := make([]byte, length)
	for i := range out {
		out[i] = byte(rand.Intn(max-min+1) + min)
	}
	return out
}

func prepareQueryForGetTables(cmd flightsql.GetTables) string {
	var b strings.Builder
	b.WriteString(`SELECT 'main' AS catalog_name, '' AS schema_name,
		name AS table_name, type AS table_type FROM sqlite_master WHERE 1=1`)

	if cmd.GetCatalog() != nil {
		b.WriteString(" and catalog_name = '")
		b.WriteString(*cmd.GetCatalog())
		b.WriteByte('\'')
	}

	if cmd.GetDBSchemaFilterPattern() != nil {
		b.WriteString(" and schema_name LIKE '")
		b.WriteString(*cmd.GetDBSchemaFilterPattern())
		b.WriteByte('\'')
	}

	if cmd.GetTableNameFilterPattern() != nil {
		b.WriteString(" and table_name LIKE '")
		b.WriteString(*cmd.GetTableNameFilterPattern())
		b.WriteByte('\'')
	}

	if len(cmd.GetTableTypes()) > 0 {
		b.WriteString(" and table_type IN (")
		for i, t := range cmd.GetTableTypes() {
			if i != 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "'%s'", t)
		}
		b.WriteByte(')')
	}

	b.WriteString(" order by table_name")
	return b.String()
}

func prepareQueryForGetKeys(filter string) string {
	return `SELECT * FROM (
		SELECT
			NULL AS pk_catalog_name,
			NULL AS pk_schema_name,
			p."table" AS pk_table_name,
			p."to" AS pk_column_name,
			NULL AS fk_catalog_name,
			NULL AS fk_schema_name,
			m.name AS fk_table_name,
			p."from" AS fk_column_name,
			p.seq AS key_sequence,
			NULL AS pk_key_name,
			NULL AS fk_key_name,
			CASE
				WHEN p.on_update = 'CASCADE' THEN 0
				WHEN p.on_update = 'RESTRICT' THEN 1
				WHEN p.on_update = 'SET NULL' THEN 2
				WHEN p.on_update = 'NO ACTION' THEN 3
				WHEN p.on_update = 'SET DEFAULT' THEN 4
			END AS update_rule,
			CASE
				WHEN p.on_delete = 'CASCADE' THEN 0
				WHEN p.on_delete = 'RESTRICT' THEN 1
				WHEN p.on_delete = 'SET NULL' THEN 2
				WHEN p.on_delete = 'NO ACTION' THEN 3
				WHEN p.on_delete = 'SET DEFAULT' THEN 4
			END AS delete_rule
		FROM sqlite_master m
		JOIN pragma_foreign_key_list(m.name) p ON m.name != p."table"
		WHERE m.type = 'table') WHERE ` + filter +
		` ORDER BY pk_catalog_name, pk_schema_name, pk_table_name, pk_key_name, key_sequence`
}

func CreateDB() (*sql.DB, error) {

	dbFile := "mysql.db"
	dataDir := "."

	dbFile = strings.TrimSpace(dbFile)
	dsn := filepath.Join(dataDir, dbFile)

	connector, err := duckdb.NewConnector(dsn, nil)
	if err != nil {
		return nil, err
	}

	db := sql.OpenDB(connector)

	_, err = db.Exec(`
	CREATE TABLE foreignTable (
		id INTEGER PRIMARY KEY  NOT NULL,
		foreignName varchar(100),
		value int);

	CREATE TABLE intTable (
		id INTEGER PRIMARY KEY NOT NULL,
		keyName varchar(100),
		value int,
		foreignId int references foreignTable(id));

	INSERT INTO foreignTable (id, foreignName, value) VALUES (1, 'keyOne', 1);
	INSERT INTO foreignTable (id, foreignName, value) VALUES (2, 'keyTwo', 0);
	INSERT INTO foreignTable (id, foreignName, value) VALUES (3, 'keyThree', -1);
	INSERT INTO intTable (id, keyName, value, foreignId) VALUES (1, 'one', 1, 1);
	INSERT INTO intTable (id, keyName, value, foreignId) VALUES (2, 'zero', 0, 1);
	INSERT INTO intTable (id, keyName, value, foreignId) VALUES (5, 'negative one', -1, 1);
	`)
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func encodeTransactionQuery(query string, transactionID flightsql.Transaction) ([]byte, error) {
	return flightsql.CreateStatementQueryTicket(
		bytes.Join([][]byte{transactionID, []byte(query)}, []byte(":")))
}

func decodeTransactionQuery(ticket []byte) (txnID, query string, err error) {
	id, queryBytes, found := bytes.Cut(ticket, []byte(":"))
	if !found {
		err = fmt.Errorf("%w: malformed ticket", arrow.ErrInvalid)
		return
	}

	txnID = string(id)
	query = string(queryBytes)
	return
}

type Statement struct {
	mu     sync.Mutex
	stmt   *sql.Stmt
	query  string
	params [][]interface{}
	// conn is set for statements prepared directly on a physical connection.
	// Transaction-owned statements point at the transaction's connection as
	// well, but do not own its lifetime.
	conn *sql.Conn
	// ownsConn is true only for a standalone prepared statement. Its connection
	// must remain checked out until ClosePreparedStatement so the statement and
	// every execution stay on the initialized physical connection.
	ownsConn bool
	// external is set for provider-owned FlightSQL handles. A standalone
	// prepared statement owns the external session; a transaction-owned
	// statement shares the transaction's session and leaves its lifetime to the
	// transaction finalizer.
	external    *catalog.ExternalSession
	transaction *transactionState
	closed      atomic.Bool
	closeOnce   sync.Once
	closeErr    error
}

type transactionState struct {
	tx       *sql.Tx
	conn     *sql.Conn
	external *catalog.ExternalSession
	handle   string
	closed   atomic.Bool
}

type managedFlightRequest struct {
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

type SQLiteFlightSQLServer struct {
	flightsql.BaseServer
	db                *sql.DB
	initializeStorage func(context.Context, *sql.Conn) error
	provider          *catalog.DatabaseProvider

	prepared         sync.Map
	openTransactions sync.Map
	requestMu        sync.Mutex
	requests         map[*managedFlightRequest]struct{}
	closed           bool
}

// NewSQLiteFlightSQLServer accepts an optional service initializer. Keeping
// the initializer at the request boundary prevents the shared database pool
// from receiving DuckLake credentials during process boot, while preserving
// the original constructor for callers that do not enable DuckLake. The
// callback receives the exact *sql.Conn selected for the request, so setup is
// applied to that physical connection rather than to an unrelated pool slot.
func NewSQLiteFlightSQLServer(db *sql.DB, initializer ...func(context.Context, *sql.Conn) error) (*SQLiteFlightSQLServer, error) {
	if len(initializer) > 1 {
		return nil, fmt.Errorf("at most one FlightSQL storage initializer is supported")
	}
	var initializeConnection func(context.Context, *sql.Conn) error
	if len(initializer) == 1 {
		initializeConnection = initializer[0]
	}
	ret := &SQLiteFlightSQLServer{db: db, initializeStorage: initializeConnection, requests: make(map[*managedFlightRequest]struct{})}
	ret.initialize()
	return ret, nil
}

// NewSQLiteFlightSQLServerWithProvider binds FlightSQL to the provider-owned
// connection pool instead of caching the startup *sql.DB. Pool-owned external
// sessions are invalidated on Close/Reset, while each in-flight operation keeps
// its storage generation alive until synchronous work or Arrow streaming ends.
func NewSQLiteFlightSQLServerWithProvider(provider *catalog.DatabaseProvider) (*SQLiteFlightSQLServer, error) {
	if provider == nil || provider.Pool() == nil {
		return nil, fmt.Errorf("FlightSQL database provider is unavailable")
	}
	ret := &SQLiteFlightSQLServer{provider: provider, requests: make(map[*managedFlightRequest]struct{})}
	ret.initialize()
	return ret, nil
}

func (s *SQLiteFlightSQLServer) initialize() {
	if s == nil {
		return
	}
	s.Alloc = memory.DefaultAllocator
	for k, v := range SqlInfoResultMap() {
		s.RegisterSqlInfo(flightsql.SqlInfo(k), v)
	}
}

// frontendConnection marks the request before selecting a physical
// connection, then initializes that same connection. A failed initializer
// never leaves the connection available to another request.
func frontendConnection(ctx context.Context, db *sql.DB, initializer func(context.Context, *sql.Conn) error) (context.Context, *sql.Conn, error) {
	ctx = mycontext.WithFrontendQuery(ctx)
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	if initializer != nil {
		if err := initializer(ctx, conn); err != nil {
			_ = conn.Close()
			return nil, nil, err
		}
	}
	return ctx, conn, nil
}

func (s *SQLiteFlightSQLServer) frontendConnection(ctx context.Context) (context.Context, *sql.Conn, error) {
	return frontendConnection(ctx, s.db, s.initializeStorage)
}

func (s *SQLiteFlightSQLServer) flightInfoForCommand(desc *flight.FlightDescriptor, schema *arrow.Schema) *flight.FlightInfo {
	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema, s.Alloc),
		TotalRecords:     -1,
		TotalBytes:       -1,
	}
}

func (s *SQLiteFlightSQLServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	query, txnid := cmd.GetQuery(), cmd.GetTransactionId()
	if err := catalog.RejectSensitiveSQL(query); err != nil {
		return nil, err
	}
	tkt, err := encodeTransactionQuery(query, txnid)
	if err != nil {
		return nil, err
	}

	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: tkt}}},
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (s *SQLiteFlightSQLServer) DoGetStatement(ctx context.Context, cmd flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	txnid, query, err := decodeTransactionQuery(cmd.GetStatementHandle())
	if err != nil {
		return nil, nil, err
	}
	if txnid != "" {
		return nil, nil, fmt.Errorf("transactions not yet supported with DuckDB")
	}
	if s.provider != nil {
		return s.managedStatelessQuery(ctx, query)
	}

	// var db dbQueryCtx = s.db
	// if txnid != "" {
	// 	tx, loaded := s.openTransactions.Load(txnid)
	// 	if !loaded {
	// 		return nil, nil, fmt.Errorf("%w: invalid transaction id specified: %s", arrow.ErrInvalid, txnid)
	// 	}
	// 	db = tx.(*sql.Tx)
	// }

	return doGetQueryWithInitializer(ctx, s.initializeStorage, s.db, query, nil)
}

func (s *SQLiteFlightSQLServer) GetFlightInfoCatalogs(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.Catalogs), nil
}

func (s *SQLiteFlightSQLServer) DoGetCatalogs(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	// https://www.sqlite.org/cli.html
	// > The ".databases" command shows a list of all databases open
	// > in the current connection. There will always be at least
	// > 2. The first one is "main", the original database opened. The
	// > second is "temp", the database used for temporary tables.
	// For our purposes, return only "main" and ignore other databases.

	schema := schema_ref.Catalogs

	catalogs, _, err := array.FromJSON(s.Alloc, arrow.BinaryTypes.String, strings.NewReader(`["main"]`))
	if err != nil {
		return nil, nil, err
	}
	defer catalogs.Release()

	batch := array.NewRecord(schema, []arrow.Array{catalogs}, 1)

	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: batch}
	close(ch)

	return schema, ch, nil
}

func (s *SQLiteFlightSQLServer) GetFlightInfoSchemas(_ context.Context, cmd flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.DBSchemas), nil
}

func (s *SQLiteFlightSQLServer) DoGetDBSchemas(_ context.Context, cmd flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	// SQLite doesn't support schemas, so pretend we have a single unnamed schema.
	schema := schema_ref.DBSchemas

	ch := make(chan flight.StreamChunk, 1)

	if cmd.GetDBSchemaFilterPattern() == nil || *cmd.GetDBSchemaFilterPattern() == "" {
		catalogs, _, err := array.FromJSON(s.Alloc, arrow.BinaryTypes.String, strings.NewReader(`["main"]`))
		if err != nil {
			return nil, nil, err
		}
		defer catalogs.Release()

		dbSchemas, _, err := array.FromJSON(s.Alloc, arrow.BinaryTypes.String, strings.NewReader(`[""]`))
		if err != nil {
			return nil, nil, err
		}
		defer dbSchemas.Release()

		batch := array.NewRecord(schema, []arrow.Array{catalogs, dbSchemas}, 1)
		ch <- flight.StreamChunk{Data: batch}
	}

	close(ch)

	return schema, ch, nil
}

func (s *SQLiteFlightSQLServer) GetFlightInfoTables(_ context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	schema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		schema = schema_ref.TablesWithIncludedSchema
	}
	return s.flightInfoForCommand(desc, schema), nil
}

func (s *SQLiteFlightSQLServer) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	query := prepareQueryForGetTables(cmd)
	if err := catalog.RejectSensitiveSQL(query); err != nil {
		return nil, nil, err
	}
	if s.provider != nil {
		return s.managedTablesQuery(ctx, query, cmd.GetIncludeSchema())
	}
	ctx, conn, err := s.frontendConnection(ctx)
	if err != nil {
		return nil, nil, err
	}
	terminal := func() { _ = conn.Close() }
	if cmd.GetIncludeSchema() {
		return startMaterializedTablesQuery(ctx, s.Alloc, conn, query, terminal)
	}
	return startArrowQuery(ctx, conn, query, [][]interface{}{nil}, terminal)
}

func (s *SQLiteFlightSQLServer) GetFlightInfoXdbcTypeInfo(_ context.Context, _ flightsql.GetXdbcTypeInfo, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.XdbcTypeInfo), nil
}

func (s *SQLiteFlightSQLServer) DoGetXdbcTypeInfo(_ context.Context, cmd flightsql.GetXdbcTypeInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	var batch arrow.Record
	if cmd.GetDataType() == nil {
		batch = GetTypeInfoResult(s.Alloc)
	} else {
		batch = GetFilteredTypeInfoResult(s.Alloc, *cmd.GetDataType())
	}

	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: batch}
	close(ch)
	return batch.Schema(), ch, nil
}

func (s *SQLiteFlightSQLServer) GetFlightInfoTableTypes(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.TableTypes), nil
}

func (s *SQLiteFlightSQLServer) DoGetTableTypes(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	query := "SELECT DISTINCT type AS table_type FROM sqlite_master"
	if s.provider != nil {
		return s.managedStatelessQuery(ctx, query)
	}
	return doGetQueryWithInitializer(ctx, s.initializeStorage, s.db, query, schema_ref.TableTypes)
}

func (s *SQLiteFlightSQLServer) DoPutCommandStatementUpdate(ctx context.Context, cmd flightsql.StatementUpdate) (int64, error) {
	if err := catalog.RejectSensitiveSQL(cmd.GetQuery()); err != nil {
		return 0, err
	}
	if s.provider != nil {
		return s.managedStatementUpdate(ctx, cmd)
	}
	ctx = mycontext.WithFrontendQuery(ctx)
	var (
		res sql.Result
		err error
	)

	if len(cmd.GetTransactionId()) > 0 {
		tx, loaded := s.openTransactions.Load(string(cmd.GetTransactionId()))
		if !loaded {
			return -1, status.Error(codes.InvalidArgument, "invalid transaction handle provided")
		}

		state, ok := tx.(*transactionState)
		if !ok || state == nil || state.tx == nil {
			return -1, status.Error(codes.InvalidArgument, "invalid transaction handle provided")
		}
		res, err = state.tx.ExecContext(ctx, cmd.GetQuery())
	} else {
		var conn *sql.Conn
		ctx, conn, err = s.frontendConnection(ctx)
		if err != nil {
			return 0, err
		}
		defer conn.Close()
		res, err = conn.ExecContext(ctx, cmd.GetQuery())
	}

	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *SQLiteFlightSQLServer) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (result flightsql.ActionCreatePreparedStatementResult, err error) {
	var stmt *sql.Stmt
	query := req.GetQuery()
	if err := catalog.RejectSensitiveSQL(query); err != nil {
		return result, err
	}
	if s.provider != nil {
		return s.createManagedPreparedStatement(ctx, req)
	}
	ctx = mycontext.WithFrontendQuery(ctx)
	var stmtConn *sql.Conn

	if len(req.GetTransactionId()) > 0 {
		tx, loaded := s.openTransactions.Load(string(req.GetTransactionId()))
		if !loaded {
			return result, status.Error(codes.InvalidArgument, "invalid transaction handle provided")
		}
		state, ok := tx.(*transactionState)
		if !ok || state == nil || state.tx == nil {
			return result, status.Error(codes.InvalidArgument, "invalid transaction handle provided")
		}
		stmt, err = state.tx.PrepareContext(ctx, req.GetQuery())
		stmtConn = state.conn
	} else {
		ctx, stmtConn, err = s.frontendConnection(ctx)
		if err == nil {
			stmt, err = stmtConn.PrepareContext(ctx, req.GetQuery())
		}
	}

	if err != nil {
		// A transaction owns its connection and releases it only when the
		// transaction ends. A standalone prepared statement checked out the
		// connection itself, so only that path may close it on prepare failure.
		if stmtConn != nil && len(req.GetTransactionId()) == 0 {
			_ = stmtConn.Close()
		}
		return result, err
	}

	handle := genRandomString()
	s.prepared.Store(string(handle), &Statement{
		stmt:     stmt,
		query:    query,
		conn:     stmtConn,
		ownsConn: len(req.GetTransactionId()) == 0,
	})

	result.Handle = handle
	// no way to get the dataset or parameter schemas from sql.DB
	return
}

func (s *SQLiteFlightSQLServer) ClosePreparedStatement(ctx context.Context, request flightsql.ActionClosePreparedStatementRequest) error {
	handle := request.GetPreparedStatementHandle()
	if val, loaded := s.prepared.LoadAndDelete(string(handle)); loaded {
		stmt, ok := val.(*Statement)
		if !ok || stmt == nil {
			return status.Error(codes.InvalidArgument, "prepared statement not found")
		}
		if stmt.external != nil && stmt.transaction == nil {
			return errors.Join(stmt.external.Close(), stmt.closePrepared())
		}
		stmtErr := stmt.closePrepared()
		if stmt.conn == nil || !stmt.ownsConn {
			return stmtErr
		}
		connErr := stmt.conn.Close()
		return errors.Join(stmtErr, connErr)
	}

	// ADBC/Python cursors close prepared statements from __exit__/__del__
	// after execute (and sometimes after commit already dropped the handle).
	// Close is idempotent.
	return nil
}

func (s *SQLiteFlightSQLServer) GetFlightInfoPreparedStatement(_ context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	value, ok := s.prepared.Load(string(cmd.GetPreparedStatementHandle()))
	stmt, valid := value.(*Statement)
	if !ok || !valid || stmt == nil || stmt.closed.Load() {
		return nil, status.Error(codes.InvalidArgument, "prepared statement not found")
	}

	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

type dbQueryCtx interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func doGetQuery(ctx context.Context, db *sql.DB, query string, schema *arrow.Schema, args ...interface{}) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return doGetQueryWithInitializer(ctx, nil, db, query, schema, args...)
}

func doGetQueryWithInitializer(ctx context.Context, initializer func(context.Context, *sql.Conn) error, db *sql.DB, query string, schema *arrow.Schema, args ...interface{}) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	if err := catalog.RejectSensitiveSQL(query); err != nil {
		return nil, nil, err
	}
	ctx, conn, err := frontendConnection(ctx, db, initializer)
	if err != nil {
		return nil, nil, err
	}
	return startArrowQuery(ctx, conn, query, [][]interface{}{args}, func() { _ = conn.Close() })
}

func (s *SQLiteFlightSQLServer) DoGetPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery) (schema *arrow.Schema, out <-chan flight.StreamChunk, err error) {
	if s.provider != nil {
		return s.managedPreparedQuery(ctx, cmd)
	}
	val, ok := s.prepared.Load(string(cmd.GetPreparedStatementHandle()))

	if !ok {
		return nil, nil, status.Error(codes.InvalidArgument, "prepared statement not found")
	}
	stmt, ok := val.(*Statement)
	if !ok || stmt == nil || stmt.closed.Load() {
		return nil, nil, status.Error(codes.InvalidArgument, "prepared statement not found")
	}
	if err := catalog.RejectSensitiveSQL(stmt.query); err != nil {
		return nil, nil, err
	}
	ctx = mycontext.WithFrontendQuery(ctx)
	// The prepared statement was bound either to its standalone checked-out
	// connection or to the transaction connection at creation time. Reusing a
	// newly acquired *sql.Conn here would lose both the prepared state and the
	// transaction, and could bypass the per-physical initializer.
	conn := stmt.conn
	if conn == nil {
		return nil, nil, fmt.Errorf("prepared statement has no owning connection")
	}

	stmt.mu.Lock()
	query := stmt.query
	argSets := make([][]interface{}, len(stmt.params))
	for i := range stmt.params {
		argSets[i] = append([]interface{}(nil), stmt.params[i]...)
	}
	stmt.mu.Unlock()
	if len(argSets) == 0 {
		argSets = [][]interface{}{nil}
	}
	return startArrowQuery(ctx, conn, query, argSets, func() {})
}

func scalarToIFace(s scalar.Scalar) (interface{}, error) {
	if !s.IsValid() {
		return nil, nil
	}

	switch val := s.(type) {
	case *scalar.Int8:
		return val.Value, nil
	case *scalar.Uint8:
		return val.Value, nil
	case *scalar.Int32:
		return val.Value, nil
	case *scalar.Int64:
		return val.Value, nil
	case *scalar.Float32:
		return val.Value, nil
	case *scalar.Float64:
		return val.Value, nil
	case *scalar.String:
		return string(val.Value.Bytes()), nil
	case *scalar.Binary:
		return val.Value.Bytes(), nil
	case scalar.DateScalar:
		return val.ToTime(), nil
	case scalar.TimeScalar:
		return val.ToTime(), nil
	case *scalar.DenseUnion:
		return scalarToIFace(val.Value)
	default:
		return nil, fmt.Errorf("unsupported type: %s", val)
	}
}

func getParamsForStatement(rdr flight.MessageReader) (params [][]interface{}, err error) {
	params = make([][]interface{}, 0)
	for rdr.Next() {
		rec := rdr.Record()

		nrows := int(rec.NumRows())
		ncols := int(rec.NumCols())

		for i := 0; i < nrows; i++ {
			invokeParams := make([]interface{}, ncols)
			for c := 0; c < ncols; c++ {
				col := rec.Column(c)
				sc, err := scalar.GetScalar(col, i)
				if err != nil {
					return nil, err
				}
				if r, ok := sc.(scalar.Releasable); ok {
					r.Release()
				}

				invokeParams[c], err = scalarToIFace(sc)
				if err != nil {
					return nil, err
				}
			}
			params = append(params, invokeParams)
		}
	}

	return params, rdr.Err()
}

func (s *SQLiteFlightSQLServer) DoPutPreparedStatementQuery(_ context.Context, cmd flightsql.PreparedStatementQuery, rdr flight.MessageReader, _ flight.MetadataWriter) ([]byte, error) {
	val, ok := s.prepared.Load(string(cmd.GetPreparedStatementHandle()))
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "prepared statement not found")
	}

	stmt, ok := val.(*Statement)
	if !ok || stmt == nil || stmt.closed.Load() {
		return nil, status.Error(codes.InvalidArgument, "prepared statement not found")
	}
	if err := catalog.RejectSensitiveSQL(stmt.query); err != nil {
		return nil, err
	}
	args, err := getParamsForStatement(rdr)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error gathering parameters for prepared statement query: %s", err.Error())
	}

	stmt.mu.Lock()
	defer stmt.mu.Unlock()
	if stmt.closed.Load() || (stmt.transaction != nil && stmt.transaction.closed.Load()) {
		return nil, status.Error(codes.InvalidArgument, "prepared statement not found")
	}
	stmt.params = args
	return cmd.GetPreparedStatementHandle(), nil
}

func (s *SQLiteFlightSQLServer) DoPutPreparedStatementUpdate(ctx context.Context, cmd flightsql.PreparedStatementUpdate, rdr flight.MessageReader) (int64, error) {
	val, ok := s.prepared.Load(string(cmd.GetPreparedStatementHandle()))
	if !ok {
		return 0, status.Error(codes.InvalidArgument, "prepared statement not found")
	}

	stmt, ok := val.(*Statement)
	if !ok || stmt == nil || stmt.closed.Load() {
		return 0, status.Error(codes.InvalidArgument, "prepared statement not found")
	}
	if err := catalog.RejectSensitiveSQL(stmt.query); err != nil {
		return 0, err
	}
	ctx = mycontext.WithFrontendQuery(ctx)
	args, err := getParamsForStatement(rdr)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "error gathering parameters for prepared statement: %s", err.Error())
	}
	if s.provider != nil {
		affected, err := s.managedPreparedUpdate(ctx, cmd, args)
		if err != nil && strings.Contains(err.Error(), "no such table") {
			return affected, status.Error(codes.NotFound, err.Error())
		}
		return affected, err
	}

	stmt.mu.Lock()
	defer stmt.mu.Unlock()
	if stmt.closed.Load() {
		return 0, status.Error(codes.InvalidArgument, "prepared statement not found")
	}
	if len(args) == 0 {
		result, err := stmt.stmt.ExecContext(ctx)
		if err != nil {
			if strings.Contains(err.Error(), "no such table") {
				return 0, status.Error(codes.NotFound, err.Error())
			}
			return 0, err
		}

		return result.RowsAffected()
	}

	var totalAffected int64
	for _, p := range args {
		result, err := stmt.stmt.ExecContext(ctx, p...)
		if err != nil {
			if strings.Contains(err.Error(), "no such table") {
				return totalAffected, status.Error(codes.NotFound, err.Error())
			}
			return totalAffected, err
		}

		n, err := result.RowsAffected()
		if err != nil {
			return totalAffected, err
		}
		totalAffected += n
	}

	return totalAffected, nil
}

func (s *SQLiteFlightSQLServer) GetFlightInfoPrimaryKeys(_ context.Context, cmd flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.PrimaryKeys), nil
}

func (s *SQLiteFlightSQLServer) DoGetPrimaryKeys(ctx context.Context, cmd flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	// the field key_name cannot be recovered by sqlite so it is
	// being set to null following the same pattern for catalog name and schema_name
	var b strings.Builder

	b.WriteString(`
	SELECT null AS catalog_name, null AS schema_name, table_name, name AS column_name, pk AS key_sequence, null as key_name
	FROM pragma_table_info(table_name)
		JOIN (SELECT null AS catalog_name, null AS schema_name, name AS table_name, type AS table_type
			FROM sqlite_master) where 1=1 AND pk !=0`)

	if cmd.Catalog != nil {
		fmt.Fprintf(&b, " and catalog_name LIKE '%s'", *cmd.Catalog)
	}
	if cmd.DBSchema != nil {
		fmt.Fprintf(&b, " and schema_name LIKE '%s'", *cmd.DBSchema)
	}

	fmt.Fprintf(&b, " and table_name LIKE '%s'", cmd.Table)

	if s.provider != nil {
		return s.managedStatelessQuery(ctx, b.String())
	}
	return doGetQueryWithInitializer(ctx, s.initializeStorage, s.db, b.String(), schema_ref.PrimaryKeys)
}

func (s *SQLiteFlightSQLServer) GetFlightInfoImportedKeys(_ context.Context, _ flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.ImportedKeys), nil
}

func (s *SQLiteFlightSQLServer) DoGetImportedKeys(ctx context.Context, ref flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	filter := "fk_table_name = '" + ref.Table + "'"
	if ref.Catalog != nil {
		filter += " AND fk_catalog_name = '" + *ref.Catalog + "'"
	}
	if ref.DBSchema != nil {
		filter += " AND fk_schema_name = '" + *ref.DBSchema + "'"
	}
	query := prepareQueryForGetKeys(filter)
	if s.provider != nil {
		return s.managedStatelessQuery(ctx, query)
	}
	return doGetQueryWithInitializer(ctx, s.initializeStorage, s.db, query, schema_ref.ImportedKeys)
}

func (s *SQLiteFlightSQLServer) GetFlightInfoExportedKeys(_ context.Context, _ flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.ExportedKeys), nil
}

func (s *SQLiteFlightSQLServer) DoGetExportedKeys(ctx context.Context, ref flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	filter := "pk_table_name = '" + ref.Table + "'"
	if ref.Catalog != nil {
		filter += " AND pk_catalog_name = '" + *ref.Catalog + "'"
	}
	if ref.DBSchema != nil {
		filter += " AND pk_schema_name = '" + *ref.DBSchema + "'"
	}
	query := prepareQueryForGetKeys(filter)
	if s.provider != nil {
		return s.managedStatelessQuery(ctx, query)
	}
	return doGetQueryWithInitializer(ctx, s.initializeStorage, s.db, query, schema_ref.ExportedKeys)
}

func (s *SQLiteFlightSQLServer) GetFlightInfoCrossReference(_ context.Context, _ flightsql.CrossTableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.CrossReference), nil
}

func (s *SQLiteFlightSQLServer) DoGetCrossReference(ctx context.Context, cmd flightsql.CrossTableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	pkref := cmd.PKRef
	filter := "pk_table_name = '" + pkref.Table + "'"
	if pkref.Catalog != nil {
		filter += " AND pk_catalog_name = '" + *pkref.Catalog + "'"
	}
	if pkref.DBSchema != nil {
		filter += " AND pk_schema_name = '" + *pkref.DBSchema + "'"
	}

	fkref := cmd.FKRef
	filter += " AND fk_table_name = '" + fkref.Table + "'"
	if fkref.Catalog != nil {
		filter += " AND fk_catalog_name = '" + *fkref.Catalog + "'"
	}
	if fkref.DBSchema != nil {
		filter += " AND fk_schema_name = '" + *fkref.DBSchema + "'"
	}
	query := prepareQueryForGetKeys(filter)
	if s.provider != nil {
		return s.managedStatelessQuery(ctx, query)
	}
	return doGetQueryWithInitializer(ctx, s.initializeStorage, s.db, query, schema_ref.ExportedKeys)
}

func (s *SQLiteFlightSQLServer) BeginTransaction(ctx context.Context, req flightsql.ActionBeginTransactionRequest) (id []byte, err error) {
	if s.provider != nil {
		return s.beginManagedTransaction(ctx)
	}
	ctx, conn, err := s.frontendConnection(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := conn.BeginTx(context.WithoutCancel(ctx), nil)
	if err != nil {
		_ = conn.Close()
		return nil, status.Errorf(codes.Internal, "failed to begin transaction: %s", err.Error())
	}

	handle := genRandomString()
	s.openTransactions.Store(string(handle), &transactionState{tx: tx, conn: conn})
	return handle, nil
}

func (s *SQLiteFlightSQLServer) EndTransaction(ctx context.Context, req flightsql.ActionEndTransactionRequest) error {
	if req.GetAction() == flightsql.EndTransactionUnspecified {
		return status.Error(codes.InvalidArgument, "must specify Commit or Rollback to end transaction")
	}
	if s.provider != nil {
		if err := s.endManagedTransaction(ctx, req); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		return nil
	}

	handle := string(req.GetTransactionId())
	if tx, loaded := s.openTransactions.LoadAndDelete(handle); loaded {
		state, ok := tx.(*transactionState)
		if !ok || state == nil || state.tx == nil {
			return status.Error(codes.InvalidArgument, "transaction id not found")
		}
		txn := state.tx
		var txErr error
		switch req.GetAction() {
		case flightsql.EndTransactionCommit:
			txErr = txn.Commit()
		case flightsql.EndTransactionRollback:
			txErr = txn.Rollback()
		}
		connErr := state.conn.Close()
		if txErr != nil {
			action := "commit"
			if req.GetAction() == flightsql.EndTransactionRollback {
				action = "rollback"
			}
			return status.Error(codes.Internal, fmt.Sprintf("failed to %s transaction: %s", action, txErr))
		}
		if connErr != nil && !errors.Is(connErr, sql.ErrConnDone) {
			return status.Error(codes.Internal, "failed to close transaction connection: "+connErr.Error())
		}
		return nil
	}

	return status.Error(codes.InvalidArgument, "transaction id not found")
}
