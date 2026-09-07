// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgserver

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime/trace"
	"strings"
	"sync"
	"time"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/apecloud/myduckserver/pgtypes"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sirupsen/logrus"
)

var printErrorStackTraces = false

const PrintErrorStackTracesEnvKey = "MYDUCK_PRINT_ERROR_STACK_TRACES"

func init() {
	if _, ok := os.LookupEnv(PrintErrorStackTracesEnvKey); ok {
		printErrorStackTraces = true
	}
}

// Result represents a query result.
type Result struct {
	Fields       []pgproto3.FieldDescription `json:"fields"`
	Rows         []Row                       `json:"rows"`
	RowsAffected uint64                      `json:"rows_affected"`
}

// Row represents a single row value in bytes format.
// |val| represents array of a single row elements,
// which each element value is in byte array format.
type Row struct {
	val [][]byte
}

const rowsBatch = 128

type QueryMode bool

const (
	SimpleQueryMode   QueryMode = false
	ExtendedQueryMode QueryMode = true
)

// DuckHandler is a handler uses DuckDB and the SQLe engine directly
// running Postgres specific queries.
type DuckHandler struct {
	e                 *sqle.Engine
	sm                *server.SessionManager
	readTimeout       time.Duration
	encodeLoggedQuery bool
	connectionHandler *ConnectionHandler
}

func (h *DuckHandler) SetConnectionHandler(handler *ConnectionHandler) {
	h.connectionHandler = handler
}

var _ Handler = &DuckHandler{}

func (h *DuckHandler) GetCatalogProvider() *catalog.DatabaseProvider {
	provider, ok := h.e.Analyzer.Catalog.DbProvider.(*catalog.DatabaseProvider)
	if !ok {
		return nil
	}
	return provider
}

// ComBind implements the Handler interface.
func (h *DuckHandler) ComBind(ctx context.Context, c *mysql.Conn, prepared PreparedStatementData, bindVars []any) ([]pgproto3.FieldDescription, error) {
	if err := catalog.RejectSensitiveSQL(prepared.Statement.String); err != nil {
		return nil, err
	}
	if auditQuery := prepared.Statement.QueryForAudit(); auditQuery != prepared.Statement.String {
		if err := catalog.RejectSensitiveSQL(auditQuery); err != nil {
			return nil, err
		}
	}
	vars := make([]driver.NamedValue, len(bindVars))
	for i, v := range bindVars {
		vars[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}

	// PostgreSQL prepared statements retain only SQL and metadata between Parse
	// and Execute. The raw DuckDB statement used during Parse is closed before
	// returning, so binding is validated at Execute on the active executor.
	if prepared.Stmt == nil {
		return prepared.ReturnFields, nil
	}
	err := prepared.Stmt.Bind(vars)
	if err != nil {
		return nil, err
	}

	return prepared.ReturnFields, nil
}

// ComExecuteBound implements the Handler interface.
func (h *DuckHandler) ComExecuteBound(ctx context.Context, conn *mysql.Conn, portal PortalData, callback func(*Result) error) error {
	if err := catalog.RejectSensitiveSQL(portal.Statement.String); err != nil {
		return err
	}
	if auditQuery := portal.Statement.QueryForAudit(); auditQuery != portal.Statement.String {
		if err := catalog.RejectSensitiveSQL(auditQuery); err != nil {
			return err
		}
	}
	if err := h.rejectReadOnly(portal.Statement); err != nil {
		return err
	}
	err := h.doQuery(ctx, conn, portal.Statement.String, portal.Statement.QueryForAudit(), portal.Statement.AST, portal.Stmt, portal.Vars, portal.ResultFormatCodes, ExtendedQueryMode, h.executeBoundPlan, callback)
	if err != nil {
		err = sql.CastSQLError(err)
	}

	return err
}

// ComPrepareParsed implements the Handler interface.
func (h *DuckHandler) ComPrepareParsed(ctx context.Context, c *mysql.Conn, query string, parsed tree.Statement) (*duckdb.Stmt, []uint32, []pgproto3.FieldDescription, error) {
	if err := catalog.RejectSensitiveSQL(query); err != nil {
		return nil, nil, nil, err
	}
	if err := h.rejectReadOnly(ConvertedStatement{String: query, AST: parsed}); err != nil {
		return nil, nil, nil, err
	}
	// DuckDB's official Go binding exposes prepared statement parameter types but not result types.
	// 1. For SELECT statements, we will supply all NULL values as parameters
	//    to execute the query with a LIMIT 0 to get the result types.
	// 2. For SHOW/CALL/PRAGMA statements, we will just execute the query and get the result types
	//    because they usually don't have parameters and are efficient to execute.
	// 3. For other statements (DDLs and DMLs), we just return the "affected rows" field.
	sqlCtx, err := h.sm.NewContextWithQuery(ctx, c, query)
	if err != nil {
		return nil, nil, nil, err
	}

	// Transaction control is executed by the protocol handler rather than
	// DuckDB's prepared-statement machinery. Keeping it unprepared also avoids
	// probing a second connection while a session transaction is active.
	switch parsed.(type) {
	case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction:
		return nil, nil, nil, nil
	}

	// Admit the operation before retaining the executor/physical owner snapshot.
	// The release is deferred because all metadata probes and result-row schema
	// reads complete before Parse returns.
	execer, conn, tx, release, err := h.getPostgresExecutionSnapshotLease(sqlCtx, parsed)
	if err != nil {
		return nil, nil, nil, err
	}
	defer release()

	prepareQuery, err := postgresCreateTableQueryForPrepare(query, parsed)
	if err != nil {
		return nil, nil, nil, err
	}
	// Resolve object-table relations against the same lifecycle snapshot used
	// for raw preparation and metadata probing. The physical relation can be
	// visible only after the session's DuckLake connection is initialized; doing
	// this before the probe keeps prepared INSERT ... RETURNING metadata aligned
	// with the relation executed later by the bound plan.
	prepareQuery, err = h.rewritePostgresObjectRelationsWithSnapshot(sqlCtx, prepareQuery, execer, conn)
	if err != nil {
		return nil, nil, nil, err
	}

	var (
		stmtType   duckdb.StmtType
		paramTypes []duckdb.Type
	)
	if tx != nil {
		// Parse runs after the protocol has opened the implicit transaction. A
		// Conn.Raw probe here would issue driver calls beside that *sql.Tx and
		// can observe a different transaction state (or deadlock the driver).
		// Use the parsed AST/tag and a parser/lexical placeholder count instead;
		// unknown parameter OIDs are represented by TYPE_INVALID below.
		stmtType = postgresDuckDBStatementType(parsed, prepareQuery)
		paramTypes = make([]duckdb.Type, postgresPlaceholderCount(query))
	} else {
		// There is no transaction owner, so a short-lived raw probe is safe and
		// preserves DuckDB's exact statement/parameter type information. The
		// retained snapshot itself prevents a transaction or replacement
		// connection from appearing while the probe is in flight.
		stmtType, paramTypes, err = prepareDuckDBStatementMetadata(sqlCtx, conn, prepareQuery)
	}
	if err != nil {
		logrus.WithField("query", catalog.RedactSensitiveSQL(query)).Errorf("unable to prepare query: %s", err.Error())
		return nil, nil, nil, err
	}
	paramOIDs := make([]uint32, len(paramTypes))
	for i, t := range paramTypes {
		paramOIDs[i] = pgtypes.DuckdbTypeToPostgresOID[t]
		// A transaction-safe Parse path cannot call DuckDB's raw
		// PrepareContext beside the active *sql.Tx, so an untyped
		// placeholder has no driver-derived OID. PostgreSQL's protocol uses
		// OID zero to mean "unspecified"; pgx then selects an encoder from
		// the concrete Go value (rather than rejecting it as UnknownOID or
		// assuming the value is a string). The server-side bind decoder
		// intentionally converts that wire value to text before DuckDB's
		// statement-context coercion.
		if paramOIDs[i] == pgtype.UnknownOID {
			paramOIDs[i] = 0
		}
	}

	var (
		fields []pgproto3.FieldDescription
		rows   *stdsql.Rows
	)
	if insert, ok := postgresInsertReturningRows(parsed); ok {
		metadataQuery := postgresInsertReturningSchemaQuery(insert)
		metadataQuery, routeErr := h.rewritePostgresObjectRelationsWithSnapshot(sqlCtx, metadataQuery, execer, conn)
		if routeErr != nil {
			return nil, nil, nil, routeErr
		}
		schema, schemaErr := inferPostgresInsertReturningSchema(sqlCtx, metadataQuery, execer)
		if schemaErr != nil {
			return nil, nil, nil, schemaErr
		}
		fields = schemaToFieldDescriptions(sqlCtx, schema, nil, ExtendedQueryMode)
		return nil, paramOIDs, fields, nil
	}
	switch stmtType {
	case duckdb.STATEMENT_TYPE_SELECT,
		duckdb.STATEMENT_TYPE_RELATION,
		duckdb.STATEMENT_TYPE_CALL,
		duckdb.STATEMENT_TYPE_PRAGMA,
		duckdb.STATEMENT_TYPE_EXPLAIN:

		// Execute the query with all NULL values as parameters to get the result types.
		// Use the rewritten preparation query for both the LIMIT 0 probe and
		// the later Execute path. Object-table relations only exist under
		// their durable DuckLake name; probing the original logical relation
		// would fail during Parse even though Execute correctly routes it.
		query := prepareQuery
		if stmtType == duckdb.STATEMENT_TYPE_SELECT ||
			stmtType == duckdb.STATEMENT_TYPE_RELATION {
			// Add LIMIT 0 to avoid executing the actual query.
			query = "SELECT * FROM (" + sql.RemoveSpaceAndDelimiter(query, ';') + ") LIMIT 0"
		}
		params := make([]any, len(paramTypes)) // all nil
		rows, err = execer.QueryContext(sqlCtx, query, params...)
		if err != nil {
			break
		}
		defer rows.Close()
		schema, err := pgtypes.InferSchema(rows)
		if err != nil {
			break
		}
		fields = schemaToFieldDescriptions(sqlCtx, schema, nil, ExtendedQueryMode)
	default:
		// For other statements, we just return the "affected rows" field.
		fields = []pgproto3.FieldDescription{
			{
				Name:         []byte("Rows"),
				DataTypeOID:  pgtype.Int4OID,
				DataTypeSize: 4,
			},
		}
	}
	if err != nil {
		return nil, nil, nil, err
	}

	return nil, paramOIDs, fields, nil
}

// ComQuery implements the Handler interface.
func (h *DuckHandler) ComQuery(ctx context.Context, c *mysql.Conn, query string, auditQuery string, parsed tree.Statement, callback func(*Result) error) error {
	if err := catalog.RejectSensitiveSQL(query); err != nil {
		return err
	}
	if auditQuery != "" && auditQuery != query {
		if err := catalog.RejectSensitiveSQL(auditQuery); err != nil {
			return err
		}
	}
	if err := h.rejectReadOnly(ConvertedStatement{String: query, AST: parsed}); err != nil {
		return err
	}
	err := h.doQuery(ctx, c, query, auditQuery, parsed, nil, nil, nil, SimpleQueryMode, h.executeQuery, callback)
	if err != nil {
		err = sql.CastSQLError(err)
	}
	return err
}

func (h *DuckHandler) rejectReadOnly(statement ConvertedStatement) error {
	if h.connectionHandler == nil {
		return nil
	}
	return h.connectionHandler.rejectReadOnly(statement)
}

// isPostgresTransactionControl identifies statements whose lifecycle is owned
// by the protocol transaction handler. They must not acquire a DuckLake
// operation or execution snapshot: COMMIT/ROLLBACK in particular must remain
// able to recover a failed transaction while the ordinary execution path is
// unavailable.
func isPostgresTransactionControl(statement tree.Statement) bool {
	switch statement.(type) {
	case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction:
		return true
	default:
		return false
	}
}

// postgresDuckLakeOperationProvider is the legacy admission surface retained
// for older provider/test doubles. Production providers also implement the
// owner-aware extension below.
type postgresDuckLakeOperationProvider interface {
	BeginDuckLakeOperation(context.Context) func()
}

type postgresDuckLakeOperationOwnerProvider interface {
	BeginDuckLakeOperationForOwner(context.Context, any) func()
}

// Error-bearing admission surfaces are implemented by the production
// provider. Legacy provider/test doubles continue to use the closure-only
// interfaces above; those adapters cannot report a poisoned generation.
type postgresDuckLakeOperationProviderWithError interface {
	BeginDuckLakeOperationWithError(context.Context) (func(), error)
}

type postgresDuckLakeOperationOwnerProviderWithError interface {
	BeginDuckLakeOperationForOwnerWithError(context.Context, any) (func(), error)
}

// beginPostgresDuckLakeOperationForProvider admits one frontend statement
// before it captures a pool execution snapshot. Passing the physical owner is
// important for PostgreSQL: its protocol transaction controls keep a raw
// *database/sql.Tx in the pool/handler rather than a GMS sql.Transaction, so
// the legacy context-derived owner would be anonymous and rollback cleanup
// would wait on its own operation lease.
func beginPostgresDuckLakeOperationForProvider(
	ctx *sql.Context,
	statement tree.Statement,
	provider postgresDuckLakeOperationProvider,
	owner any,
) func() {
	release, _ := beginPostgresDuckLakeOperationForProviderWithError(ctx, statement, provider, owner)
	return release
}

func beginPostgresDuckLakeOperationForProviderWithError(
	ctx *sql.Context,
	statement tree.Statement,
	provider postgresDuckLakeOperationProvider,
	owner any,
) (func(), error) {
	if ctx == nil || isPostgresTransactionControl(statement) ||
		mycontext.QueryOrigin(ctx) != mycontext.FrontendQueryOrigin || provider == nil {
		return func() {}, nil
	}
	if ownerProvider, ok := provider.(postgresDuckLakeOperationOwnerProviderWithError); ok {
		return ownerProvider.BeginDuckLakeOperationForOwnerWithError(ctx, owner)
	}
	if providerWithError, ok := provider.(postgresDuckLakeOperationProviderWithError); ok {
		return providerWithError.BeginDuckLakeOperationWithError(ctx)
	}
	if ownerProvider, ok := provider.(postgresDuckLakeOperationOwnerProvider); ok {
		return ownerProvider.BeginDuckLakeOperationForOwner(ctx, owner), nil
	}
	return provider.BeginDuckLakeOperation(ctx), nil
}

// postgresDuckLakeOperationOwner resolves the same physical transaction
// identity that PostgreSQL rollback finalization will pass to the provider
// barrier. The handler's implicit marker is preferred because it is already
// captured for the current protocol scope; the atomic session binding covers
// explicit transactions and minimal handler contexts. A GMS logical
// transaction remains a final fallback for test/session doubles that do not
// expose a physical binding.
func postgresDuckLakeOperationOwner(ctx *sql.Context, handler *DuckHandler) any {
	if handler != nil && handler.connectionHandler != nil && handler.connectionHandler.implicitTx != nil {
		return handler.connectionHandler.implicitTx
	}
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	if _, ok := ctx.Session.(adapter.ConnectionHolder); ok {
		if _, tx := adapter.TryGetTxnBinding(ctx); tx != nil {
			return tx
		}
	}
	return ctx.GetTransaction()
}

// beginPostgresDuckLakeOperationForOwner is the explicit-owner entry point for
// asynchronous protocol paths that already captured a transaction identity.
// Keeping it separate lets COPY/extended callers pass their retained physical
// owner without re-reading a mutable session binding.
func (h *DuckHandler) beginPostgresDuckLakeOperationForOwner(
	ctx *sql.Context,
	statement tree.Statement,
	owner any,
) func() {
	release, _ := h.beginPostgresDuckLakeOperationForOwnerWithError(ctx, statement, owner)
	return release
}

func (h *DuckHandler) beginPostgresDuckLakeOperationForOwnerWithError(
	ctx *sql.Context,
	statement tree.Statement,
	owner any,
) (func(), error) {
	if h == nil || h.e == nil {
		return func() {}, nil
	}
	provider := h.GetCatalogProvider()
	return beginPostgresDuckLakeOperationForProviderWithError(ctx, statement, provider, owner)
}

// beginPostgresDuckLakeOperation admits one frontend statement before it
// captures a pool execution snapshot. The provider itself fail-closes for
// replication/maintenance origins and disabled storage; the explicit origin
// check here keeps protocol code from ever registering those paths as logical
// frontend work.
func (h *DuckHandler) beginPostgresDuckLakeOperation(ctx *sql.Context, statement tree.Statement) func() {
	return h.beginPostgresDuckLakeOperationForOwner(ctx, statement, postgresDuckLakeOperationOwner(ctx, h))
}

func (h *DuckHandler) beginPostgresDuckLakeOperationWithError(ctx *sql.Context, statement tree.Statement) (func(), error) {
	return h.beginPostgresDuckLakeOperationForOwnerWithError(ctx, statement, postgresDuckLakeOperationOwner(ctx, h))
}

// combinePostgresExecutionReleases closes a retained execution snapshot before
// releasing the logical-operation admission. Keeping the pool generation alive
// until the child iterator is closed is the important ordering guarantee; the
// once guard also covers legacy holders whose callbacks are not idempotent.
func combinePostgresExecutionReleases(snapshotRelease, operationRelease func()) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			if snapshotRelease != nil {
				snapshotRelease()
			}
			if operationRelease != nil {
				operationRelease()
			}
		})
	}
}

// getPostgresExecutionSnapshotLease admits a frontend operation before taking
// the retained pool snapshot. Callers that return an iterator transfer the
// returned release callback to backend.WrapRowIterWithRelease; metadata-only
// callers defer it locally.
func (h *DuckHandler) getPostgresExecutionSnapshotLease(
	ctx *sql.Context,
	statement tree.Statement,
) (adapter.SQLExecutor, *stdsql.Conn, *stdsql.Tx, func(), error) {
	operationRelease, admissionErr := h.beginPostgresDuckLakeOperationWithError(ctx, statement)
	if admissionErr != nil {
		return nil, nil, nil, func() {}, admissionErr
	}
	execer, conn, tx, snapshotRelease, err := adapter.GetExecutionSnapshotWithLease(ctx)
	if err != nil {
		operationRelease()
		return nil, nil, tx, func() {}, err
	}
	return execer, conn, tx, combinePostgresExecutionReleases(snapshotRelease, operationRelease), nil
}

// ComResetConnection implements the Handler interface.
func (h *DuckHandler) ComResetConnection(c *mysql.Conn) error {
	logrus.WithField("connectionId", c.ConnectionID).Debug("COM_RESET_CONNECTION command received")

	// Grab the currently selected database name
	db := h.sm.GetCurrentDB(c)

	// Dispose of the connection's current session
	h.maybeReleaseAllLocks(c)
	h.e.CloseSession(c.ConnectionID)

	// Create a new session and set the current database
	frontendCtx := mycontext.WithFrontendQuery(context.Background())
	err := h.sm.NewSession(frontendCtx, c)
	if err != nil {
		return err
	}
	return h.sm.SetDB(frontendCtx, c, db)
}

// ConnectionClosed implements the Handler interface.
func (h *DuckHandler) ConnectionClosed(c *mysql.Conn) {
	defer h.sm.RemoveConn(c)
	defer h.e.CloseSession(c.ConnectionID)

	h.maybeReleaseAllLocks(c)

	logrus.WithField(sql.ConnectionIdLogField, c.ConnectionID).Infof("ConnectionClosed")
}

// NewConnection implements the Handler interface.
func (h *DuckHandler) NewConnection(c *mysql.Conn) {
	h.sm.AddConn(c)
	sql.StatusVariables.IncrementGlobal("Connections", 1)

	c.DisableClientMultiStatements = true // TODO: h.disableMultiStmts
	logrus.WithField(sql.ConnectionIdLogField, c.ConnectionID).WithField("DisableClientMultiStatements", c.DisableClientMultiStatements).Infof("NewConnection")
}

// NewContext implements the Handler interface.
func (h *DuckHandler) NewContext(ctx context.Context, c *mysql.Conn, query string) (*sql.Context, error) {
	return h.sm.NewContextWithQuery(ctx, c, query)
}

func (h *DuckHandler) getStatementTag(mysqlConn *mysql.Conn, query string) (string, error) {
	if err := catalog.RejectSensitiveSQL(query); err != nil {
		return "", err
	}
	ctx := mycontext.WithFrontendQuery(context.Background())
	sqlCtx, err := h.NewContext(ctx, mysqlConn, "")
	if err != nil {
		return "", err
	}
	_, conn, tx, release, err := h.getPostgresExecutionSnapshotLease(sqlCtx, nil)
	if err != nil {
		return "", err
	}
	defer release()
	if tx != nil {
		// Statement-tag lookup is called after the protocol has opened its
		// implicit transaction. Do not prepare through Conn.Raw beside that
		// transaction; the lexical tag is sufficient for CommandComplete.
		return GuessStatementTag(query), nil
	}
	var tag string
	stmtType, _, prepareErr := prepareDuckDBStatementMetadata(sqlCtx, conn, query)
	if prepareErr == nil {
		tag = duckDBStatementTag(stmtType)
	}
	err = prepareErr
	return tag, err
}

// assertPostgresExecutionSnapshot verifies that a metadata probe did not race
// a transaction or connection replacement. The check is intentionally
// fail-closed: reacquiring a different connection would make the caller lose
// the transaction/physical-owner affinity it captured at the start.
func assertPostgresExecutionSnapshot(ctx *sql.Context, expectedConn *stdsql.Conn, expectedTx *stdsql.Tx) error {
	if ctx == nil || expectedConn == nil {
		return fmt.Errorf("PostgreSQL execution snapshot is unavailable")
	}
	_, actualConn, actualTx, err := adapter.GetExecutionSnapshot(ctx)
	if err != nil {
		return err
	}
	if actualConn != expectedConn || actualTx != expectedTx {
		return fmt.Errorf("PostgreSQL execution snapshot changed during metadata probe")
	}
	return nil
}

// prepareDuckDBStatementMetadata performs the driver-specific probe used only
// when no session transaction is active. The returned statement is always
// closed before the function returns; callers retain no raw driver state
// across protocol messages.
func prepareDuckDBStatementMetadata(ctx context.Context, conn *stdsql.Conn, query string) (stmtType duckdb.StmtType, paramTypes []duckdb.Type, err error) {
	if conn == nil {
		return duckdb.STATEMENT_TYPE_INVALID, nil, fmt.Errorf("DuckDB execution connection is unavailable")
	}
	err = conn.Raw(func(driverConn any) (callbackErr error) {
		dc, ok := driverConn.(*duckdb.Conn)
		if !ok {
			return fmt.Errorf("prepared PostgreSQL statement connection has unexpected driver type")
		}
		driverStmt, prepareErr := dc.PrepareContext(ctx, query)
		if prepareErr != nil {
			return prepareErr
		}
		defer func() {
			callbackErr = errors.Join(callbackErr, driverStmt.Close())
		}()
		stmt, ok := driverStmt.(*duckdb.Stmt)
		if !ok {
			return fmt.Errorf("prepared PostgreSQL statement has unexpected driver statement type")
		}
		stmtType, prepareErr = stmt.StatementType()
		if prepareErr != nil {
			return prepareErr
		}
		paramTypes = make([]duckdb.Type, driverStmt.NumInput())
		for i := range paramTypes {
			paramTypes[i], prepareErr = stmt.ParamType(i + 1) // 1-based index
			if prepareErr != nil {
				return prepareErr
			}
		}
		return nil
	})
	return stmtType, paramTypes, err
}

// postgresDuckDBStatementType classifies a statement without touching a raw
// driver connection. Parsing the SQL again, when possible, distinguishes a
// real WITH/INSERT (or other CTE statement) from the synthetic SELECT AST used
// for DuckDB-only syntax. The supplied AST remains the fallback for rewritten
// SQL that the PostgreSQL parser cannot consume a second time.
func postgresDuckDBStatementType(parsed tree.Statement, query string) duckdb.StmtType {
	parseQuery := sql.RemoveSpaceAndDelimiter(query, ';')
	if parseQuery != "" {
		if reparsed, parseErr := parser.ParseOne(parseQuery); parseErr == nil && reparsed.AST != nil {
			parsed = reparsed.AST
		}
	}
	if parsed != nil {
		if stmtType := duckDBStatementTypeForTag(parsed.StatementTag()); stmtType != duckdb.STATEMENT_TYPE_INVALID {
			return stmtType
		}
	}
	return duckDBStatementTypeForTag(GuessStatementTag(query))
}

func duckDBStatementTypeForTag(tag string) duckdb.StmtType {
	tag = strings.ToUpper(strings.TrimSpace(tag))
	switch {
	case tag == "SELECT", tag == "SHOW", tag == "VALUES", tag == "WITH", tag == "FROM", strings.HasPrefix(tag, "SHOW "):
		return duckdb.STATEMENT_TYPE_SELECT
	case tag == "CALL", strings.HasPrefix(tag, "CALL "):
		return duckdb.STATEMENT_TYPE_CALL
	case tag == "PRAGMA", strings.HasPrefix(tag, "PRAGMA "):
		return duckdb.STATEMENT_TYPE_PRAGMA
	case tag == "EXPLAIN", strings.HasPrefix(tag, "EXPLAIN "):
		return duckdb.STATEMENT_TYPE_EXPLAIN
	case tag == "INSERT", strings.HasPrefix(tag, "INSERT "):
		return duckdb.STATEMENT_TYPE_INSERT
	case tag == "UPDATE", strings.HasPrefix(tag, "UPDATE "):
		return duckdb.STATEMENT_TYPE_UPDATE
	case tag == "DELETE", strings.HasPrefix(tag, "DELETE "):
		return duckdb.STATEMENT_TYPE_DELETE
	case tag == "COPY", strings.HasPrefix(tag, "COPY "):
		return duckdb.STATEMENT_TYPE_COPY
	case tag == "ALTER", strings.HasPrefix(tag, "ALTER "):
		return duckdb.STATEMENT_TYPE_ALTER
	case tag == "CREATE", strings.HasPrefix(tag, "CREATE "):
		return duckdb.STATEMENT_TYPE_CREATE
	case tag == "DROP", strings.HasPrefix(tag, "DROP "):
		return duckdb.STATEMENT_TYPE_DROP
	case tag == "PREPARE", strings.HasPrefix(tag, "PREPARE "):
		return duckdb.STATEMENT_TYPE_PREPARE
	case tag == "EXECUTE", strings.HasPrefix(tag, "EXECUTE "):
		return duckdb.STATEMENT_TYPE_EXECUTE
	case tag == "ATTACH", strings.HasPrefix(tag, "ATTACH "):
		return duckdb.STATEMENT_TYPE_ATTACH
	case tag == "DETACH", strings.HasPrefix(tag, "DETACH "):
		return duckdb.STATEMENT_TYPE_DETACH
	case tag == "TRANSACTION", tag == "BEGIN", tag == "COMMIT", tag == "ROLLBACK":
		return duckdb.STATEMENT_TYPE_TRANSACTION
	case tag == "ANALYZE", strings.HasPrefix(tag, "ANALYZE "):
		return duckdb.STATEMENT_TYPE_ANALYZE
	case tag == "SET", strings.HasPrefix(tag, "SET "):
		return duckdb.STATEMENT_TYPE_SET
	case tag == "LOAD", strings.HasPrefix(tag, "LOAD "):
		return duckdb.STATEMENT_TYPE_LOAD
	case tag == "EXPORT", strings.HasPrefix(tag, "EXPORT "):
		return duckdb.STATEMENT_TYPE_EXPORT
	default:
		return duckdb.STATEMENT_TYPE_INVALID
	}
}

func duckDBStatementTag(stmtType duckdb.StmtType) string {
	switch stmtType {
	case duckdb.STATEMENT_TYPE_SELECT, duckdb.STATEMENT_TYPE_RELATION:
		return "SELECT"
	case duckdb.STATEMENT_TYPE_INSERT:
		return "INSERT"
	case duckdb.STATEMENT_TYPE_UPDATE:
		return "UPDATE"
	case duckdb.STATEMENT_TYPE_DELETE:
		return "DELETE"
	case duckdb.STATEMENT_TYPE_CALL:
		return "CALL"
	case duckdb.STATEMENT_TYPE_PRAGMA:
		return "PRAGMA"
	case duckdb.STATEMENT_TYPE_COPY:
		return "COPY"
	case duckdb.STATEMENT_TYPE_ALTER:
		return "ALTER"
	case duckdb.STATEMENT_TYPE_CREATE, duckdb.STATEMENT_TYPE_CREATE_FUNC:
		return "CREATE"
	case duckdb.STATEMENT_TYPE_DROP:
		return "DROP"
	case duckdb.STATEMENT_TYPE_PREPARE:
		return "PREPARE"
	case duckdb.STATEMENT_TYPE_EXECUTE:
		return "EXECUTE"
	case duckdb.STATEMENT_TYPE_ATTACH:
		return "ATTACH"
	case duckdb.STATEMENT_TYPE_DETACH:
		return "DETACH"
	case duckdb.STATEMENT_TYPE_TRANSACTION:
		return "TRANSACTION"
	case duckdb.STATEMENT_TYPE_ANALYZE:
		return "ANALYZE"
	case duckdb.STATEMENT_TYPE_EXPLAIN:
		return "EXPLAIN"
	case duckdb.STATEMENT_TYPE_SET:
		return "SET"
	case duckdb.STATEMENT_TYPE_VARIABLE_SET:
		return "SET VARIABLE"
	case duckdb.STATEMENT_TYPE_EXPORT:
		return "EXPORT"
	case duckdb.STATEMENT_TYPE_LOAD:
		return "LOAD"
	default:
		return "UNKNOWN"
	}
}

// postgresPlaceholderCount returns PostgreSQL's highest placeholder index. A
// parser result is authoritative for valid PostgreSQL syntax; the lexical
// fallback covers DuckDB-only statements that are intentionally represented by
// a synthetic AST during protocol parsing.
func postgresPlaceholderCount(query string) int {
	parseQuery := sql.RemoveSpaceAndDelimiter(query, ';')
	if parseQuery != "" {
		if parsed, parseErr := parser.ParseOne(parseQuery); parseErr == nil {
			return parsed.NumPlaceholders
		}
	}
	return lexicalPostgresPlaceholderCount(query)
}

func lexicalPostgresPlaceholderCount(query string) int {
	maxIndex, questionCount := 0, 0
	for i := 0; i < len(query); {
		switch query[i] {
		case '\'', '"', '`':
			quote := query[i]
			i++
			for i < len(query) {
				if query[i] == '\\' && quote != '`' && i+1 < len(query) {
					i += 2
					continue
				}
				if query[i] == quote {
					if i+1 < len(query) && query[i+1] == quote {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		case '-':
			if i+1 < len(query) && query[i+1] == '-' {
				i += 2
				for i < len(query) && query[i] != '\n' {
					i++
				}
				continue
			}
		case '/':
			if i+1 < len(query) && query[i+1] == '*' {
				if end := strings.Index(query[i+2:], "*/"); end >= 0 {
					i += end + 4
				} else {
					return maxIndex
				}
				continue
			}
		case '$':
			if i+1 < len(query) && query[i+1] >= '0' && query[i+1] <= '9' {
				j := i + 1
				for j < len(query) && query[j] >= '0' && query[j] <= '9' {
					j++
				}
				var index int
				for k := i + 1; k < j; k++ {
					index = index*10 + int(query[k]-'0')
				}
				if index > maxIndex {
					maxIndex = index
				}
				i = j
				continue
			}
		case '?':
			questionCount++
		}
		i++
	}
	if questionCount > maxIndex {
		return questionCount
	}
	return maxIndex
}

var queryLoggingRegex = regexp.MustCompile(`[\r\n\t ]+`)

func (h *DuckHandler) doQuery(ctx context.Context, c *mysql.Conn, query string, auditQuery string, parsed tree.Statement, stmt *duckdb.Stmt, vars []any, resultFormatCodes []int16, mode QueryMode, queryExec QueryExecutor, callback func(*Result) error) (returnErr error) {
	if err := catalog.RejectSensitiveSQL(query); err != nil {
		return err
	}
	if auditQuery != "" {
		if err := catalog.RejectSensitiveSQL(auditQuery); err != nil {
			return err
		}
	}
	redactedQuery := catalog.RedactSensitiveSQL(query)
	redactedAuditQuery := catalog.RedactSensitiveSQL(auditQuery)
	if auditQuery != "" {
		audit := backend.NewQueryAudit(c, "postgres", redactedAuditQuery)
		originalCallback := callback
		callback = func(res *Result) error {
			if err := originalCallback(res); err != nil {
				return err
			}
			audit.AddRows(len(res.Rows))
			return nil
		}
		defer func() {
			audit.Complete(returnErr)
		}()
	}

	sqlCtx, err := h.sm.NewContextWithQuery(ctx, c, redactedQuery)
	if err != nil {
		return err
	}
	sqlCtx.GetLogger().WithFields(logrus.Fields{
		"query":    redactedQuery,
		"protocol": "postgres",
	}).Trace("doQuery")

	start := time.Now()
	var queryStrToLog string
	if h.encodeLoggedQuery {
		queryStrToLog = base64.StdEncoding.EncodeToString([]byte(redactedQuery))
	} else if logrus.IsLevelEnabled(logrus.DebugLevel) {
		// this is expensive, so skip this unless we're logging at DEBUG level
		queryStrToLog = string(queryLoggingRegex.ReplaceAll([]byte(redactedQuery), []byte(" ")))
	}

	if queryStrToLog != "" {
		sqlCtx.SetLogger(sqlCtx.GetLogger().WithField("query", queryStrToLog))
	}
	sqlCtx.GetLogger().Debugf("Starting query")
	sqlCtx.GetLogger().Tracef("beginning execution")

	oCtx := ctx

	// TODO: it would be nice to put this logic in the engine, not the handler, but we don't want the process to be
	//  marked done until we're done spooling rows over the wire
	ctx, err = sqlCtx.ProcessList.BeginQuery(sqlCtx, query)
	defer func() {
		if err != nil && ctx != nil {
			sqlCtx.ProcessList.EndQuery(sqlCtx)
		}
	}()

	schema, rowIter, qFlags, err := queryExec(sqlCtx, query, parsed, stmt, vars)
	if err != nil {
		if printErrorStackTraces {
			fmt.Printf("error running query: %+v\n", err)
		}
		sqlCtx.GetLogger().WithError(err).Warn("error running query")
		return err
	}
	// Normalize the iterator boundary even for transaction-control and
	// replication paths that intentionally carry no execution lease. The
	// wrapper makes Close idempotent when a result helper already closed the
	// child, and guarantees any retained snapshot lease is released before the
	// session schema is inspected below.
	rowIter = backend.WrapRowIterWithRelease(rowIter, nil)
	defer func() {
		if rowIter != nil {
			closeErr := rowIter.Close(sqlCtx)
			err = errors.Join(err, closeErr)
			returnErr = errors.Join(returnErr, closeErr)
		}
		// CurrentSchemaOfUnderlyingConn re-enters the session/pool lifecycle
		// lock. It must run only after the iterator wrapper has closed its child
		// and released the retained execution snapshot.
		if isPostgresTransactionControl(parsed) {
			return
		}
		if session, ok := sqlCtx.Session.(*backend.Session); ok && session != nil {
			if currentSchema := session.CurrentSchemaOfUnderlyingConn(); len(currentSchema) > 0 {
				sqlCtx.SetCurrentDatabase(currentSchema)
			}
		}
	}()
	rowIter = backend.ApplyQueryRowLimit(sqlCtx, schema, rowIter)

	// create result before goroutines to avoid |ctx| racing
	var r *Result
	var processedAtLeastOneBatch bool

	// zero/single return schema use spooling shortcut
	if types.IsOkResultSchema(schema) {
		r, err = resultForOkIter(sqlCtx, rowIter)
	} else if schema == nil {
		r, err = resultForEmptyIter(sqlCtx, rowIter)
	} else if analyzer.FlagIsSet(qFlags, sql.QFlagMax1Row) {
		resultFields := schemaToFieldDescriptions(sqlCtx, schema, resultFormatCodes, mode)
		r, err = h.resultForMax1RowIter(sqlCtx, schema, rowIter, resultFields)
	} else {
		resultFields := schemaToFieldDescriptions(sqlCtx, schema, resultFormatCodes, mode)
		r, processedAtLeastOneBatch, err = h.resultForDefaultIter(sqlCtx, schema, rowIter, callback, resultFields)
	}
	if err != nil {
		return err
	}

	// errGroup context is now canceled
	ctx = oCtx

	sqlCtx.GetLogger().Debugf("Query finished in %d ms", time.Since(start).Milliseconds())

	sqlCtx.GetLogger().Tracef("AtLeastOneBatch=%v RowsInLastBatch=%d", processedAtLeastOneBatch, len(r.Rows))

	// processedAtLeastOneBatch means we already called callback() at least
	// once, so no need to call it if RowsAffected == 0.
	if r != nil && (r.RowsAffected == 0 && processedAtLeastOneBatch) {
		return nil
	}

	return callback(r)
}

// QueryExecutor is a function that executes a query and returns the result as a schema and iterator. Either of
// |parsed| or |analyzed| can be nil depending on the use case
type QueryExecutor func(ctx *sql.Context, query string, parsed tree.Statement, stmt *duckdb.Stmt, vars []any) (sql.Schema, sql.RowIter, *sql.QueryFlags, error)

// executeQuery is a QueryExecutor that calls QueryWithBindings on the given engine using the given query and parsed
// statement, which may be nil.
func (h *DuckHandler) executeQuery(ctx *sql.Context, query string, parsed tree.Statement, _ *duckdb.Stmt, _ []any) (schema sql.Schema, iter sql.RowIter, qFlags *sql.QueryFlags, returnErr error) {
	// return h.e.QueryWithBindings(ctx, query, parsed, nil, nil)

	sql.IncrementStatusVariable(ctx, "Questions", 1)
	if _, ok := parsed.(*tree.Select); ok {
		sql.IncrementStatusVariable(ctx, "Com_select", 1)
	}

	var (
		rows   *stdsql.Rows
		result stdsql.Result
		err    error
	)
	// Transaction controls are protocol lifecycle operations. Resolve them
	// before routing or acquiring an execution snapshot: a failed-block
	// COMMIT/ROLLBACK must remain able to finalize its owner even when ordinary
	// statement routing or connection acquisition is no longer usable.
	switch transaction := parsed.(type) {
	case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction:
		handled, controlErr := postgresTransactionControl(ctx, transaction, h.GetCatalogProvider())
		if !handled {
			return nil, nil, nil, fmt.Errorf("unsupported PostgreSQL transaction statement %T", parsed)
		}
		if controlErr != nil {
			return nil, nil, nil, controlErr
		}
		return types.OkResultSchema, sql.RowsToRowIter(sql.NewRow(types.OkResult{})), nil, nil
	}
	if err := rejectPostgresDatabaseDDLInTransaction(ctx, parsed); err != nil {
		return nil, nil, nil, err
	}
	execer, ownerConn, _, leaseRelease, snapshotErr := h.getPostgresExecutionSnapshotLease(ctx, parsed)
	if snapshotErr != nil {
		return nil, nil, nil, snapshotErr
	}
	// Transfer the retained snapshot/operation lease to the returned iterator.
	// Wrapping even OK-result iterators gives doQuery one idempotent close point;
	// on every error or nil-iterator return the wrapper releases immediately.
	defer func() {
		if leaseRelease != nil {
			iter = backend.WrapRowIterWithRelease(iter, leaseRelease)
			leaseRelease = nil
		}
	}()
	routedQuery, routeErr := h.rewritePostgresObjectRelationsWithSnapshot(ctx, query, execer, ownerConn)
	if routeErr != nil {
		return nil, nil, nil, routeErr
	}

	// NOTE: The query is parsed using Postgres parser, which does not support all DuckDB syntax.
	//   Consequently, the following classification is not perfect.
	switch parsed := parsed.(type) {
	case *tree.CreateTable:
		selection, createErr := setPostgresCreateTableStorage(ctx, parsed)
		if createErr != nil {
			err = createErr
			break
		}
		if parsed.Persistence == tree.PersistenceTemporary && selection.IsObjectStorage() {
			err = fmt.Errorf("%w: temporary tables cannot use object storage", catalog.ErrInvalidTableStorage)
			break
		}
		executionQuery, _, createErr := postgresCreateTableExecutionQuery(query, parsed)
		if createErr != nil {
			err = createErr
			break
		}
		result, err = execer.ExecContext(ctx, executionQuery)
		if err == nil {
			err = persistPostgresCreateTableStorageWithExecutor(ctx, parsed, selection, h.GetCatalogProvider(), execer, ownerConn)
		}
		if err != nil {
			break
		}
		affected, _ := result.RowsAffected()
		insertId, _ := result.LastInsertId()
		schema = types.OkResultSchema
		iter = sql.RowsToRowIter(sql.NewRow(types.OkResult{
			RowsAffected: uint64(affected),
			InsertID:     uint64(insertId),
		}))
	case *tree.DropTable, *tree.AlterTable:
		if ddlResult, handled, ddlErr := h.executePostgresObjectDDL(ctx, query, routedQuery, parsed, execer, ownerConn); handled {
			result, err = ddlResult, ddlErr
			if err != nil {
				break
			}
			affected, _ := result.RowsAffected()
			insertId, _ := result.LastInsertId()
			schema = types.OkResultSchema
			iter = sql.RowsToRowIter(sql.NewRow(types.OkResult{
				RowsAffected: uint64(affected),
				InsertID:     uint64(insertId),
			}))
			break
		} else if ddlErr != nil {
			err = ddlErr
			break
		}
		result, err = execer.ExecContext(ctx, routedQuery)
		if err != nil {
			break
		}
		affected, _ := result.RowsAffected()
		insertId, _ := result.LastInsertId()
		schema = types.OkResultSchema
		iter = sql.RowsToRowIter(sql.NewRow(types.OkResult{
			RowsAffected: uint64(affected),
			InsertID:     uint64(insertId),
		}))
	case *tree.CreateIndex, *tree.DropIndex,
		*tree.Update, *tree.Delete, *tree.Truncate, *tree.CopyFrom, *tree.CopyTo, *tree.SetVar:
		result, err = execer.ExecContext(ctx, routedQuery)
		if err != nil {
			break
		}
		affected, _ := result.RowsAffected()
		insertId, _ := result.LastInsertId()
		schema = types.OkResultSchema
		iter = sql.RowsToRowIter(sql.NewRow(types.OkResult{
			RowsAffected: uint64(affected),
			InsertID:     uint64(insertId),
		}))
	case *tree.Insert:
		if _, ok := postgresInsertReturningRows(parsed); ok {
			rows, err = execer.QueryContext(ctx, routedQuery)
			if err != nil {
				break
			}
			schema, err = pgtypes.InferSchema(rows)
			if err != nil {
				rows.Close()
				break
			}
			iter, err = backend.NewSQLRowIter(rows, schema)
			if err != nil {
				rows.Close()
			}
			break
		}
		result, err = execer.ExecContext(ctx, routedQuery)
		if err != nil {
			break
		}
		affected, _ := result.RowsAffected()
		insertID, _ := result.LastInsertId()
		schema = types.OkResultSchema
		iter = sql.RowsToRowIter(sql.NewRow(types.OkResult{
			RowsAffected: uint64(affected),
			InsertID:     uint64(insertID),
		}))
	case *tree.CreateDatabase:
		provider := h.GetCatalogProvider()
		if provider == nil {
			err = fmt.Errorf("database provider not found")
			break
		}
		dbName := parsed.Name.String()
		err = provider.CreateCatalogOnConn(ctx, ownerConn, dbName, parsed.IfNotExists)
		if err != nil {
			break
		}
		schema = types.OkResultSchema
		iter = sql.RowsToRowIter(sql.NewRow(types.OkResult{}))
	case *tree.DropDatabase:
		provider := h.GetCatalogProvider()
		if provider == nil {
			err = fmt.Errorf("database provider not found")
			break
		}
		dbName := parsed.Name.String()
		err = provider.DropCatalogOnConn(ctx, ownerConn, dbName, parsed.IfExists)
		if err != nil {
			break
		}
		schema = types.OkResultSchema
		iter = sql.RowsToRowIter(sql.NewRow(types.OkResult{}))
	case *tree.Select, tree.SelectStatement:
		rows, schema, err = queryCatalogWithJSONScan(ctx, routedQuery, execer)
		if err != nil {
			break
		}
		iter, err = backend.NewSQLRowIter(rows, schema)
		if err != nil {
			rows.Close()
			break
		}
	default:
		rows, err = execer.QueryContext(ctx, routedQuery)
		if err != nil {
			break
		}
		schema, err = pgtypes.InferSchema(rows)
		if err != nil {
			rows.Close()
			break
		}
		iter, err = backend.NewSQLRowIter(rows, schema)
		if err != nil {
			rows.Close()
			break
		}
	}
	if err != nil {
		return nil, nil, nil, err
	}

	return schema, iter, nil, nil
}

// queryCatalogWithJSONScan performs both the schema probe and the final query
// through the caller's captured executor. The caller must retain the matching
// execution lease until the returned rows have been consumed and closed; this
// helper deliberately never reacquires a session snapshot.
func queryCatalogWithJSONScan(ctx *sql.Context, query string, execer adapter.SQLExecutor, vars ...any) (*stdsql.Rows, sql.Schema, error) {
	if execer == nil {
		return nil, nil, fmt.Errorf("query executor is unavailable")
	}
	probeQuery := "SELECT * FROM (" + sql.RemoveSpaceAndDelimiter(query, ';') + ") LIMIT 0"
	probeRows, err := execer.QueryContext(ctx, probeQuery, vars...)
	if err != nil {
		// The PostgreSQL parser can classify DuckDB-only statements such as
		// CREATE OR REPLACE TABLE as a SelectStatement. The probe wrapper is
		// invalid for those statements, so preserve the pre-projection direct
		// execution path instead of returning the wrapper's parser error.
		rows, directErr := execer.QueryContext(ctx, query, vars...)
		if directErr != nil {
			return nil, nil, directErr
		}
		schema, schemaErr := pgtypes.InferSchema(rows)
		if schemaErr != nil {
			rows.Close()
			return nil, nil, schemaErr
		}
		return rows, schema, nil
	}
	schema, err := pgtypes.InferSchema(probeRows)
	closeErr := probeRows.Close()
	if err != nil {
		return nil, nil, err
	}
	if closeErr != nil {
		return nil, nil, closeErr
	}

	rows, err := execer.QueryContext(ctx, backend.QueryForJSONScan(query, schema), vars...)
	if err != nil {
		return nil, nil, err
	}
	return rows, schema, nil
}

// executeBoundPlan is a QueryExecutor that calls QueryWithBindings on the given engine using the given query and parsed
// statement, which may be nil.
func (h *DuckHandler) executeBoundPlan(ctx *sql.Context, query string, parsed tree.Statement, stmt *duckdb.Stmt, vars []any) (schema sql.Schema, iter sql.RowIter, qFlags *sql.QueryFlags, returnErr error) {
	// return h.e.PrepQueryPlanForExecution(ctx, query, plan, nil)

	// TODO(fan): Currently, the result of executing the bound query is occasionally incorrect.
	//   For example, for the "concurrent writes" test in the "TestReplication" test case,
	//   this approach returns [[2 x] [4 i]] instead of [[2 three] [4 five]].
	//   However, `x` and `i` never appear in the data.
	//   The reason is not clear and needs further investigation.
	//   Therefore, we fall back to the unbound query execution for now.
	//
	// var (
	// 	schema sql.Schema
	// 	iter   sql.RowIter
	// 	rows   driver.Rows
	// 	result driver.Result
	// 	err    error
	// )
	// switch stmt.StatementType() {
	// case duckdb.STATEMENT_TYPE_SELECT,
	// 	duckdb.STATEMENT_TYPE_RELATION,
	// 	duckdb.STATEMENT_TYPE_CALL,
	// 	duckdb.STATEMENT_TYPE_PRAGMA,
	// 	duckdb.STATEMENT_TYPE_EXPLAIN:
	// 	rows, err = stmt.QueryBound(ctx)
	// 	if err != nil {
	// 		break
	// 	}
	// 	schema, err = pgtypes.InferDriverSchema(rows)
	// 	if err != nil {
	// 		rows.Close()
	// 		break
	// 	}
	// 	iter, err = NewDriverRowIter(rows, schema)
	// 	if err != nil {
	// 		rows.Close()
	// 		break
	// 	}
	// default:
	// 	result, err = stmt.ExecBound(ctx)
	// 	if err != nil {
	// 		break
	// 	}
	// 	affected, _ := result.RowsAffected()
	// 	insertId, _ := result.LastInsertId()
	// 	schema = types.OkResultSchema
	// 	iter = sql.RowsToRowIter(sql.NewRow(types.OkResult{
	// 		RowsAffected: uint64(affected),
	// 		InsertID:     uint64(insertId),
	// 	}))
	// }
	// if err != nil {
	// 	return nil, nil, nil, err
	// }

	// As in simple-query execution, transaction controls must not depend on an
	// executor snapshot. In particular, failed-state COMMIT is the recovery
	// operation that clears a potentially unusable transaction binding.
	switch transaction := parsed.(type) {
	case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction:
		handled, controlErr := postgresTransactionControl(ctx, transaction, h.GetCatalogProvider())
		if !handled {
			return nil, nil, nil, fmt.Errorf("unsupported PostgreSQL transaction statement %T", parsed)
		}
		if controlErr != nil {
			return nil, nil, nil, controlErr
		}
		return types.OkResultSchema, sql.RowsToRowIter(sql.NewRow(types.OkResult{})), nil, nil
	}

	var (
		rows   *stdsql.Rows
		result stdsql.Result
		err    error
	)
	execer, ownerConn, tx, leaseRelease, snapshotErr := h.getPostgresExecutionSnapshotLease(ctx, parsed)
	if snapshotErr != nil {
		return nil, nil, nil, snapshotErr
	}
	// Keep the selected connection/transaction and provider admission alive
	// through iterator consumption. The defer handles every setup error and
	// transfers ownership to the returned iterator on success.
	defer func() {
		if leaseRelease != nil {
			iter = backend.WrapRowIterWithRelease(iter, leaseRelease)
			leaseRelease = nil
		}
	}()
	routedQuery, routeErr := h.rewritePostgresObjectRelationsWithSnapshot(ctx, query, execer, ownerConn)
	if routeErr != nil {
		return nil, nil, nil, routeErr
	}
	var stmtType duckdb.StmtType
	if tx != nil {
		// A parsed PostgreSQL portal is executed through the active *sql.Tx.
		// Never inspect or prepare a raw driver statement on ownerConn here;
		// doing so bypasses the transaction executor.
		stmtType = postgresDuckDBStatementType(parsed, routedQuery)
	} else if stmt != nil {
		stmtType, err = stmt.StatementType()
		if err != nil {
			return nil, nil, nil, err
		}
	} else {
		// Parse/Bind deliberately retain no raw DuckDB statement. In the
		// no-transaction case only, use a short-lived raw probe for exact
		// DuckDB classification. The retained execution snapshot keeps the
		// physical owner stable while the probe runs.
		prepareQuery := routedQuery
		if create, ok := parsed.(*tree.CreateTable); ok {
			if normalized, normalizeErr := postgresCreateTableQueryForPrepare(query, create); normalizeErr == nil {
				prepareQuery = normalized
			}
		}
		stmtType, _, err = prepareDuckDBStatementMetadata(ctx, ownerConn, prepareQuery)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	if _, ok := postgresInsertReturningRows(parsed); ok {
		rows, err = execer.QueryContext(ctx, routedQuery, vars...)
		if err == nil {
			schema, err = pgtypes.InferSchema(rows)
		}
		if err == nil {
			iter, err = NewSqlRowIter(rows, schema)
		}
		if err != nil && rows != nil {
			rows.Close()
		}
		return schema, iter, nil, err
	}

	// Prepared PostgreSQL DDL is executed through this bound-plan path rather
	// than executeQuery. Keep CREATE TABLE's selector and metadata behavior
	// identical to the simple-query path, and remove MyDuck-only WITH options
	// before handing the statement to DuckDB.
	if create, ok := parsed.(*tree.CreateTable); ok {
		selection, createErr := setPostgresCreateTableStorage(ctx, create)
		if createErr != nil {
			return nil, nil, nil, createErr
		}
		if create.Persistence == tree.PersistenceTemporary && selection.IsObjectStorage() {
			return nil, nil, nil, fmt.Errorf("%w: temporary tables cannot use object storage", catalog.ErrInvalidTableStorage)
		}
		executionQuery, _, createErr := postgresCreateTableExecutionQuery(query, create)
		if createErr != nil {
			return nil, nil, nil, createErr
		}
		result, createErr := execer.ExecContext(ctx, executionQuery, vars...)
		if createErr != nil {
			return nil, nil, nil, createErr
		}
		if createErr = persistPostgresCreateTableStorageWithExecutor(ctx, create, selection, h.GetCatalogProvider(), execer, ownerConn); createErr != nil {
			return nil, nil, nil, createErr
		}
		affected, _ := result.RowsAffected()
		insertID, _ := result.LastInsertId()
		return types.OkResultSchema, sql.RowsToRowIter(sql.NewRow(types.OkResult{
			RowsAffected: uint64(affected),
			InsertID:     uint64(insertID),
		})), nil, nil
	}
	if _, ddl := parsed.(*tree.DropTable); ddl {
		result, handled, ddlErr := h.executePostgresObjectDDL(ctx, query, routedQuery, parsed, execer, ownerConn, vars...)
		if ddlErr != nil {
			return nil, nil, nil, ddlErr
		}
		if handled {
			affected, _ := result.RowsAffected()
			insertID, _ := result.LastInsertId()
			return types.OkResultSchema, sql.RowsToRowIter(sql.NewRow(types.OkResult{
				RowsAffected: uint64(affected),
				InsertID:     uint64(insertID),
			})), nil, nil
		}
	}
	if _, ddl := parsed.(*tree.AlterTable); ddl {
		result, handled, ddlErr := h.executePostgresObjectDDL(ctx, query, routedQuery, parsed, execer, ownerConn, vars...)
		if ddlErr != nil {
			return nil, nil, nil, ddlErr
		}
		if handled {
			affected, _ := result.RowsAffected()
			insertID, _ := result.LastInsertId()
			return types.OkResultSchema, sql.RowsToRowIter(sql.NewRow(types.OkResult{
				RowsAffected: uint64(affected),
				InsertID:     uint64(insertID),
			})), nil, nil
		}
	}

	switch stmtType {
	case duckdb.STATEMENT_TYPE_SELECT,
		duckdb.STATEMENT_TYPE_RELATION:
		rows, schema, err = queryCatalogWithJSONScan(ctx, routedQuery, execer, vars...)
		if err != nil {
			break
		}
		iter, err = NewSqlRowIter(rows, schema)
		if err != nil {
			rows.Close()
			break
		}
	case duckdb.STATEMENT_TYPE_CALL,
		duckdb.STATEMENT_TYPE_PRAGMA,
		duckdb.STATEMENT_TYPE_EXPLAIN:
		rows, err = execer.QueryContext(ctx, routedQuery, vars...)
		if err != nil {
			break
		}
		schema, err = pgtypes.InferSchema(rows)
		if err != nil {
			rows.Close()
			break
		}
		iter, err = NewSqlRowIter(rows, schema)
		if err != nil {
			rows.Close()
			break
		}
	default:
		result, err = execer.ExecContext(ctx, routedQuery, vars...)
		if err != nil {
			break
		}
		affected, _ := result.RowsAffected()
		insertId, _ := result.LastInsertId()
		schema = types.OkResultSchema
		iter = sql.RowsToRowIter(sql.NewRow(types.OkResult{
			RowsAffected: uint64(affected),
			InsertID:     uint64(insertId),
		}))
	}
	if err != nil {
		return nil, nil, nil, err
	}
	return schema, iter, nil, nil
}

func postgresInsertReturningRows(stmt tree.Statement) (*tree.Insert, bool) {
	insert, ok := stmt.(*tree.Insert)
	if !ok {
		return nil, false
	}
	_, ok = insert.Returning.(*tree.ReturningExprs)
	return insert, ok
}

func postgresInsertReturningSchemaQuery(insert *tree.Insert) string {
	if insert == nil {
		return ""
	}
	returning, ok := insert.Returning.(*tree.ReturningExprs)
	if !ok {
		return ""
	}
	return "SELECT " + tree.AsString((*tree.SelectExprs)(returning)) +
		" FROM " + tree.AsString(insert.Table) + " LIMIT 0"
}

func inferPostgresInsertReturningSchema(ctx *sql.Context, query string, execer adapter.SQLExecutor) (sql.Schema, error) {
	if execer == nil {
		return nil, fmt.Errorf("INSERT ... RETURNING metadata executor is unavailable")
	}
	if strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("INSERT ... RETURNING metadata query is empty")
	}
	rows, err := execer.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgtypes.InferSchema(rows)
}

// maybeReleaseAllLocks makes a best effort attempt to release all locks on the given connection. If the attempt fails,
// an error is logged but not returned.
func (h *DuckHandler) maybeReleaseAllLocks(c *mysql.Conn) {
	if ctx, err := h.sm.NewContextWithQuery(context.Background(), c, ""); err != nil {
		logrus.Errorf("unable to release all locks on session close: %s", err)
		logrus.Errorf("unable to unlock tables on session close: %s", err)
	} else {
		_, err = h.e.LS.ReleaseAll(ctx)
		if err != nil {
			logrus.Errorf("unable to release all locks on session close: %s", err)
		}
		if err = h.e.Analyzer.Catalog.UnlockTables(ctx, c.ConnectionID); err != nil {
			logrus.Errorf("unable to unlock tables on session close: %s", err)
		}
	}
}

func schemaToFieldDescriptions(ctx *sql.Context, s sql.Schema, resultFormatCodes []int16, mode QueryMode) []pgproto3.FieldDescription {
	fields := make([]pgproto3.FieldDescription, len(s))
	for i, c := range s {
		var oid uint32
		var size int16
		var format int16
		var err error
		if pgType, ok := c.Type.(pgtypes.PostgresType); ok {
			oid = pgType.PG.OID
			if mode == SimpleQueryMode {
				// https://www.postgresql.org/docs/current/protocol-flow.html
				// > In simple Query mode, the format of retrieved values is always text, except ...
				format = pgproto3.TextFormat
			} else {
				if resultFormatCodes != nil && len(resultFormatCodes) > 0 {
					// Specified overall or per-column format codes
					if len(resultFormatCodes) == 1 {
						format = resultFormatCodes[0]
					} else {
						format = resultFormatCodes[i]
					}
				} else {
					format = pgType.PG.Codec.PreferredFormat()
				}
			}
			size = int16(pgType.Size)
		} else {
			oid, err = VitessTypeToObjectID(c.Type.Type())
			if err != nil {
				panic(err)
			}
			size = int16(c.Type.MaxTextResponseByteLength(ctx))
			format = pgproto3.TextFormat
		}

		// "Format" field: The format code being used for the field.
		// Currently, will be zero (text) or one (binary).
		// In a RowDescription returned from the statement variant of Describe,
		// the format code is not yet known and will always be zero.

		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(c.Name),
			TableOID:             uint32(0),
			TableAttributeNumber: uint16(0),
			DataTypeOID:          oid,
			DataTypeSize:         size,
			TypeModifier:         int32(-1), // TODO: used for domain type, which we don't support yet
			Format:               format,
		}
	}

	return fields
}

// resultForOkIter reads a maximum of one result row from a result iterator.
func resultForOkIter(ctx *sql.Context, iter sql.RowIter) (*Result, error) {
	defer trace.StartRegion(ctx, "DoltgresHandler.resultForOkIter").End()

	row, err := iter.Next(ctx)
	if err != nil {
		return nil, err
	}
	_, err = iter.Next(ctx)
	if err != io.EOF {
		return nil, fmt.Errorf("result schema iterator returned more than one row")
	}
	if err := iter.Close(ctx); err != nil {
		return nil, err
	}

	return &Result{
		RowsAffected: row[0].(types.OkResult).RowsAffected,
	}, nil
}

// resultForEmptyIter ensures that an expected empty iterator returns no rows.
func resultForEmptyIter(ctx *sql.Context, iter sql.RowIter) (*Result, error) {
	defer trace.StartRegion(ctx, "DuckHandler.resultForEmptyIter").End()
	if _, err := iter.Next(ctx); err != io.EOF {
		return nil, fmt.Errorf("result schema iterator returned more than zero rows")
	}
	if err := iter.Close(ctx); err != nil {
		return nil, err
	}
	return &Result{Fields: nil}, nil
}

// resultForMax1RowIter ensures that an empty iterator returns at most one row
func (h *DuckHandler) resultForMax1RowIter(ctx *sql.Context, schema sql.Schema, iter sql.RowIter, resultFields []pgproto3.FieldDescription) (*Result, error) {
	defer trace.StartRegion(ctx, "DuckHandler.resultForMax1RowIter").End()
	row, err := iter.Next(ctx)
	if err == io.EOF {
		return &Result{Fields: resultFields}, nil
	} else if err != nil {
		return nil, err
	}

	if _, err = iter.Next(ctx); err != io.EOF {
		return nil, fmt.Errorf("result max1Row iterator returned more than one row")
	}
	if err := iter.Close(ctx); err != nil {
		return nil, err
	}

	outputRow, err := h.rowToBytes(ctx, schema, resultFields, row)
	if err != nil {
		return nil, err
	}

	ctx.GetLogger().Tracef("spooling result row %s", outputRow)

	return &Result{Fields: resultFields, Rows: []Row{{outputRow}}, RowsAffected: 1}, nil
}

// resultForDefaultIter reads batches of rows from the iterator
// and writes results into the callback function.
func (h *DuckHandler) resultForDefaultIter(ctx *sql.Context, schema sql.Schema, iter sql.RowIter, callback func(*Result) error, resultFields []pgproto3.FieldDescription) (r *Result, processedAtLeastOneBatch bool, returnErr error) {
	defer trace.StartRegion(ctx, "DuckHandler.resultForDefaultIter").End()

	eg, ctx := ctx.NewErrgroup()

	var rowChan = make(chan sql.Row, 512)

	pan2err := func() {
		if recoveredPanic := recover(); recoveredPanic != nil {
			returnErr = fmt.Errorf("DoltgresHandler caught panic: %v", recoveredPanic)
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(2)
	// Read rows off the row iterator and send them to the row channel.
	eg.Go(func() error {
		defer pan2err()
		defer wg.Done()
		defer close(rowChan)
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				row, err := iter.Next(ctx)
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				select {
				case rowChan <- row:
				case <-ctx.Done():
					return nil
				}
			}
		}
	})

	// Default waitTime is one minute if there is no timeout configured, in which case
	// it will loop to iterate again unless the socket died by the OS timeout or other problems.
	// If there is a timeout, it will be enforced to ensure that Vitess has a chance to
	// call DoltgresHandler.CloseConnection()
	waitTime := 1 * time.Minute
	if h.readTimeout > 0 {
		waitTime = h.readTimeout
	}
	timer := time.NewTimer(waitTime)
	defer timer.Stop()

	// reads rows from the channel, converts them to wire format,
	// and calls |callback| to give them to vitess.
	eg.Go(func() error {
		defer pan2err()
		// defer cancelF()
		defer wg.Done()
		for {
			if r == nil {
				r = &Result{Fields: resultFields}
			}
			if r.RowsAffected == rowsBatch {
				if err := callback(r); err != nil {
					return err
				}
				r = nil
				processedAtLeastOneBatch = true
				continue
			}

			select {
			case <-ctx.Done():
				return nil
			case row, ok := <-rowChan:
				if !ok {
					return nil
				}
				if types.IsOkResult(row) {
					if len(r.Rows) > 0 {
						panic("Got OkResult mixed with RowResult")
					}
					result := row[0].(types.OkResult)
					r = &Result{
						RowsAffected: result.RowsAffected,
					}
					continue
				}

				outputRow, err := h.rowToBytes(ctx, schema, resultFields, row)
				if err != nil {
					return err
				}

				ctx.GetLogger().Tracef("spooling result row %+v", outputRow)
				r.Rows = append(r.Rows, Row{outputRow})
				r.RowsAffected++
			case <-timer.C:
				if h.readTimeout != 0 {
					// Cancel and return so Vitess can call the CloseConnection callback
					ctx.GetLogger().Tracef("connection timeout")
					return fmt.Errorf("row read wait bigger than connection timeout")
				}
			}
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(waitTime)
		}
	})

	// Close() kills this PID in the process list,
	// wait until all rows have be sent over the wire
	eg.Go(func() error {
		defer pan2err()
		wg.Wait()
		return iter.Close(ctx)
	})

	err := eg.Wait()
	if err != nil {
		ctx.GetLogger().WithError(err).Warn("error running query")
		returnErr = err
	}

	return
}

func (h *DuckHandler) rowToBytes(ctx *sql.Context, s sql.Schema, fields []pgproto3.FieldDescription, row sql.Row) ([][]byte, error) {
	if logger := ctx.GetLogger(); logger.Logger.Level >= logrus.TraceLevel {
		logger = logger.WithField("func", "rowToBytes")
		logger.Tracef("row: %+v\n", row)
		types := make([]sql.Type, len(s))
		for i, c := range s {
			types[i] = c.Type
		}
		logger.Tracef("types: %+v\n", types)
		logger.Tracef("fields: %+v\n", fields)
	}
	if len(row) == 0 {
		return nil, nil
	}
	if len(s) == 0 {
		// should not happen
		return nil, fmt.Errorf("received empty schema")
	}
	o := make([][]byte, len(row))
	for i, v := range row {
		if v == nil {
			o[i] = nil
			continue
		}
		wireValue, err := normalizePGWireValue(v)
		if err != nil {
			return nil, err
		}
		v = wireValue

		// TODO(fan): Preallocate the buffer
		if _, ok := s[i].Type.(pgtypes.PostgresType); ok {
			bytes, err := h.connectionHandler.pgTypeMap.Encode(fields[i].DataTypeOID, fields[i].Format, v, nil)
			if err != nil {
				return nil, err
			}
			o[i] = bytes
		} else {
			val, err := s[i].Type.SQL(ctx, []byte{}, v)
			if err != nil {
				return nil, err
			}
			o[i] = val.ToBytes()
		}
	}
	return o, nil
}

func normalizePGWireValue(v any) (any, error) {
	if jsonValue, ok := v.(interface{ JSONString() (string, error) }); ok {
		return jsonValue.JSONString()
	}
	return v, nil
}
