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
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime/trace"
	"sync"
	"time"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/apecloud/myduckserver/pgtypes"
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

	conn, err := adapter.GetConn(sqlCtx)
	if err != nil {
		return nil, nil, nil, err
	}

	prepareQuery, err := postgresCreateTableQueryForPrepare(query, parsed)
	if err != nil {
		return nil, nil, nil, err
	}

	var (
		stmt       *duckdb.Stmt
		stmtType   duckdb.StmtType
		paramTypes []duckdb.Type
	)
	// This is a bit of a hack to get DuckDB's underlying prepared statement.
	// But we know that the connection is a DuckDB connection and it is kept alive.
	err = conn.Raw(func(driverConn interface{}) error {
		dc := driverConn.(*duckdb.Conn)
		s, err := dc.PrepareContext(sqlCtx, prepareQuery)
		if err != nil {
			return err
		}
		n := s.NumInput()
		stmt = s.(*duckdb.Stmt)
		stmtType, err = stmt.StatementType()
		if err != nil {
			return err
		}
		paramTypes = make([]duckdb.Type, n)
		for i := 0; i < n; i++ {
			paramTypes[i], err = stmt.ParamType(i + 1) // 1-based index
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		logrus.WithField("query", catalog.RedactSensitiveSQL(query)).Errorf("unable to prepare query: %s", err.Error())
		return nil, nil, nil, err
	}

	paramOIDs := make([]uint32, len(paramTypes))
	for i, t := range paramTypes {
		paramOIDs[i] = pgtypes.DuckdbTypeToPostgresOID[t]
	}

	var (
		fields []pgproto3.FieldDescription
		rows   *stdsql.Rows
	)
	if insert, ok := postgresInsertReturningRows(parsed); ok {
		schema, schemaErr := inferPostgresInsertReturningSchema(sqlCtx, insert)
		if schemaErr != nil {
			defer stmt.Close()
			return nil, nil, nil, schemaErr
		}
		fields = schemaToFieldDescriptions(sqlCtx, schema, nil, ExtendedQueryMode)
		return stmt, paramOIDs, fields, nil
	}
	switch stmtType {
	case duckdb.STATEMENT_TYPE_SELECT,
		duckdb.STATEMENT_TYPE_RELATION,
		duckdb.STATEMENT_TYPE_CALL,
		duckdb.STATEMENT_TYPE_PRAGMA,
		duckdb.STATEMENT_TYPE_EXPLAIN:

		// Execute the query with all NULL values as parameters to get the result types.
		query := query
		if stmtType == duckdb.STATEMENT_TYPE_SELECT ||
			stmtType == duckdb.STATEMENT_TYPE_RELATION {
			// Add LIMIT 0 to avoid executing the actual query.
			query = "SELECT * FROM (" + sql.RemoveSpaceAndDelimiter(query, ';') + ") LIMIT 0"
		}
		params := make([]any, len(paramTypes)) // all nil
		rows, err = conn.QueryContext(sqlCtx, query, params...)
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
		defer stmt.Close()
		return nil, nil, nil, err
	}

	return stmt, paramOIDs, fields, nil
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
	conn, err := adapter.GetConn(sqlCtx)
	if err != nil {
		return "", err
	}
	var tag string
	err = conn.Raw(func(driverConn any) error {
		c := driverConn.(*duckdb.Conn)
		s, err := c.PrepareContext(sqlCtx, query)
		if err != nil {
			return err
		}
		defer s.Close()
		stmt := s.(*duckdb.Stmt)
		tag = GetStatementTag(stmt)
		return nil
	})
	return tag, err
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
	rowIter = backend.ApplyQueryRowLimit(sqlCtx, schema, rowIter)

	// If the query is "USE <database>", we need to update the current database in the session.
	if currentSchema := sqlCtx.Session.(*backend.Session).CurrentSchemaOfUnderlyingConn(); len(currentSchema) > 0 {
		sqlCtx.SetCurrentDatabase(currentSchema)
	}

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
func (h *DuckHandler) executeQuery(ctx *sql.Context, query string, parsed tree.Statement, _ *duckdb.Stmt, _ []any) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	// return h.e.QueryWithBindings(ctx, query, parsed, nil, nil)

	sql.IncrementStatusVariable(ctx, "Questions", 1)
	if _, ok := parsed.(*tree.Select); ok {
		sql.IncrementStatusVariable(ctx, "Com_select", 1)
	}

	var (
		schema sql.Schema
		iter   sql.RowIter
		rows   *stdsql.Rows
		result stdsql.Result
		err    error
	)

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
		existing, createErr := postgresCreateTableAlreadyExists(ctx, parsed)
		if createErr != nil {
			err = createErr
			break
		}
		executionQuery, _, createErr := postgresCreateTableExecutionQuery(query, parsed)
		if createErr != nil {
			err = createErr
			break
		}
		result, err = adapter.Exec(ctx, executionQuery)
		if err == nil && !existing {
			err = persistPostgresCreateTableStorage(ctx, parsed, selection)
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
	case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction,
		*tree.DropTable, *tree.AlterTable, *tree.CreateIndex, *tree.DropIndex,
		*tree.Update, *tree.Delete, *tree.Truncate, *tree.CopyFrom, *tree.CopyTo, *tree.SetVar:
		result, err = adapter.Exec(ctx, query)
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
			rows, err = adapter.QueryCatalog(ctx, query)
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
		result, err = adapter.Exec(ctx, query)
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
		err = provider.CreateCatalog(dbName, parsed.IfNotExists)
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
		err = provider.DropCatalog(dbName, parsed.IfExists)
		if err != nil {
			break
		}
		schema = types.OkResultSchema
		iter = sql.RowsToRowIter(sql.NewRow(types.OkResult{}))
	case *tree.Select, tree.SelectStatement:
		rows, schema, err = queryCatalogWithJSONScan(ctx, query)
		if err != nil {
			break
		}
		iter, err = backend.NewSQLRowIter(rows, schema)
		if err != nil {
			rows.Close()
			break
		}
	default:
		rows, err = adapter.QueryCatalog(ctx, query)
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

func queryCatalogWithJSONScan(ctx *sql.Context, query string, vars ...any) (*stdsql.Rows, sql.Schema, error) {
	probeQuery := "SELECT * FROM (" + sql.RemoveSpaceAndDelimiter(query, ';') + ") LIMIT 0"
	probeRows, err := adapter.QueryCatalog(ctx, probeQuery, vars...)
	if err != nil {
		// The PostgreSQL parser can classify DuckDB-only statements such as
		// CREATE OR REPLACE TABLE as a SelectStatement. The probe wrapper is
		// invalid for those statements, so preserve the pre-projection direct
		// execution path instead of returning the wrapper's parser error.
		rows, directErr := adapter.QueryCatalog(ctx, query, vars...)
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

	rows, err := adapter.QueryCatalog(ctx, backend.QueryForJSONScan(query, schema), vars...)
	if err != nil {
		return nil, nil, err
	}
	return rows, schema, nil
}

// executeBoundPlan is a QueryExecutor that calls QueryWithBindings on the given engine using the given query and parsed
// statement, which may be nil.
func (h *DuckHandler) executeBoundPlan(ctx *sql.Context, query string, parsed tree.Statement, stmt *duckdb.Stmt, vars []any) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
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

	stmtType, err := stmt.StatementType()
	if err != nil {
		return nil, nil, nil, err
	}

	var (
		schema sql.Schema
		iter   sql.RowIter
		rows   *stdsql.Rows
		result stdsql.Result
	)
	if _, ok := postgresInsertReturningRows(parsed); ok {
		rows, err = adapter.QueryCatalog(ctx, query, vars...)
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
		existing, createErr := postgresCreateTableAlreadyExists(ctx, create)
		if createErr != nil {
			return nil, nil, nil, createErr
		}
		executionQuery, _, createErr := postgresCreateTableExecutionQuery(query, create)
		if createErr != nil {
			return nil, nil, nil, createErr
		}
		result, createErr := adapter.ExecCatalog(ctx, executionQuery, vars...)
		if createErr != nil {
			return nil, nil, nil, createErr
		}
		if !existing {
			createErr = persistPostgresCreateTableStorage(ctx, create, selection)
		}
		if createErr != nil {
			return nil, nil, nil, createErr
		}
		affected, _ := result.RowsAffected()
		insertID, _ := result.LastInsertId()
		return types.OkResultSchema, sql.RowsToRowIter(sql.NewRow(types.OkResult{
			RowsAffected: uint64(affected),
			InsertID:     uint64(insertID),
		})), nil, nil
	}

	switch stmtType {
	case duckdb.STATEMENT_TYPE_SELECT,
		duckdb.STATEMENT_TYPE_RELATION:
		rows, schema, err = queryCatalogWithJSONScan(ctx, query, vars...)
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
		rows, err = adapter.QueryCatalog(ctx, query, vars...)
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
		result, err = adapter.ExecCatalog(ctx, query, vars...)
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

func inferPostgresInsertReturningSchema(ctx *sql.Context, insert *tree.Insert) (sql.Schema, error) {
	returning := insert.Returning.(*tree.ReturningExprs)
	query := "SELECT " + tree.AsString((*tree.SelectExprs)(returning)) +
		" FROM " + tree.AsString(insert.Table) + " LIMIT 0"
	rows, err := adapter.QueryCatalog(ctx, query)
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
