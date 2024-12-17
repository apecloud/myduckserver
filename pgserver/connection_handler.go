// Copyright 2023 Dolthub, Inc.
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
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime/debug"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sirupsen/logrus"
)

// ConnectionHandler is responsible for the entire lifecycle of a user connection: receiving messages they send,
// executing queries, sending the correct messages in return, and terminating the connection when appropriate.
type ConnectionHandler struct {
	mysqlConn          *mysql.Conn
	preparedStatements map[string]PreparedStatementData
	portals            map[string]PortalData
	duckHandler        *DuckHandler
	backend            *pgproto3.Backend
	pgTypeMap          *pgtype.Map
	waitForSync        bool
	// copyFromStdinState is set when this connection is in the COPY FROM STDIN mode, meaning it is waiting on
	// COPY DATA messages from the client to import data into tables.
	copyFromStdinState *copyFromStdinState

	server *Server
	logger *logrus.Entry
}

// Set this env var to disable panic handling in the connection, which is useful when debugging a panic
const disablePanicHandlingEnvVar = "DOLT_PGSQL_PANIC"

// HandlePanics determines whether panics should be handled in the connection handler. See |disablePanicHandlingEnvVar|.
var HandlePanics = true

func init() {
	if _, ok := os.LookupEnv(disablePanicHandlingEnvVar); ok {
		HandlePanics = false
	}
}

// NewConnectionHandler returns a new ConnectionHandler for the connection provided
func NewConnectionHandler(conn net.Conn, handler mysql.Handler, engine *gms.Engine, sm *server.SessionManager, connID uint32, server *Server) *ConnectionHandler {
	mysqlConn := &mysql.Conn{
		Conn:        conn,
		PrepareData: make(map[uint32]*mysql.PrepareData),
	}
	mysqlConn.ConnectionID = connID

	// Postgres has a two-stage procedure for prepared queries. First the query is parsed via a |Parse| message, and
	// the result is stored in the |preparedStatements| map by the name provided. Then one or more |Bind| messages
	// provide parameters for the query, and the result is stored in |portals|. Finally, a call to |Execute| executes
	// the named portal.
	preparedStatements := make(map[string]PreparedStatementData)
	portals := make(map[string]PortalData)

	// TODO: possibly should define engine and session manager ourselves
	//  instead of depending on the GetRunningServer method.
	duckHandler := &DuckHandler{
		e:                 engine,
		sm:                sm,
		readTimeout:       0,     // cfg.ConnReadTimeout,
		encodeLoggedQuery: false, // cfg.EncodeLoggedQuery,
	}

	return &ConnectionHandler{
		mysqlConn:          mysqlConn,
		preparedStatements: preparedStatements,
		portals:            portals,
		duckHandler:        duckHandler,
		backend:            pgproto3.NewBackend(conn, conn),
		pgTypeMap:          pgtype.NewMap(),

		server: server,
		logger: logrus.WithFields(logrus.Fields{
			"connectionID": connID,
			"protocol":     "pg",
		}),
	}
}

func (h *ConnectionHandler) closeBackendConn() {
	ctx, err := h.duckHandler.NewContext(context.Background(), h.mysqlConn, "")
	if err != nil {
		fmt.Println(err.Error())
	}
	adapter.CloseBackendConn(ctx)
}

// HandleConnection handles a connection's session, reading messages, executing queries, and sending responses.
// Expected to run in a goroutine per connection.
func (h *ConnectionHandler) HandleConnection() {
	var returnErr error
	if HandlePanics {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("Listener recovered panic: %v\n%s\n", r, string(debug.Stack()))

				var eomErr error
				if returnErr != nil {
					eomErr = returnErr
				} else if rErr, ok := r.(error); ok {
					eomErr = rErr
				} else {
					eomErr = fmt.Errorf("panic: %v", r)
				}

				// Sending eom can panic, which means we must recover again
				defer func() {
					if r := recover(); r != nil {
						fmt.Printf("Listener recovered panic: %v\n%s\n", r, string(debug.Stack()))
					}
				}()
				h.endOfMessages(eomErr)
			}

			if returnErr != nil {
				fmt.Println(returnErr.Error())
			}

			h.duckHandler.ConnectionClosed(h.mysqlConn)
			h.closeBackendConn()
			if err := h.Conn().Close(); err != nil {
				fmt.Printf("Failed to properly close connection:\n%v\n", err)
			}
		}()
	}
	h.duckHandler.NewConnection(h.mysqlConn)

	if proceed, err := h.handleStartup(); err != nil || !proceed {
		returnErr = err
		return
	}

	// Main session loop: read messages one at a time off the connection until we receive a |Terminate| message, in
	// which case we hang up, or the connection is closed by the client, which generates an io.EOF from the connection.
	for {
		stop, err := h.receiveMessage()
		if err != nil {
			returnErr = err
			break
		}

		if stop {
			break
		}
	}
}

// Conn returns the underlying net.Conn for this connection.
func (h *ConnectionHandler) Conn() net.Conn {
	return h.mysqlConn.Conn
}

// setConn sets a new underlying net.Conn for this connection.
func (h *ConnectionHandler) setConn(conn net.Conn) {
	h.mysqlConn.Conn = conn
	h.backend = pgproto3.NewBackend(conn, conn)
}

// handleStartup handles the entire startup routine, including SSL requests, authentication, etc. Returns false if the
// connection has been terminated, or if we should not proceed with the message loop.
func (h *ConnectionHandler) handleStartup() (bool, error) {
	startupMessage, err := h.backend.ReceiveStartupMessage()
	if err == io.EOF {
		// Receiving EOF means that the connection has terminated, so we should just return
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("error receiving startup message: %w", err)
	}

	switch sm := startupMessage.(type) {
	case *pgproto3.StartupMessage:
		if err = h.handleAuthentication(sm); err != nil {
			return false, err
		}
		if err = h.sendClientStartupMessages(); err != nil {
			return false, err
		}
		if err = h.chooseInitialDatabase(sm); err != nil {
			return false, err
		}
		return true, h.send(&pgproto3.ReadyForQuery{
			TxStatus: byte(ReadyForQueryTransactionIndicator_Idle),
		})
	case *pgproto3.SSLRequest:
		hasCertificate := len(certificate.Certificate) > 0
		var performSSL = []byte("N")
		if hasCertificate {
			performSSL = []byte("S")
		}
		_, err = h.Conn().Write(performSSL)
		if err != nil {
			return false, fmt.Errorf("error sending SSL request: %w", err)
		}
		// If we have a certificate and the client has asked for SSL support, then we switch here.
		// This involves swapping out our underlying net connection for a new one.
		// We can't start in SSL mode, as the client does not attempt the handshake until after our response.
		if hasCertificate {
			h.setConn(tls.Server(h.Conn(), &tls.Config{
				Certificates: []tls.Certificate{certificate},
			}))
		}
		return h.handleStartup()
	case *pgproto3.GSSEncRequest:
		// we don't support GSSAPI
		_, err = h.Conn().Write([]byte("N"))
		if err != nil {
			return false, fmt.Errorf("error sending response to GSS Enc Request: %w", err)
		}
		return h.handleStartup()
	default:
		return false, fmt.Errorf("terminating connection: unexpected start message: %#v", startupMessage)
	}
}

// sendClientStartupMessages sends introductory messages to the client and returns any error
func (h *ConnectionHandler) sendClientStartupMessages() error {
	sessParams := []struct {
		Name  string
		Value any
	}{
		// These are session parameter status messages that are sent to the client
		// to simulate a real PostgreSQL connection. Some clients may expect these
		// to be sent, like pgpool, which will not work without them. Because
		// if the paramter status message list sent by this server differs from
		// the list of the other real PostgreSQL servers, pgpool can not establish
		// a connection to this server.
		// Some of these may not exists in postgresConfigParameters(in doltgresql),
		// which lists all the available parameters in PostgreSQL. In that case,
		// we will use a mock value for that parameter. e.g. "on" for "is_superuser".
		{"in_hot_standby", nil},
		{"integer_datetimes", "on"},
		{"TimeZone", nil},
		{"IntervalStyle", nil},
		{"is_superuser", "on"}, // This is not specified in postgresConfigParameters now.
		{"application_name", nil},
		{"default_transaction_read_only", nil},
		{"scram_iterations", nil},
		{"DateStyle", nil},
		{"standard_conforming_strings", nil},
		{"session_authorization", "postgres"}, // This is not specified in postgresConfigParameters now.
		{"client_encoding", nil},
		{"server_version", nil},
		{"server_encoding", nil},
	}

	for _, param := range sessParams {
		var value string
		if param.Value != nil {
			value = fmt.Sprintf("%v", param.Value)
		} else {
			_, v, ok := sql.SystemVariables.GetGlobal(param.Name)
			if !ok {
				return fmt.Errorf("error: %v variable was not found", param.Name)
			}
			value = fmt.Sprintf("%v", v)
		}
		if err := h.send(&pgproto3.ParameterStatus{
			Name:  param.Name,
			Value: value,
		}); err != nil {
			return err
		}
	}
	return h.send(&pgproto3.BackendKeyData{
		ProcessID: processID,
		SecretKey: 0, // TODO: this should represent an ID that can uniquely identify this connection, so that CancelRequest will work
	})
}

// chooseInitialDatabase attempts to choose the initial database for the connection,
// if one is specified in the startup message provided
func (h *ConnectionHandler) chooseInitialDatabase(startupMessage *pgproto3.StartupMessage) error {
	db, ok := startupMessage.Parameters["database"]
	dbSpecified := ok && len(db) > 0
	if !dbSpecified {
		db = h.mysqlConn.User
	}
	if db == "postgres" {
		if provider, ok := h.duckHandler.e.Analyzer.Catalog.DbProvider.(*catalog.DatabaseProvider); ok {
			db = provider.CatalogName()
		}
	}

	useStmt := fmt.Sprintf("USE %s.public;", db)
	setStmt := fmt.Sprintf("SET database TO %s;", db)
	parsed, err := parser.ParseOne(setStmt)
	if err != nil {
		return err
	}
	err = h.duckHandler.ComQuery(context.Background(), h.mysqlConn, useStmt, parsed.AST, func(res *Result) error {
		return nil
	})
	// If a database isn't specified, then we attempt to connect to a database with the same name as the user,
	// ignoring any error
	if err != nil && dbSpecified {
		_ = h.send(&pgproto3.ErrorResponse{
			Severity: string(ErrorResponseSeverity_Fatal),
			Code:     "3D000",
			Message:  fmt.Sprintf(`"database "%s" does not exist"`, db),
			Routine:  "InitPostgres",
		})
		return err
	}
	return nil
}

// receiveMessage reads a single message off the connection and processes it, returning an error if no message could be
// received from the connection. Otherwise, (a message is received successfully), the message is processed and any
// error is handled appropriately. The return value indicates whether the connection should be closed.
func (h *ConnectionHandler) receiveMessage() (bool, error) {
	var endOfMessages bool
	// For the time being, we handle panics in this function and treat them the same as errors so that they don't
	// forcibly close the connection. Contrast this with the panic handling logic in HandleConnection, where we treat any
	// panic as unrecoverable to the connection. As we fill out the implementation, we can revisit this decision and
	// rethink our posture over whether panics should terminate a connection.
	if HandlePanics {
		defer func() {
			if r := recover(); r != nil {
				h.logger.Debugf("Listener recovered panic: %v\n%s\n", r, string(debug.Stack()))

				var eomErr error
				if rErr, ok := r.(error); ok {
					eomErr = rErr
				} else {
					eomErr = fmt.Errorf("panic: %v", r)
				}

				if !endOfMessages && h.waitForSync {
					if syncErr := h.discardToSync(); syncErr != nil {
						h.logger.Error(syncErr.Error())
					}
				}
				h.endOfMessages(eomErr)
			}
		}()
	}

	msg, err := h.backend.Receive()
	if err != nil {
		return false, fmt.Errorf("error receiving message: %w", err)
	}

	if m, ok := msg.(json.Marshaler); ok && logrus.IsLevelEnabled(logrus.DebugLevel) {
		msgInfo, err := m.MarshalJSON()
		if err != nil {
			return false, err
		}
		logrus.Debugf("Received message: %s", string(msgInfo))
	} else {
		logrus.Debugf("Received message: %t", msg)
	}

	var stop bool
	stop, endOfMessages, err = h.handleMessage(msg)
	if err != nil {
		if !endOfMessages && h.waitForSync {
			if syncErr := h.discardToSync(); syncErr != nil {
				fmt.Println(syncErr.Error())
			}
		}
		h.endOfMessages(err)
	} else if endOfMessages {
		h.endOfMessages(nil)
	}

	return stop, nil
}

// handleMessages processes the message provided and returns status flags indicating what the connection should do next.
// If the |stop| response parameter is true, it indicates that the connection should be closed by the caller. If the
// |endOfMessages| response parameter is true, it indicates that no more messages are expected for the current operation
// and a READY FOR QUERY message should be sent back to the client, so it can send the next query.
func (h *ConnectionHandler) handleMessage(msg pgproto3.Message) (stop, endOfMessages bool, err error) {
	logrus.Tracef("Handling message: %T", msg)
	switch message := msg.(type) {
	case *pgproto3.Terminate:
		return true, false, nil
	case *pgproto3.Sync:
		h.waitForSync = false
		return false, true, nil
	case *pgproto3.Query:
		endOfMessages, err = h.handleQuery(message)
		return false, endOfMessages, err
	case *pgproto3.Parse:
		return false, false, h.handleParse(message)
	case *pgproto3.Describe:
		return false, false, h.handleDescribe(message)
	case *pgproto3.Bind:
		return false, false, h.handleBind(message)
	case *pgproto3.Execute:
		return false, false, h.handleExecute(message)
	case *pgproto3.Close:
		if message.ObjectType == 'S' {
			h.deletePreparedStatement(message.Name)
		} else {
			h.deletePortal(message.Name)
		}
		return false, false, h.send(&pgproto3.CloseComplete{})
	case *pgproto3.CopyData:
		return h.handleCopyData(message)
	case *pgproto3.CopyDone:
		return h.handleCopyDone(message)
	case *pgproto3.CopyFail:
		return h.handleCopyFail(message)
	default:
		return false, true, fmt.Errorf(`unhandled message "%t"`, message)
	}
}

// handleQuery handles a query message, and returns a boolean flag, |endOfMessages| indicating if no other messages are
// expected as part of this query, in which case the server will send a READY FOR QUERY message back to the client so
// that it can send its next query.
func (h *ConnectionHandler) handleQuery(message *pgproto3.Query) (endOfMessages bool, err error) {
	// usql use ";" to test if the connection is alive. If we don't handle it, this will return an error. So we need to
	// manually handle it here.
	if message.String == ";" {
		err := h.send(makeCommandComplete("", 0))
		if err != nil {
			return true, err
		}
		return true, nil
	}

	handled, err := h.handledPSQLCommands(message.String)
	if handled || err != nil {
		return true, err
	}

	// TODO: Remove this once we support `SELECT * FROM function()` syntax
	// Github issue: https://github.com/dolthub/doltgresql/issues/464
	handled, err = h.handledWorkbenchCommands(message.String)
	if handled || err != nil {
		return true, err
	}

	statements, err := h.convertQuery(message.String)
	if err != nil {
		return true, err
	}

	// A query message destroys the unnamed statement and the unnamed portal
	h.deletePreparedStatement("")
	h.deletePortal("")

	for _, statement := range statements {
		// Certain statement types get handled directly by the handler instead of being passed to the engine
		handled, endOfMessages, err = h.handleStatementOutsideEngine(statement)
		if handled {
			if err != nil {
				h.logger.Warnf("Failed to handle statement %v outside engine: %v", statement, err)
				return true, err
			}
		} else {
			if err != nil {
				h.logger.Warnf("Failed to handle statement %v outside engine: %v", statement, err)
			}
			endOfMessages, err = true, h.run(statement)
			if err != nil {
				return true, err
			}
		}
	}

	return endOfMessages, nil
}

// handleStatementOutsideEngine handles any queries that should be handled by the handler directly, rather than being
// passed to the engine. The response parameter |handled| is true if the query was handled, |endOfMessages| is true
// if no more messages are expected for this query and server should send the client a READY FOR QUERY message,
// and any error that occurred while handling the query.
func (h *ConnectionHandler) handleStatementOutsideEngine(statement ConvertedStatement) (handled bool, endOfMessages bool, err error) {
	switch stmt := statement.AST.(type) {
	case *tree.Deallocate:
		// TODO: handle ALL keyword
		return true, true, h.deallocatePreparedStatement(stmt.Name.String(), h.preparedStatements, statement, h.Conn())
	case *tree.Discard:
		return true, true, h.discardAll(statement)
	case *tree.CopyFrom:
		// When copying data from STDIN, the data is sent to the server as CopyData messages
		// We send endOfMessages=false since the server will be in COPY DATA mode and won't
		// be ready for more queries util COPY DATA mode is completed.
		if stmt.Stdin {
			return true, false, h.handleCopyFromStdinQuery(statement, stmt, "")
		}
	case *tree.CopyTo:
		return true, true, h.handleCopyToStdout(statement, stmt, "" /* unused */, stmt.Options.CopyFormat, "")
	}

	if statement.Tag == "COPY" {
		if target, format, options, ok := ParseCopyFrom(statement.String); ok {
			stmt, err := parser.ParseOne("COPY " + target + " FROM STDIN")
			if err != nil {
				return false, true, err
			}
			copyFrom := stmt.AST.(*tree.CopyFrom)
			copyFrom.Options.CopyFormat = format
			return true, false, h.handleCopyFromStdinQuery(statement, copyFrom, options)
		}
		if subquery, format, options, ok := ParseCopyTo(statement.String); ok {
			if strings.HasPrefix(subquery, "(") && strings.HasSuffix(subquery, ")") {
				// subquery may be richer than Postgres supports, so we just pass it as a string
				return true, true, h.handleCopyToStdout(statement, nil, subquery, format, options)
			}
			// subquery is "table [(column_list)]", so we can parse it and pass the AST
			stmt, err := parser.ParseOne("COPY " + subquery + " TO STDOUT")
			if err != nil {
				return false, true, err
			}
			copyTo := stmt.AST.(*tree.CopyTo)
			copyTo.Options.CopyFormat = format
			return true, true, h.handleCopyToStdout(statement, copyTo, "", format, options)
		}
	}

	handled, err = h.handlePgCatalogQueries(statement)
	if handled || err != nil {
		return true, true, err
	}

	return false, true, nil
}

// handleParse handles a parse message, returning any error that occurs
func (h *ConnectionHandler) handleParse(message *pgproto3.Parse) error {
	h.waitForSync = true

	// TODO: "Named prepared statements must be explicitly closed before they can be redefined by another Parse message, but this is not required for the unnamed statement"
	statements, err := h.convertQuery(message.Query)
	if err != nil {
		return err
	}

	// TODO(Noy): handle multiple statements
	statement := statements[0]
	if statement.AST == nil {
		// special case: empty query
		h.preparedStatements[message.Name] = PreparedStatementData{
			Statement: statement,
		}
		return nil
	}

	handledOutsideEngine, err := shouldQueryBeHandledInPlace(statement)
	if err != nil {
		return err
	}
	if handledOutsideEngine {
		h.preparedStatements[message.Name] = PreparedStatementData{
			Statement:    statement,
			ReturnFields: nil,
			BindVarTypes: nil,
			Stmt:         nil,
			Closed:       new(atomic.Bool),
		}
		return h.send(&pgproto3.ParseComplete{})
	}

	stmt, params, fields, err := h.duckHandler.ComPrepareParsed(context.Background(), h.mysqlConn, statement.String, statement.AST)
	if err != nil {
		return err
	}

	if !statement.PgParsable {
		statement.Tag = GetStatementTag(stmt)
	}

	// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
	// > A parameter data type can be left unspecified by setting it to zero,
	// > or by making the array of parameter type OIDs shorter than the number of
	// > parameter symbols ($n)used in the query string.
	// > ...
	// > Parameter data types can be specified by OID;
	// > if not given, the parser attempts to infer the data types in the same way
	// > as it would do for untyped literal string constants.
	bindVarTypes := message.ParameterOIDs
	if len(bindVarTypes) < len(params) {
		bindVarTypes = append(bindVarTypes, params[len(bindVarTypes):]...)
	}
	for i := range params {
		if bindVarTypes[i] == 0 {
			bindVarTypes[i] = params[i]
		}
	}
	h.preparedStatements[message.Name] = PreparedStatementData{
		Statement:    statement,
		ReturnFields: fields,
		BindVarTypes: bindVarTypes,
		Stmt:         stmt,
		Closed:       new(atomic.Bool),
	}

	return h.send(&pgproto3.ParseComplete{})
}

// handleDescribe handles a Describe message, returning any error that occurs
func (h *ConnectionHandler) handleDescribe(message *pgproto3.Describe) error {
	var fields []pgproto3.FieldDescription
	var bindvarTypes []uint32
	var tag string

	h.waitForSync = true
	if message.ObjectType == 'S' {
		preparedStatementData, ok := h.preparedStatements[message.Name]
		if !ok {
			return fmt.Errorf("prepared statement %s does not exist", message.Name)
		}

		// https://www.postgresql.org/docs/current/protocol-flow.html
		// > Note that since Bind has not yet been issued, the formats to be used for returned columns are not yet known to the backend;
		// > the format code fields in the RowDescription message will be zeroes in this case.
		if preparedStatementData.Stmt != nil {
			fields = slices.Clone(preparedStatementData.ReturnFields)
			for i := range fields {
				fields[i].Format = 0
			}

			bindvarTypes = preparedStatementData.BindVarTypes
			tag = preparedStatementData.Statement.Tag
		}

		if bindvarTypes == nil {
			bindvarTypes = make([]uint32, 0)
		}
	} else {
		portalData, ok := h.portals[message.Name]
		if !ok {
			return fmt.Errorf("portal %s does not exist", message.Name)
		}

		if portalData.Stmt != nil {
			fields = portalData.Fields
			tag = portalData.Statement.Tag
		}
	}

	return h.sendDescribeResponse(fields, bindvarTypes, tag)
}

// handleBind handles a bind message, returning any error that occurs
func (h *ConnectionHandler) handleBind(message *pgproto3.Bind) error {
	h.waitForSync = true

	// TODO: a named portal object lasts till the end of the current transaction, unless explicitly destroyed
	//  we need to destroy the named portal as a side effect of the transaction ending
	logrus.Tracef("binding portal %q to prepared statement %s", message.DestinationPortal, message.PreparedStatement)
	preparedData, ok := h.preparedStatements[message.PreparedStatement]
	if !ok {
		return fmt.Errorf("prepared statement %s does not exist", message.PreparedStatement)
	}

	if preparedData.Stmt == nil {
		h.portals[message.DestinationPortal] = PortalData{
			Statement: preparedData.Statement,
			Fields:    nil,
			Stmt:      nil,
			Vars:      nil,
		}
		return h.send(&pgproto3.BindComplete{})
	}

	if preparedData.Statement.AST == nil {
		// special case: empty query
		h.portals[message.DestinationPortal] = PortalData{
			Statement:    preparedData.Statement,
			IsEmptyQuery: true,
		}
		return h.send(&pgproto3.BindComplete{})
	}

	bindVars, err := h.convertBindParameters(preparedData.BindVarTypes, message.ParameterFormatCodes, message.Parameters)
	if err != nil {
		return err
	}

	fields, err := h.duckHandler.ComBind(context.Background(), h.mysqlConn, preparedData, bindVars)
	if err != nil {
		return err
	}

	h.portals[message.DestinationPortal] = PortalData{
		Statement:         preparedData.Statement,
		Fields:            fields,
		ResultFormatCodes: message.ResultFormatCodes,
		Stmt:              preparedData.Stmt,
		Closed:            preparedData.Closed,
		Vars:              bindVars,
	}
	return h.send(&pgproto3.BindComplete{})
}

// handleExecute handles an execute message, returning any error that occurs
func (h *ConnectionHandler) handleExecute(message *pgproto3.Execute) error {
	h.waitForSync = true

	// TODO: implement the RowMax
	portalData, ok := h.portals[message.Portal]
	if !ok {
		return fmt.Errorf("portal %s does not exist", message.Portal)
	}

	logrus.Tracef("executing portal %s with contents %v", message.Portal, portalData)
	query := portalData.Statement

	if portalData.IsEmptyQuery {
		return h.send(&pgproto3.EmptyQueryResponse{})
	}

	// Certain statement types get handled directly by the handler instead of being passed to the engine
	handled, _, err := h.handleStatementOutsideEngine(query)
	if handled {
		return err
	}

	// |rowsAffected| gets altered by the callback below
	rowsAffected := int32(0)

	callback := h.spoolRowsCallback(query.Tag, &rowsAffected, true)
	err = h.duckHandler.ComExecuteBound(context.Background(), h.mysqlConn, portalData, callback)
	if err != nil {
		return err
	}

	return h.send(makeCommandComplete(query.Tag, rowsAffected))
}

func makeCommandComplete(tag string, rows int32) *pgproto3.CommandComplete {
	switch tag {
	case "INSERT", "DELETE", "UPDATE", "MERGE", "SELECT", "CREATE TABLE AS", "MOVE", "FETCH", "COPY":
		if tag == "INSERT" {
			tag = "INSERT 0"
		}
		tag = fmt.Sprintf("%s %d", tag, rows)
	}

	return &pgproto3.CommandComplete{
		CommandTag: []byte(tag),
	}
}

// handleCopyData handles the COPY DATA message, by loading the data sent from the client. The |stop| response parameter
// is true if the connection handler should shut down the connection, |endOfMessages| is true if no more COPY DATA
// messages are expected, and the server should tell the client that it is ready for the next query, and |err| contains
// any error that occurred while processing the COPY DATA message.
func (h *ConnectionHandler) handleCopyData(message *pgproto3.CopyData) (stop bool, endOfMessages bool, err error) {
	helper, messages, err := h.handleCopyDataHelper(message)
	if err != nil {
		h.copyFromStdinState.copyErr = err
	}
	return helper, messages, err
}

// handleCopyDataHelper is a helper function that should only be invoked by handleCopyData. handleCopyData wraps this
// function so that it can capture any returned error message and store it in the saved state.
func (h *ConnectionHandler) handleCopyDataHelper(message *pgproto3.CopyData) (stop bool, endOfMessages bool, err error) {
	if h.copyFromStdinState == nil {
		return false, true, fmt.Errorf("COPY DATA message received without a COPY FROM STDIN operation in progress")
	}

	// Grab a sql.Context.
	sqlCtx, err := h.duckHandler.NewContext(context.Background(), h.mysqlConn, "")
	if err != nil {
		return false, false, err
	}

	dataLoader := h.copyFromStdinState.dataLoader
	if dataLoader == nil {
		copyFrom := h.copyFromStdinState.copyFromStdinNode
		if copyFrom == nil {
			return false, false, fmt.Errorf("no COPY FROM STDIN node found")
		}
		table := h.copyFromStdinState.targetTable
		if table == nil {
			return false, true, fmt.Errorf("no target table found")
		}
		rawOptions := h.copyFromStdinState.rawOptions

		switch copyFrom.Options.CopyFormat {
		case CopyFormatArrow:
			dataLoader, err = NewArrowDataLoader(
				sqlCtx, h.duckHandler,
				copyFrom.Table.Schema(), table, copyFrom.Columns,
				rawOptions,
			)
		case tree.CopyFormatText:
			// Remove `\.` from the end of the message data, if it exists
			if bytes.HasSuffix(message.Data, []byte{'\\', '.', '\n'}) {
				message.Data = message.Data[:len(message.Data)-3]
			}
			if bytes.HasSuffix(message.Data, []byte{'\\', '.', '\r', '\n'}) {
				message.Data = message.Data[:len(message.Data)-4]
			}
			fallthrough
		case tree.CopyFormatCSV:
			dataLoader, err = NewCsvDataLoader(
				sqlCtx, h.duckHandler,
				copyFrom.Table.Schema(), table, copyFrom.Columns,
				&copyFrom.Options,
				rawOptions,
			)
		case tree.CopyFormatBinary:
			err = fmt.Errorf("BINARY format is not supported for COPY FROM")
		default:
			err = fmt.Errorf("unknown format specified for COPY FROM: %v", copyFrom.Options.CopyFormat)
		}

		if err != nil {
			return false, false, err
		}

		ready := dataLoader.Start()
		if err, hasErr := <-ready; hasErr {
			return false, false, err
		}

		h.copyFromStdinState.dataLoader = dataLoader
	}

	if err = dataLoader.LoadChunk(sqlCtx, message.Data); err != nil {
		return false, false, err
	}

	// We expect to see more CopyData messages until we see either a CopyDone or CopyFail message, so
	// return false for endOfMessages
	return false, false, nil
}

// handleCopyDone handles a COPY DONE message by finalizing the in-progress COPY DATA operation and committing the
// loaded table data. The |stop| response parameter is true if the connection handler should shut down the connection,
// |endOfMessages| is true if no more COPY DATA messages are expected, and the server should tell the client that it is
// ready for the next query, and |err| contains any error that occurred while processing the COPY DATA message.
func (h *ConnectionHandler) handleCopyDone(_ *pgproto3.CopyDone) (stop bool, endOfMessages bool, err error) {
	if h.copyFromStdinState == nil {
		return false, true,
			fmt.Errorf("COPY DONE message received without a COPY FROM STDIN operation in progress")
	}

	// If there was a previous error returned from processing a CopyData message, then don't return an error here
	// and don't send endOfMessage=true, since the CopyData error already sent endOfMessage=true. If we do send
	// endOfMessage=true here, then the client gets confused about the unexpected/extra Idle message since the
	// server has already reported it was idle in the last message after the returned error.
	if h.copyFromStdinState.copyErr != nil {
		return false, false, nil
	}

	dataLoader := h.copyFromStdinState.dataLoader
	if dataLoader == nil {
		return false, true,
			fmt.Errorf("no data loader found for COPY FROM STDIN operation")
	}

	sqlCtx, err := h.duckHandler.NewContext(context.Background(), h.mysqlConn, "")
	if err != nil {
		return false, false, err
	}

	loadDataResults, err := dataLoader.Finish(sqlCtx)
	if err != nil {
		return false, false, err
	}

	h.copyFromStdinState = nil
	// We send back endOfMessage=true, since the COPY DONE message ends the COPY DATA flow and the server is ready
	// to accept the next query now.
	return false, true, h.send(&pgproto3.CommandComplete{
		CommandTag: []byte(fmt.Sprintf("COPY %d", loadDataResults.RowsLoaded)),
	})
}

// handleCopyFail handles a COPY FAIL message by aborting the in-progress COPY DATA operation.  The |stop| response
// parameter is true if the connection handler should shut down the connection, |endOfMessages| is true if no more
// COPY DATA messages are expected, and the server should tell the client that it is ready for the next query, and
// |err| contains any error that occurred while processing the COPY DATA message.
func (h *ConnectionHandler) handleCopyFail(_ *pgproto3.CopyFail) (stop bool, endOfMessages bool, err error) {
	if h.copyFromStdinState == nil {
		return false, true,
			fmt.Errorf("COPY FAIL message received without a COPY FROM STDIN operation in progress")
	}

	dataLoader := h.copyFromStdinState.dataLoader
	if dataLoader == nil {
		return false, true,
			fmt.Errorf("no data loader found for COPY FROM STDIN operation")
	}

	h.copyFromStdinState = nil
	// We send back endOfMessage=true, since the COPY FAIL message ends the COPY DATA flow and the server is ready
	// to accept the next query now.
	return false, true, nil
}

func (h *ConnectionHandler) deallocatePreparedStatement(name string, preparedStatements map[string]PreparedStatementData, query ConvertedStatement, conn net.Conn) error {
	_, ok := preparedStatements[name]
	if !ok {
		return fmt.Errorf("prepared statement %s does not exist", name)
	}
	h.deletePreparedStatement(name)

	return h.send(&pgproto3.CommandComplete{
		CommandTag: []byte(query.Tag),
	})
}

func (h *ConnectionHandler) deletePreparedStatement(name string) {
	ps, ok := h.preparedStatements[name]
	if ok {
		delete(h.preparedStatements, name)
		if ps.Closed.CompareAndSwap(false, true) {
			ps.Stmt.Close()
		}
	}
}

func (h *ConnectionHandler) deletePortal(name string) {
	p, ok := h.portals[name]
	if ok {
		delete(h.portals, name)
		if p.Closed.CompareAndSwap(false, true) {
			p.Stmt.Close()
		}
	}
}

// convertBindParameters handles the conversion from bind parameters to variable values.
func (h *ConnectionHandler) convertBindParameters(types []uint32, formatCodes []int16, values [][]byte) ([]any, error) {
	if len(types) != len(values) {
		return nil, fmt.Errorf("number of values does not match number of parameters")
	}
	bindings := make([]pgtype.Text, len(values))
	for i := range values {
		typ := types[i]
		// We'll rely on a library to decode each format, which will deal with text and binary representations for us
		if err := h.pgTypeMap.Scan(typ, formatCodes[i], values[i], &bindings[i]); err != nil {
			return nil, err
		}
	}

	vars := make([]any, len(bindings))
	for i, b := range bindings {
		vars[i] = b.String
	}
	return vars, nil
}

// run runs the given statement and sends a CommandComplete message to the client
func (h *ConnectionHandler) run(statement ConvertedStatement) error {
	h.logger.Tracef("running statement %v", statement)

	// |rowsAffected| gets altered by the callback below
	rowsAffected := int32(0)

	// Get the accurate statement tag for the statement
	if !statement.PgParsable && !IsWellKnownStatementTag(statement.Tag) {
		tag, err := h.duckHandler.getStatementTag(h.mysqlConn, statement.String)
		if err != nil {
			return err
		}
		h.logger.Tracef("getting statement tag for statement %v via preparing in DuckDB: %s", statement, tag)
		statement.Tag = tag
	}

	if statement.SubscriptionConfig != nil {
		return h.executeSubscriptionSQL(statement.SubscriptionConfig)
	} else if statement.BackupConfig != nil {
		msg, err := h.executeBackup(statement.BackupConfig)
		if err != nil {
			return err
		}
		return h.send(&pgproto3.ErrorResponse{
			Message: msg,
		})
	}

	callback := h.spoolRowsCallback(statement.Tag, &rowsAffected, false)
	if err := h.duckHandler.ComQuery(
		context.Background(),
		h.mysqlConn,
		statement.String,
		statement.AST,
		callback,
	); err != nil {
		return fmt.Errorf("fallback statement execution failed: %w", err)
	}

	return h.send(makeCommandComplete(statement.Tag, rowsAffected))
}

// spoolRowsCallback returns a callback function that will send RowDescription message,
// then a DataRow message for each row in the result set.
func (h *ConnectionHandler) spoolRowsCallback(tag string, rows *int32, isExecute bool) func(res *Result) error {
	// IsIUD returns whether the query is either an INSERT, UPDATE, or DELETE query.
	isIUD := tag == "INSERT" || tag == "UPDATE" || tag == "DELETE"
	return func(res *Result) error {
		logrus.Tracef("spooling %d rows for tag %s (execute = %v)", res.RowsAffected, tag, isExecute)
		if returnsRow(tag) {
			// EXECUTE does not send RowDescription; instead it should be sent from DESCRIBE prior to it
			if !isExecute {
				logrus.Tracef("sending RowDescription %+v for tag %s", res.Fields, tag)
				if err := h.send(&pgproto3.RowDescription{
					Fields: res.Fields,
				}); err != nil {
					return err
				}
			}

			logrus.Tracef("sending Rows %+v for tag %s", res.Rows, tag)
			for _, row := range res.Rows {
				if err := h.send(&pgproto3.DataRow{
					Values: row.val,
				}); err != nil {
					return err
				}
			}
		}

		if isIUD {
			*rows = int32(res.RowsAffected)
		} else {
			*rows += int32(len(res.Rows))
		}

		return nil
	}
}

// sendDescribeResponse sends a response message for a Describe message
func (h *ConnectionHandler) sendDescribeResponse(fields []pgproto3.FieldDescription, types []uint32, tag string) error {
	// The prepared statement variant of the describe command returns the OIDs of the parameters.
	if types != nil {
		if err := h.send(&pgproto3.ParameterDescription{
			ParameterOIDs: types,
		}); err != nil {
			return err
		}
	}

	if returnsRow(tag) {
		// Both variants finish with a row description.
		return h.send(&pgproto3.RowDescription{
			Fields: fields,
		})
	} else {
		return h.send(&pgproto3.NoData{})
	}
}

// handledPSQLCommands handles the special PSQL commands, such as \l and \dt.
func (h *ConnectionHandler) handledPSQLCommands(statement string) (bool, error) {
	statement = strings.ToLower(statement)
	// Command: \l
	if statement == "select d.datname as \"name\",\n       pg_catalog.pg_get_userbyid(d.datdba) as \"owner\",\n       pg_catalog.pg_encoding_to_char(d.encoding) as \"encoding\",\n       d.datcollate as \"collate\",\n       d.datctype as \"ctype\",\n       d.daticulocale as \"icu locale\",\n       case d.datlocprovider when 'c' then 'libc' when 'i' then 'icu' end as \"locale provider\",\n       pg_catalog.array_to_string(d.datacl, e'\\n') as \"access privileges\"\nfrom pg_catalog.pg_database d\norder by 1;" {
		query, err := h.convertQuery(`select d.datname as "Name", 'postgres' as "Owner", 'UTF8' as "Encoding", 'en_US.UTF-8' as "Collate", 'en_US.UTF-8' as "Ctype", 'en-US' as "ICU Locale", case d.datlocprovider when 'c' then 'libc' when 'i' then 'icu' end as "locale provider", '' as "access privileges" from pg_catalog.pg_database d order by 1;`)
		if err != nil {
			return false, err
		}
		return true, h.run(query[0])
	}
	// Command: \l on psql 16
	if statement == "select\n  d.datname as \"name\",\n  pg_catalog.pg_get_userbyid(d.datdba) as \"owner\",\n  pg_catalog.pg_encoding_to_char(d.encoding) as \"encoding\",\n  case d.datlocprovider when 'c' then 'libc' when 'i' then 'icu' end as \"locale provider\",\n  d.datcollate as \"collate\",\n  d.datctype as \"ctype\",\n  d.daticulocale as \"icu locale\",\n  null as \"icu rules\",\n  pg_catalog.array_to_string(d.datacl, e'\\n') as \"access privileges\"\nfrom pg_catalog.pg_database d\norder by 1;" {
		query, err := h.convertQuery(`select d.datname as "Name", 'postgres' as "Owner", 'UTF8' as "Encoding", 'en_US.UTF-8' as "Collate", 'en_US.UTF-8' as "Ctype", 'en-US' as "ICU Locale", case d.datlocprovider when 'c' then 'libc' when 'i' then 'icu' end as "locale provider", '' as "access privileges" from pg_catalog.pg_database d order by 1;`)
		if err != nil {
			return false, err
		}
		return true, h.run(query[0])
	}
	// Command: \dt
	if statement == "select n.nspname as \"schema\",\n  c.relname as \"name\",\n  case c.relkind when 'r' then 'table' when 'v' then 'view' when 'm' then 'materialized view' when 'i' then 'index' when 's' then 'sequence' when 't' then 'toast table' when 'f' then 'foreign table' when 'p' then 'partitioned table' when 'i' then 'partitioned index' end as \"type\",\n  pg_catalog.pg_get_userbyid(c.relowner) as \"owner\"\nfrom pg_catalog.pg_class c\n     left join pg_catalog.pg_namespace n on n.oid = c.relnamespace\n     left join pg_catalog.pg_am am on am.oid = c.relam\nwhere c.relkind in ('r','p','')\n      and n.nspname <> 'pg_catalog'\n      and n.nspname !~ '^pg_toast'\n      and n.nspname <> 'information_schema'\n  and pg_catalog.pg_table_is_visible(c.oid)\norder by 1,2;" {
		return true, h.run(ConvertedStatement{
			String: `SELECT table_schema AS "Schema", TABLE_NAME AS "Name", 'table' AS "Type", 'postgres' AS "Owner" FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA <> 'pg_catalog' AND TABLE_SCHEMA <> 'information_schema' AND TABLE_TYPE = 'BASE TABLE' ORDER BY 2;`,
			Tag:    "SELECT",
		})
	}
	// Command: \d
	if statement == "select n.nspname as \"schema\",\n  c.relname as \"name\",\n  case c.relkind when 'r' then 'table' when 'v' then 'view' when 'm' then 'materialized view' when 'i' then 'index' when 's' then 'sequence' when 't' then 'toast table' when 'f' then 'foreign table' when 'p' then 'partitioned table' when 'i' then 'partitioned index' end as \"type\",\n  pg_catalog.pg_get_userbyid(c.relowner) as \"owner\"\nfrom pg_catalog.pg_class c\n     left join pg_catalog.pg_namespace n on n.oid = c.relnamespace\n     left join pg_catalog.pg_am am on am.oid = c.relam\nwhere c.relkind in ('r','p','v','m','s','f','')\n      and n.nspname <> 'pg_catalog'\n      and n.nspname !~ '^pg_toast'\n      and n.nspname <> 'information_schema'\n  and pg_catalog.pg_table_is_visible(c.oid)\norder by 1,2;" {
		return true, h.run(ConvertedStatement{
			String: `SELECT table_schema AS "Schema", TABLE_NAME AS "Name", IF(TABLE_TYPE = 'VIEW', 'view', 'table') AS "Type", 'postgres' AS "Owner" FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA <> 'pg_catalog' AND TABLE_SCHEMA <> 'information_schema' AND TABLE_TYPE = 'BASE TABLE' OR TABLE_TYPE = 'VIEW' ORDER BY 2;`,
			Tag:    "SELECT",
		})
	}
	// Alternate \d for psql 14
	if statement == "select n.nspname as \"schema\",\n  c.relname as \"name\",\n  case c.relkind when 'r' then 'table' when 'v' then 'view' when 'm' then 'materialized view' when 'i' then 'index' when 's' then 'sequence' when 's' then 'special' when 't' then 'toast table' when 'f' then 'foreign table' when 'p' then 'partitioned table' when 'i' then 'partitioned index' end as \"type\",\n  pg_catalog.pg_get_userbyid(c.relowner) as \"owner\"\nfrom pg_catalog.pg_class c\n     left join pg_catalog.pg_namespace n on n.oid = c.relnamespace\n     left join pg_catalog.pg_am am on am.oid = c.relam\nwhere c.relkind in ('r','p','v','m','s','f','')\n      and n.nspname <> 'pg_catalog'\n      and n.nspname !~ '^pg_toast'\n      and n.nspname <> 'information_schema'\n  and pg_catalog.pg_table_is_visible(c.oid)\norder by 1,2;" {
		return true, h.run(ConvertedStatement{
			String: `SELECT table_schema AS "Schema", TABLE_NAME AS "Name", IF(TABLE_TYPE = 'VIEW', 'view', 'table') AS "Type", 'postgres' AS "Owner" FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA <> 'pg_catalog' AND TABLE_SCHEMA <> 'information_schema' AND TABLE_TYPE = 'BASE TABLE' OR TABLE_TYPE = 'VIEW' ORDER BY 2;`,
			Tag:    "SELECT",
		})
	}
	// Command: \d table_name
	if strings.HasPrefix(statement, "select c.oid,\n  n.nspname,\n  c.relname\nfrom pg_catalog.pg_class c\n     left join pg_catalog.pg_namespace n on n.oid = c.relnamespace\nwhere c.relname operator(pg_catalog.~) '^(") && strings.HasSuffix(statement, ")$' collate pg_catalog.default\n  and pg_catalog.pg_table_is_visible(c.oid)\norder by 2, 3;") {
		// There are >at least< 15 separate statements sent for this command, which is far too much to validate and
		// implement, so we'll just return an error for now
		return true, fmt.Errorf("PSQL command not yet supported")
	}
	// Command: \dn
	if statement == "select n.nspname as \"name\",\n  pg_catalog.pg_get_userbyid(n.nspowner) as \"owner\"\nfrom pg_catalog.pg_namespace n\nwhere n.nspname !~ '^pg_' and n.nspname <> 'information_schema'\norder by 1;" {
		return true, h.run(ConvertedStatement{
			String: `SELECT 'public' AS "Name", 'pg_database_owner' AS "Owner";`,
			Tag:    "SELECT",
		})
	}
	// Command: \df
	if statement == "select n.nspname as \"schema\",\n  p.proname as \"name\",\n  pg_catalog.pg_get_function_result(p.oid) as \"result data type\",\n  pg_catalog.pg_get_function_arguments(p.oid) as \"argument data types\",\n case p.prokind\n  when 'a' then 'agg'\n  when 'w' then 'window'\n  when 'p' then 'proc'\n  else 'func'\n end as \"type\"\nfrom pg_catalog.pg_proc p\n     left join pg_catalog.pg_namespace n on n.oid = p.pronamespace\nwhere pg_catalog.pg_function_is_visible(p.oid)\n      and n.nspname <> 'pg_catalog'\n      and n.nspname <> 'information_schema'\norder by 1, 2, 4;" {
		return true, h.run(ConvertedStatement{
			String: `SELECT '' AS "Schema", '' AS "Name", '' AS "Result data type", '' AS "Argument data types", '' AS "Type" LIMIT 0;`,
			Tag:    "SELECT",
		})
	}
	// Command: \dv
	if statement == "select n.nspname as \"schema\",\n  c.relname as \"name\",\n  case c.relkind when 'r' then 'table' when 'v' then 'view' when 'm' then 'materialized view' when 'i' then 'index' when 's' then 'sequence' when 't' then 'toast table' when 'f' then 'foreign table' when 'p' then 'partitioned table' when 'i' then 'partitioned index' end as \"type\",\n  pg_catalog.pg_get_userbyid(c.relowner) as \"owner\"\nfrom pg_catalog.pg_class c\n     left join pg_catalog.pg_namespace n on n.oid = c.relnamespace\nwhere c.relkind in ('v','')\n      and n.nspname <> 'pg_catalog'\n      and n.nspname !~ '^pg_toast'\n      and n.nspname <> 'information_schema'\n  and pg_catalog.pg_table_is_visible(c.oid)\norder by 1,2;" {
		return true, h.run(ConvertedStatement{
			String: `SELECT table_schema AS "Schema", TABLE_NAME AS "Name", 'view' AS "Type", 'postgres' AS "Owner" FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA <> 'pg_catalog' AND TABLE_SCHEMA <> 'information_schema' AND TABLE_TYPE = 'VIEW' ORDER BY 2;`,
			Tag:    "SELECT",
		})
	}
	// Command: \du
	if statement == "select r.rolname, r.rolsuper, r.rolinherit,\n  r.rolcreaterole, r.rolcreatedb, r.rolcanlogin,\n  r.rolconnlimit, r.rolvaliduntil,\n  array(select b.rolname\n        from pg_catalog.pg_auth_members m\n        join pg_catalog.pg_roles b on (m.roleid = b.oid)\n        where m.member = r.oid) as memberof\n, r.rolreplication\n, r.rolbypassrls\nfrom pg_catalog.pg_roles r\nwhere r.rolname !~ '^pg_'\norder by 1;" {
		// We don't support users yet, so we'll just return nothing for now
		return true, h.run(ConvertedStatement{
			String: `SELECT '' FROM dual LIMIT 0;`,
			Tag:    "SELECT",
		})
	}
	return false, nil
}

// handledWorkbenchCommands handles commands used by some workbenches, such as dolt-workbench.
func (h *ConnectionHandler) handledWorkbenchCommands(statement string) (bool, error) {
	lower := strings.ToLower(statement)
	if lower == "select * from current_schema()" || lower == "select * from current_schema();" {
		return true, h.run(ConvertedStatement{
			String: `SELECT search_path AS "current_schema";`,
			Tag:    "SELECT",
		})
	}
	if lower == "select * from current_database()" || lower == "select * from current_database();" {
		return true, h.run(ConvertedStatement{
			String: `SELECT DATABASE() AS "current_database";`,
			Tag:    "SELECT",
		})
	}
	return false, nil
}

// endOfMessages should be called from HandleConnection or a function within HandleConnection. This represents the end
// of the message slice, which may occur naturally (all relevant response messages have been sent) or on error. Once
// endOfMessages has been called, no further messages should be sent, and the connection loop should wait for the next
// query. A nil error should be provided if this is being called naturally.
func (h *ConnectionHandler) endOfMessages(err error) {
	if err != nil {
		h.sendError(err)
	}
	if sendErr := h.send(&pgproto3.ReadyForQuery{
		TxStatus: byte(ReadyForQueryTransactionIndicator_Idle),
	}); sendErr != nil {
		// We panic here for the same reason as above.
		panic(sendErr)
	}
}

// sendError sends the given error to the client. This should generally never be called directly.
func (h *ConnectionHandler) sendError(err error) {
	fmt.Println(err.Error())
	if sendErr := h.send(&pgproto3.ErrorResponse{
		Severity: string(ErrorResponseSeverity_Error),
		Code:     "XX000", // internal_error for now
		Message:  err.Error(),
	}); sendErr != nil {
		// If we're unable to send anything to the connection, then there's something wrong with the connection and
		// we should terminate it. This will be caught in HandleConnection's defer block.
		panic(sendErr)
	}
}

// convertQuery takes the given Postgres query, and converts it as a list of ast.ConvertedStatement that will work with the handler.
func (h *ConnectionHandler) convertQuery(query string, modifiers ...QueryModifier) ([]ConvertedStatement, error) {
	for _, modifier := range modifiers {
		query = modifier(query)
	}

	// Check if the query is a subscription query, and if so, parse it as a subscription query.
	subscriptionConfig, err := parseSubscriptionSQL(query)
	if subscriptionConfig != nil && err == nil {
		return []ConvertedStatement{{
			String:             query,
			PgParsable:         true,
			SubscriptionConfig: subscriptionConfig,
		}}, nil
	}

	// Check if the query is a backup query, and if so, parse it as a backup query.
	backupConfig, err := parseBackupSQL(query)
	if backupConfig != nil && err == nil {
		return []ConvertedStatement{{
			String:       query,
			PgParsable:   true,
			BackupConfig: backupConfig,
		}}, nil
	}

	stmts, err := parser.Parse(query)
	if err != nil {
		// DuckDB syntax is not fully compatible with PostgreSQL, so we need to handle some queries differently.
		stmts, _ = parser.Parse("SELECT 'SQL syntax is incompatible with PostgreSQL' AS error")
		return []ConvertedStatement{{
			String:     query,
			AST:        stmts[0].AST,
			Tag:        GuessStatementTag(query),
			PgParsable: false,
		}}, nil
	}

	if len(stmts) == 0 {
		return []ConvertedStatement{{String: query}}, nil
	}

	convertedStmts := make([]ConvertedStatement, len(stmts))
	for i, stmt := range stmts {
		convertedStmts[i].String = stmt.SQL
		convertedStmts[i].AST = stmt.AST
		convertedStmts[i].Tag = stmt.AST.StatementTag()
		convertedStmts[i].PgParsable = true
	}
	return convertedStmts, nil
}

// discardAll handles the DISCARD ALL command
func (h *ConnectionHandler) discardAll(query ConvertedStatement) error {
	h.closeBackendConn()

	return h.send(&pgproto3.CommandComplete{
		CommandTag: []byte(query.Tag),
	})
}

// handleCopyFromStdinQuery handles the COPY FROM STDIN query at the Doltgres layer, without passing it to the engine.
// COPY FROM STDIN can't be handled directly by the GMS engine, since COPY FROM STDIN relies on multiple messages sent
// over the wire.
func (h *ConnectionHandler) handleCopyFromStdinQuery(
	query ConvertedStatement, copyFrom *tree.CopyFrom,
	rawOptions string, // For non-PG-parseable COPY FROM
) error {
	sqlCtx, err := h.duckHandler.NewContext(context.Background(), h.mysqlConn, query.String)
	if err != nil {
		return err
	}
	sqlCtx.SetLogger(sqlCtx.GetLogger().WithField("query", query.String))

	table, err := ValidateCopyFrom(copyFrom, sqlCtx)
	if err != nil {
		return err
	}

	h.copyFromStdinState = &copyFromStdinState{
		copyFromStdinNode: copyFrom,
		targetTable:       table,
		rawOptions:        rawOptions,
	}

	var format byte
	switch copyFrom.Options.CopyFormat {
	case tree.CopyFormatText, tree.CopyFormatCSV, CopyFormatJSON:
		format = 0 // text format
	default:
		format = 1 // binary format
	}

	return h.send(&pgproto3.CopyInResponse{
		OverallFormat: format,
	})
}

// DiscardToSync discards all messages in the buffer until a Sync has been reached. If a Sync was never sent, then this
// may cause the connection to lock until the client send a Sync, as their request structure was malformed.
func (h *ConnectionHandler) discardToSync() error {
	for {
		message, err := h.backend.Receive()
		if err != nil {
			return err
		}

		if _, ok := message.(*pgproto3.Sync); ok {
			return nil
		}
	}
}

// Send sends the given message over the connection.
func (h *ConnectionHandler) send(message pgproto3.BackendMessage) error {
	h.backend.Send(message)
	return h.backend.Flush()
}

// returnsRow returns whether the query returns set of rows such as SELECT and FETCH statements.
func returnsRow(tag string) bool {
	switch tag {
	case "SELECT", "SHOW", "FETCH", "EXPLAIN", "SHOW TABLES":
		return true
	default:
		return false
	}
}

func (h *ConnectionHandler) handleCopyToStdout(query ConvertedStatement, copyTo *tree.CopyTo, subquery string, format tree.CopyFormat, rawOptions string) error {
	ctx, err := h.duckHandler.NewContext(context.Background(), h.mysqlConn, query.String)
	if err != nil {
		return err
	}
	ctx.SetLogger(ctx.GetLogger().WithField("query", query.String))

	// Create cancelable context
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctx = ctx.WithContext(childCtx)

	var (
		schema  string
		table   sql.Table
		columns tree.NameList
		stmt    string
		options *tree.CopyOptions
	)

	if copyTo != nil {
		// PG-parsable COPY TO
		table, err = ValidateCopyTo(copyTo, ctx)
		if err != nil {
			return err
		}
		if copyTo.Statement != nil {
			stmt = `(` + copyTo.Statement.String() + `)`
		}
		schema = copyTo.Table.Schema()
		columns = copyTo.Columns
		options = &copyTo.Options
	} else {
		// Non-PG-parsable COPY TO, which is parsed via regex.
		stmt = subquery
		options = &tree.CopyOptions{
			CopyFormat: format,
			HasFormat:  true,
		}
	}

	var writer DataWriter

	switch format {
	case CopyFormatArrow:
		writer, err = NewArrowWriter(
			ctx, h.duckHandler,
			schema, table, columns,
			stmt,
			rawOptions,
		)
	default:
		writer, err = NewDuckDataWriter(
			ctx, h.duckHandler,
			schema, table, columns,
			stmt,
			options, rawOptions,
		)
	}
	if err != nil {
		return err
	}
	defer writer.Close()

	pipePath, ch, err := writer.Start()
	if err != nil {
		return err
	}

	done := make(chan struct{})
	var sendErr atomic.Value
	go func() {
		defer close(done)

		// Open the pipe for reading.
		ctx.GetLogger().Tracef("Opening FIFO pipe for reading: %s", pipePath)
		pipe, err := os.OpenFile(pipePath, os.O_RDONLY, os.ModeNamedPipe)
		if err != nil {
			sendErr.Store(fmt.Errorf("failed to open pipe for reading: %w", err))
			cancel()
			return
		}
		defer pipe.Close()

		ctx.GetLogger().Debug("Copying data from the pipe to the client")
		defer func() {
			ctx.GetLogger().Debug("Finished copying data from the pipe to the client")
		}()

		sendCopyOutResponse := func(numberOfColumns int) error {
			ctx.GetLogger().Debug("sending CopyOutResponse to the client")
			columnsFormatCodes := make([]uint16, numberOfColumns)
			copyOutResponse := &pgproto3.CopyOutResponse{
				OverallFormat:     0,                  // 0 for text format
				ColumnFormatCodes: columnsFormatCodes, // 0 for text format
			}
			return h.send(copyOutResponse)
		}

		sendCopyData := func(copyData []byte) error {
			ctx.GetLogger().Debugf("sending CopyData (%d bytes) to the client", len(copyData))
			return h.send(&pgproto3.CopyData{Data: copyData})
		}

		switch format {
		case tree.CopyFormatText:
			flag := true
			reader := bufio.NewReader(pipe)
			for {
				line, err := reader.ReadSlice('\n')
				if err != nil {
					if err == io.EOF {
						break
					}
					sendErr.Store(err)
					cancel()
					return
				}
				if flag {
					flag = false
					count := bytes.Count(line, []byte{'\t'})
					err := sendCopyOutResponse(count + 1)
					if err != nil {
						sendErr.Store(err)
						cancel()
						return
					}
				}
				err = sendCopyData(line)
				if err != nil {
					sendErr.Store(err)
					cancel()
					return
				}
			}
		default:
			err := sendCopyOutResponse(1)
			if err != nil {
				sendErr.Store(err)
				cancel()
				return
			}

			buf := make([]byte, 1<<20) // 1MB buffer
			for {
				n, err := pipe.Read(buf)
				if err != nil {
					if err == io.EOF {
						break
					}
					sendErr.Store(err)
					cancel()
					return
				}
				if n > 0 {
					err := sendCopyData(buf[:n])
					if err != nil {
						sendErr.Store(err)
						cancel()
						return
					}
				}
			}
		}
	}()

	select {
	case <-ctx.Done(): // Context is canceled
		<-done
		err, _ := sendErr.Load().(error)
		return errors.Join(ctx.Err(), err)
	case result := <-ch:
		<-done

		if result.Err != nil {
			return fmt.Errorf("failed to copy data: %w", result.Err)
		}

		if err, ok := sendErr.Load().(error); ok {
			return err
		}

		// After data is sent and the producer side is finished without errors, send CopyDone
		ctx.GetLogger().Debug("sending CopyDone to the client")
		if err := h.send(&pgproto3.CopyDone{}); err != nil {
			return err
		}

		// Send CommandComplete with the number of rows copied
		ctx.GetLogger().Debugf("sending CommandComplete to the client")
		return h.send(&pgproto3.CommandComplete{
			CommandTag: []byte(fmt.Sprintf("COPY %d", result.RowCount)),
		})
	}
}
