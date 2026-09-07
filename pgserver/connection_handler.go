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
	stdsql "database/sql"
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
	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/jackc/pgx/v5/pgconn"
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
	// implicitTx is the physical transaction opened for a protocol message
	// group when the client has not issued an explicit BEGIN. It is kept
	// separate from txStatus: PostgreSQL advertises T while the group is open,
	// but returns to I as soon as the group is committed or rolled back.
	implicitTx *stdsql.Tx
	// implicitTxCtx and implicitTxConn are the exact lifecycle identity captured
	// when implicitTx was opened. They must be reused for finalization; creating
	// a fresh context can observe a replacement pool binding.
	implicitTxCtx      *sql.Context
	implicitTxConn     *stdsql.Conn
	implicitTxPromoted bool
	// deferredCommandComplete is used only by the final statement of a simple
	// query. Its command tag is held until the implicit transaction commit has
	// succeeded, matching PostgreSQL's completion ordering on commit failure.
	deferredCommandComplete bool
	// pendingCommandCompletes are response tags whose statement has finished,
	// but whose message-group transaction still needs to commit. COPY uses this
	// queue in the extended protocol because the client may send Sync only after
	// receiving all data frames. Keep a queue rather than a single slot so a
	// pipelined group cannot silently lose an earlier COPY completion.
	pendingCommandCompletes []*pgproto3.CommandComplete
	// pendingProtocolMessages is the ordered form of the deferred response
	// queue. Most statements only need a CommandComplete, but in-place handlers
	// such as SET also emit ParameterStatus; keeping both messages in one queue
	// preserves their wire order until an implicit simple-query commit succeeds.
	pendingProtocolMessages []pgproto3.BackendMessage
	// txStatus is the transaction state advertised by ReadyForQuery. It is
	// deliberately kept at the protocol layer because PostgreSQL transaction
	// control is handled outside GMS's transaction iterator.
	txStatus ReadyForQueryTransactionIndicator
	// copyFromStdinState is set when this connection is in the COPY FROM STDIN mode, meaning it is waiting on
	// COPY DATA messages from the client to import data into tables.
	copyFromStdinState *copyFromStdinState
	// pendingCopyRelease keeps a successful extended-protocol COPY-IN lease
	// alive from CopyDone through the following Sync, where the implicit
	// transaction is committed. It is idempotent and nil outside that window.
	pendingCopyRelease func()

	server   *Server
	readOnly bool
	logger   *logrus.Entry
}

// frontendContext explicitly marks ordinary PostgreSQL protocol work. The
// origin marker is propagated into sql.Context and eventually into the
// provider's per-connection initializer; unmarked and replication contexts
// remain fail-closed.
func frontendContext(ctx context.Context) context.Context {
	return mycontext.WithFrontendQuery(ctx)
}

// statementLogText keeps diagnostic logging useful without formatting the
// full ConvertedStatement. The latter may carry a BackupConfig/RestoreConfig
// whose ObjectStorageConfig contains client credentials, and prepared portals
// may also carry bound values that must never be serialized into logs.
func statementLogText(statement ConvertedStatement) string {
	if statement.BackupConfig != nil || statement.RestoreConfig != nil {
		// The parsed config contains ObjectStorage credentials even when the
		// original text was embedded in a larger statement. Never serialize that
		// text or config into diagnostics.
		return catalog.RedactedSensitiveSQL
	}
	query := statement.QueryForAudit()
	if query == "" {
		query = statement.String
	}
	return catalog.RedactSensitiveSQL(query)
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
func NewConnectionHandler(conn net.Conn, handler mysql.Handler, engine *gms.Engine, sm *server.SessionManager, connID uint32, server *Server, readOnly bool) *ConnectionHandler {
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

	connectionHandler := ConnectionHandler{
		mysqlConn:          mysqlConn,
		preparedStatements: preparedStatements,
		portals:            portals,
		duckHandler:        duckHandler,
		backend:            pgproto3.NewBackend(conn, conn),
		pgTypeMap:          pgtype.NewMap(),
		txStatus:           ReadyForQueryTransactionIndicator_Idle,

		server:   server,
		readOnly: readOnly,
		logger: logrus.WithFields(logrus.Fields{
			"connectionID": connID,
			"protocol":     "pg",
		}),
	}
	connectionHandler.duckHandler.SetConnectionHandler(&connectionHandler)
	return &connectionHandler
}

// readyForQueryStatus returns a valid PostgreSQL transaction indicator. Keep
// the zero value usable for tests and for any handler constructed without the
// normal constructor.
func (h *ConnectionHandler) readyForQueryStatus() ReadyForQueryTransactionIndicator {
	switch h.txStatus {
	case ReadyForQueryTransactionIndicator_Idle,
		ReadyForQueryTransactionIndicator_TransactionBlock,
		ReadyForQueryTransactionIndicator_FailedTransactionBlock:
		return h.txStatus
	default:
		h.txStatus = ReadyForQueryTransactionIndicator_Idle
		return h.txStatus
	}
}

// markTransactionError records an error in an explicit transaction block.
// Errors while idle do not create a transaction, and a failed block remains
// failed until COMMIT or ROLLBACK finalizes it.
func (h *ConnectionHandler) markTransactionError() {
	// An implicit protocol scope is rolled back immediately by its owning
	// message handler. Do not expose that transient scope as PostgreSQL's sticky
	// failed explicit block.
	if h.implicitTx != nil && !h.implicitTxPromoted {
		return
	}
	if h.readyForQueryStatus() == ReadyForQueryTransactionIndicator_TransactionBlock {
		h.txStatus = ReadyForQueryTransactionIndicator_FailedTransactionBlock
	}
}

// observeTransactionBinding reports whether the session still has a physical
// transaction bound to this PostgreSQL connection.  A real handler can inspect
// the pool through a fresh lifecycle context; minimal unit-test handlers do not
// have a DuckHandler/session and are treated as having no observable binding.
// The second return value is false when the production binding could not be
// inspected, in which case callers must fail closed and retain a non-idle
// protocol state.
func (h *ConnectionHandler) observeTransactionBinding() (*stdsql.Tx, bool) {
	if h == nil {
		return nil, true
	}
	if h.implicitTx != nil {
		if h.implicitTxCtx == nil {
			return h.implicitTx, true
		}
		_, tx := adapter.TryGetTxnBindingForRelease(h.implicitTxCtx)
		return tx, true
	}
	if h.duckHandler == nil || h.mysqlConn == nil {
		return nil, true
	}
	ctx, err := h.newProtocolSQLContext("")
	if err != nil || ctx == nil {
		return nil, false
	}
	_, tx := adapter.TryGetTxnBindingForRelease(ctx)
	return tx, true
}

// recordTransactionResult applies the protocol-visible state transition for a
// statement. COMMIT and ROLLBACK both leave the session idle even when their
// finalizer reports an error: the lifecycle code removes/evicts the physical
// transaction on such errors, and a rollback cleanup error must not advertise
// a transaction that has already been finalized.
func (h *ConnectionHandler) recordTransactionResult(stmt tree.Statement, err error) {
	switch stmt.(type) {
	case *tree.RollbackTransaction, *tree.CommitTransaction:
		bound, known := h.observeTransactionBinding()
		if known && bound == nil {
			// The transaction-control executor has finalized (or evicted) the
			// physical binding. Clear the protocol-owned marker as well so a
			// later boundary cannot attempt to finalize a stale transaction.
			h.clearImplicitScope()
			h.clearPortals()
			h.txStatus = ReadyForQueryTransactionIndicator_Idle
			return
		}
		// A failed explicit control, or a control that claimed success while
		// leaving a transaction bound, must not advertise Idle. Keep an
		// existing failed state; otherwise transition an active block to E.
		if !known || bound != nil {
			h.txStatus = ReadyForQueryTransactionIndicator_FailedTransactionBlock
		}
	case *tree.BeginTransaction:
		if err == nil && h.readyForQueryStatus() != ReadyForQueryTransactionIndicator_FailedTransactionBlock {
			h.txStatus = ReadyForQueryTransactionIndicator_TransactionBlock
			return
		}
		// BEGIN issued while T is already active returns 25001 but keeps the
		// original transaction block alive. Other BEGIN failures while idle also
		// leave the connection idle; neither case creates PostgreSQL's failed E
		// state.
		return
	default:
		if err != nil {
			h.markTransactionError()
		}
	}
}

func (h *ConnectionHandler) closeBackendConn() {
	// A client can disconnect while COPY FROM STDIN is still being fed by a
	// loader goroutine. Abort and wait for that loader before rolling back or
	// closing its physical owner; otherwise it can continue inserting after the
	// rollback (or race a reused connection).
	if h.copyFromStdinState != nil {
		if err := h.abortCopyFromStdinState(ErrCopyAborted); err != nil && h.logger != nil {
			h.logger.WithError(err).Error("Failed to abort COPY while closing backend connection")
		}
	}
	// An extended COPY may have completed its loader but is still waiting for
	// Sync to commit the implicit transaction. Disconnecting at that boundary
	// must release the deferred operation/snapshot lease as well.
	h.releasePendingCopyLease()
	if h.duckHandler == nil || h.mysqlConn == nil {
		return
	}

	// Resolve the binding once and retain its complete identity. A delayed close
	// from an old protocol handler must never roll back or close a replacement
	// transaction that reused the same logical session (or even the same
	// physical connection).
	ctx, err := h.newProtocolSQLContext("")
	if err != nil {
		if h.logger != nil {
			h.logger.WithError(err).Error("Failed to inspect PostgreSQL transaction on close")
		}
		return
	}
	if h.implicitTxCtx != nil {
		ctx = h.implicitTxCtx
	}
	expectedConn, expectedTx := adapter.TryGetTxnBindingForRelease(ctx)
	if h.implicitTx != nil {
		expectedConn = h.implicitTxConn
		expectedTx = h.implicitTx
	}
	if expectedConn == nil {
		return
	}
	currentConn, currentTx := adapter.TryGetTxnBindingForRelease(ctx)
	if currentConn != expectedConn || currentTx != expectedTx {
		if h.logger != nil {
			h.logger.Warn("Skipping PostgreSQL close after transaction binding replacement")
		}
		return
	}

	var provider postgresRollbackOrphanCleaner
	if catalogProvider := h.duckHandler.GetCatalogProvider(); catalogProvider != nil {
		provider = catalogProvider
	}
	if expectedTx != nil {
		var rollbackErr error
		if h.implicitTx != nil && h.implicitScopeActive() {
			// Keep the existing implicit finalizer semantics (including protocol
			// state transitions); the post-finalization identity check below
			// prevents this teardown from closing a replacement binding.
			rollbackErr = h.finalizeImplicitPostgresTransaction(false)
		} else {
			rollbackErr = rollbackPostgresBinding(ctx, expectedConn, expectedTx, provider)
		}
		if rollbackErr != nil && h.logger != nil {
			h.logger.WithError(rollbackErr).Error("Failed to rollback PostgreSQL transaction on close")
		}
	}

	// A successful rollback removes the old transaction mapping. Recheck both
	// identities before closing; if a replacement appeared, leave it untouched.
	currentConn, currentTx = adapter.TryGetTxnBindingForRelease(ctx)
	if currentConn != expectedConn || currentTx != nil {
		return
	}
	if closeErr := adapter.CloseConnIfBinding(ctx, expectedConn, nil); closeErr != nil && h.logger != nil {
		h.logger.WithError(closeErr).Error("Failed to close backend connection")
	}
}

// rollbackPostgresBinding is the close-path variant of the protocol rollback
// helper. It carries the expected transaction through the adapter finalizer so
// a replacement cannot be selected between inspection and rollback.
func rollbackPostgresBinding(
	ctx *sql.Context,
	expectedConn *stdsql.Conn,
	expectedTx *stdsql.Tx,
	provider postgresRollbackOrphanCleaner,
) error {
	if expectedTx == nil {
		return nil
	}
	currentConn, currentTx := adapter.TryGetTxnBindingForRelease(ctx)
	if currentConn != expectedConn || currentTx != expectedTx {
		return fmt.Errorf("postgres transaction binding changed before close rollback")
	}
	var cleanup func(*stdsql.Conn) error
	releaseCleanup, reservationErr := beginPostgresRollbackCleanupReservationWithError(ctx, provider, expectedTx)
	if reservationErr == nil {
		defer releaseCleanup()
	} else {
		// Keep the physical rollback path usable after a poisoned-generation
		// admission failure; maintenance is skipped and the error is joined below.
		releaseCleanup = nil
	}
	if provider != nil && reservationErr == nil {
		cleanup = func(conn *stdsql.Conn) error {
			base := context.WithoutCancel(ctx)
			base = catalog.WithRollbackCleanupOwner(base, conn)
			base = catalog.WithDuckLakeCleanupLease(base)
			cleanupCtx := ctx.WithContext(base)
			return cleanupPostgresRollbackOrphans(cleanupCtx, &tree.RollbackTransaction{}, provider, conn)
		}
	}
	_, err := adapter.FinalizeRollbackWithCleanup(ctx, expectedTx, cleanup)
	return errors.Join(reservationErr, err)
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

			h.closePreparedObjects()
			// Release any transaction/connection-owned state before removing the
			// GMS session; rollback cleanup needs the session binding to remain
			// addressable.
			h.closeBackendConn()
			h.duckHandler.ConnectionClosed(h.mysqlConn)
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
		SecretKey: make([]byte, 4), // TODO: this should represent an ID that can uniquely identify this connection, so that CancelRequest will work
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
	if db == "postgres" || db == "mysql" {
		if provider := h.duckHandler.GetCatalogProvider(); provider != nil {
			db = provider.DefaultCatalogName()
		}
	}

	useStmt := fmt.Sprintf("USE %s.public;", db)
	setStmt := fmt.Sprintf("SET database TO %s;", db)
	parsed, err := parser.ParseOne(setStmt)
	if err != nil {
		return err
	}
	err = h.duckHandler.ComQuery(frontendContext(context.Background()), h.mysqlConn, useStmt, "", parsed.AST, func(res *Result) error {
		return nil
	})
	// If a database isn't specified, then we attempt to connect to a database with the same name as the user,
	// ignoring any error
	if err != nil && dbSpecified {
		_ = h.send(&pgproto3.ErrorResponse{
			Severity: string(ErrorResponseSeverity_Fatal),
			Code:     "3D000",
			Message:  fmt.Sprintf(`"database "%s" does not exist, err: %v"`, db, err),
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

				h.handleProtocolError(eomErr, !endOfMessages && h.waitForSync)
			}
		}()
	}

	msg, err := h.backend.Receive()
	if err != nil {
		return false, fmt.Errorf("error receiving message: %w", err)
	}

	// Message bodies can contain credentials or service-managed SQL. Keep the
	// receive-stage diagnostic useful without logging any client payload before
	// the protocol-specific sensitive-SQL gate runs.
	logrus.Debugf("Received message type: %T", msg)

	var stop bool
	stop, endOfMessages, err = h.handleMessage(msg)
	if err != nil {
		if syncErr := h.handleProtocolError(err, !endOfMessages && h.waitForSync); syncErr != nil {
			return false, syncErr
		}
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
	// COPY FROM STDIN switches the frontend into a dedicated sub-protocol.
	// PostgreSQL explicitly ignores Flush and Sync while the copy is still
	// active, accepts only CopyData/CopyDone/CopyFail, and treats every other
	// message as a protocol error. A state carrying copyErr is already a
	// terminal server-error sentinel; its messages are consumed by the normal
	// recovery path instead of generating a second error.
	if state := h.copyFromStdinState; state != nil {
		switch msg.(type) {
		case *pgproto3.Terminate, *pgproto3.CopyData, *pgproto3.CopyDone, *pgproto3.CopyFail:
			// Handled by the switch below (Terminate may close the connection).
		case *pgproto3.Flush:
			if state.copyErr != nil && !h.waitForSync {
				// Simple-query COPY already sent its ErrorResponse/ReadyForQuery;
				// release the terminal sentinel before accepting a new exchange.
				h.releaseCopyLease(state)
				h.copyFromStdinState = nil
			}
			return false, false, nil
		case *pgproto3.Sync:
			if state.copyErr == nil {
				// Sync is ignored during healthy COPY-in. In particular, do not
				// clear waitForSync or finalize the implicit transaction yet.
				return false, false, nil
			}
			if !h.waitForSync {
				// A simple-query error has no discard-until-Sync phase. Clear the
				// sentinel, then let the ordinary Sync branch provide its boundary.
				h.releaseCopyLease(state)
				h.copyFromStdinState = nil
			}
		default:
			if state.copyErr != nil {
				// Once COPY has reported an error, the extended protocol skips
				// all messages until Sync. discardToSync normally owns this loop;
				// retain the same behavior for direct handler calls as well.
				if h.waitForSync {
					return false, false, nil
				}
				// Simple-query COPY already returned to the normal command loop;
				// release the sentinel and process this message as a new exchange.
				h.releaseCopyLease(state)
				h.copyFromStdinState = nil
				break
			}
			protocolErr := &pgconn.PgError{
				Severity: string(ErrorResponseSeverity_Error),
				Code:     "08P01",
				Message:  fmt.Sprintf("unexpected message type %T during COPY FROM STDIN", msg),
			}
			if h.waitForSync {
				// Keep the terminal sentinel until discardToSync consumes the
				// client's Sync, just as a server-side CopyData failure does.
				err = h.abortCopyLoaderState(state, protocolErr)
				h.releaseCopyLease(state)
				return false, false, err
			}
			// Simple-query COPY has no Sync recovery boundary. Abort and clear
			// the state before returning the ErrorResponse/Ready pair.
			err = h.abortCopyFromStdinState(protocolErr)
			h.releaseCopyLease(state)
			return false, true, err
		}
	}
	switch message := msg.(type) {
	case *pgproto3.Terminate:
		if state := h.copyFromStdinState; state != nil {
			// Terminate closes the connection without another frontend boundary;
			// drain the loader and release its retained lease before teardown.
			_ = h.abortCopyFromStdinState(ErrCopyAborted)
			h.releaseCopyLease(state)
		}
		return true, false, nil
	case *pgproto3.Sync:
		// A healthy COPY-in returns early above, because PostgreSQL ignores Sync
		// until CopyDone/CopyFail terminates the sub-protocol. This branch only
		// handles an ordinary Sync or a terminal server-error sentinel.
		h.waitForSync = false
		copyAborted := false
		var syncErr error
		if h.copyFromStdinState != nil {
			// A server-side COPY error is reported before the client's Sync. In
			// that case the state is only a terminal sentinel and must be cleared
			// without emitting a second ErrorResponse/ReadyForQuery pair. An
			// interrupted but otherwise successful COPY still needs normal abort
			// reporting here.
			copyAborted = true
			state := h.copyFromStdinState
			if state.copyErr != nil {
				h.releaseCopyLease(state)
				h.copyFromStdinState = nil
			} else {
				// Sync is the protocol boundary for an interrupted COPY, not a
				// client COPY FAIL. Pass no synthetic cause so a clean loader abort
				// remains a normal boundary; only an actual cleanup failure is
				// surfaced to the client.
				syncErr = h.abortCopyFromStdinState(nil)
				h.releaseCopyLease(state)
			}
			// COPY cancellation is a statement failure for an explicit
			// PostgreSQL block. An implicit message-group scope is rolled back
			// below and remains protocol-idle, so markTransactionError leaves it
			// alone in that case.
			h.markTransactionError()
			// Any earlier completions belong to the same message group. The
			// rollback below invalidates them; do not report success for work that
			// did not commit.
			h.clearPendingCommandCompletes()
		}
		if h.implicitScopeActive() {
			// A healthy COPY returns before reaching this branch because Sync is
			// ignored until CopyDone. For an ordinary boundary (or a terminal
			// server-error sentinel), finalize the implicit scope normally; the
			// latter is already marked for rollback by the COPY error path.
			finalizeErr := h.finalizeImplicitPostgresTransaction(!copyAborted)
			if finalizeErr != nil {
				syncErr = errors.Join(syncErr, finalizeErr)
			}
		}
		h.releasePendingCopyLease()
		if syncErr != nil {
			return false, true, syncErr
		}
		if err := h.sendPendingCommandCompletes(); err != nil {
			return false, true, err
		}
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
	case *pgproto3.Flush:
		if h.backend == nil {
			return false, false, nil
		}
		return false, false, h.backend.Flush()
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
		// Unknown messages in an extended exchange are errors until the
		// client's Sync. Returning endOfMessages=false makes receiveMessage
		// send ErrorResponse, consume that Sync, and emit exactly one Ready.
		return false, false, fmt.Errorf(`unhandled message "%t"`, message)
	}
}

// handleQuery handles a query message, and returns a boolean flag, |endOfMessages| indicating if no other messages are
// expected as part of this query, in which case the server will send a READY FOR QUERY message back to the client so
// that it can send its next query.
func (h *ConnectionHandler) handleQuery(message *pgproto3.Query) (endOfMessages bool, err error) {
	h.pendingCommandCompletes = nil
	h.pendingProtocolMessages = nil
	h.deferredCommandComplete = false
	defer func() {
		pending := h.takePendingProtocolMessages()
		h.deferredCommandComplete = false
		if err != nil {
			err = h.finishImplicitPostgresError(err)
			return
		}
		// A simple Query message is one implicit transaction scope. COPY FROM
		// STDIN is the exception: its data arrives in later protocol messages,
		// so handleCopyDone owns the eventual commit.
		if endOfMessages && h.implicitScopeActive() {
			err = h.finalizeImplicitPostgresTransaction(true)
			if err != nil {
				return
			}
		}
		for _, response := range pending {
			if err = h.send(response); err != nil {
				return
			}
		}
	}()

	// Sensitive SQL is normally rejected before any parser or shortcut. In a
	// failed transaction, preserve PostgreSQL's 25P02 contract even for a
	// sensitive payload, while still keeping the lexical security gate first.
	if h.readyForQueryStatus() == ReadyForQueryTransactionIndicator_FailedTransactionBlock && catalog.IsSensitiveSQL(message.String) {
		return true, postgresFailedTransactionError()
	}
	if err := catalog.RejectSensitiveSQL(message.String); err != nil {
		return true, err
	}

	statements, err := h.convertQuery(message.String)
	if err != nil {
		// Once a transaction is failed, even a statement that cannot be
		// converted must be ignored rather than reaching a shortcut or the
		// executor. Preserve the protocol-level SQLSTATE in that state.
		if h.readyForQueryStatus() == ReadyForQueryTransactionIndicator_FailedTransactionBlock {
			return true, postgresFailedTransactionError()
		}
		return true, err
	}
	if err := h.rejectFailedQueryStatements(statements); err != nil {
		return true, err
	}
	// COPY FROM STDIN switches the connection into a message-driven mode: the
	// client must send CopyData/CopyDone before any later SQL can run. Reject a
	// trailing batch up front so no statement executes ahead of the COPY data.
	if err := rejectCopyFromStdinTrailingStatements(statements, message.String); err != nil {
		return true, err
	}
	// usql use ";" to test if the connection is alive. If we don't handle it, this will return an error. So we need to
	// manually handle it here. Keep this shortcut after the failed-transaction
	// gate and sensitive-SQL check so it cannot bypass protocol state handling.
	if message.String == ";" {
		err := h.send(makeCommandComplete("", 0))
		if err != nil {
			return true, err
		}
		return true, nil
	}
	if len(statements) == 0 {
		return true, h.send(&pgproto3.EmptyQueryResponse{})
	}

	// PSQL/workbench replacements are only safe after the transaction gate has
	// classified the original SQL. In a failed block they must never execute a
	// replacement SELECT behind the client's rejected command.
	if h.readyForQueryStatus() != ReadyForQueryTransactionIndicator_FailedTransactionBlock {
		if len(statements) == 1 {
			// These replacements call run directly, before the normal statement
			// loop sets its final-statement marker. Keep their completion behind
			// the implicit message-group commit as well.
			h.deferredCommandComplete = true
			handled, shortcutErr := h.handledPSQLCommands(message.String)
			h.deferredCommandComplete = false
			if handled || shortcutErr != nil {
				return true, shortcutErr
			}

			// TODO: Remove this once we support `SELECT * FROM function()` syntax
			// Github issue: https://github.com/dolthub/doltgresql/issues/464
			h.deferredCommandComplete = true
			handled, shortcutErr = h.handledWorkbenchCommands(message.String)
			h.deferredCommandComplete = false
			if handled || shortcutErr != nil {
				return true, shortcutErr
			}
		}
	}

	// A query message destroys the unnamed statement and the unnamed portal
	h.deletePreparedStatement("")
	h.deletePortal("")

	var handled bool
	lastStatement := -1
	for i, statement := range statements {
		if !isEmptyConvertedStatement(statement) {
			lastStatement = i
		}
	}
	for i, statement := range statements {
		statement.IsExtendedQuery = false
		if gateErr := h.rejectStatementIfTransactionFailed(statement); gateErr != nil {
			return true, gateErr
		}
		if statement.AST == nil && strings.TrimSpace(statement.String) == "" {
			if err := h.send(&pgproto3.EmptyQueryResponse{}); err != nil {
				return true, err
			}
			endOfMessages = true
			continue
		}
		if ensureErr := h.ensureImplicitPostgresTransaction(statement); ensureErr != nil {
			return true, ensureErr
		}
		if statementErr := h.rejectReadOnly(statement); statementErr != nil {
			h.recordTransactionResult(statement.AST, statementErr)
			return true, statementErr
		}
		// Certain statement types get handled directly by the handler instead of being passed to the engine
		var statementErr error
		// In-place handlers such as PostgreSQL compatibility SELECT rewrites may
		// call run themselves. Carry the same final-command deferral into those
		// paths so an implicit commit failure cannot follow an already-sent
		// CommandComplete.
		h.deferredCommandComplete = i == lastStatement
		handled, endOfMessages, statementErr = h.handleStatementOutsideEngine(statement)
		h.deferredCommandComplete = false
		if handled {
			h.recordTransactionResult(statement.AST, statementErr)
			if statementErr != nil {
				h.logger.Warnf("Failed to handle statement %s outside engine: %v", statementLogText(statement), statementErr)
				return true, statementErr
			}
		} else {
			if statementErr != nil {
				h.logger.Warnf("Failed to handle statement %s outside engine: %v", statementLogText(statement), statementErr)
			}
			h.deferredCommandComplete = i == lastStatement
			endOfMessages, statementErr = true, h.run(statement)
			h.deferredCommandComplete = false
			h.recordTransactionResult(statement.AST, statementErr)
			if statementErr != nil {
				return true, statementErr
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
	if err := h.rejectStatementIfTransactionFailed(statement); err != nil {
		return true, true, err
	}
	if err := h.ensureImplicitPostgresTransaction(statement); err != nil {
		return true, true, err
	}
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

	handled, err = h.handleInPlaceQueries(statement)
	if handled || err != nil {
		return true, true, err
	}

	return false, true, nil
}

// handleParse handles a parse message, returning any error that occurs
func (h *ConnectionHandler) handleParse(message *pgproto3.Parse) (err error) {
	h.waitForSync = true
	defer func() {
		if err != nil {
			err = h.finishImplicitPostgresError(err)
		}
	}()
	// Keep the lexical sensitive-SQL gate ahead of parser/prepared-statement
	// handling. A failed block still reports 25P02 for such a payload.
	if h.readyForQueryStatus() == ReadyForQueryTransactionIndicator_FailedTransactionBlock && catalog.IsSensitiveSQL(message.Query) {
		return postgresFailedTransactionError()
	}
	if err := catalog.RejectSensitiveSQL(message.Query); err != nil {
		return err
	}

	// TODO: "Named prepared statements must be explicitly closed before they can be redefined by another Parse message, but this is not required for the unnamed statement"
	statements, err := h.convertQuery(message.Query)
	if err != nil {
		if h.readyForQueryStatus() == ReadyForQueryTransactionIndicator_FailedTransactionBlock {
			return postgresFailedTransactionError()
		}
		return err
	}
	if err := h.rejectFailedQueryStatements(statements); err != nil {
		return err
	}

	// TODO(Noy): handle multiple statements
	if len(statements) == 0 {
		return fmt.Errorf("cannot prepare an empty statement list")
	}
	statement := statements[0]
	statement.IsExtendedQuery = true
	if err := h.rejectStatementIfTransactionFailed(statement); err != nil {
		return err
	}
	if err := h.rejectReadOnly(statement); err != nil {
		return err
	}
	if statement.AST == nil && strings.TrimSpace(statement.String) == "" {
		// special case: empty query
		h.preparedStatements[message.Name] = PreparedStatementData{
			Statement: statement,
		}
		return h.send(&pgproto3.ParseComplete{})
	}
	if err := h.ensureImplicitPostgresTransaction(statement); err != nil {
		return err
	}

	handledOutsideEngine, err := shouldQueryBeHandledInPlace(h, &statement)
	if err != nil {
		return err
	}
	if handledOutsideEngine {
		h.preparedStatements[message.Name] = PreparedStatementData{
			Statement:    statement,
			Prepared:     false,
			ReturnFields: nil,
			BindVarTypes: nil,
			Stmt:         nil,
			Closed:       new(atomic.Bool),
		}
		return h.send(&pgproto3.ParseComplete{})
	}

	stmt, params, fields, err := h.duckHandler.ComPrepareParsed(frontendContext(context.Background()), h.mysqlConn, statement.String, statement.AST)
	if err != nil {
		return err
	}

	if !statement.PgParsable {
		if stmt != nil {
			statement.Tag = GetStatementTag(stmt)
		} else {
			statement.Tag = GuessStatementTag(statement.String)
		}
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
		Prepared:     true,
		ReturnFields: fields,
		BindVarTypes: bindVarTypes,
		Stmt:         stmt,
		Closed:       new(atomic.Bool),
	}

	return h.send(&pgproto3.ParseComplete{})
}

// handleDescribe handles a Describe message, returning any error that occurs
func (h *ConnectionHandler) handleDescribe(message *pgproto3.Describe) (err error) {
	defer func() {
		if err != nil {
			err = h.finishImplicitPostgresError(err)
		}
	}()
	var fields []pgproto3.FieldDescription
	var bindvarTypes []uint32
	var tag string
	var returnsRows bool
	var statement ConvertedStatement

	h.waitForSync = true
	if message.ObjectType == 'S' {
		preparedStatementData, ok := h.preparedStatements[message.Name]
		if !ok {
			if h.readyForQueryStatus() == ReadyForQueryTransactionIndicator_FailedTransactionBlock {
				return postgresFailedTransactionError()
			}
			return fmt.Errorf("prepared statement %s does not exist", message.Name)
		}
		if err := h.rejectStatementIfTransactionFailed(preparedStatementData.Statement); err != nil {
			return err
		}
		statement = preparedStatementData.Statement

		// https://www.postgresql.org/docs/current/protocol-flow.html
		// > Note that since Bind has not yet been issued, the formats to be used for returned columns are not yet known to the backend;
		// > the format code fields in the RowDescription message will be zeroes in this case.
		if preparedStatementData.Prepared {
			fields = slices.Clone(preparedStatementData.ReturnFields)
			for i := range fields {
				fields[i].Format = 0
			}

			bindvarTypes = preparedStatementData.BindVarTypes
			tag = preparedStatementData.Statement.Tag
			returnsRows = statementReturnsRows(preparedStatementData.Statement)
		}

		if bindvarTypes == nil {
			bindvarTypes = make([]uint32, 0)
		}
	} else {
		portalData, ok := h.portals[message.Name]
		if !ok {
			if h.readyForQueryStatus() == ReadyForQueryTransactionIndicator_FailedTransactionBlock {
				return postgresFailedTransactionError()
			}
			return fmt.Errorf("portal %s does not exist", message.Name)
		}
		if err := h.rejectStatementIfTransactionFailed(portalData.Statement); err != nil {
			return err
		}
		statement = portalData.Statement

		// Prepared PostgreSQL portals retain SQL/metadata but no raw DuckDB
		// statement. Use the explicit lifecycle marker rather than testing Stmt,
		// so a portal Describe still returns fields for exec/cache_describe modes.
		if portalData.Prepared {
			fields = portalData.Fields
			tag = portalData.Statement.Tag
			returnsRows = statementReturnsRows(portalData.Statement)
		} else {
			// The RowDescription message will be sent by the inplace handler if this statement
			// is intercepted internally.
			return nil
		}
	}
	if err := h.ensureImplicitPostgresTransaction(statement); err != nil {
		return err
	}

	if !returnsRows {
		returnsRows = returnsRow(tag)
	}
	return h.sendDescribeResponse(fields, bindvarTypes, returnsRows)
}

// handleBind handles a bind message, returning any error that occurs
func (h *ConnectionHandler) handleBind(message *pgproto3.Bind) (err error) {
	h.waitForSync = true
	defer func() {
		if err != nil {
			err = h.finishImplicitPostgresError(err)
		}
	}()

	// TODO: a named portal object lasts till the end of the current transaction, unless explicitly destroyed
	//  we need to destroy the named portal as a side effect of the transaction ending
	preparedData, ok := h.preparedStatements[message.PreparedStatement]
	if !ok {
		if h.readyForQueryStatus() == ReadyForQueryTransactionIndicator_FailedTransactionBlock {
			return postgresFailedTransactionError()
		}
		return fmt.Errorf("prepared statement %s does not exist", message.PreparedStatement)
	}
	if err := h.rejectStatementIfTransactionFailed(preparedData.Statement); err != nil {
		return err
	}
	if err := catalog.RejectSensitiveSQL(preparedData.Statement.String); err != nil {
		return err
	}
	if auditQuery := preparedData.Statement.QueryForAudit(); auditQuery != preparedData.Statement.String {
		if err := catalog.RejectSensitiveSQL(auditQuery); err != nil {
			return err
		}
	}
	if err := h.ensureImplicitPostgresTransaction(preparedData.Statement); err != nil {
		return err
	}
	logrus.Tracef("binding portal %q to prepared statement %s", message.DestinationPortal, message.PreparedStatement)

	if !preparedData.Prepared {
		h.portals[message.DestinationPortal] = PortalData{
			Statement:    preparedData.Statement,
			IsEmptyQuery: strings.TrimSpace(preparedData.Statement.String) == "",
			Fields:       nil,
			Stmt:         nil,
			Vars:         nil,
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

	fields, err := h.duckHandler.ComBind(frontendContext(context.Background()), h.mysqlConn, preparedData, bindVars)
	if err != nil {
		return err
	}

	h.portals[message.DestinationPortal] = PortalData{
		Statement:         preparedData.Statement,
		Prepared:          preparedData.Prepared,
		Fields:            fields,
		ResultFormatCodes: message.ResultFormatCodes,
		Stmt:              preparedData.Stmt,
		Closed:            preparedData.Closed,
		Vars:              bindVars,
	}
	return h.send(&pgproto3.BindComplete{})
}

// handleExecute handles an execute message, returning any error that occurs
func (h *ConnectionHandler) handleExecute(message *pgproto3.Execute) (err error) {
	h.waitForSync = true
	var statement tree.Statement
	defer func() {
		if err != nil {
			err = h.finishImplicitPostgresError(err)
		}
		h.recordTransactionResult(statement, err)
	}()

	// TODO: implement the RowMax
	portalData, ok := h.portals[message.Portal]
	if !ok {
		if h.readyForQueryStatus() == ReadyForQueryTransactionIndicator_FailedTransactionBlock {
			return postgresFailedTransactionError()
		}
		return fmt.Errorf("portal %s does not exist", message.Portal)
	}

	query := portalData.Statement
	statement = query.AST
	if err := h.rejectStatementIfTransactionFailed(query); err != nil {
		return err
	}
	if err := catalog.RejectSensitiveSQL(query.String); err != nil {
		return err
	}
	if auditQuery := query.QueryForAudit(); auditQuery != query.String {
		if err := catalog.RejectSensitiveSQL(auditQuery); err != nil {
			return err
		}
	}
	if err := h.ensureImplicitPostgresTransaction(query); err != nil {
		return err
	}
	if err := h.sendNestedBeginWarning(query.AST); err != nil {
		return err
	}
	logrus.Tracef("executing portal %s with statement %s and %d bound values", message.Portal, statementLogText(query), len(portalData.Vars))

	if portalData.IsEmptyQuery {
		err = h.send(&pgproto3.NoData{})
		if err != nil {
			return fmt.Errorf("error sending NoData message: %w", err)
		}
		return h.send(&pgproto3.EmptyQueryResponse{})
	}

	// Certain statement types get handled directly by the handler instead of being passed to the engine
	if strings.ToUpper(query.Tag) != "SELECT" || !portalData.Prepared {
		handled, _, outsideErr := h.handleStatementOutsideEngine(query)
		if outsideErr != nil {
			err = outsideErr
		}
		if handled || outsideErr != nil {
			return err
		}
	}

	// |rowsAffected| gets altered by the callback below
	rowsAffected := int32(0)

	callback := h.spoolRowsCallback(query, &rowsAffected, true)
	err = h.duckHandler.ComExecuteBound(h.protocolContext(), h.mysqlConn, portalData, callback)
	if err != nil {
		return err
	}

	commandComplete := makeCommandComplete(h.protocolCommandTag(query), rowsAffected)
	if h.shouldDeferCommandComplete() {
		h.queueCommandComplete(commandComplete)
		return nil
	}
	return h.send(commandComplete)
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
	stop, endOfMessages, err = h.handleCopyDataHelper(message)
	if err == nil {
		return stop, endOfMessages, nil
	}

	// A COPY DATA failure aborts the loader immediately, but retains a terminal
	// state sentinel until CopyDone/CopyFail (or Sync) arrives. pgx sends
	// CopyDone after it observes a server-side ErrorResponse; clearing the state
	// here would make that protocol message look like a second, invalid COPY.
	// The loader is still fully drained before the error is reported, so no
	// reader goroutine can continue inserting after the transaction rollback.
	state := h.copyFromStdinState
	err = h.abortCopyLoaderState(state, err)
	// The loader is fully drained before releasing its lease. Release before
	// implicit rollback cleanup: the provider cleanup barrier waits for active
	// logical operations, so retaining this operation while rolling back would
	// deadlock.
	h.releaseCopyLease(state)
	err = h.finishImplicitPostgresError(err)
	return stop, h.copyErrorEndOfMessages(), err
}

// copyOperationContext returns the context associated with a COPY operation.
// Production COPY states retain their setup context; the empty-context
// fallback keeps lifecycle cleanup safe for minimal handlers used in tests.
func (h *ConnectionHandler) copyOperationContext(state *copyFromStdinState) (*sql.Context, error) {
	if state != nil && state.ctx != nil {
		return state.ctx, nil
	}
	if h != nil && h.duckHandler != nil {
		return h.duckHandler.NewContext(frontendContext(context.Background()), h.mysqlConn, "")
	}
	return sql.NewEmptyContext(), nil
}

// copyErrorEndOfMessages preserves the extended-query protocol's Sync
// discard path. A COPY error in an extended exchange must leave
// waitForSync=true so receiveMessage consumes the pending Sync before sending
// ReadyForQuery; simple-query and direct lifecycle calls can finish now.
func (h *ConnectionHandler) copyErrorEndOfMessages() bool {
	return h == nil || !h.waitForSync
}

func (h *ConnectionHandler) copySuccessEndOfMessages() bool {
	return h == nil || !h.waitForSync
}

// releaseCopyLease closes the state-owned COPY snapshot and releases provider
// operation admission when no physical transaction finalizer has registered a
// callback. A registered operation lease intentionally remains held through
// COMMIT/ROLLBACK; the backend Session invokes its callback from the exact
// physical finalizer.
func (h *ConnectionHandler) releaseCopyLease(state *copyFromStdinState) {
	if state != nil {
		state.binding.releaseTerminal()
	}
}

// releaseCopyOperationFallback releases only operation admission for a COPY
// whose physical transaction could not accept a finalization callback. A
// registered operation lease belongs to the transaction's COMMIT/ROLLBACK
// callback and must remain held until that callback runs.
func (h *ConnectionHandler) releaseCopyOperationFallback(state *copyFromStdinState) {
	if state == nil {
		return
	}
	lease := state.binding.lease
	if lease == nil || !lease.OperationDeferred() {
		state.binding.releaseOperation()
	}
}

func (h *ConnectionHandler) releasePendingCopyLease() {
	if h == nil || h.pendingCopyRelease == nil {
		return
	}
	release := h.pendingCopyRelease
	h.pendingCopyRelease = nil
	release()
}

func (h *ConnectionHandler) deferCopyLeaseUntilSync(state *copyFromStdinState) {
	if h == nil || state == nil {
		return
	}
	// This fallback is used only by legacy/test sessions that cannot register a
	// physical post-finalization callback. Keep the operation half until Sync
	// so a successful extended COPY commits before it is released; registered
	// leases are released by the finalizer and do not need this slot.
	if state.binding.lease == nil || state.binding.lease.OperationDeferred() {
		return
	}
	release := state.binding.releaseOperation
	if release == nil {
		return
	}
	if h.pendingCopyRelease == nil {
		h.pendingCopyRelease = release
		return
	}
	// There should normally be only one active COPY exchange per handler, but
	// compose releases defensively if a test or pipelined path supplies both.
	previous := h.pendingCopyRelease
	h.pendingCopyRelease = func() {
		previous()
		release()
	}
}

// abortCopyLoaderState aborts and waits for an active COPY loader without
// changing the handler's state pointer. Keeping the state is important after a
// server-side CopyData failure: clients commonly send CopyDone after reading
// ErrorResponse, and that terminal message must be consumed exactly once.
// Repeated calls are idempotent, including for custom loaders whose Abort and
// Wait methods are not themselves idempotent.
func (h *ConnectionHandler) abortCopyLoaderState(state *copyFromStdinState, cause error) error {
	if state == nil {
		return cause
	}
	if state.copyAborted {
		if state.copyErr != nil {
			return state.copyErr
		}
		return cause
	}
	if cause == nil {
		cause = state.copyErr
	} else if state.copyErr != nil && !errors.Is(cause, state.copyErr) {
		cause = errors.Join(state.copyErr, cause)
	} else if state.copyErr != nil {
		cause = state.copyErr
	}

	var abortErr error
	if state.dataLoader != nil {
		ctx := state.ctx
		if ctx == nil {
			ctx = sql.NewEmptyContext()
		}
		abortErr = state.dataLoader.Abort(ctx)
		if waiter, ok := state.dataLoader.(interface{ Wait() }); ok {
			waiter.Wait()
		}
	}
	state.copyAborted = true
	if abortErr != nil {
		if cause == nil {
			cause = abortErr
		} else if !errors.Is(cause, abortErr) {
			cause = errors.Join(cause, abortErr)
		}
	}
	state.copyErr = cause
	return cause
}

// abortCopyFromStdinState aborts and waits for the active COPY loader, then
// clears the connection's COPY state on every return path. The original cause
// is retained alongside any cleanup error so callers can report both.
func (h *ConnectionHandler) abortCopyFromStdinState(cause error) error {
	if h == nil {
		return cause
	}
	state := h.copyFromStdinState
	if state == nil {
		return cause
	}
	err := h.abortCopyLoaderState(state, cause)
	// The state is cleared immediately after the loader has been drained. This
	// helper is used by protocol error/close paths that may proceed directly to
	// rollback cleanup, so release before returning to avoid blocking that
	// cleanup on our own operation admission lease.
	h.releaseCopyLease(state)
	h.copyFromStdinState = nil
	return err
}

// handleCopyDataHelper is a helper function that should only be invoked by
// handleCopyData. The wrapper owns aborting the loader; a failed COPY retains a
// terminal sentinel until the protocol consumes CopyDone/CopyFail or Sync.
func (h *ConnectionHandler) handleCopyDataHelper(message *pgproto3.CopyData) (stop bool, endOfMessages bool, err error) {
	state := h.copyFromStdinState
	if state == nil {
		if h.readyForQueryStatus() == ReadyForQueryTransactionIndicator_FailedTransactionBlock {
			return false, h.copyErrorEndOfMessages(), postgresFailedTransactionError()
		}
		return false, true, fmt.Errorf("COPY DATA message received without a COPY FROM STDIN operation in progress")
	}
	// Once a prior chunk failed, ignore any additional data until the client
	// sends its terminal CopyDone/CopyFail. Returning nil here avoids emitting a
	// second error/Ready pair while preserving the sentinel for that terminal
	// message.
	if state.copyErr != nil {
		return false, false, nil
	}
	if h.readyForQueryStatus() == ReadyForQueryTransactionIndicator_FailedTransactionBlock {
		return false, h.copyErrorEndOfMessages(), postgresFailedTransactionError()
	}

	// Grab a sql.Context.
	sqlCtx, err := h.copyOperationContext(state)
	if err != nil {
		return false, false, err
	}
	if err := state.binding.validate(sqlCtx); err != nil {
		return false, false, err
	}

	dataLoader := state.dataLoader
	if dataLoader == nil {
		copyFrom := state.copyFromStdinNode
		if copyFrom == nil {
			return false, false, fmt.Errorf("no COPY FROM STDIN node found")
		}
		table := state.targetTable
		if table == nil {
			return false, true, fmt.Errorf("no target table found")
		}
		rawOptions := state.rawOptions

		switch copyFrom.Options.CopyFormat {
		case CopyFormatArrow:
			if err := rejectArrowCopyBinding(sqlCtx, state.binding); err != nil {
				return false, false, err
			}
			dataLoader, err = newArrowDataLoaderWithBinding(
				sqlCtx, h.duckHandler,
				copyFrom.Table.Schema(), table, copyFrom.Columns,
				rawOptions, state.binding,
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
			dataLoader, err = newCsvDataLoaderWithBinding(
				sqlCtx, h.duckHandler,
				copyFrom.Table.Schema(), table, copyFrom.Columns,
				&copyFrom.Options,
				rawOptions, state.binding,
			)
		case tree.CopyFormatBinary:
			err = fmt.Errorf("BINARY format is not supported for COPY FROM")
		default:
			err = fmt.Errorf("unknown format specified for COPY FROM: %v", copyFrom.Options.CopyFormat)
		}

		if err != nil {
			return false, false, err
		}

		// Publish the loader before Start so a setup failure can still be
		// aborted by handleCopyData's common cleanup path.
		state.dataLoader = dataLoader
		if err := state.binding.validate(sqlCtx); err != nil {
			return false, false, err
		}
		ready := dataLoader.Start()
		if err, hasErr := <-ready; hasErr {
			return false, false, err
		}
	}

	if err := state.binding.validate(sqlCtx); err != nil {
		return false, false, err
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
	defer func() {
		if err != nil {
			err = h.finishImplicitPostgresError(err)
		}
	}()

	state := h.copyFromStdinState
	if state == nil {
		if h.readyForQueryStatus() == ReadyForQueryTransactionIndicator_FailedTransactionBlock {
			err = h.abortCopyFromStdinState(postgresFailedTransactionError())
			return false, h.copyErrorEndOfMessages(), err
		}
		return false, true,
			fmt.Errorf("COPY DONE message received without a COPY FROM STDIN operation in progress")
	}

	// A previous COPY DATA failure may have been observed by the protocol
	// layer before COPY DONE arrives. The loader was already aborted by
	// handleCopyData; consume this terminal message and clear the sentinel
	// without sending another ErrorResponse or ReadyForQuery. This is required
	// for clients such as pgx, which send CopyDone after seeing the server error.
	if state.copyErr != nil {
		h.releaseCopyLease(state)
		h.copyFromStdinState = nil
		return false, false, nil
	}
	if h.readyForQueryStatus() == ReadyForQueryTransactionIndicator_FailedTransactionBlock {
		err = h.abortCopyFromStdinState(postgresFailedTransactionError())
		return false, h.copyErrorEndOfMessages(), err
	}

	dataLoader := state.dataLoader
	if dataLoader == nil {
		err = h.abortCopyFromStdinState(fmt.Errorf("no data loader found for COPY FROM STDIN operation"))
		return false, h.copyErrorEndOfMessages(), err
	}

	sqlCtx, err := h.copyOperationContext(state)
	if err != nil {
		err = h.abortCopyFromStdinState(err)
		return false, h.copyErrorEndOfMessages(), err
	}
	if bindingErr := state.binding.validate(sqlCtx); bindingErr != nil {
		err = h.abortCopyFromStdinState(bindingErr)
		return false, h.copyErrorEndOfMessages(), err
	}

	loadDataResults, err := dataLoader.Finish(sqlCtx)
	if err != nil {
		err = h.abortCopyLoaderState(state, err)
		h.releaseCopyLease(state)
		h.copyFromStdinState = nil
		return false, h.copyErrorEndOfMessages(), err
	}
	// Finish has drained the loader and its child iterator. The pool snapshot is
	// therefore no longer needed, but a registered provider operation lease may
	// still be protecting the surrounding physical transaction until its
	// finalizer. Keep those two lifetimes independent.
	state.binding.releaseSnapshot()
	if loadDataResults == nil {
		err = h.abortCopyLoaderState(state, fmt.Errorf("COPY FROM STDIN returned no load results"))
		h.releaseCopyOperationFallback(state)
		h.copyFromStdinState = nil
		return false, h.copyErrorEndOfMessages(), err
	}

	h.copyFromStdinState = nil
	if h.implicitScopeActive() {
		if h.waitForSync {
			// Extended COPY sends its completion before Sync. Keep the physical
			// transaction's operation admission until that Sync commits the
			// implicit transaction. The snapshot was released immediately after
			// Finish above.
			h.deferCopyLeaseUntilSync(state)
		} else {
			finalizeErr := h.finalizeImplicitPostgresTransaction(true)
			// A finalizer callback normally releases registered operation leases.
			// Release again unconditionally so a finalization error that evicts the
			// transaction cannot strand a lease; postgresCopyLease is idempotent.
			state.binding.releaseOperation()
			if finalizeErr != nil {
				return false, true, finalizeErr
			}
		}
	} else {
		// Explicit transactions remain open after COPY DONE, but the COPY
		// producer/loader has completed. Only an unregistered fallback operation
		// lease can end now; a registered lease belongs to the later physical
		// COMMIT/ROLLBACK callback.
		h.releaseCopyOperationFallback(state)
	}
	commandComplete := &pgproto3.CommandComplete{
		CommandTag: []byte(fmt.Sprintf("COPY %d", loadDataResults.RowsLoaded)),
	}
	if h.shouldDeferCopyCommandComplete() {
		// A simple Query's final COPY is held until its implicit transaction
		// commits. Extended COPY leaves this branch false so its tag preserves
		// the protocol ordering relative to later pipelined messages.
		h.queueCommandComplete(commandComplete)
		return false, false, nil
	}
	// We send back endOfMessage=true, since the COPY DONE message ends the COPY DATA flow and the server is ready
	// to accept the next query now.
	return false, h.copySuccessEndOfMessages(), h.send(commandComplete)
}

// handleCopyFail handles a COPY FAIL message by aborting the in-progress COPY DATA operation.  The |stop| response
// parameter is true if the connection handler should shut down the connection, |endOfMessages| is true if no more
// COPY DATA messages are expected, and the server should tell the client that it is ready for the next query, and
// |err| contains any error that occurred while processing the COPY DATA message.
func (h *ConnectionHandler) handleCopyFail(message *pgproto3.CopyFail) (stop bool, endOfMessages bool, err error) {
	defer func() {
		if err != nil {
			err = h.finishImplicitPostgresError(err)
		}
	}()

	state := h.copyFromStdinState
	if state == nil {
		if h.readyForQueryStatus() == ReadyForQueryTransactionIndicator_FailedTransactionBlock {
			return false, h.copyErrorEndOfMessages(), postgresFailedTransactionError()
		}
		return false, true,
			fmt.Errorf("COPY FAIL message received without a COPY FROM STDIN operation in progress")
	}
	// A server-side COPY error has already been reported and the loader has
	// already been drained. CopyFail is then only the client's terminal
	// acknowledgement; consume it and clear the sentinel without producing a
	// duplicate response.
	if state.copyErr != nil {
		h.releaseCopyLease(state)
		h.copyFromStdinState = nil
		return false, false, nil
	}

	// COPY FAIL is itself a failed command. Preserve the client's reason when
	// present, while retaining ErrCopyAborted for callers that need to classify
	// an intentional cancellation.
	cause := state.copyErr
	if cause == nil {
		cause = ErrCopyAborted
	}
	if message != nil && message.Message != "" {
		cause = fmt.Errorf("%w: %s", cause, message.Message)
	}
	err = h.abortCopyFromStdinState(cause)
	// We send back endOfMessage=true for a simple exchange; an extended query
	// keeps waitForSync set so receiveMessage can discard the pending Sync.
	return false, h.copyErrorEndOfMessages(), err
}

func (h *ConnectionHandler) deallocatePreparedStatement(name string, preparedStatements map[string]PreparedStatementData, query ConvertedStatement, conn net.Conn) error {
	_, ok := preparedStatements[name]
	if !ok {
		return fmt.Errorf("prepared statement %s does not exist", name)
	}
	h.deletePreparedStatement(name)

	return h.sendOrQueueProtocolMessage(&pgproto3.CommandComplete{
		CommandTag: []byte(query.Tag),
	})
}

func (h *ConnectionHandler) deletePreparedStatement(name string) {
	ps, ok := h.preparedStatements[name]
	if ok {
		delete(h.preparedStatements, name)
		if ps.Closed == nil {
			if ps.Stmt != nil {
				_ = ps.Stmt.Close()
			}
			return
		}
		if ps.Closed.CompareAndSwap(false, true) && ps.Stmt != nil {
			ps.Stmt.Close()
		}
	}
}

func (h *ConnectionHandler) deletePortal(name string) {
	p, ok := h.portals[name]
	if ok {
		delete(h.portals, name)
		if p.Closed == nil {
			if p.Stmt != nil {
				_ = p.Stmt.Close()
			}
			return
		}
		if p.Closed.CompareAndSwap(false, true) && p.Stmt != nil {
			p.Stmt.Close()
		}
	}
}

// clearPortals releases portals whose lifetime is bounded by the transaction
// that created them. Prepared statement metadata is retained separately and
// can be rebound in a later protocol cycle.
func (h *ConnectionHandler) clearPortals() {
	for name := range h.portals {
		h.deletePortal(name)
	}
}

// closePreparedObjects releases any transient raw statements left by legacy
// or test handlers before the protocol connection is torn down. Current
// PostgreSQL prepared plans retain SQL/metadata only, but keeping teardown
// identity-safe makes connection replacement harmless for older entries.
func (h *ConnectionHandler) closePreparedObjects() {
	for name := range h.preparedStatements {
		h.deletePreparedStatement(name)
	}
	for name := range h.portals {
		h.deletePortal(name)
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
	if err := h.rejectStatementIfTransactionFailed(statement); err != nil {
		return err
	}
	if err := h.ensureImplicitPostgresTransaction(statement); err != nil {
		return err
	}
	if err := catalog.RejectSensitiveSQL(statement.String); err != nil {
		return err
	}
	if auditQuery := statement.QueryForAudit(); auditQuery != statement.String {
		if err := catalog.RejectSensitiveSQL(auditQuery); err != nil {
			return err
		}
	}
	if err := h.sendNestedBeginWarning(statement.AST); err != nil {
		return err
	}
	h.logger.Tracef("running statement %s", statementLogText(statement))

	// |rowsAffected| gets altered by the callback below
	rowsAffected := int32(0)

	// Get the accurate statement tag for the statement
	if !statement.PgParsable && !IsWellKnownStatementTag(statement.Tag) {
		tag, err := h.duckHandler.getStatementTag(h.mysqlConn, statement.String)
		if err != nil {
			return err
		}
		h.logger.Tracef("getting statement tag for statement %s via preparing in DuckDB: %s", statementLogText(statement), tag)
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
	} else if statement.RestoreConfig != nil {
		msg, err := h.executeRestore(statement.RestoreConfig)
		if err != nil {
			return err
		}
		return h.send(&pgproto3.ErrorResponse{
			Message: msg,
		})
	}

	callback := h.spoolRowsCallback(statement, &rowsAffected, false)
	if err := h.duckHandler.ComQuery(
		h.protocolContext(),
		h.mysqlConn,
		statement.String,
		statement.QueryForAudit(),
		statement.AST,
		callback,
	); err != nil {
		return fmt.Errorf("fallback statement execution failed: %w", err)
	}

	commandComplete := makeCommandComplete(h.protocolCommandTag(statement), rowsAffected)
	if h.shouldDeferCommandComplete() {
		h.queueCommandComplete(commandComplete)
		return nil
	}
	return h.send(commandComplete)
}

func (h *ConnectionHandler) rejectReadOnly(statement ConvertedStatement) error {
	if !h.readOnly {
		return nil
	}
	if statement.SubscriptionConfig != nil || statement.BackupConfig != nil || statement.RestoreConfig != nil {
		return sql.ErrReadOnly.New()
	}
	if statement.AST != nil {
		if tree.CanWriteData(statement.AST) || tree.CanModifySchema(statement.AST) {
			return sql.ErrReadOnly.New()
		}
	}
	if backend.IsWriteQueryText(statement.String) {
		return sql.ErrReadOnly.New()
	}
	return nil
}

// spoolRowsCallback returns a callback function that will send RowDescription message,
// then a DataRow message for each row in the result set.
func (h *ConnectionHandler) spoolRowsCallback(statement ConvertedStatement, rows *int32, isExecute bool) func(res *Result) error {
	// IsIUD returns whether the query is either an INSERT, UPDATE, or DELETE query.
	tag := statement.Tag
	isIUD := tag == "INSERT" || tag == "UPDATE" || tag == "DELETE"
	returnsRows := statementReturnsRows(statement)
	return func(res *Result) error {
		logrus.Tracef("spooling %d rows for tag %s (execute = %v)", res.RowsAffected, tag, isExecute)
		if returnsRows {
			// EXECUTE does not send RowDescription; instead it should be sent from DESCRIBE prior to it
			// We only send RowDescription once per statement execution.
			if !isExecute && !statement.HasSentRowDesc {
				logrus.Tracef("sending RowDescription %+v for tag %s", res.Fields, tag)
				if err := h.send(&pgproto3.RowDescription{
					Fields: res.Fields,
				}); err != nil {
					return err
				}
				statement.HasSentRowDesc = true
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

// sendNestedBeginWarning mirrors PostgreSQL's behavior for BEGIN issued while
// an explicit transaction block is already active: it is a successful no-op,
// but emits a WARNING with SQLSTATE 25001 before the command completion.
func (h *ConnectionHandler) sendNestedBeginWarning(statement tree.Statement) error {
	if h == nil || h.readyForQueryStatus() != ReadyForQueryTransactionIndicator_TransactionBlock {
		return nil
	}
	if _, ok := statement.(*tree.BeginTransaction); !ok {
		return nil
	}
	return h.send(&pgproto3.NoticeResponse{
		Severity: string(ErrorResponseSeverity_Warning),
		Code:     "25001",
		Message:  "there is already a transaction in progress",
	})
}

// sendDescribeResponse sends a response message for a Describe message
func (h *ConnectionHandler) sendDescribeResponse(fields []pgproto3.FieldDescription, types []uint32, returnsRows bool) error {
	// The prepared statement variant of the describe command returns the OIDs of the parameters.
	if types != nil {
		if err := h.send(&pgproto3.ParameterDescription{
			ParameterOIDs: types,
		}); err != nil {
			return err
		}
	}

	if returnsRows {
		// Both variants finish with a row description.
		return h.send(&pgproto3.RowDescription{
			Fields: fields,
		})
	} else {
		return h.send(&pgproto3.NoData{})
	}
}

func statementReturnsRows(statement ConvertedStatement) bool {
	if returnsRow(statement.Tag) {
		return true
	}
	_, ok := postgresInsertReturningRows(statement.AST)
	return ok
}

// handledPSQLCommands handles the special PSQL commands, such as \l and \dt.
func (h *ConnectionHandler) handledPSQLCommands(statement string) (bool, error) {
	originalStatement := statement
	statement = strings.ToLower(statement)
	// Command: \l
	if statement == "select d.datname as \"name\",\n       pg_catalog.pg_get_userbyid(d.datdba) as \"owner\",\n       pg_catalog.pg_encoding_to_char(d.encoding) as \"encoding\",\n       d.datcollate as \"collate\",\n       d.datctype as \"ctype\",\n       d.daticulocale as \"icu locale\",\n       case d.datlocprovider when 'c' then 'libc' when 'i' then 'icu' end as \"locale provider\",\n       pg_catalog.array_to_string(d.datacl, e'\\n') as \"access privileges\"\nfrom pg_catalog.pg_database d\norder by 1;" {
		query, err := h.convertQuery(`select d.datname as "Name", 'postgres' as "Owner", 'UTF8' as "Encoding", 'en_US.UTF-8' as "Collate", 'en_US.UTF-8' as "Ctype", 'en-US' as "ICU Locale", case d.datlocprovider when 'c' then 'libc' when 'i' then 'icu' end as "locale provider", '' as "access privileges" from pg_catalog.pg_database d order by 1;`)
		if err != nil {
			return false, err
		}
		query[0].OriginalString = originalStatement
		return true, h.run(query[0])
	}
	// Command: \l on psql 16
	if statement == "select\n  d.datname as \"name\",\n  pg_catalog.pg_get_userbyid(d.datdba) as \"owner\",\n  pg_catalog.pg_encoding_to_char(d.encoding) as \"encoding\",\n  case d.datlocprovider when 'c' then 'libc' when 'i' then 'icu' end as \"locale provider\",\n  d.datcollate as \"collate\",\n  d.datctype as \"ctype\",\n  d.daticulocale as \"icu locale\",\n  null as \"icu rules\",\n  pg_catalog.array_to_string(d.datacl, e'\\n') as \"access privileges\"\nfrom pg_catalog.pg_database d\norder by 1;" {
		query, err := h.convertQuery(`select d.datname as "Name", 'postgres' as "Owner", 'UTF8' as "Encoding", 'en_US.UTF-8' as "Collate", 'en_US.UTF-8' as "Ctype", 'en-US' as "ICU Locale", case d.datlocprovider when 'c' then 'libc' when 'i' then 'icu' end as "locale provider", '' as "access privileges" from pg_catalog.pg_database d order by 1;`)
		if err != nil {
			return false, err
		}
		query[0].OriginalString = originalStatement
		return true, h.run(query[0])
	}
	// Command: \dt
	if statement == "select n.nspname as \"schema\",\n  c.relname as \"name\",\n  case c.relkind when 'r' then 'table' when 'v' then 'view' when 'm' then 'materialized view' when 'i' then 'index' when 's' then 'sequence' when 't' then 'toast table' when 'f' then 'foreign table' when 'p' then 'partitioned table' when 'i' then 'partitioned index' end as \"type\",\n  pg_catalog.pg_get_userbyid(c.relowner) as \"owner\"\nfrom pg_catalog.pg_class c\n     left join pg_catalog.pg_namespace n on n.oid = c.relnamespace\n     left join pg_catalog.pg_am am on am.oid = c.relam\nwhere c.relkind in ('r','p','')\n      and n.nspname <> 'pg_catalog'\n      and n.nspname !~ '^pg_toast'\n      and n.nspname <> 'information_schema'\n  and pg_catalog.pg_table_is_visible(c.oid)\norder by 1,2;" {
		return true, h.run(ConvertedStatement{
			String:         `SELECT table_schema AS "Schema", TABLE_NAME AS "Name", 'table' AS "Type", 'postgres' AS "Owner" FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA <> 'pg_catalog' AND TABLE_SCHEMA <> 'information_schema' AND TABLE_TYPE = 'BASE TABLE' ORDER BY 2;`,
			OriginalString: originalStatement,
			Tag:            "SELECT",
		})
	}
	// Command: \d
	if statement == "select n.nspname as \"schema\",\n  c.relname as \"name\",\n  case c.relkind when 'r' then 'table' when 'v' then 'view' when 'm' then 'materialized view' when 'i' then 'index' when 's' then 'sequence' when 't' then 'toast table' when 'f' then 'foreign table' when 'p' then 'partitioned table' when 'i' then 'partitioned index' end as \"type\",\n  pg_catalog.pg_get_userbyid(c.relowner) as \"owner\"\nfrom pg_catalog.pg_class c\n     left join pg_catalog.pg_namespace n on n.oid = c.relnamespace\n     left join pg_catalog.pg_am am on am.oid = c.relam\nwhere c.relkind in ('r','p','v','m','s','f','')\n      and n.nspname <> 'pg_catalog'\n      and n.nspname !~ '^pg_toast'\n      and n.nspname <> 'information_schema'\n  and pg_catalog.pg_table_is_visible(c.oid)\norder by 1,2;" {
		return true, h.run(ConvertedStatement{
			String:         `SELECT table_schema AS "Schema", TABLE_NAME AS "Name", IF(TABLE_TYPE = 'VIEW', 'view', 'table') AS "Type", 'postgres' AS "Owner" FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA <> 'pg_catalog' AND TABLE_SCHEMA <> 'information_schema' AND TABLE_TYPE = 'BASE TABLE' OR TABLE_TYPE = 'VIEW' ORDER BY 2;`,
			OriginalString: originalStatement,
			Tag:            "SELECT",
		})
	}
	// Alternate \d for psql 14
	if statement == "select n.nspname as \"schema\",\n  c.relname as \"name\",\n  case c.relkind when 'r' then 'table' when 'v' then 'view' when 'm' then 'materialized view' when 'i' then 'index' when 's' then 'sequence' when 's' then 'special' when 't' then 'toast table' when 'f' then 'foreign table' when 'p' then 'partitioned table' when 'i' then 'partitioned index' end as \"type\",\n  pg_catalog.pg_get_userbyid(c.relowner) as \"owner\"\nfrom pg_catalog.pg_class c\n     left join pg_catalog.pg_namespace n on n.oid = c.relnamespace\n     left join pg_catalog.pg_am am on am.oid = c.relam\nwhere c.relkind in ('r','p','v','m','s','f','')\n      and n.nspname <> 'pg_catalog'\n      and n.nspname !~ '^pg_toast'\n      and n.nspname <> 'information_schema'\n  and pg_catalog.pg_table_is_visible(c.oid)\norder by 1,2;" {
		return true, h.run(ConvertedStatement{
			String:         `SELECT table_schema AS "Schema", TABLE_NAME AS "Name", IF(TABLE_TYPE = 'VIEW', 'view', 'table') AS "Type", 'postgres' AS "Owner" FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA <> 'pg_catalog' AND TABLE_SCHEMA <> 'information_schema' AND TABLE_TYPE = 'BASE TABLE' OR TABLE_TYPE = 'VIEW' ORDER BY 2;`,
			OriginalString: originalStatement,
			Tag:            "SELECT",
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
			String:         `SELECT 'public' AS "Name", 'pg_database_owner' AS "Owner";`,
			OriginalString: originalStatement,
			Tag:            "SELECT",
		})
	}
	// Command: \df
	if statement == "select n.nspname as \"schema\",\n  p.proname as \"name\",\n  pg_catalog.pg_get_function_result(p.oid) as \"result data type\",\n  pg_catalog.pg_get_function_arguments(p.oid) as \"argument data types\",\n case p.prokind\n  when 'a' then 'agg'\n  when 'w' then 'window'\n  when 'p' then 'proc'\n  else 'func'\n end as \"type\"\nfrom pg_catalog.pg_proc p\n     left join pg_catalog.pg_namespace n on n.oid = p.pronamespace\nwhere pg_catalog.pg_function_is_visible(p.oid)\n      and n.nspname <> 'pg_catalog'\n      and n.nspname <> 'information_schema'\norder by 1, 2, 4;" {
		return true, h.run(ConvertedStatement{
			String:         `SELECT '' AS "Schema", '' AS "Name", '' AS "Result data type", '' AS "Argument data types", '' AS "Type" LIMIT 0;`,
			OriginalString: originalStatement,
			Tag:            "SELECT",
		})
	}
	// Command: \dv
	if statement == "select n.nspname as \"schema\",\n  c.relname as \"name\",\n  case c.relkind when 'r' then 'table' when 'v' then 'view' when 'm' then 'materialized view' when 'i' then 'index' when 's' then 'sequence' when 't' then 'toast table' when 'f' then 'foreign table' when 'p' then 'partitioned table' when 'i' then 'partitioned index' end as \"type\",\n  pg_catalog.pg_get_userbyid(c.relowner) as \"owner\"\nfrom pg_catalog.pg_class c\n     left join pg_catalog.pg_namespace n on n.oid = c.relnamespace\nwhere c.relkind in ('v','')\n      and n.nspname <> 'pg_catalog'\n      and n.nspname !~ '^pg_toast'\n      and n.nspname <> 'information_schema'\n  and pg_catalog.pg_table_is_visible(c.oid)\norder by 1,2;" {
		return true, h.run(ConvertedStatement{
			String:         `SELECT table_schema AS "Schema", TABLE_NAME AS "Name", 'view' AS "Type", 'postgres' AS "Owner" FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA <> 'pg_catalog' AND TABLE_SCHEMA <> 'information_schema' AND TABLE_TYPE = 'VIEW' ORDER BY 2;`,
			OriginalString: originalStatement,
			Tag:            "SELECT",
		})
	}
	// Command: \du
	if statement == "select r.rolname, r.rolsuper, r.rolinherit,\n  r.rolcreaterole, r.rolcreatedb, r.rolcanlogin,\n  r.rolconnlimit, r.rolvaliduntil,\n  array(select b.rolname\n        from pg_catalog.pg_auth_members m\n        join pg_catalog.pg_roles b on (m.roleid = b.oid)\n        where m.member = r.oid) as memberof\n, r.rolreplication\n, r.rolbypassrls\nfrom pg_catalog.pg_roles r\nwhere r.rolname !~ '^pg_'\norder by 1;" {
		// We don't support users yet, so we'll just return nothing for now
		return true, h.run(ConvertedStatement{
			String:         `SELECT '' FROM dual LIMIT 0;`,
			OriginalString: originalStatement,
			Tag:            "SELECT",
		})
	}
	return false, nil
}

// handledWorkbenchCommands handles commands used by some workbenches, such as dolt-workbench.
func (h *ConnectionHandler) handledWorkbenchCommands(statement string) (bool, error) {
	lower := strings.ToLower(statement)
	if lower == "select * from current_schema()" || lower == "select * from current_schema();" {
		return true, h.run(ConvertedStatement{
			String:         `SELECT search_path AS "current_schema";`,
			OriginalString: statement,
			Tag:            "SELECT",
		})
	}
	if lower == "select * from current_database()" || lower == "select * from current_database();" {
		return true, h.run(ConvertedStatement{
			String:         `SELECT DATABASE() AS "current_database";`,
			OriginalString: statement,
			Tag:            "SELECT",
		})
	}
	return false, nil
}

// sendReadyForQuery emits the protocol boundary after a message group. Keep
// this separate from endOfMessages so extended-query errors can send their
// ErrorResponse first, consume the client's Sync, and only then advertise the
// final transaction state.
func (h *ConnectionHandler) sendReadyForQuery() error {
	return h.send(&pgproto3.ReadyForQuery{
		TxStatus: byte(h.readyForQueryStatus()),
	})
}

func (h *ConnectionHandler) queueCommandComplete(commandComplete *pgproto3.CommandComplete) {
	if h == nil || commandComplete == nil {
		return
	}
	h.pendingCommandCompletes = append(h.pendingCommandCompletes, commandComplete)
	h.pendingProtocolMessages = append(h.pendingProtocolMessages, commandComplete)
}

// queueProtocolMessage appends an in-place response to the same ordered queue
// used by deferred CommandComplete messages. It is intentionally private to
// the protocol handler: callers must still decide whether a message is safe to
// emit immediately or should be held behind an implicit commit.
func (h *ConnectionHandler) queueProtocolMessage(message pgproto3.BackendMessage) {
	if h == nil || message == nil {
		return
	}
	h.pendingProtocolMessages = append(h.pendingProtocolMessages, message)
}

// sendOrQueueProtocolMessage keeps direct protocol handlers from advertising
// success before the final implicit simple-query transaction has committed.
// Extended Execute paths leave deferredCommandComplete false and retain their
// normal response timing.
func (h *ConnectionHandler) sendOrQueueProtocolMessage(message pgproto3.BackendMessage) error {
	if h == nil || message == nil {
		return nil
	}
	if h.shouldDeferCommandComplete() {
		h.queueProtocolMessage(message)
		return nil
	}
	return h.send(message)
}

// takePendingProtocolMessages returns the ordered queue and clears both the
// unified queue and the legacy command-complete mirror. The fallback conversion
// keeps direct unit tests that seed pendingCommandCompletes working.
func (h *ConnectionHandler) takePendingProtocolMessages() []pgproto3.BackendMessage {
	if h == nil {
		return nil
	}
	if len(h.pendingProtocolMessages) == 0 && len(h.pendingCommandCompletes) > 0 {
		pending := make([]pgproto3.BackendMessage, 0, len(h.pendingCommandCompletes))
		for _, commandComplete := range h.pendingCommandCompletes {
			if commandComplete != nil {
				pending = append(pending, commandComplete)
			}
		}
		h.pendingCommandCompletes = nil
		return pending
	}
	pending := h.pendingProtocolMessages
	h.pendingProtocolMessages = nil
	h.pendingCommandCompletes = nil
	return pending
}

func (h *ConnectionHandler) takePendingCommandCompletes() []*pgproto3.CommandComplete {
	if h == nil || len(h.pendingCommandCompletes) == 0 {
		return nil
	}
	pending := h.pendingCommandCompletes
	h.pendingCommandCompletes = nil
	return pending
}

func (h *ConnectionHandler) clearPendingCommandCompletes() {
	if h != nil {
		h.pendingCommandCompletes = nil
		h.pendingProtocolMessages = nil
	}
}

func (h *ConnectionHandler) shouldDeferCommandComplete() bool {
	// Ordinary extended Execute messages must expose their completion as soon
	// as the statement finishes. The client may be waiting on that tag before it
	// sends or consumes the next pipelined Execute. Only a simple-query's final
	// statement marks deferredCommandComplete; COPY uses the specialized helper
	// below because its completion is produced by a later protocol message.
	return h != nil && h.deferredCommandComplete
}

// shouldDeferCopyCommandComplete preserves the simple-query final-statement
// deferral used to keep a completion tag behind an implicit commit. Extended
// COPY must not be deferred: PostgreSQL sends CommandComplete immediately
// after CopyDone, before any later pipelined Parse/Bind/Execute or Sync. COPY
// FROM/TO can finish in a later handler, but the simple-query marker remains
// authoritative for the protocol mode that opened it.
func (h *ConnectionHandler) shouldDeferCopyCommandComplete() bool {
	return h != nil && h.deferredCommandComplete
}

// sendPendingCommandCompletes emits delayed tags only after the surrounding
// extended-query transaction has finalized successfully. Callers clear the
// queue on any finalization error, so a failed group cannot report success.
func (h *ConnectionHandler) sendPendingCommandCompletes() error {
	for _, response := range h.takePendingProtocolMessages() {
		if err := h.send(response); err != nil {
			return err
		}
	}
	return nil
}

// handleProtocolError applies the transaction error policy and completes an
// extended protocol exchange. PostgreSQL sends ErrorResponse immediately and
// ignores messages until Sync; waiting for Sync before sending the error can
// deadlock clients that send Sync only after observing the error.
func (h *ConnectionHandler) handleProtocolError(err error, waitForSync bool) error {
	if err == nil {
		return nil
	}
	err = h.finishImplicitPostgresError(err)
	// Any delayed completion belongs to the failed message group and must not
	// be emitted after its ErrorResponse/ReadyForQuery boundary.
	h.clearPendingCommandCompletes()
	if waitForSync && h.waitForSync {
		h.sendError(err)
		if syncErr := h.discardToSync(); syncErr != nil {
			return syncErr
		}
		return h.sendReadyForQuery()
	}
	h.endOfMessages(err)
	return nil
}

// endOfMessages should be called from HandleConnection or a function within HandleConnection. This represents the end
// of the message slice, which may occur naturally (all relevant response messages have been sent) or on error. Once
// endOfMessages has been called, no further messages should be sent, and the connection loop should wait for the next
// query. A nil error should be provided if this is being called naturally.
func (h *ConnectionHandler) endOfMessages(err error) {
	if err != nil {
		// Keep the transaction indicator in sync before sending the error and
		// trailing ReadyForQuery. This also covers parse/bind/protocol errors
		// that do not have a ConvertedStatement available at this layer.
		h.markTransactionError()
		h.sendError(err)
	}
	if sendErr := h.sendReadyForQuery(); sendErr != nil {
		// We panic here for the same reason as above.
		panic(sendErr)
	}
}

// sendError sends the given error to the client. This should generally never be called directly.
func (h *ConnectionHandler) sendError(err error) {
	fmt.Println(err.Error())
	severity := string(ErrorResponseSeverity_Error)
	code := "XX000" // internal_error for now
	message := err.Error()
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		severity = pgErr.Severity
		if severity == "" {
			severity = string(ErrorResponseSeverity_Error)
		}
		code = pgErr.Code
		message = pgErr.Message
	}
	if sendErr := h.send(&pgproto3.ErrorResponse{
		Severity: severity,
		Code:     code,
		Message:  message,
	}); sendErr != nil {
		// If we're unable to send anything to the connection, then there's something wrong with the connection and
		// we should terminate it. This will be caught in HandleConnection's defer block.
		panic(sendErr)
	}
}

// convertQuery takes the given Postgres query, and converts it as a list of ast.ConvertedStatement that will work with the handler.
func (h *ConnectionHandler) convertQuery(query string, modifiers ...QueryModifier) ([]ConvertedStatement, error) {
	originalQuery := query
	for _, modifier := range modifiers {
		query = modifier(query)
	}

	// Check if the query is a subscription query, and if so, parse it as a subscription query.
	subscriptionConfig, err := parseSubscriptionSQL(query)
	if subscriptionConfig != nil && err == nil {
		return []ConvertedStatement{{
			String:             query,
			OriginalString:     originalQuery,
			PgParsable:         true,
			SubscriptionConfig: subscriptionConfig,
		}}, nil
	}

	// Check if the query is a backup/restore query, and if so, parse it as a backup/restore query.
	backupConfig, err := parseBackupSQL(query)
	if backupConfig != nil && err == nil {
		return []ConvertedStatement{{
			String:         query,
			OriginalString: originalQuery,
			PgParsable:     true,
			BackupConfig:   backupConfig,
		}}, nil
	}
	restoreConfig, err := parseRestoreSQL(query)
	if restoreConfig != nil && err == nil {
		return []ConvertedStatement{{
			String:         query,
			OriginalString: originalQuery,
			PgParsable:     true,
			RestoreConfig:  restoreConfig,
		}}, nil
	}

	stmts, err := parser.Parse(query)
	if err != nil {
		// DuckDB syntax is not fully compatible with PostgreSQL, so we need to handle some queries differently.
		stmts, _ = parser.Parse("SELECT 'SQL syntax is incompatible with PostgreSQL' AS error")
		return []ConvertedStatement{{
			String:         query,
			OriginalString: originalQuery,
			AST:            stmts[0].AST,
			Tag:            GuessStatementTag(query),
			PgParsable:     false,
		}}, nil
	}

	if len(stmts) == 0 {
		return []ConvertedStatement{{String: query, OriginalString: originalQuery}}, nil
	}

	convertedStmts := make([]ConvertedStatement, len(stmts))
	for i, stmt := range stmts {
		// Check if the query is a full match query, and if so, handle it as a full match query.
		fullMatchQuery := handleFullMatchQuery(stmt.SQL)
		if fullMatchQuery != "" {
			convertedStmts[i].String = fullMatchQuery
		} else {
			convertedStmts[i].String = stmt.SQL
		}
		if len(stmts) == 1 {
			convertedStmts[i].OriginalString = originalQuery
		} else {
			convertedStmts[i].OriginalString = stmt.SQL
		}
		convertedStmts[i].AST = stmt.AST
		convertedStmts[i].Tag = stmt.AST.StatementTag()
		convertedStmts[i].PgParsable = true
	}
	return convertedStmts, nil
}

// discardAll handles the DISCARD ALL command
func (h *ConnectionHandler) discardAll(query ConvertedStatement) error {
	// DISCARD ALL resets the physical session. PostgreSQL forbids it while a
	// transaction block is active; closing the pooled connection here would
	// otherwise roll back (or replace) the transaction while leaving the wire
	// status at T. The same guard covers an implicit simple-query scope and a
	// promoted BEGIN scope.
	if h != nil {
		status := h.readyForQueryStatus()
		if h.implicitTx != nil || status == ReadyForQueryTransactionIndicator_TransactionBlock {
			return &pgconn.PgError{
				Severity: "ERROR",
				Code:     "25001",
				Message:  "DISCARD ALL cannot run inside a transaction block",
			}
		}
	}
	h.closeBackendConn()

	return h.sendOrQueueProtocolMessage(&pgproto3.CommandComplete{
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
	if err := h.rejectStatementIfTransactionFailed(query); err != nil {
		return err
	}
	if err := catalog.RejectSensitiveSQL(query.String); err != nil {
		return err
	}
	if auditQuery := query.QueryForAudit(); auditQuery != query.String {
		if err := catalog.RejectSensitiveSQL(auditQuery); err != nil {
			return err
		}
	}
	sqlCtx, err := h.duckHandler.NewContext(frontendContext(context.Background()), h.mysqlConn, query.String)
	if err != nil {
		return err
	}
	sqlCtx.SetLogger(sqlCtx.GetLogger().WithField("query", catalog.RedactSensitiveSQL(query.QueryForAudit())))
	if copyFrom.Options.CopyFormat == CopyFormatArrow {
		if err := rejectArrowCopyGMSInTransaction(sqlCtx); err != nil {
			return err
		}
	}

	table, err := ValidateCopyFrom(copyFrom, sqlCtx)
	if err != nil {
		return err
	}
	binding, err := capturePostgresCopyBindingWithHandler(sqlCtx, h.duckHandler, copyFrom)
	if err != nil {
		return err
	}
	if copyFrom.Options.CopyFormat == CopyFormatArrow {
		if err := rejectArrowCopyBinding(sqlCtx, binding); err != nil {
			binding.releaseLease()
			return err
		}
	}

	h.copyFromStdinState = &copyFromStdinState{
		ctx:                  sqlCtx,
		copyFromStdinNode:    copyFrom,
		targetTable:          table,
		rawOptions:           rawOptions,
		binding:              binding,
		deferCommandComplete: h.deferredCommandComplete,
	}

	var format byte
	switch copyFrom.Options.CopyFormat {
	case tree.CopyFormatText, tree.CopyFormatCSV, CopyFormatJSON:
		format = 0 // text format
	default:
		format = 1 // binary format
	}

	if err := h.send(&pgproto3.CopyInResponse{
		OverallFormat: format,
	}); err != nil {
		// The client never entered COPY mode, so no later terminal message can
		// release this retained snapshot. Release it on the setup failure and
		// clear the state before returning the wire error.
		binding.releaseLease()
		h.copyFromStdinState = nil
		return err
	}
	return nil
}

// DiscardToSync discards all messages in the buffer until a Sync has been reached. If a Sync was never sent, then this
// may cause the connection to lock until the client send a Sync, as their request structure was malformed.
//
// A receive failure is terminal for the current exchange. It cannot be treated
// like a normal loop return: an active COPY loader may still own a worker and a
// pool snapshot, while an implicit protocol transaction may still need its
// rollback callback to release the operation lease. Keep the receive error as
// the primary cause, but finish all local lifecycle work before returning it.
func (h *ConnectionHandler) cleanupDiscardToSyncReceiveError(receiveErr error) error {
	if h == nil {
		return receiveErr
	}

	// There will be no later Sync after EOF/decode failure. Clear this marker
	// up front so a teardown/recovery caller cannot leave the handler in a stale
	// extended-exchange state.
	h.waitForSync = false
	state := h.copyFromStdinState
	var cleanupErr error
	if state != nil {
		// Passing a nil cause keeps a successful abort from manufacturing a
		// second error; Abort/Wait errors are still returned by the helper.
		cleanupErr = h.abortCopyFromStdinState(nil)
	}

	// Completions from the interrupted group must never be emitted after its
	// receive boundary. A completed extended COPY can also have a lease held
	// until Sync; release it before entering rollback so cleanup cannot wait on
	// its own operation admission.
	h.clearPendingCommandCompletes()
	h.releasePendingCopyLease()

	var rollbackErr error
	if h.implicitScopeActive() {
		rollbackErr = h.finalizeImplicitPostgresTransaction(false)
		// A registered physical callback normally releases this half during the
		// finalizer. Keep the defensive idempotent release used by CopyDone so a
		// failed/evicted finalizer cannot strand the lease.
		if state != nil {
			state.binding.releaseOperation()
		}
	} else {
		// Explicit transaction blocks remain owned by their physical finalizer;
		// record the protocol failure without claiming that the transaction was
		// rolled back here.
		h.markTransactionError()
	}

	return errors.Join(receiveErr, cleanupErr, rollbackErr)
}

func (h *ConnectionHandler) discardToSync() error {
	for {
		message, err := h.backend.Receive()
		if err != nil {
			return h.cleanupDiscardToSyncReceiveError(err)
		}

		switch message.(type) {
		case *pgproto3.CopyDone, *pgproto3.CopyFail:
			// COPY errors are sent before the client terminal message. Consume
			// that message while discarding the extended exchange, then let Sync
			// provide the single ReadyForQuery boundary.
			if state := h.copyFromStdinState; state != nil && state.copyErr != nil {
				h.releaseCopyLease(state)
				h.copyFromStdinState = nil
			}
		case *pgproto3.Sync:
			h.waitForSync = false
			var syncErr error
			if state := h.copyFromStdinState; state != nil {
				if state.copyErr != nil {
					// A terminal COPY sentinel is already aborted and only needs
					// to be released at the protocol boundary.
					h.releaseCopyLease(state)
					h.copyFromStdinState = nil
				} else {
					syncErr = h.abortCopyFromStdinState(nil)
				}
				h.markTransactionError()
				h.clearPendingCommandCompletes()
			}
			// Error paths normally roll back before entering this loop. Keep the
			// boundary self-contained for callers that invoke discardToSync
			// directly or for a panic recovered outside a statement handler.
			if h.implicitScopeActive() {
				if rollbackErr := h.finalizeImplicitPostgresTransaction(false); rollbackErr != nil {
					syncErr = errors.Join(syncErr, rollbackErr)
				}
			}
			h.releasePendingCopyLease()
			if syncErr != nil {
				return syncErr
			}
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

// copyToStdoutCancellationError preserves a concrete error recorded by the
// asynchronous COPY reader/producer when it cancels the protocol context.
// Keeping this selection in one place prevents the stale setup error from
// masking the failure that actually reached the client-facing path.
func copyToStdoutCancellationError(ctxErr error, globalErr *atomic.Pointer[error]) error {
	if errPtr := globalErr.Load(); errPtr != nil {
		return errors.Join(ctxErr, *errPtr)
	}
	return ctxErr
}

func (h *ConnectionHandler) handleCopyToStdout(query ConvertedStatement, copyTo *tree.CopyTo, subquery string, format tree.CopyFormat, rawOptions string) error {
	if err := h.rejectStatementIfTransactionFailed(query); err != nil {
		return err
	}
	if err := catalog.RejectSensitiveSQL(query.String); err != nil {
		return err
	}
	if auditQuery := query.QueryForAudit(); auditQuery != query.String {
		if err := catalog.RejectSensitiveSQL(auditQuery); err != nil {
			return err
		}
	}
	ctx, err := h.duckHandler.NewContext(frontendContext(context.Background()), h.mysqlConn, query.String)
	if err != nil {
		return err
	}
	ctx.SetLogger(ctx.GetLogger().WithField("query", catalog.RedactSensitiveSQL(query.QueryForAudit())))
	if format == CopyFormatArrow {
		if err := rejectArrowCopyGMSInTransaction(ctx); err != nil {
			return err
		}
	}

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

	var globalErr atomic.Pointer[error]
	pipePath, ch, err := writer.Start(&globalErr)
	if err != nil {
		return err
	}

	done := make(chan struct{})
	var blocked atomic.Bool
	blocked.Store(true)
	go func() {
		defer close(done)

		// Open the pipe for reading.
		ctx.GetLogger().Tracef("Opening FIFO pipe for reading: %s", pipePath)
		pipe, err := os.OpenFile(pipePath, os.O_RDONLY, os.ModeNamedPipe)
		blocked.Store(false)
		if err != nil {
			err = fmt.Errorf("failed to open pipe for reading: %w", err)
			globalErr.Store(&err)
			cancel()
			return
		}
		defer pipe.Close()

		// If the error has been set, then we should cancel the operation.
		if globalErr.Load() != nil {
			cancel()
			return
		}

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
			responsed := false
			reader := bufio.NewReader(pipe)
			for {
				line, err := reader.ReadSlice('\n')
				if err != nil {
					if err == io.EOF {
						break
					}
					globalErr.Store(&err)
					cancel()
					return
				}
				if !responsed {
					responsed = true
					count := bytes.Count(line, []byte{'\t'})
					err := sendCopyOutResponse(count + 1)
					if err != nil {
						globalErr.Store(&err)
						cancel()
						return
					}
				}
				err = sendCopyData(line)
				if err != nil {
					globalErr.Store(&err)
					cancel()
					return
				}
			}
		default:
			err := sendCopyOutResponse(1)
			if err != nil {
				globalErr.Store(&err)
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
					globalErr.Store(&err)
					cancel()
					return
				}
				if n > 0 {
					err := sendCopyData(buf[:n])
					if err != nil {
						globalErr.Store(&err)
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
		// The reader/producer goroutine may have canceled the context after
		// recording a concrete COPY or wire error. Preserve that error rather
		// than returning the stale setup error from writer.Start.
		return copyToStdoutCancellationError(ctx.Err(), &globalErr)
	case result := <-ch:
		if blocked.Load() {
			// If the pipe is still opened for reading but the writer has exited,
			// then we need to open the pipe for writing again to unblock the reader.
			pipe, _ := os.OpenFile(pipePath, os.O_WRONLY, os.ModeNamedPipe)
			<-done
			if pipe != nil {
				pipe.Close()
			}
		} else {
			<-done
		}

		if result.Err != nil {
			return fmt.Errorf("failed to copy data: %w", result.Err)
		}

		if errPtr := globalErr.Load(); errPtr != nil {
			return *errPtr
		}

		// After data is sent and the producer side is finished without errors, send CopyDone
		ctx.GetLogger().Debug("sending CopyDone to the client")
		if err := h.send(&pgproto3.CopyDone{}); err != nil {
			return err
		}

		// Send CommandComplete with the number of rows copied
		ctx.GetLogger().Debugf("sending CommandComplete to the client")
		commandComplete := &pgproto3.CommandComplete{
			CommandTag: []byte(fmt.Sprintf("COPY %d", result.RowCount)),
		}
		if h.shouldDeferCopyCommandComplete() {
			h.queueCommandComplete(commandComplete)
			return nil
		}
		return h.send(commandComplete)
	}
}
