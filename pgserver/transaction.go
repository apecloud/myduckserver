package pgserver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5/pgconn"
)

const postgresFailedTransactionMessage = "current transaction is aborted, commands ignored until end of transaction block"

// postgresFailedTransactionControlKey marks a protocol context whose COMMIT
// must use the failed-transaction rollback semantics. The marker lives on the
// context rather than in the transaction map so direct handler calls and GMS
// query execution can share the same transaction-control implementation.
type postgresFailedTransactionControlKey struct{}

// postgresConnectionHandlerKey carries the wire handler through the context
// used by the GMS executor. Transaction-control statements are executed below
// the protocol layer, but BEGIN still needs to promote a protocol-owned
// implicit scope when it appears after ordinary work in the same exchange.
type postgresConnectionHandlerKey struct{}

func withPostgresConnectionHandler(ctx context.Context, handler *ConnectionHandler) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, postgresConnectionHandlerKey{}, handler)
}

func postgresConnectionHandler(ctx context.Context) *ConnectionHandler {
	if ctx == nil {
		return nil
	}
	handler, _ := ctx.Value(postgresConnectionHandlerKey{}).(*ConnectionHandler)
	return handler
}

func withPostgresFailedTransactionControl(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, postgresFailedTransactionControlKey{}, true)
}

func isPostgresFailedTransactionControl(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	failed, _ := ctx.Value(postgresFailedTransactionControlKey{}).(bool)
	return failed
}

func postgresFailedTransactionError() error {
	return &pgconn.PgError{
		Severity: "ERROR",
		Code:     "25P02",
		Message:  postgresFailedTransactionMessage,
	}
}

// rejectStatementIfTransactionFailed implements PostgreSQL's sticky failed
// transaction state. COMMIT and ROLLBACK are the only controls supported by
// this handler that can end the block; all other SQL must be rejected before
// any handler, metadata probe, or executor is touched.
func (h *ConnectionHandler) rejectStatementIfTransactionFailed(statement ConvertedStatement) error {
	if h == nil || h.readyForQueryStatus() != ReadyForQueryTransactionIndicator_FailedTransactionBlock {
		return nil
	}
	if isEmptyConvertedStatement(statement) {
		return nil
	}
	switch statement.AST.(type) {
	case *tree.CommitTransaction, *tree.RollbackTransaction:
		return nil
	default:
		return postgresFailedTransactionError()
	}
}

func isEmptyConvertedStatement(statement ConvertedStatement) bool {
	if statement.AST != nil {
		return false
	}
	switch strings.TrimSpace(statement.String) {
	case "", ";":
		// Empty queries are protocol no-ops and PostgreSQL keeps the failed
		// state. A standalone semicolon follows the same rule.
		return true
	default:
		return false
	}
}

// postgresDatabaseDDLName identifies database-file DDL handled by the
// PostgreSQL adapter rather than DuckDB's transactional executor. DuckDB's
// ATTACH/DETACH plus the backing-file mutation cannot be rolled back as part of
// a user transaction, so these statements must be rejected while a block is
// active and must not open an implicit transaction when run alone.
func postgresDatabaseDDLName(statement tree.Statement) (string, bool) {
	switch statement.(type) {
	case *tree.CreateDatabase:
		return "CREATE DATABASE", true
	case *tree.DropDatabase:
		return "DROP DATABASE", true
	default:
		return "", false
	}
}

func postgresDatabaseDDLTransactionError(statement tree.Statement) error {
	name, ok := postgresDatabaseDDLName(statement)
	if !ok {
		return nil
	}
	return &pgconn.PgError{
		Severity: "ERROR",
		Code:     "25001",
		Message:  name + " cannot run inside a transaction block",
	}
}

// rejectPostgresDatabaseDDLInTransaction is shared by protocol lifecycle
// gates and the direct PostgreSQL query handler. The handler state catches an
// explicit or message-group transaction even when a test/minimal session does
// not expose a transaction map; the binding check covers direct callers that
// have an active session transaction but no protocol handler marker.
func rejectPostgresDatabaseDDLInTransaction(ctx *sql.Context, statement tree.Statement) error {
	if _, ok := postgresDatabaseDDLName(statement); !ok {
		return nil
	}
	if handler := postgresConnectionHandler(ctx); handler != nil {
		switch handler.readyForQueryStatus() {
		case ReadyForQueryTransactionIndicator_FailedTransactionBlock:
			return postgresFailedTransactionError()
		case ReadyForQueryTransactionIndicator_TransactionBlock:
			return postgresDatabaseDDLTransactionError(statement)
		}
		if handler.implicitTx != nil {
			return postgresDatabaseDDLTransactionError(statement)
		}
	}
	if ctx != nil {
		if _, tx := adapter.TryGetTxnBinding(ctx); tx != nil {
			return postgresDatabaseDDLTransactionError(statement)
		}
	}
	return nil
}

// rejectFailedQueryStatements checks a complete simple-query/Parse payload
// before sensitive-SQL gates or protocol shortcuts run. A simple query may
// contain multiple statements, so model the local status as transaction
// controls are encountered: COMMIT/ROLLBACK clear E and statements after the
// control are evaluated as idle. The handler's real state is changed only by
// successful execution.
func (h *ConnectionHandler) rejectFailedQueryStatements(statements []ConvertedStatement) error {
	if h == nil {
		return nil
	}
	status := h.readyForQueryStatus()
	for _, statement := range statements {
		if status != ReadyForQueryTransactionIndicator_FailedTransactionBlock {
			continue
		}
		if isEmptyConvertedStatement(statement) {
			continue
		}
		switch statement.AST.(type) {
		case *tree.CommitTransaction, *tree.RollbackTransaction:
			status = ReadyForQueryTransactionIndicator_Idle
		default:
			return postgresFailedTransactionError()
		}
	}
	return nil
}

// protocolContext returns the ordinary frontend context and carries the
// failed-state marker needed to turn COMMIT into rollback when appropriate.
func (h *ConnectionHandler) protocolContext() context.Context {
	ctx := withPostgresConnectionHandler(frontendContext(context.Background()), h)
	if h != nil && h.readyForQueryStatus() == ReadyForQueryTransactionIndicator_FailedTransactionBlock {
		ctx = withPostgresFailedTransactionControl(ctx)
	}
	return ctx
}

// protocolCommandTag reports PostgreSQL's command tag for a protocol
// transaction control statement. COMMIT in a failed block discards the block,
// so the server reports ROLLBACK just as PostgreSQL does.
func (h *ConnectionHandler) protocolCommandTag(statement ConvertedStatement) string {
	if h != nil && h.readyForQueryStatus() == ReadyForQueryTransactionIndicator_FailedTransactionBlock {
		if _, ok := statement.AST.(*tree.CommitTransaction); ok {
			return "ROLLBACK"
		}
	}
	return statement.Tag
}

// isInactiveTransactionError recognizes only the two driver outcomes that
// prove a transaction is already inactive. Other rollback failures leave the
// physical connection untrusted and must be surfaced to the client.
func isInactiveTransactionError(err error) bool {
	if err == nil {
		return true
	}
	return errors.Is(err, stdsql.ErrTxDone) ||
		strings.Contains(strings.ToLower(err.Error()), "no transaction is active")
}

func postgresBeginOptions(stmt *tree.BeginTransaction) *stdsql.TxOptions {
	if stmt != nil && stmt.Modes.ReadWriteMode == tree.ReadOnly {
		return &stdsql.TxOptions{ReadOnly: true}
	}
	return &stdsql.TxOptions{}
}

// implicitScopeActive reports whether the handler owns a transaction that is
// still bounded by the current protocol message group. Once BEGIN promotes the
// scope, the same physical binding remains in the pool but Sync must no longer
// commit it automatically.
func (h *ConnectionHandler) implicitScopeActive() bool {
	return h != nil && h.implicitTx != nil && !h.implicitTxPromoted
}

func (h *ConnectionHandler) clearImplicitScope() {
	if h == nil {
		return
	}
	h.implicitTx = nil
	h.implicitTxCtx = nil
	h.implicitTxConn = nil
	h.implicitTxPromoted = false
}

func (h *ConnectionHandler) implicitScopeContext() (*sql.Context, error) {
	if h == nil {
		return nil, fmt.Errorf("postgres transaction context is unavailable")
	}
	if h.implicitTxCtx != nil {
		return h.implicitTxCtx, nil
	}
	ctx, err := h.newProtocolSQLContext("")
	if err != nil {
		return nil, err
	}
	h.implicitTxCtx = ctx
	return ctx, nil
}

func samePostgresBinding(expectedConn, expectedTx, currentConn, currentTx any) bool {
	return expectedConn == currentConn && expectedTx == currentTx
}

// closeKnownPostgresTransaction removes only the supplied transaction mapping.
// It is deliberately best-effort: a malformed or replacement mapping must not
// be guessed at, and the caller still reports the lifecycle error that caused
// cleanup to be attempted.
func closeKnownPostgresTransaction(ctx *sql.Context, tx *stdsql.Tx) {
	if ctx == nil || tx == nil {
		return
	}
	adapter.CloseTxnIf(ctx, tx)
}

// postgresTransactionBinding returns the complete physical identity for an
// active transaction. A transaction pointer without its owner is not a usable
// binding: remove that exact mapping when the session supports it and fail
// closed so callers cannot fall back to a replacement connection.
func postgresTransactionBinding(ctx *sql.Context, operation string) (*stdsql.Conn, *stdsql.Tx, error) {
	if ctx == nil {
		return nil, nil, fmt.Errorf("%s: postgres transaction context is unavailable", operation)
	}
	conn, tx := adapter.TryGetTxnBindingForRelease(ctx)
	if tx == nil {
		return conn, nil, nil
	}
	if conn == nil {
		closeKnownPostgresTransaction(ctx, tx)
		return nil, nil, fmt.Errorf("%s: active postgres transaction has no physical owner", operation)
	}
	return conn, tx, nil
}

// invalidateImplicitScope closes the exact physical owner when possible and
// drops the local marker. It never closes the currently observed replacement
// connection: callers pass only the owner captured when the scope was opened.
func (h *ConnectionHandler) invalidateImplicitScope(cause error) error {
	if h == nil {
		return cause
	}
	owner := h.implicitTxConn
	ctx := h.implicitTxCtx
	tx := h.implicitTx
	promoted := h.implicitTxPromoted
	// Only evict the physical owner if the exact transaction is still bound.
	// A replacement transaction may reuse the same *sql.Conn; closing by
	// connection identity alone would take that replacement down with it.
	ownerStillBound := owner != nil
	if ownerStillBound && ctx != nil {
		currentConn, currentTx := adapter.TryGetTxnBindingForRelease(ctx)
		ownerStillBound = samePostgresBinding(owner, tx, currentConn, currentTx)
	}
	if ownerStillBound {
		var closeErr error
		if ctx != nil {
			// Keep the transaction identity in the close operation. A replacement
			// transaction may reuse the same physical connection; a connection-only
			// close would evict that replacement after the binding changed.
			closeErr = adapter.CloseConnIfBinding(ctx, owner, tx)
		} else {
			closeErr = owner.Close()
		}
		cause = errors.Join(cause, closeErr)
	}
	// If the owner was already missing, CloseConnIfBinding cannot remove the
	// transaction entry. Remove only the captured transaction instead; an
	// identity-aware session leaves a replacement transaction untouched.
	closeKnownPostgresTransaction(ctx, tx)
	remainingTx := false
	if ctx != nil {
		_, currentTx := adapter.TryGetTxnBindingForRelease(ctx)
		remainingTx = currentTx != nil
	}
	h.clearImplicitScope()
	// Any identity violation is unrecoverable at this layer, even when the map
	// happens to be empty after cleanup: the driver state of the captured
	// transaction was not proven inactive. Keep a sticky failed state rather
	// than advertising ReadyForQuery=I over an uncertain lifecycle.
	_ = promoted
	_ = remainingTx
	h.txStatus = ReadyForQueryTransactionIndicator_FailedTransactionBlock
	return cause
}

func (h *ConnectionHandler) validateImplicitBinding() error {
	if h == nil || h.implicitTx == nil {
		return nil
	}
	if h.implicitTxConn == nil {
		return h.invalidateImplicitScope(fmt.Errorf("postgres implicit transaction owner is missing"))
	}
	ctx, err := h.implicitScopeContext()
	if err != nil {
		// The scope cannot be finalized without its saved context. Drop the
		// marker and retain a sticky failed state so a later statement cannot
		// silently open a different transaction.
		h.clearImplicitScope()
		h.txStatus = ReadyForQueryTransactionIndicator_FailedTransactionBlock
		return err
	}
	currentConn, currentTx := adapter.TryGetTxnBinding(ctx)
	if currentConn != nil && currentTx != nil &&
		samePostgresBinding(h.implicitTxConn, h.implicitTx, currentConn, currentTx) {
		return nil
	}
	return h.invalidateImplicitScope(fmt.Errorf("postgres implicit transaction binding changed"))
}

// promoteImplicitPostgresTransaction converts an implicit message-group
// transaction into PostgreSQL's explicit transaction block without opening a
// second physical transaction. The saved context/owner remains the identity
// authority for subsequent controls and cleanup.
func (h *ConnectionHandler) promoteImplicitPostgresTransaction() error {
	if h == nil || h.implicitTx == nil {
		return nil
	}
	if h.implicitTxPromoted {
		return h.validateImplicitBinding()
	}
	if err := h.validateImplicitBinding(); err != nil {
		return err
	}
	h.implicitTxPromoted = true
	h.txStatus = ReadyForQueryTransactionIndicator_TransactionBlock
	return nil
}

// beginPostgresTransaction binds an explicit PostgreSQL BEGIN to the same
// session transaction map used by MySQL. The direct PostgreSQL handler does
// not run GMS's transaction iterator, so this binding must be established here.
func beginPostgresTransaction(ctx *sql.Context, stmt *tree.BeginTransaction) error {
	h := postgresConnectionHandler(ctx)
	if h != nil {
		status := h.readyForQueryStatus()
		// PostgreSQL's failed block is sticky. BEGIN is an ordinary command in
		// that state and must not open a fresh physical transaction.
		if status == ReadyForQueryTransactionIndicator_FailedTransactionBlock {
			return postgresFailedTransactionError()
		}
		if h.implicitTx != nil {
			// BEGIN after ordinary work in this protocol exchange promotes the
			// existing physical transaction instead of becoming a nested BEGIN.
			if !h.implicitTxPromoted {
				return h.promoteImplicitPostgresTransaction()
			}
			return h.validateImplicitBinding()
		}
		currentConn, currentTx := adapter.TryGetTxnBinding(ctx)
		switch status {
		case ReadyForQueryTransactionIndicator_TransactionBlock:
			if currentTx == nil {
				h.txStatus = ReadyForQueryTransactionIndicator_FailedTransactionBlock
				return fmt.Errorf("postgres explicit transaction binding is missing")
			}
			if currentConn == nil {
				closeKnownPostgresTransaction(ctx, currentTx)
				h.txStatus = ReadyForQueryTransactionIndicator_FailedTransactionBlock
				return fmt.Errorf("postgres explicit transaction owner is missing")
			}
		case ReadyForQueryTransactionIndicator_Idle:
			if currentTx == nil {
				break
			}
			// An unmarked binding while the wire state is idle is stale. Evict
			// only its observed owner and refuse to execute on a replacement.
			staleErr := fmt.Errorf("postgres transaction binding exists while protocol is idle")
			if currentConn != nil {
				staleErr = errors.Join(staleErr, adapter.CloseConnIfBinding(ctx, currentConn, currentTx))
			}
			closeKnownPostgresTransaction(ctx, currentTx)
			if currentConn == nil {
				h.txStatus = ReadyForQueryTransactionIndicator_FailedTransactionBlock
				staleErr = errors.Join(staleErr, fmt.Errorf("postgres stale transaction owner is missing"))
			}
			return staleErr
		}
	}
	// PostgreSQL treats BEGIN inside an active transaction block as a successful
	// no-op. The wire handler emits the documented WARNING separately; retaining
	// the existing binding here keeps the physical transaction untouched.
	currentConn, currentTx := adapter.TryGetTxnBinding(ctx)
	if currentTx != nil {
		if currentConn == nil {
			closeKnownPostgresTransaction(ctx, currentTx)
			if h != nil {
				h.txStatus = ReadyForQueryTransactionIndicator_FailedTransactionBlock
			}
			return fmt.Errorf("postgres transaction owner is missing")
		}
		return nil
	}
	tx, err := adapter.GetTxn(ctx, postgresBeginOptions(stmt))
	if err != nil {
		return err
	}
	if tx == nil {
		return fmt.Errorf("postgres BEGIN returned no transaction")
	}
	owner, boundTx := adapter.TryGetTxnBinding(ctx)
	if owner == nil || boundTx != tx {
		closeKnownPostgresTransaction(ctx, tx)
		if owner != nil {
			// The pair check prevents a replacement transaction from being
			// evicted when GetTxn raced with another lifecycle operation.
			_ = adapter.CloseConnIfBinding(ctx, owner, tx)
		}
		if h != nil {
			h.txStatus = ReadyForQueryTransactionIndicator_FailedTransactionBlock
		}
		if boundTx != tx {
			return fmt.Errorf("postgres explicit transaction binding changed while opening")
		}
		return fmt.Errorf("postgres explicit transaction owner missing while opening")
	}
	return nil
}

// implicitPostgresStatement reports whether a statement participates in the
// transaction that PostgreSQL creates around a simple-query message or an
// extended-query exchange. Protocol controls are handled separately and a few
// session-object commands deliberately stay outside a DuckDB transaction
// because they operate on the connection itself (for example DISCARD ALL).
func implicitPostgresStatement(statement ConvertedStatement) bool {
	if statement.AST == nil || isEmptyConvertedStatement(statement) {
		return false
	}
	switch statement.AST.(type) {
	case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction,
		*tree.Deallocate, *tree.Discard, *tree.SetVar, *tree.SetSessionCharacteristics,
		*tree.ShowVar, *tree.CreateDatabase, *tree.DropDatabase:
		return false
	case *tree.CopyFrom:
		// Arrow COPY currently registers a view through *sql.Conn.Raw and is
		// deliberately rejected when a transaction is already bound. Do not
		// create an implicit binding that would make every ordinary Arrow COPY
		// fail before its loader can start.
		return statement.AST.(*tree.CopyFrom).Options.CopyFormat != CopyFormatArrow
	case *tree.CopyTo:
		return statement.AST.(*tree.CopyTo).Options.CopyFormat != CopyFormatArrow
	default:
		if statement.Tag == "COPY" {
			if _, format, _, ok := ParseCopyFrom(statement.String); ok && format == CopyFormatArrow {
				return false
			}
			if _, format, _, ok := ParseCopyTo(statement.String); ok && format == CopyFormatArrow {
				return false
			}
		}
		return true
	}
}

// newProtocolSQLContext creates a context for lifecycle work that is not tied
// to a single client request. In particular, implicit transaction finalizers
// must remain runnable after a request context has been canceled.
func (h *ConnectionHandler) newProtocolSQLContext(query string) (*sql.Context, error) {
	if h == nil || h.duckHandler == nil {
		return nil, fmt.Errorf("postgres transaction context is unavailable")
	}
	ctx := withPostgresConnectionHandler(frontendContext(context.Background()), h)
	return h.duckHandler.NewContext(ctx, h.mysqlConn, query)
}

// ensureImplicitPostgresTransaction starts the physical DuckDB transaction
// used by a PostgreSQL implicit transaction scope. The handler keeps the
// pointer so the scope can be committed at the end of a successful simple
// query or Sync, or rolled back after a protocol error. Explicit BEGIN owns its
// binding directly and therefore never sets implicitTx.
func (h *ConnectionHandler) ensureImplicitPostgresTransaction(statement ConvertedStatement) error {
	if h == nil {
		return nil
	}
	if _, databaseDDL := postgresDatabaseDDLName(statement.AST); databaseDDL {
		// Database-file DDL is deliberately outside the implicit transaction
		// scope. Check the protocol marker first so minimal/direct handlers do
		// not need to manufacture a SQL context merely to reject an active block.
		switch h.readyForQueryStatus() {
		case ReadyForQueryTransactionIndicator_FailedTransactionBlock:
			return postgresFailedTransactionError()
		case ReadyForQueryTransactionIndicator_TransactionBlock:
			return postgresDatabaseDDLTransactionError(statement.AST)
		}
		if h.implicitTx != nil {
			return postgresDatabaseDDLTransactionError(statement.AST)
		}
		if h.duckHandler == nil || h.mysqlConn == nil {
			return nil
		}
		ctx, err := h.newProtocolSQLContext(statement.String)
		if err != nil {
			return err
		}
		return rejectPostgresDatabaseDDLInTransaction(ctx, statement.AST)
	}
	if !implicitPostgresStatement(statement) {
		return nil
	}
	if h.implicitTx != nil {
		return h.validateImplicitBinding()
	}
	if h.readyForQueryStatus() == ReadyForQueryTransactionIndicator_FailedTransactionBlock {
		return postgresFailedTransactionError()
	}
	ctx, err := h.newProtocolSQLContext(statement.String)
	if err != nil {
		return err
	}
	if err := rejectPostgresDatabaseDDLInTransaction(ctx, statement.AST); err != nil {
		return err
	}
	status := h.readyForQueryStatus()
	currentConn, existing := adapter.TryGetTxnBinding(ctx)
	if status == ReadyForQueryTransactionIndicator_TransactionBlock {
		if existing == nil {
			h.txStatus = ReadyForQueryTransactionIndicator_FailedTransactionBlock
			return fmt.Errorf("postgres explicit transaction binding is missing")
		}
		if currentConn == nil {
			closeKnownPostgresTransaction(ctx, existing)
			h.txStatus = ReadyForQueryTransactionIndicator_FailedTransactionBlock
			return fmt.Errorf("postgres explicit transaction owner is missing")
		}
		// The explicit transaction is owned by the pool; do not claim it as
		// implicit or let a later Sync commit it.
		return nil
	}
	if existing != nil {
		staleErr := fmt.Errorf("postgres transaction binding exists while protocol is idle")
		if currentConn != nil {
			staleErr = errors.Join(staleErr, adapter.CloseConnIfBinding(ctx, currentConn, existing))
		}
		closeKnownPostgresTransaction(ctx, existing)
		if currentConn == nil {
			h.txStatus = ReadyForQueryTransactionIndicator_FailedTransactionBlock
			staleErr = errors.Join(staleErr, fmt.Errorf("postgres stale transaction owner is missing"))
		}
		return staleErr
	}
	tx, err := adapter.GetTxn(ctx, &stdsql.TxOptions{})
	if err != nil {
		return err
	}
	owner, boundTx := adapter.TryGetTxnBinding(ctx)
	if boundTx != tx || owner == nil {
		closeKnownPostgresTransaction(ctx, tx)
		if owner != nil {
			// The strict pair check prevents this cleanup from evicting a
			// replacement transaction that won the race after GetTxn returned.
			_ = adapter.CloseConnIfBinding(ctx, owner, tx)
		}
		if boundTx != tx {
			h.txStatus = ReadyForQueryTransactionIndicator_FailedTransactionBlock
			return fmt.Errorf("postgres implicit transaction binding changed while opening")
		}
		h.txStatus = ReadyForQueryTransactionIndicator_FailedTransactionBlock
		return fmt.Errorf("postgres implicit transaction owner missing while opening")
	}
	h.implicitTx = tx
	h.implicitTxCtx = ctx
	h.implicitTxConn = owner
	h.implicitTxPromoted = false
	return nil
}

// finalizeImplicitPostgresTransaction closes the handler-owned implicit scope
// and always returns the protocol to Idle. Rollback uses the same provider
// cleanup hook as an explicit PostgreSQL ROLLBACK, preserving the physical
// connection and making cleanup errors visible to the caller.
func (h *ConnectionHandler) finalizeImplicitPostgresTransaction(commit bool) error {
	if h == nil || !h.implicitScopeActive() {
		return nil
	}
	tx := h.implicitTx
	ctx, err := h.implicitScopeContext()
	if err != nil {
		closeKnownPostgresTransaction(h.implicitTxCtx, tx)
		h.clearImplicitScope()
		h.txStatus = ReadyForQueryTransactionIndicator_FailedTransactionBlock
		return err
	}
	if h.implicitTxConn == nil {
		return h.invalidateImplicitScope(fmt.Errorf("postgres implicit transaction owner is missing before finalization"))
	}
	currentConn, current := adapter.TryGetTxnBindingForRelease(ctx)
	if !samePostgresBinding(h.implicitTxConn, tx, currentConn, current) {
		return h.invalidateImplicitScope(fmt.Errorf("postgres implicit transaction binding changed before finalization"))
	}
	if commit {
		err = adapter.FinalizeCommit(ctx, tx)
	} else {
		var provider postgresRollbackOrphanCleaner
		if h.duckHandler != nil {
			provider = h.duckHandler.GetCatalogProvider()
		}
		if provider != nil {
			err = rollbackPostgresTransactionControl(ctx, &tree.RollbackTransaction{}, provider)
		} else {
			err = rollbackPostgresTransactionControl(ctx, &tree.RollbackTransaction{})
		}
	}
	// A successful finalizer removes the exact mapping. The transaction pointer
	// is the decisive state signal: afterTx == nil means the expected block is
	// gone (even if the generic connection was replaced), while a non-nil afterTx
	// means a transaction is still active and the protocol must not advertise I.
	afterConn, afterTx := adapter.TryGetTxnBindingForRelease(ctx)
	if afterTx == nil {
		h.clearImplicitScope()
		h.clearPortals()
		h.txStatus = ReadyForQueryTransactionIndicator_Idle
		return err
	}
	if err == nil {
		// A finalizer claiming success while leaving the exact transaction
		// mapped is inconsistent with the lifecycle contract. Evict only the
		// still-matching owner and surface an explicit failure instead of
		// advertising ReadyForQuery=I over an active transaction.
		err = fmt.Errorf("postgres implicit transaction remained bound after finalization")
	}
	if h.implicitTxConn != nil {
		err = errors.Join(err, adapter.CloseConnIfBinding(ctx, h.implicitTxConn, tx))
		afterConn, afterTx = adapter.TryGetTxnBindingForRelease(ctx)
	}
	closeKnownPostgresTransaction(ctx, tx)
	afterConn, afterTx = adapter.TryGetTxnBindingForRelease(ctx)
	if afterTx == nil {
		h.clearImplicitScope()
		h.clearPortals()
		h.txStatus = ReadyForQueryTransactionIndicator_Idle
		return err
	}
	// A different non-nil transaction is a replacement, not proof that the old
	// scope finalized. Drop the stale local marker but retain a sticky failed
	// state so later statements cannot run on that replacement silently.
	if !samePostgresBinding(h.implicitTxConn, tx, afterConn, afterTx) {
		err = errors.Join(err, fmt.Errorf("postgres implicit transaction binding replaced before finalization"))
	}
	h.clearImplicitScope()
	h.clearPortals()
	h.txStatus = ReadyForQueryTransactionIndicator_FailedTransactionBlock
	return err
}

// finishImplicitPostgresError rolls back a protocol-owned implicit scope and
// returns the original error together with any rollback/cleanup error. An
// explicit transaction has no implicit marker and therefore retains the
// normal sticky E state.
func (h *ConnectionHandler) finishImplicitPostgresError(err error) error {
	if err == nil {
		return nil
	}
	// Extended COPY FROM can finish its loader before the protocol receives the
	// following Sync. Its snapshot/operation lease is retained in
	// pendingCopyRelease so a successful group commits before releasing it. If a
	// later pipelined message fails before Sync, however, rollback must not wait
	// on that same lease: release the completed COPY first, then enter the
	// provider rollback reservation. The callback is idempotent and is also safe
	// on paths that already released the lease.
	if h != nil {
		h.releasePendingCopyLease()
	}
	if h != nil && h.implicitScopeActive() {
		return errors.Join(err, h.finalizeImplicitPostgresTransaction(false))
	}
	if h != nil {
		h.markTransactionError()
	}
	return err
}

func commitPostgresTransaction(ctx *sql.Context) error {
	_, tx, err := postgresTransactionBinding(ctx, "postgres COMMIT")
	if err != nil {
		return err
	}
	if tx == nil {
		return nil
	}
	return adapter.FinalizeCommit(ctx, tx)
}

func rollbackPostgresTransactionWithOwner(ctx *sql.Context) (*stdsql.Conn, bool, error) {
	conn, tx, err := postgresTransactionBinding(ctx, "postgres ROLLBACK")
	if err != nil {
		return nil, false, err
	}
	if tx == nil {
		return conn, true, nil
	}
	return adapter.FinalizeRollback(ctx, tx)
}

func rollbackPostgresTransaction(ctx *sql.Context) error {
	_, _, err := rollbackPostgresTransactionWithOwner(ctx)
	return err
}

// rollbackPostgresTransactionControl performs the protocol rollback path for
// both ROLLBACK and COMMIT issued while a block is failed. The latter is
// deliberately represented as a synthetic RollbackTransaction for cleanup:
// PostgreSQL discards the failed block and reports a rollback-equivalent
// command tag rather than attempting to commit an already-aborted driver tx.
func rollbackPostgresTransactionControl(
	ctx *sql.Context,
	transaction *tree.RollbackTransaction,
	providers ...postgresRollbackOrphanCleaner,
) error {
	_, tx, bindingErr := postgresTransactionBinding(ctx, "postgres rollback cleanup")
	if bindingErr != nil {
		return bindingErr
	}
	var releaseCleanup func()
	var reservationErr error
	if len(providers) > 0 {
		releaseCleanup, reservationErr = beginPostgresRollbackCleanupReservationWithError(ctx, providers[0], tx)
		if reservationErr == nil {
			defer releaseCleanup()
		} else {
			// A poisoned generation must not block the driver's rollback. Skip
			// maintenance, but preserve the admission error for the protocol caller.
			releaseCleanup = nil
		}
	}
	if tx == nil {
		if reservationErr != nil {
			return reservationErr
		}
		if len(providers) == 0 {
			return nil
		}
		// A GMS-only rollback has no physical *sql.Tx, but the reservation above
		// still owns the provider cleanup barrier. Mark that ownership explicitly
		// before entering the helper so it does not try to acquire the same
		// provider mutex a second time. Reuse the captured idle connection when one
		// exists; this keeps DuckLake attachment/session state stable.
		cleanupBase := catalog.WithDuckLakeCleanupLease(context.WithoutCancel(ctx))
		cleanupCtx := ctx.WithContext(cleanupBase)
		if conn, _ := adapter.TryGetTxnBindingForRelease(ctx); conn != nil {
			return cleanupPostgresRollbackOrphans(cleanupCtx, transaction, providers[0], conn)
		}
		return cleanupPostgresRollbackOrphans(cleanupCtx, transaction, providers[0])
	}
	var cleanup func(*stdsql.Conn) error
	if len(providers) > 0 && reservationErr == nil {
		provider := providers[0]
		cleanup = func(conn *stdsql.Conn) error {
			// The pool finalizer holds the session lock while this callback
			// runs. Mark the exact owner so provider checks do not re-enter
			// that lock or observe a replacement transaction.
			base := context.WithoutCancel(ctx)
			base = catalog.WithRollbackCleanupOwner(base, conn)
			base = catalog.WithDuckLakeCleanupLease(base)
			cleanupCtx := ctx.WithContext(base)
			return cleanupPostgresRollbackOrphans(cleanupCtx, transaction, provider, conn)
		}
	}
	_, rollbackErr := adapter.FinalizeRollbackWithCleanup(ctx, tx, cleanup)
	return errors.Join(reservationErr, rollbackErr)
}

// postgresTransactionControl handles transaction statements that bypass the
// GMS transaction iterator. A PostgreSQL rollback owns its connection through
// finalization, then performs any eligible DuckLake cleanup on that same
// connection before returning to the wire layer.
func postgresTransactionControl(ctx *sql.Context, stmt tree.Statement, providers ...postgresRollbackOrphanCleaner) (handled bool, err error) {
	switch transaction := stmt.(type) {
	case *tree.BeginTransaction:
		return true, beginPostgresTransaction(ctx, transaction)
	case *tree.CommitTransaction:
		if isPostgresFailedTransactionControl(ctx) {
			return true, rollbackPostgresTransactionControl(ctx, &tree.RollbackTransaction{}, providers...)
		}
		return true, commitPostgresTransaction(ctx)
	case *tree.RollbackTransaction:
		return true, rollbackPostgresTransactionControl(ctx, transaction, providers...)
	default:
		return false, nil
	}
}
