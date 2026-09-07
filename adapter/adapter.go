package adapter

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

// ErrTransactionBindingChanged reports that a finalizer was asked to operate
// on a physical transaction which is no longer the current binding for the
// logical session. A stale callback must be visible to the caller: treating it
// as a successful commit can make the protocol advertise success while the
// transaction was already rolled back or replaced.
var ErrTransactionBindingChanged = errors.New("transaction binding is no longer current")

type ConnectionHolder interface {
	GetConn(ctx context.Context) (*stdsql.Conn, error)
	GetTxn(ctx context.Context, options *stdsql.TxOptions) (*stdsql.Tx, error)
	GetCatalogConn(ctx context.Context) (*stdsql.Conn, error)
	GetCatalogTxn(ctx context.Context, options *stdsql.TxOptions) (*stdsql.Tx, error)
	TryGetTxn() *stdsql.Tx
	GetCurrentCatalog() string
	GetCurrentSchema() string
	CloseTxn()
	CloseConn()
}

// TransactionBindingHolder is an optional extension implemented by session
// types that can return the physical connection and transaction as one
// lifecycle snapshot. Keeping this separate from ConnectionHolder preserves
// compatibility with the small test/session implementations supplied by GMS.
type TransactionBindingHolder interface {
	GetTxnBinding() (*stdsql.Conn, *stdsql.Tx)
}

// ReleaseTransactionBindingHolder is the release-side counterpart to
// TransactionBindingHolder. Implementations may admit this snapshot while a
// pool shutdown is waiting for ordinary readers to drain. Callers must use it
// only to capture state for a subsequent identity-safe release/finalizer.
type ReleaseTransactionBindingHolder interface {
	GetTxnBindingForRelease() (*stdsql.Conn, *stdsql.Tx)
}

// ReleaseTransactionHolder exposes a transaction-only release snapshot for
// legacy call sites that do not need the physical owner. Production sessions
// implement this alongside ReleaseTransactionBindingHolder.
type ReleaseTransactionHolder interface {
	TryGetTxnForRelease() *stdsql.Tx
}

// ExecutionSnapshotHolder can acquire the connection/transaction pair under
// the session's lifecycle lock. The catalog flag selects a connection without
// schema switching for metadata work; ordinary execution selects the current
// logical schema.
type ExecutionSnapshotHolder interface {
	GetExecutionSnapshot(context.Context, bool) (*stdsql.Conn, *stdsql.Tx, error)
}

// ExecutionSnapshotLeaseHolder is the stronger execution-snapshot surface.
// Implementations keep the pool generation alive until release is called. The
// release callback must be safe to invoke more than once; adapter callers also
// wrap it defensively so a failed construction path cannot leak a lifecycle
// reader.
//
// The interface is optional to preserve compatibility with the small session
// doubles used by GMS and by downstream embedders. Those holders receive a
// no-op release from GetExecutionSnapshotWithLease.
type ExecutionSnapshotLeaseHolder interface {
	GetExecutionSnapshotLease(context.Context, bool) (*stdsql.Conn, *stdsql.Tx, func(), error)
}

// IdentityTransactionCloser removes a transaction mapping only when it still
// refers to the supplied transaction. This prevents an old commit/rollback
// defer from deleting a newer transaction created for the same session ID.
type IdentityTransactionCloser interface {
	CloseTxnIf(*stdsql.Tx)
}

// IdentityConnectionCloser removes and closes a connection only when it still
// refers to the supplied physical connection.
type IdentityConnectionCloser interface {
	CloseConnIf(*stdsql.Conn) error
}

// IdentityConnectionBindingCloser removes and closes a connection only when
// both the physical connection and its expected transaction binding still
// match. This closes the same-connection replacement race that a connection-
// only identity check cannot see.
type IdentityConnectionBindingCloser interface {
	CloseConnIfBinding(*stdsql.Conn, *stdsql.Tx) error
}

// TransactionFinalizer performs the driver operation and lifecycle-map
// update as one session-scoped operation. Implementations must keep a stale
// transaction from evicting a replacement binding for the same session.
type TransactionFinalizer interface {
	CommitTxn(*stdsql.Tx) error
	RollbackTxn(*stdsql.Tx) (*stdsql.Conn, bool, error)
}

// RollbackCleanupFinalizer extends rollback finalization with an optional
// callback that runs while the session still owns its lifecycle lock. This
// closes the gap in which a replacement transaction could otherwise be
// admitted before post-rollback maintenance has finished.
type RollbackCleanupFinalizer interface {
	RollbackTxnWithCleanup(*stdsql.Tx, func(*stdsql.Conn) error) (bool, error)
}

// SQLExecutor is the common database/sql surface used by statement execution.
// Both *sql.Conn and *sql.Tx satisfy it. Keeping the interface here lets query
// paths bind to an already active session transaction without giving up the
// connection-specific setup needed by callers that acquire a *sql.Conn.
type SQLExecutor interface {
	ExecContext(context.Context, string, ...any) (stdsql.Result, error)
	QueryContext(context.Context, string, ...any) (*stdsql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *stdsql.Row
	PrepareContext(context.Context, string) (*stdsql.Stmt, error)
}

// LeasedRows keeps an execution snapshot alive until the result set is closed
// or exhausted. database/sql normally closes Rows at EOF; the explicit wrapper
// also releases the pool lifecycle reader at that same terminal boundary.
type LeasedRows struct {
	*stdsql.Rows
	release func()
	once    sync.Once
}

func newLeasedRows(rows *stdsql.Rows, release func()) *LeasedRows {
	return &LeasedRows{Rows: rows, release: idempotentRelease(release)}
}

func (r *LeasedRows) releaseLease() {
	if r == nil {
		return
	}
	r.once.Do(r.release)
}

func (r *LeasedRows) Close() (err error) {
	if r == nil {
		return nil
	}
	defer r.releaseLease()
	if r.Rows == nil {
		return nil
	}
	return r.Rows.Close()
}

func (r *LeasedRows) Next() bool {
	if r == nil || r.Rows == nil {
		if r != nil {
			r.releaseLease()
		}
		return false
	}
	if r.Rows.Next() {
		return true
	}
	_ = r.Close()
	return false
}

func (r *LeasedRows) NextResultSet() bool {
	if r == nil || r.Rows == nil {
		if r != nil {
			r.releaseLease()
		}
		return false
	}
	if r.Rows.NextResultSet() {
		return true
	}
	_ = r.Close()
	return false
}

// LeasedRow retains a snapshot until Scan consumes the database/sql row. A Row
// has no Close method, so Scan is its only normal terminal operation.
type LeasedRow struct {
	*stdsql.Row
	release func()
	err     error
	once    sync.Once
}

func NewLeasedRow(row *stdsql.Row, release func()) *LeasedRow {
	return &LeasedRow{Row: row, release: idempotentRelease(release)}
}

// NewLeasedRowError preserves an error that occurred before database/sql could
// construct a Row. QueryRow-style APIs surface errors through Scan, so callers
// must receive the original admission or lifecycle error there as well.
func NewLeasedRowError(err error) *LeasedRow {
	return &LeasedRow{release: idempotentRelease(nil), err: err}
}

func (r *LeasedRow) Scan(dest ...any) (err error) {
	if r == nil {
		return fmt.Errorf("query row is unavailable")
	}
	defer r.once.Do(r.release)
	if r.err != nil {
		return r.err
	}
	if r.Row == nil {
		return fmt.Errorf("query row is unavailable")
	}
	return r.Row.Scan(dest...)
}

func GetConn(ctx *sql.Context) (*stdsql.Conn, error) {
	return ctx.Session.(ConnectionHolder).GetConn(ctx)
}

func GetCatalogConn(ctx *sql.Context) (*stdsql.Conn, error) {
	return ctx.Session.(ConnectionHolder).GetCatalogConn(ctx)
}

func CloseConn(ctx *sql.Context) {
	ctx.Session.(ConnectionHolder).CloseConn()
}

func GetTxn(ctx *sql.Context, options *stdsql.TxOptions) (*stdsql.Tx, error) {
	return ctx.Session.(ConnectionHolder).GetTxn(ctx, options)
}

func GetCatalogTxn(ctx *sql.Context, options *stdsql.TxOptions) (*stdsql.Tx, error) {
	return ctx.Session.(ConnectionHolder).GetCatalogTxn(ctx, options)
}

func TryGetTxn(ctx *sql.Context) *stdsql.Tx {
	return ctx.Session.(ConnectionHolder).TryGetTxn()
}

// TryGetTxnBinding returns an atomic (connection, transaction) snapshot when
// the session supplies that stronger lifecycle surface. Older holders retain
// the transaction-only fallback; callers that require connection affinity can
// treat a nil connection as unavailable rather than guessing another pool
// connection.
func TryGetTxnBinding(ctx *sql.Context) (*stdsql.Conn, *stdsql.Tx) {
	holder := ctx.Session.(ConnectionHolder)
	if binding, ok := holder.(TransactionBindingHolder); ok {
		return binding.GetTxnBinding()
	}
	return nil, holder.TryGetTxn()
}

// TryGetTxnBindingForRelease captures a binding from a release-aware session
// surface. The production pool admits this read while shutdown is pending,
// which prevents a close/finalizer path from deadlocking behind the writer it
// is about to help drain. Legacy holders fall back to the ordinary snapshot.
func TryGetTxnBindingForRelease(ctx *sql.Context) (*stdsql.Conn, *stdsql.Tx) {
	holder := ctx.Session.(ConnectionHolder)
	if binding, ok := holder.(ReleaseTransactionBindingHolder); ok {
		return binding.GetTxnBindingForRelease()
	}
	return TryGetTxnBinding(ctx)
}

// TryGetTxnForRelease is the transaction-only form of
// TryGetTxnBindingForRelease.
func TryGetTxnForRelease(ctx *sql.Context) *stdsql.Tx {
	holder := ctx.Session.(ConnectionHolder)
	if release, ok := holder.(ReleaseTransactionHolder); ok {
		return release.TryGetTxnForRelease()
	}
	_, tx := TryGetTxnBindingForRelease(ctx)
	return tx
}

func GetCurrentCatalog(ctx *sql.Context) string {
	return ctx.Session.(ConnectionHolder).GetCurrentCatalog()
}

func GetCurrentSchema(ctx *sql.Context) string {
	return ctx.Session.(ConnectionHolder).GetCurrentSchema()
}

func CloseTxn(ctx *sql.Context) {
	ctx.Session.(ConnectionHolder).CloseTxn()
}

// CloseTxnIf performs an identity-safe transaction mapping removal when the
// session supports it, with the legacy unconditional behavior as a fallback.
func CloseTxnIf(ctx *sql.Context, tx *stdsql.Tx) {
	if tx == nil {
		return
	}
	holder := ctx.Session.(ConnectionHolder)
	if closer, ok := holder.(IdentityTransactionCloser); ok {
		closer.CloseTxnIf(tx)
		return
	}
	// Legacy holders expose no identity-aware removal primitive. Compare the
	// current pointer before removing it so a delayed callback does not evict a
	// replacement transaction in the common, non-concurrent case.
	if TryGetTxnForRelease(ctx) == tx {
		holder.CloseTxn()
	}
}

// CloseConnIf performs an identity-safe connection removal when available.
// The fallback closes the supplied connection but cannot update a legacy
// holder's map; production sessions implement the stronger surface.
func CloseConnIf(ctx *sql.Context, conn *stdsql.Conn) error {
	if conn == nil {
		return nil
	}
	holder := ctx.Session.(ConnectionHolder)
	if closer, ok := holder.(IdentityConnectionCloser); ok {
		return closer.CloseConnIf(conn)
	}
	return conn.Close()
}

// CloseConnIfBinding performs an atomic connection/transaction identity check
// when the session supports it. Legacy holders fall back to a strict snapshot
// check before closing the supplied connection.
func CloseConnIfBinding(ctx *sql.Context, conn *stdsql.Conn, tx *stdsql.Tx) error {
	if conn == nil {
		return nil
	}
	holder := ctx.Session.(ConnectionHolder)
	if closer, ok := holder.(IdentityConnectionBindingCloser); ok {
		return closer.CloseConnIfBinding(conn, tx)
	}
	currentConn, currentTx := TryGetTxnBindingForRelease(ctx)
	if currentConn != conn || currentTx != tx {
		return nil
	}
	return conn.Close()
}

// FinalizeCommit commits tx and removes its session binding. Production
// sessions implement TransactionFinalizer so the commit and identity check
// are serialized by the connection pool. The fallback preserves compatibility
// with small test/session holders.
func FinalizeCommit(ctx *sql.Context, tx *stdsql.Tx) error {
	if tx == nil {
		return nil
	}
	holder := ctx.Session.(ConnectionHolder)
	if finalizer, ok := holder.(TransactionFinalizer); ok {
		return finalizer.CommitTxn(tx)
	}
	conn, current := TryGetTxnBindingForRelease(ctx)
	// A fallback holder cannot atomically finalize a transaction that is no
	// longer its current binding. In particular, a nil current transaction is
	// not permission to operate on the caller's stale *sql.Tx: doing so can
	// commit or roll back a transaction whose owner has already been replaced
	// or evicted. Production holders use TransactionFinalizer above; this strict
	// check keeps legacy/test holders fail-closed as well.
	if current != tx {
		return ErrTransactionBindingChanged
	}
	err := tx.Commit()
	CloseTxnIf(ctx, tx)
	if err != nil {
		if conn != nil {
			err = errors.Join(err, CloseConnIf(ctx, conn))
		}
	}
	return err
}

// FinalizeRollback rolls back tx, removes its session binding, and returns the
// owning physical connection plus whether the driver proved the transaction
// inactive. The connection is nil when the transaction was stale and no
// longer owned by this session, which prevents cleanup from touching a
// replacement transaction.
func FinalizeRollback(ctx *sql.Context, tx *stdsql.Tx) (*stdsql.Conn, bool, error) {
	if tx == nil {
		return nil, true, nil
	}
	holder := ctx.Session.(ConnectionHolder)
	if finalizer, ok := holder.(TransactionFinalizer); ok {
		return finalizer.RollbackTxn(tx)
	}
	conn, current := TryGetTxnBindingForRelease(ctx)
	// Do not guess at ownership when the mapping disappeared. A stale rollback
	// must not touch the transaction or acquire/reuse a connection that may now
	// belong to another session lifecycle.
	if current != tx {
		return nil, false, nil
	}
	err := tx.Rollback()
	inactive := transactionInactiveError(err)
	CloseTxnIf(ctx, tx)
	if !inactive && conn != nil {
		err = errors.Join(err, CloseConnIf(ctx, conn))
	}
	return conn, inactive, err
}

// FinalizeRollbackWithCleanup rolls back tx and, when supported by the
// session, runs cleanup while the old transaction's physical owner remains
// reserved. Stale transactions never invoke the callback.
func FinalizeRollbackWithCleanup(
	ctx *sql.Context,
	tx *stdsql.Tx,
	cleanup func(*stdsql.Conn) error,
) (bool, error) {
	if tx == nil {
		return true, nil
	}
	holder := ctx.Session.(ConnectionHolder)
	if finalizer, ok := holder.(RollbackCleanupFinalizer); ok {
		return finalizer.RollbackTxnWithCleanup(tx, cleanup)
	}
	conn, inactive, err := FinalizeRollback(ctx, tx)
	if inactive && conn != nil && cleanup != nil {
		err = errors.Join(err, cleanup(conn))
	}
	return inactive, err
}

func transactionInactiveError(err error) bool {
	if err == nil {
		return true
	}
	return errors.Is(err, stdsql.ErrTxDone) ||
		strings.Contains(strings.ToLower(err.Error()), "no transaction is active")
}

// IsTransactionInactiveError reports the narrow set of driver outcomes that
// prove a transaction has already ended. Callers that perform replication or
// connection teardown can ignore those outcomes while still surfacing an
// unexpected rollback failure.
func IsTransactionInactiveError(err error) bool {
	return transactionInactiveError(err)
}

// SQLExecutorForConn returns the active session transaction when one exists;
// otherwise it returns the already acquired connection. Callers that need
// driver-connection operations (for example, DuckLake ATTACH) should continue
// using the original *sql.Conn directly.
func SQLExecutorForConn(ctx *sql.Context, conn *stdsql.Conn) SQLExecutor {
	if _, tx := TryGetTxnBinding(ctx); tx != nil {
		return tx
	}
	return conn
}

// GetSQLExecutor acquires the normal session connection only when no active
// transaction owns it. It is intended for statement helpers whose work must
// participate in the session transaction when present.
func GetSQLExecutor(ctx *sql.Context) (SQLExecutor, error) {
	execer, _, _, err := GetExecutionSnapshot(ctx)
	return execer, err
}

// GetCatalogExecutor is the catalog equivalent of GetSQLExecutor. Metadata
// reads and writes therefore observe the same transaction as object-table DML.
func GetCatalogExecutor(ctx *sql.Context) (SQLExecutor, error) {
	execer, _, _, err := getExecutionSnapshot(ctx, true)
	return execer, err
}

// GetExecutionSnapshot acquires the executor used for statement work together
// with its underlying physical connection and transaction. Session
// implementations with an atomic binding avoid a Load -> acquire -> Load
// race; legacy holders still receive the best available snapshot.
func GetExecutionSnapshot(ctx *sql.Context) (SQLExecutor, *stdsql.Conn, *stdsql.Tx, error) {
	return getExecutionSnapshot(ctx, false)
}

// GetExecutionSnapshotWithLease is the lifetime-safe form of
// GetExecutionSnapshot. The returned release function keeps the selected
// physical connection and transaction generation usable until the caller has
// finished executing and consuming the result. Callers must release it on
// every terminal path, including iterator construction errors.
func GetExecutionSnapshotWithLease(ctx *sql.Context) (SQLExecutor, *stdsql.Conn, *stdsql.Tx, func(), error) {
	return getExecutionSnapshotWithLease(ctx, false)
}

// GetCatalogExecutionSnapshot is the metadata variant of
// GetExecutionSnapshot. It preserves the active transaction binding while
// avoiding an implicit current-schema switch on a newly acquired connection.
func GetCatalogExecutionSnapshot(ctx *sql.Context) (SQLExecutor, *stdsql.Conn, *stdsql.Tx, error) {
	return getExecutionSnapshot(ctx, true)
}

// GetCatalogExecutionSnapshotWithLease is the metadata variant of
// GetExecutionSnapshotWithLease.
func GetCatalogExecutionSnapshotWithLease(ctx *sql.Context) (SQLExecutor, *stdsql.Conn, *stdsql.Tx, func(), error) {
	return getExecutionSnapshotWithLease(ctx, true)
}

// GetTxnExecutionSnapshotWithLease opens or observes the session transaction,
// then retains the generation containing that exact transaction for subsequent
// driver work. A transition between the two snapshots is reported as a binding
// change; callers never execute on the stale pointer returned by GetTxn.
func GetTxnExecutionSnapshotWithLease(ctx *sql.Context, options *stdsql.TxOptions) (*stdsql.Conn, *stdsql.Tx, func(), error) {
	return getTxnExecutionSnapshotWithLease(ctx, options, false)
}

// GetCatalogTxnExecutionSnapshotWithLease is the catalog-only counterpart to
// GetTxnExecutionSnapshotWithLease.
func GetCatalogTxnExecutionSnapshotWithLease(ctx *sql.Context, options *stdsql.TxOptions) (*stdsql.Conn, *stdsql.Tx, func(), error) {
	return getTxnExecutionSnapshotWithLease(ctx, options, true)
}

func getTxnExecutionSnapshotWithLease(
	ctx *sql.Context,
	options *stdsql.TxOptions,
	catalogOnly bool,
) (*stdsql.Conn, *stdsql.Tx, func(), error) {
	holder := ctx.Session.(ConnectionHolder)
	var (
		tx  *stdsql.Tx
		err error
	)
	if catalogOnly {
		tx, err = holder.GetCatalogTxn(ctx, options)
	} else {
		tx, err = holder.GetTxn(ctx, options)
	}
	if err != nil {
		return nil, nil, func() {}, err
	}
	_, conn, current, release, err := getExecutionSnapshotWithLease(ctx, catalogOnly)
	if err != nil {
		return nil, nil, release, err
	}
	if current == nil || current != tx {
		release()
		return nil, nil, func() {}, ErrTransactionBindingChanged
	}
	return conn, current, release, nil
}

func idempotentRelease(release func()) func() {
	if release == nil {
		return func() {}
	}
	var once sync.Once
	return func() {
		once.Do(release)
	}
}

func getExecutionSnapshotWithLease(ctx *sql.Context, catalogOnly bool) (SQLExecutor, *stdsql.Conn, *stdsql.Tx, func(), error) {
	holder := ctx.Session.(ConnectionHolder)
	if leased, ok := holder.(ExecutionSnapshotLeaseHolder); ok {
		conn, tx, release, err := leased.GetExecutionSnapshotLease(ctx, catalogOnly)
		release = idempotentRelease(release)
		if err != nil {
			// A holder may have admitted a lease before discovering an invalid
			// snapshot. Do not make every caller remember to release on error.
			release()
			return nil, nil, tx, func() {}, err
		}
		if tx != nil {
			if conn == nil {
				release()
				return nil, nil, tx, func() {}, fmt.Errorf("transaction execution snapshot has no connection")
			}
			return tx, conn, tx, release, nil
		}
		if conn == nil {
			release()
			return nil, nil, nil, func() {}, fmt.Errorf("execution snapshot has no connection")
		}
		return conn, conn, nil, release, nil
	}

	// Legacy holders cannot retain a pool lifecycle reader, so preserve the
	// existing best-effort snapshot and make the lease a no-op.
	execer, conn, tx, err := getExecutionSnapshot(ctx, catalogOnly)
	return execer, conn, tx, func() {}, err
}

func getExecutionSnapshot(ctx *sql.Context, catalogOnly bool) (SQLExecutor, *stdsql.Conn, *stdsql.Tx, error) {
	holder := ctx.Session.(ConnectionHolder)
	getConn := holder.GetConn
	if catalogOnly {
		getConn = holder.GetCatalogConn
	}
	if snapshot, ok := holder.(ExecutionSnapshotHolder); ok {
		conn, tx, err := snapshot.GetExecutionSnapshot(ctx, catalogOnly)
		if err != nil {
			return nil, nil, tx, err
		}
		if tx != nil {
			if conn == nil {
				return nil, nil, tx, fmt.Errorf("transaction execution snapshot has no connection")
			}
			return tx, conn, tx, nil
		}
		if conn == nil {
			return nil, nil, nil, fmt.Errorf("execution snapshot has no connection")
		}
		return conn, conn, nil, nil
	}
	conn, tx := TryGetTxnBinding(ctx)
	if tx != nil {
		if conn == nil {
			// A legacy holder exposes only the transaction pointer. Reacquiring
			// a session connection here would guess at the transaction's physical
			// owner and could execute against a replacement connection. Refuse the
			// snapshot instead; production holders provide the atomic owner map.
			return nil, nil, tx, fmt.Errorf("active transaction has no physical owner")
		}
		return tx, conn, tx, nil
	}
	if conn == nil {
		var err error
		conn, err = getConn(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	// A transaction may have been established while the connection was being
	// acquired by a legacy holder. Recheck before selecting the executor.
	if reboundConn, reboundTx := TryGetTxnBinding(ctx); reboundTx != nil {
		if reboundConn == nil {
			return nil, nil, reboundTx, fmt.Errorf("active transaction has no physical owner")
		}
		conn = reboundConn
		return reboundTx, conn, reboundTx, nil
	}
	return conn, conn, nil, nil
}

func Query(ctx *sql.Context, query string, args ...any) (*LeasedRows, error) {
	execer, _, _, release, err := GetExecutionSnapshotWithLease(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := execer.QueryContext(ctx, query, args...)
	if err != nil {
		release()
		return nil, err
	}
	return newLeasedRows(rows, release), nil
}

func QueryRow(ctx *sql.Context, query string, args ...any) *LeasedRow {
	execer, _, _, release, err := GetExecutionSnapshotWithLease(ctx)
	if err != nil {
		return NewLeasedRowError(err)
	}
	return NewLeasedRow(execer.QueryRowContext(ctx, query, args...), release)
}

// QueryCatalog is a helper function to query the catalog, such as information_schema.
// Unlike QueryContext, this function does not require a schema name to be set on the connection,
// and the current schema of the connection does not matter.
func QueryCatalog(ctx *sql.Context, query string, args ...any) (*LeasedRows, error) {
	execer, _, _, release, err := GetCatalogExecutionSnapshotWithLease(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := execer.QueryContext(ctx, query, args...)
	if err != nil {
		release()
		return nil, err
	}
	return newLeasedRows(rows, release), nil
}

func QueryRowCatalog(ctx *sql.Context, query string, args ...any) *LeasedRow {
	execer, _, _, release, err := GetCatalogExecutionSnapshotWithLease(ctx)
	if err != nil {
		return NewLeasedRowError(err)
	}
	return NewLeasedRow(execer.QueryRowContext(ctx, query, args...), release)
}

func Exec(ctx *sql.Context, query string, args ...any) (stdsql.Result, error) {
	execer, _, _, release, err := GetExecutionSnapshotWithLease(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return execer.ExecContext(ctx, query, args...)
}

// ExecCatalog is a helper function to execute a catalog modification query, such as creating a database.
// Unlike ExecContext, this function does not require a schema name to be set on the connection,
// and the current schema of the connection does not matter.
func ExecCatalog(ctx *sql.Context, query string, args ...any) (stdsql.Result, error) {
	execer, _, _, release, err := GetCatalogExecutionSnapshotWithLease(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return execer.ExecContext(ctx, query, args...)
}

func ExecCatalogInTxn(ctx *sql.Context, query string, args ...any) (stdsql.Result, error) {
	_, tx, release, err := GetCatalogTxnExecutionSnapshotWithLease(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer release()
	return tx.ExecContext(ctx, query, args...)
}

func ExecInTxn(ctx *sql.Context, query string, args ...any) (stdsql.Result, error) {
	_, tx, release, err := GetTxnExecutionSnapshotWithLease(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer release()
	return tx.ExecContext(ctx, query, args...)
}

func CommitAndCloseTxn(sqlCtx *sql.Context) error {
	tx := TryGetTxnForRelease(sqlCtx)
	if tx != nil {
		if err := FinalizeCommit(sqlCtx, tx); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
	}
	return nil
}
