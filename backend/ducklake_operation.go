package backend

import (
	stdsql "database/sql"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/dolthub/go-mysql-server/sql"
)

// duckLakeOperationIter keeps statement resources alive until the wrapped
// child is finished. Snapshot and provider-operation leases are separate:
// the snapshot lease ends as soon as the child closes, while an operation lease
// may remain held until an implicit transaction's commit/rollback completes.
type duckLakeOperationIter struct {
	state *duckLakeOperationState
}

// GMS commits autocommit transactions from TransactionCommittingIter.Close.
// An iterator abandoned before EOF must therefore surface an error from Close,
// otherwise a short-circuited operation can be committed after its child has
// already been retired.
var errDuckLakeOperationClosedBeforeEOF = errors.New("ducklake operation iterator closed before EOF")

var errDuckLakeOperationValueIterUnavailable = errors.New("ducklake operation child does not support ValueRowIter")

// duckLakeOperationState is shared by every mutable alias returned by
// WithChildIter. In particular, aliases never retain their own child pointer:
// GMS may call Next on an older alias after a rewrite replaced the child.
type duckLakeOperationState struct {
	mu      sync.Mutex
	current sql.RowIter

	snapshotRelease  func()
	operationRelease func()
	snapshotOnce     sync.Once
	operationOnce    sync.Once
	closeOnce        sync.Once
	closeErr         error
	closeCtx         *sql.Context
	terminalErr      error

	closed   atomic.Bool
	terminal atomic.Bool
	failed   atomic.Bool

	// cleanupHandle belongs to the pre-finalization implicit cleanup hook. It
	// is deliberately called with false by finish so the hook remains armed
	// until the session's commit/rollback code decides the final outcome.
	cleanupHandle     func(bool)
	operationDeferred atomic.Bool
}

func newDuckLakeOperationIter(child sql.RowIter, release func()) *duckLakeOperationIter {
	return newDuckLakeOperationIterWithLeases(child, release, nil)
}

func newDuckLakeOperationIterWithLeases(
	child sql.RowIter,
	snapshotRelease func(),
	operationRelease func(),
) *duckLakeOperationIter {
	return &duckLakeOperationIter{
		state: &duckLakeOperationState{
			current:          child,
			snapshotRelease:  snapshotRelease,
			operationRelease: operationRelease,
		},
	}
}

func (i *duckLakeOperationIter) currentChild() sql.RowIter {
	if i == nil || i.state == nil || i.state.closed.Load() {
		return nil
	}
	i.state.mu.Lock()
	child := i.state.current
	i.state.mu.Unlock()
	return child
}

func (i *duckLakeOperationIter) releaseSnapshot() {
	if i == nil || i.state == nil || i.state.snapshotRelease == nil {
		return
	}
	i.state.snapshotOnce.Do(i.state.snapshotRelease)
}

func (i *duckLakeOperationIter) releaseOperation() {
	if i == nil || i.state == nil || i.state.operationRelease == nil {
		return
	}
	i.state.operationOnce.Do(i.state.operationRelease)
}

// done is retained for callers of the old wrapper. It now means release all
// remaining leases, and is only used on construction/fallback paths where no
// transaction post-finalization hook owns the operation lease.
func (i *duckLakeOperationIter) done() {
	if i == nil || i.state == nil {
		return
	}
	i.releaseSnapshot()
	if !i.state.operationDeferred.Load() {
		i.releaseOperation()
	}
}

// closeChild closes the shared current child at most once. The current child
// is captured and cleared while holding the state mutex so GetChildIter and
// every alias observe the same ownership transition.
func (i *duckLakeOperationIter) closeChild(ctx *sql.Context) error {
	if i == nil || i.state == nil {
		return nil
	}
	i.state.closeOnce.Do(func() {
		i.state.mu.Lock()
		i.state.closed.Store(true)
		i.state.closeCtx = ctx
		child := i.state.current
		i.state.current = nil
		i.state.mu.Unlock()

		var closeErr error
		if child != nil {
			closeErr = child.Close(ctx)
		}
		i.state.mu.Lock()
		i.state.closeErr = closeErr
		i.state.mu.Unlock()
	})
	i.state.mu.Lock()
	defer i.state.mu.Unlock()
	return i.state.closeErr
}

func (i *duckLakeOperationIter) setTerminalError(err error) {
	if i == nil || i.state == nil || err == nil || errors.Is(err, io.EOF) {
		return
	}
	i.state.mu.Lock()
	i.state.terminalErr = errors.Join(i.state.terminalErr, err)
	i.state.mu.Unlock()
	i.state.failed.Store(true)
}

func (i *duckLakeOperationIter) terminalError() error {
	if i == nil || i.state == nil {
		return nil
	}
	i.state.mu.Lock()
	defer i.state.mu.Unlock()
	return i.state.terminalErr
}

// finish closes/releases the child and updates the implicit-transaction
// registration. A false result deliberately leaves the registration armed so
// GMS's SetTransaction(nil) error boundary can run the callback and rollback.
func (i *duckLakeOperationIter) finish(ctx *sql.Context, success bool) error {
	if i == nil || i.state == nil {
		return nil
	}
	if !success {
		i.state.failed.Store(true)
	}
	closeErr := i.closeChild(ctx)
	if closeErr != nil {
		// A child close failure is a statement failure. Keeping failed=true makes
		// TransactionCommittingIter skip an implicit commit and take rollback.
		i.state.failed.Store(true)
	}
	i.releaseSnapshot()
	if i.state.cleanupHandle != nil {
		i.state.cleanupHandle(false)
	}
	if !i.state.operationDeferred.Load() {
		i.releaseOperation()
	}
	return errors.Join(i.terminalError(), closeErr)
}

func (i *duckLakeOperationIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i == nil || i.state == nil {
		return nil, io.EOF
	}
	if terminalErr := i.terminalError(); terminalErr != nil {
		return nil, terminalErr
	}
	if i.state.terminal.Load() {
		return nil, io.EOF
	}
	child := i.currentChild()
	if child == nil {
		_ = i.finish(ctx, true)
		return nil, io.EOF
	}
	row, err := child.Next(ctx)
	if err != nil {
		isEOF := errors.Is(err, io.EOF)
		if !isEOF {
			i.setTerminalError(err)
		}
		i.state.terminal.Store(isEOF)
		if closeErr := i.finish(ctx, isEOF); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}
	return row, err
}

func (i *duckLakeOperationIter) Close(ctx *sql.Context) error {
	if i == nil || i.state == nil {
		return nil
	}
	// A close before the child reported EOF is an aborted/short-circuited
	// execution. Mark it failed so a GMS implicit transaction cannot commit it.
	if !i.state.terminal.Load() && !i.state.failed.Load() {
		i.setTerminalError(errDuckLakeOperationClosedBeforeEOF)
	}
	success := i.state.terminal.Load() && !i.state.failed.Load()
	return i.finish(ctx, success)
}

// GetChildIter and WithChildIter preserve the mutable-iterator contract used
// by GMS projection rewrites. The returned child is always the shared current
// child, so an older alias cannot continue driving an abandoned iterator.
func (i *duckLakeOperationIter) GetChildIter() sql.RowIter {
	if i == nil || i.state == nil {
		return nil
	}
	i.state.mu.Lock()
	defer i.state.mu.Unlock()
	return i.state.current
}

func (i *duckLakeOperationIter) WithChildIter(child sql.RowIter) sql.RowIter {
	if i == nil || i.state == nil {
		return child
	}
	i.state.mu.Lock()
	if i.state.closed.Load() {
		// A rewrite after terminal close cannot become the owner of this lease.
		// Close the supplied child immediately so a late rewrite cannot leak a
		// resource after the shared lease has already finished.
		closeCtx := i.state.closeCtx
		i.state.mu.Unlock()
		if child != nil {
			_ = child.Close(closeCtx)
		}
		return child
	}
	i.state.current = child
	i.state.mu.Unlock()
	return i.aliasForChild(child)
}

func (i *duckLakeOperationIter) aliasForChild(child sql.RowIter) sql.RowIter {
	if _, ok := child.(sql.ValueRowIter); ok {
		return &duckLakeOperationValueIter{
			duckLakeOperationIter: &duckLakeOperationIter{state: i.state},
		}
	}
	return &duckLakeOperationIter{state: i.state}
}

// duckLakeOperationValueIter is selected only when the wrapped child supports
// sql.ValueRowIter. Its NextValueRow method dynamically resolves the shared
// current child; it intentionally stores no child pointer of its own.
type duckLakeOperationValueIter struct {
	*duckLakeOperationIter
}

func (i *duckLakeOperationValueIter) currentValueChild() sql.ValueRowIter {
	if i == nil || i.duckLakeOperationIter == nil {
		return nil
	}
	child := i.currentChild()
	valueChild, _ := child.(sql.ValueRowIter)
	return valueChild
}

func (i *duckLakeOperationValueIter) NextValueRow(ctx *sql.Context) (sql.ValueRow, error) {
	if i == nil || i.state == nil {
		return nil, io.EOF
	}
	if terminalErr := i.terminalError(); terminalErr != nil {
		return nil, terminalErr
	}
	if i.state.terminal.Load() {
		return nil, io.EOF
	}
	valueChild := i.currentValueChild()
	if valueChild == nil {
		if i.currentChild() != nil {
			err := errDuckLakeOperationValueIterUnavailable
			i.setTerminalError(err)
			_ = i.finish(ctx, false)
			return nil, err
		}
		_ = i.finish(ctx, true)
		return nil, io.EOF
	}
	row, err := valueChild.NextValueRow(ctx)
	if err != nil {
		isEOF := errors.Is(err, io.EOF)
		if !isEOF {
			i.setTerminalError(err)
		}
		i.state.terminal.Store(isEOF)
		if closeErr := i.finish(ctx, isEOF); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}
	return row, err
}

func (i *duckLakeOperationValueIter) IsValueRowIter(ctx *sql.Context) bool {
	valueChild := i.currentValueChild()
	return valueChild != nil && valueChild.IsValueRowIter(ctx)
}

func (i *duckLakeOperationValueIter) WithChildIter(child sql.RowIter) sql.RowIter {
	if i == nil || i.state == nil {
		return child
	}
	return i.duckLakeOperationIter.WithChildIter(child)
}

func registerPostTransactionCleanup(
	ctx *sql.Context,
	tx sql.Transaction,
	release func(success bool),
) bool {
	if ctx == nil || ctx.Session == nil || tx == nil || release == nil {
		return false
	}
	registrar, ok := ctx.Session.(PostTransactionCleanupRegistrar)
	if !ok {
		return false
	}
	return registrar.RegisterPostTransactionCleanup(ctx, tx, release)
}

// RegisterPostPhysicalTransactionCleanupForContext lets protocol adapters
// defer an operation release until a raw database/sql transaction has been
// finalized. It is deliberately optional so narrow session doubles retain the
// legacy iterator-close behavior.
func RegisterPostPhysicalTransactionCleanupForContext(
	ctx *sql.Context,
	tx *stdsql.Tx,
	release func(success bool),
) bool {
	if ctx == nil || ctx.Session == nil || tx == nil || release == nil {
		return false
	}
	registrar, ok := ctx.Session.(PhysicalPostTransactionCleanupRegistrar)
	if !ok {
		return false
	}
	return registrar.RegisterPostPhysicalTransactionCleanup(ctx, tx, release)
}

// implicitTransactionCleanupState is an optional session-side probe used to
// distinguish a GMS implicit autocommit transaction from an explicit or
// adapter-owned transaction. RegisterImplicitTransactionCleanup runs a
// callback immediately when its marker is already gone, which is correct for
// a racing implicit rollback but would incorrectly retire a child belonging
// to an explicit transaction (or a MySQL DDL transaction that deliberately
// has no implicit marker).
type implicitTransactionCleanupState interface {
	HasImplicitTransactionCleanup(sql.Transaction) bool
}

func hasImplicitTransactionCleanup(ctx *sql.Context, tx sql.Transaction) bool {
	if ctx == nil || ctx.Session == nil || tx == nil {
		return false
	}
	if state, ok := ctx.Session.(implicitTransactionCleanupState); ok {
		return state.HasImplicitTransactionCleanup(tx)
	}
	// Legacy session implementations do not expose marker state. Preserve the
	// historical registration behavior for those adapters.
	return true
}

func wrapDuckLakeOperationIter(iter sql.RowIter, release func()) sql.RowIter {
	return wrapDuckLakeOperationIterWithLeases(nil, iter, release, nil)
}

// wrapDuckLakeOperationIterWithContext preserves the historical one-release
// contract. New execution paths that acquire separate leases should call
// wrapDuckLakeOperationIterWithLeases instead.
func wrapDuckLakeOperationIterWithContext(ctx *sql.Context, iter sql.RowIter, release func()) sql.RowIter {
	return wrapDuckLakeOperationIterWithLeases(ctx, iter, release, nil)
}

// wrapDuckLakeOperationIterWithLeases wraps an iterator with independent
// snapshot and provider-operation releases. For an implicit transaction, the
// operation release is handed to the session's post-finalization hook while
// snapshot release remains tied to child close. If no such hook accepts the
// transaction, both releases retain the legacy child-close boundary.
func wrapDuckLakeOperationIterWithLeases(
	ctx *sql.Context,
	iter sql.RowIter,
	snapshotRelease func(),
	operationRelease func(),
) sql.RowIter {
	return wrapDuckLakeOperationIterWithLeasesAndTx(
		ctx,
		iter,
		nil,
		snapshotRelease,
		operationRelease,
	)
}

// wrapDuckLakeOperationIterWithLeasesAndTx is the implementation shared by
// the legacy and binding-aware wrappers. Keeping the old four-argument entry
// point above preserves existing GMS/test adapters while execution callers can
// carry the atomic physical transaction explicitly. The physical transaction
// is passed through from that atomic snapshot instead of being looked up again
// while snapshotRelease still holds the pool lifecycle reader. A second
// ordinary GetTxnBinding call can block forever once pool shutdown has queued
// its writer (the lifecycle gate rejects new readers during shutdown).
//
// physicalTx may be nil when the snapshot selected an idle connection. In that
// case a logical GMS transaction, when present, remains the fallback callback
// boundary. Callers using this entry point must pass the exact transaction
// returned by adapter.GetExecutionSnapshotWithLease; the function never guesses
// a replacement binding.
func wrapDuckLakeOperationIterWithLeasesAndTx(
	ctx *sql.Context,
	iter sql.RowIter,
	physicalTx *stdsql.Tx,
	snapshotRelease func(),
	operationRelease func(),
) sql.RowIter {
	if iter == nil {
		if snapshotRelease != nil {
			snapshotRelease()
		}
		if operationRelease != nil {
			operationRelease()
		}
		return nil
	}
	base := newDuckLakeOperationIterWithLeases(iter, snapshotRelease, operationRelease)

	if ctx != nil {
		if operationRelease != nil {
			// A transaction-backed session can expose the raw physical owner as
			// well as the logical GMS transaction. The owner is supplied by the
			// execution snapshot, so registration never re-enters the pool's
			// ordinary lifecycle reader while snapshotRelease is held.
			deferred := false
			if physicalTx != nil {
				deferred = RegisterPostPhysicalTransactionCleanupForContext(ctx, physicalTx, func(bool) {
					base.releaseOperation()
				})
			}
			if !deferred && ctx.GetTransaction() != nil {
				deferred = registerPostTransactionCleanup(ctx, ctx.GetTransaction(), func(bool) {
					base.releaseOperation()
				})
			}
			base.state.operationDeferred.Store(deferred)
		}
		// This pre-finalization callback closes the child and snapshot if GMS
		// clears an implicit transaction before the iterator reaches its normal
		// terminal path. It intentionally does not release a deferred operation.
		transaction := ctx.GetTransaction()
		if transaction != nil && hasImplicitTransactionCleanup(ctx, transaction) {
			base.state.cleanupHandle = RegisterImplicitTransactionCleanupForContext(ctx, transaction, func() error {
				err := base.closeChild(ctx)
				if err == nil {
					base.releaseSnapshot()
				} else {
					base.state.failed.Store(true)
					base.releaseSnapshot()
				}
				if !base.state.operationDeferred.Load() {
					base.releaseOperation()
				}
				return errors.Join(err, base.terminalError())
			})
		}
	}

	if _, ok := iter.(sql.ValueRowIter); ok {
		return &duckLakeOperationValueIter{duckLakeOperationIter: base}
	}
	return base
}

// WrapRowIterWithLeases is the legacy explicit split entry point. The snapshot
// lease is released at child termination; operationRelease is deferred until a
// logical transaction's post-finalization hook when one is available. Callers
// that already hold an atomic physical transaction should use
// WrapRowIterWithLeasesAndTx so wrapper construction never performs a binding
// lookup.
func WrapRowIterWithLeases(
	ctx *sql.Context,
	iter sql.RowIter,
	snapshotRelease func(),
	operationRelease func(),
) sql.RowIter {
	return wrapDuckLakeOperationIterWithLeases(ctx, iter, snapshotRelease, operationRelease)
}

// WrapRowIterWithLeasesAndTx is the binding-aware exported entry point for
// protocol adapters that already hold an atomic execution snapshot. Passing
// physicalTx avoids a second lifecycle lookup while the snapshot lease is
// active; the legacy four-argument function remains available for adapters
// that do not expose a physical transaction.
func WrapRowIterWithLeasesAndTx(
	ctx *sql.Context,
	iter sql.RowIter,
	physicalTx *stdsql.Tx,
	snapshotRelease func(),
	operationRelease func(),
) sql.RowIter {
	return wrapDuckLakeOperationIterWithLeasesAndTx(
		ctx,
		iter,
		physicalTx,
		snapshotRelease,
		operationRelease,
	)
}
