package pgserver

import (
	stdsql "database/sql"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
)

// postgresCopyLease owns the two independent lifecycle resources captured for
// an asynchronous COPY exchange. The pool snapshot protects the physical
// executor only until the producer/loader has closed its child. Provider
// operation admission may outlive that child until the surrounding physical
// transaction has committed or rolled back. The pointer is shared by state,
// loader and writer copies so every terminal path can safely release either
// half exactly once.
type postgresCopyLease struct {
	snapshotRelease   func()
	operationRelease  func()
	snapshotOnce      sync.Once
	operationOnce     sync.Once
	snapshotReleased  atomic.Bool
	operationDeferred atomic.Bool
}

// newPostgresCopyLease keeps the one-argument form source-compatible with the
// older COPY helpers: that callback is treated as the snapshot release and
// Release still invokes it. New callers pass the provider-operation release as
// the optional second argument.
func newPostgresCopyLease(snapshotRelease func(), operationRelease ...func()) *postgresCopyLease {
	var operation func()
	if len(operationRelease) > 0 {
		operation = operationRelease[0]
	}
	if snapshotRelease == nil && operation == nil {
		return nil
	}
	return &postgresCopyLease{
		snapshotRelease:  snapshotRelease,
		operationRelease: operation,
	}
}

// ReleaseSnapshot ends the pool lifecycle lease. It deliberately marks the
// snapshot released only after the callback returns: a concurrent binding
// validation must continue to trust the captured identity while the pool
// reader is still held by the callback.
func (lease *postgresCopyLease) ReleaseSnapshot() {
	if lease == nil || lease.snapshotRelease == nil {
		return
	}
	lease.snapshotOnce.Do(func() {
		lease.snapshotRelease()
		lease.snapshotReleased.Store(true)
	})
}

// ReleaseOperation ends provider operation admission. A physical transaction
// registrar may invoke this callback after commit/rollback; the once guard
// keeps that callback compatible with explicit teardown and fallback paths.
func (lease *postgresCopyLease) ReleaseOperation() {
	if lease == nil || lease.operationRelease == nil {
		return
	}
	lease.operationOnce.Do(lease.operationRelease)
}

// Release releases both resources in dependency order. This is used only for
// construction/teardown paths that cannot retain a transaction-finalization
// callback; normal COPY terminal paths use ReleaseSnapshot plus a deferred
// operation release where appropriate.
func (lease *postgresCopyLease) Release() {
	if lease == nil {
		return
	}
	lease.ReleaseSnapshot()
	lease.ReleaseOperation()
}

// SnapshotHeld reports whether a pool snapshot callback is still active. It
// is used by binding validation to avoid a second lifecycle read while a
// shutdown writer is waiting on the retained snapshot.
func (lease *postgresCopyLease) SnapshotHeld() bool {
	return lease != nil && lease.snapshotRelease != nil && !lease.snapshotReleased.Load()
}

// OperationDeferred reports whether provider operation release is owned by a
// physical transaction finalizer. If registration is unavailable, callers
// must release the operation at their terminal boundary instead.
func (lease *postgresCopyLease) OperationDeferred() bool {
	return lease != nil && lease.operationDeferred.Load()
}

// registerPhysicalFinalization hands operation admission to the session's
// raw-transaction completion registry. The registry callback receives the
// commit/rollback outcome but the lease has the same release behavior for both
// outcomes. A false result leaves the caller responsible for a fallback
// terminal release.
func (lease *postgresCopyLease) registerPhysicalFinalization(ctx *sql.Context, tx *stdsql.Tx) bool {
	if lease == nil || tx == nil || lease.operationRelease == nil || ctx == nil || ctx.Session == nil {
		return false
	}
	registrar, ok := ctx.Session.(interface {
		RegisterPostPhysicalTransactionCleanup(*sql.Context, *stdsql.Tx, func(bool)) bool
	})
	if !ok {
		return false
	}
	lease.operationDeferred.Store(true)
	if !registrar.RegisterPostPhysicalTransactionCleanup(ctx, tx, func(bool) {
		lease.ReleaseOperation()
	}) {
		lease.operationDeferred.Store(false)
		return false
	}
	return true
}

// postgresCopyBinding is the lifecycle identity captured when a COPY FROM
// STDIN exchange is accepted. COPY data arrives in later frontend messages,
// so reacquiring a connection at the first chunk can silently move execution
// outside the transaction that was active when COPY was opened.
type postgresCopyBinding struct {
	execer           adapter.SQLExecutor
	ownerConn        *stdsql.Conn
	ownerTx          *stdsql.Tx
	snapshotCaptured bool
	lease            *postgresCopyLease
}

// capturePostgresCopyBinding obtains the executor and its physical owner as a
// single snapshot. Minimal unit-test handlers may not have a session at all;
// those retain the legacy lifecycle and intentionally skip identity checks.
func capturePostgresCopyBinding(ctx *sql.Context) (postgresCopyBinding, error) {
	if ctx == nil || ctx.Session == nil {
		return postgresCopyBinding{}, nil
	}
	if _, ok := ctx.Session.(adapter.ConnectionHolder); !ok {
		return postgresCopyBinding{}, nil
	}
	execer, ownerConn, ownerTx, err := adapter.GetExecutionSnapshot(ctx)
	if err != nil {
		return postgresCopyBinding{}, err
	}
	if execer == nil || ownerConn == nil {
		return postgresCopyBinding{}, fmt.Errorf("COPY execution snapshot has no physical owner")
	}
	return postgresCopyBinding{
		execer:           execer,
		ownerConn:        ownerConn,
		ownerTx:          ownerTx,
		snapshotCaptured: true,
	}, nil
}

// capturePostgresCopyBindingWithHandler captures a retained snapshot for a
// frontend COPY exchange. Operation admission is acquired before the pool
// snapshot so rollback cleanup cannot overtake an accepted operation. The
// returned lease remains owned by the caller until the protocol has drained
// the loader and finalized its surrounding transaction.
func capturePostgresCopyBindingWithHandler(
	ctx *sql.Context,
	handler *DuckHandler,
	statement tree.Statement,
) (postgresCopyBinding, error) {
	if ctx == nil || ctx.Session == nil {
		return postgresCopyBinding{}, nil
	}
	if _, ok := ctx.Session.(adapter.ConnectionHolder); !ok {
		return postgresCopyBinding{}, nil
	}
	execer, ownerConn, ownerTx, lease, err := postgresCopySnapshotLease(ctx, handler, statement)
	if err != nil {
		return postgresCopyBinding{}, err
	}
	if execer == nil || ownerConn == nil {
		if lease != nil {
			lease.Release()
		}
		return postgresCopyBinding{}, fmt.Errorf("COPY execution snapshot has no physical owner")
	}
	return postgresCopyBinding{
		execer:           execer,
		ownerConn:        ownerConn,
		ownerTx:          ownerTx,
		snapshotCaptured: true,
		lease:            lease,
	}, nil
}

func (binding postgresCopyBinding) releaseLease() {
	if binding.lease != nil {
		binding.lease.Release()
	}
}

func (binding postgresCopyBinding) releaseSnapshot() {
	if binding.lease != nil {
		binding.lease.ReleaseSnapshot()
	}
}

func (binding postgresCopyBinding) releaseOperation() {
	if binding.lease != nil {
		binding.lease.ReleaseOperation()
	}
}

// releaseTerminal closes the physical snapshot and releases operation
// admission only when no transaction finalizer owns it. This is the common
// COPY error/abort boundary; a registered physical transaction callback keeps
// operation admission through COMMIT/ROLLBACK.
func (binding postgresCopyBinding) releaseTerminal() {
	binding.releaseSnapshot()
	if binding.lease == nil || !binding.lease.OperationDeferred() {
		binding.releaseOperation()
	}
}

func (binding postgresCopyBinding) leaseHeld() bool {
	return binding.lease != nil && binding.lease.SnapshotHeld()
}

func copyExecutorForBinding(conn *stdsql.Conn, tx *stdsql.Tx) adapter.SQLExecutor {
	if tx != nil {
		return tx
	}
	return conn
}

// sameCopyExecutor compares the concrete database/sql pointer behind the
// executor interface without assuming that every possible test double is
// directly comparable.
func sameCopyExecutor(left, right adapter.SQLExecutor) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftType, rightType := reflect.TypeOf(left), reflect.TypeOf(right)
	if leftType != rightType || !leftType.Comparable() {
		return false
	}
	return reflect.ValueOf(left).Interface() == reflect.ValueOf(right).Interface()
}

// validate checks both halves of the physical binding before a COPY operation
// touches the database. A missing binding and a replacement binding are both
// failures; neither is permission to acquire a fresh connection.
func (binding postgresCopyBinding) validate(ctx *sql.Context) error {
	if !binding.snapshotCaptured {
		return nil
	}
	// A retained execution lease pins the selected pool generation and the
	// protocol serializes this session's transaction binding. Re-reading the
	// binding here would re-enter the pool lifecycle lock; when shutdown is
	// waiting, that second read can block behind the writer while the current
	// lease is still held. The captured identity is therefore authoritative for
	// leased COPY state.
	if binding.leaseHeld() {
		return nil
	}
	if ctx == nil || ctx.Session == nil {
		return fmt.Errorf("COPY execution binding is unavailable")
	}
	if _, ok := ctx.Session.(adapter.ConnectionHolder); !ok {
		return fmt.Errorf("COPY execution session has no connection holder")
	}
	currentConn, currentTx := adapter.TryGetTxnBinding(ctx)
	if currentConn != binding.ownerConn || currentTx != binding.ownerTx {
		return fmt.Errorf("COPY execution binding changed")
	}
	if expected := copyExecutorForBinding(currentConn, currentTx); !sameCopyExecutor(binding.execer, expected) {
		return fmt.Errorf("COPY execution executor changed")
	}
	return nil
}
