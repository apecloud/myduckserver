package backend

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apecloud/myduckserver/mycontext"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func newFrontendImplicitSession(t *testing.T) (*Session, *sql.Context, *Transaction) {
	t.Helper()
	sess := NewSession(memory.NewSession(sql.NewBaseSession(), nil), nil)
	ctx := sql.NewContext(
		mycontext.WithFrontendQuery(context.Background()),
		sql.WithSession(sess),
	)
	tx, err := sess.StartTransaction(ctx, sql.ReadWrite)
	require.NoError(t, err)
	transaction, ok := tx.(*Transaction)
	require.True(t, ok)
	ctx.SetTransaction(transaction)
	require.Same(t, transaction, sess.Session.GetTransaction())
	require.NotNil(t, sess.implicit)
	return sess, ctx, transaction
}

func TestImplicitCleanupSetTransactionRollsBackAfterCallback(t *testing.T) {
	sess, ctx, transaction := newFrontendImplicitSession(t)
	callbackCalls := 0
	var callbackTx sql.Transaction
	sess.RegisterImplicitTransactionCleanup(ctx, transaction, func() error {
		callbackCalls++
		callbackTx = sess.Session.GetTransaction()
		return nil
	})

	ctx.SetTransaction(nil)

	require.Equal(t, 1, callbackCalls)
	require.Same(t, transaction, callbackTx, "callback must run while the logical transaction is current")
	require.Nil(t, sess.Session.GetTransaction())
	require.Nil(t, sess.implicit)
	ctx.SetTransaction(nil)
	require.Equal(t, 1, callbackCalls, "cleanup must be exactly once")
}

func TestImplicitCleanupRetainsCallbackErrorAndStillRollsBack(t *testing.T) {
	sess, ctx, transaction := newFrontendImplicitSession(t)
	callbackErr := errors.New("iterator close failed")
	callbackCalls := 0
	sess.RegisterImplicitTransactionCleanup(ctx, transaction, func() error {
		callbackCalls++
		return callbackErr
	})

	ctx.SetTransaction(nil)

	require.Equal(t, 1, callbackCalls)
	require.ErrorIs(t, sess.AutocommitCleanupError(), callbackErr)
	require.ErrorIs(t, sess.TakeAutocommitCleanupError(), callbackErr)
	require.Nil(t, sess.Session.GetTransaction())
	require.Nil(t, sess.AutocommitCleanupError())
}

func TestImplicitCleanupCommitRunsCallbackBeforeCommitAndDisarms(t *testing.T) {
	sess, ctx, transaction := newFrontendImplicitSession(t)
	callbackCalls := 0
	var callbackTx sql.Transaction
	derived := ctx.WithContext(context.WithValue(ctx.Context, struct{}{}, "derived"))
	sess.RegisterImplicitTransactionCleanup(derived, transaction, func() error {
		callbackCalls++
		callbackTx = sess.Session.GetTransaction()
		return nil
	})

	require.NoError(t, sess.CommitTransaction(ctx, transaction))
	require.Equal(t, 1, callbackCalls)
	require.Same(t, transaction, callbackTx)
	require.Nil(t, sess.implicit)
	ctx.SetTransaction(nil)
	require.Equal(t, 1, callbackCalls)
}

func TestImplicitCleanupFailedHandleRejectsCommitUntilRollback(t *testing.T) {
	sess, ctx, transaction := newFrontendImplicitSession(t)
	callbackCalls := 0
	callbackErr := errors.New("terminal iterator failure")
	handle := sess.RegisterImplicitTransactionCleanup(ctx, transaction, func() error {
		callbackCalls++
		return callbackErr
	})
	handle(false)
	handle(false)

	err := sess.CommitTransaction(ctx, transaction)
	require.ErrorIs(t, err, callbackErr)
	require.Same(t, transaction, sess.Session.GetTransaction())
	require.NotNil(t, sess.implicit, "failed marker must remain armed")
	require.Equal(t, 1, callbackCalls, "commit probes the callback before deciding whether to commit")

	ctx.SetTransaction(nil)
	require.Equal(t, 1, callbackCalls, "rollback callback is idempotent")
	require.Nil(t, sess.Session.GetTransaction())
	require.Nil(t, sess.implicit)
}

func TestImplicitCleanupMismatchLeavesReplacementAndMarker(t *testing.T) {
	sess, ctx, transaction := newFrontendImplicitSession(t)
	replacement := &Transaction{}
	callbackCalls := 0
	sess.RegisterImplicitTransactionCleanup(ctx, transaction, func() error {
		callbackCalls++
		return nil
	})

	// Simulate a GMS temporary transaction swap. Use the embedded setter to
	// avoid changing the marker while constructing the replacement.
	sess.Session.SetTransaction(replacement)
	ctx.SetTransaction(nil)
	require.Same(t, replacement, sess.Session.GetTransaction())
	require.NotNil(t, sess.implicit, "a mismatched nil setter must not consume the marker")
	require.Zero(t, callbackCalls)

	// Once the original transaction is current again, the nil setter can safely
	// consume its marker and perform the rollback.
	sess.Session.SetTransaction(transaction)
	ctx.SetTransaction(nil)
	require.Equal(t, 1, callbackCalls)
	require.Nil(t, sess.Session.GetTransaction())
	require.Nil(t, sess.implicit)
}

func TestImplicitCleanupRegistrationAndDetachAreAtomic(t *testing.T) {
	// Hold the state mutex so registration and rollback contend at the exact
	// check/append boundary. With a split implicitMu check followed by add, the
	// detach path can finish first and registration then appends to an unreachable
	// state. The atomic implementation keeps implicitMu held until add completes.
	for iteration := 0; iteration < 100; iteration++ {
		sess, ctx, transaction := newFrontendImplicitSession(t)
		state := sess.implicit
		require.NotNil(t, state)

		var callbackCalls atomic.Int32
		state.mu.Lock()
		registerDone := make(chan struct{})
		go func() {
			sess.RegisterImplicitTransactionCleanup(ctx, transaction, func() error {
				callbackCalls.Add(1)
				return nil
			})
			close(registerDone)
		}()
		// Let the registration goroutine reach the state-level append lock before
		// starting the competing detach. The bounded wait below keeps the test
		// deterministic without depending on a scheduler-specific sleep.
		for i := 0; i < 8; i++ {
			runtime.Gosched()
		}

		detachDone := make(chan struct{})
		go func() {
			ctx.SetTransaction(nil)
			close(detachDone)
		}()
		select {
		case <-detachDone:
		case <-time.After(5 * time.Millisecond):
		}
		state.mu.Unlock()

		select {
		case <-registerDone:
		case <-time.After(time.Second):
			t.Fatal("implicit cleanup registration did not finish")
		}
		select {
		case <-detachDone:
		case <-time.After(time.Second):
			t.Fatal("implicit cleanup detach did not finish")
		}

		state.mu.Lock()
		callbackCount := len(state.callbacks)
		cleanupStarted := state.cleanupStarted
		state.mu.Unlock()
		if callbackCalls.Load() == 0 {
			// A registration that loses the race must be rejected completely; it
			// must not remain queued on the detached marker.
			require.Zero(t, callbackCount)
		} else {
			require.Equal(t, int32(1), callbackCalls.Load())
			require.True(t, cleanupStarted)
			require.Equal(t, 1, callbackCount)
		}
	}
}

func TestImplicitCleanupStoredProcedureDetachRestoresOuterMarker(t *testing.T) {
	sess, ctx, outer := newFrontendImplicitSession(t)
	callbackCalls := 0
	var callbackTx sql.Transaction
	sess.RegisterImplicitTransactionCleanup(ctx, outer, func() error {
		callbackCalls++
		callbackTx = sess.Session.GetTransaction()
		return nil
	})

	// This is the sequence in GMS rowexec/proc.go: buildCall saves oldTx,
	// clears it before running the body, then restores oldTx in a defer.
	endProcedure := sess.beginProcedureTransactionScope()
	ctx.SetTransaction(nil)
	require.Nil(t, sess.Session.GetTransaction())
	require.Nil(t, sess.implicit)
	require.Zero(t, callbackCalls, "temporary procedure detach must not clean up outer state")

	innerSQLTx, err := sess.StartTransaction(ctx, sql.ReadWrite)
	require.NoError(t, err)
	inner, ok := innerSQLTx.(*Transaction)
	require.True(t, ok)
	ctx.SetTransaction(inner)

	// The deferred restore must identify the original transaction, resolve any
	// procedure-owned child, and re-arm the original marker.
	ctx.SetTransaction(outer)
	require.Same(t, outer, sess.Session.GetTransaction())
	require.Same(t, outer, sess.implicit.tx)
	require.Zero(t, callbackCalls)

	endProcedure()
	ctx.SetTransaction(nil)
	require.Equal(t, 1, callbackCalls)
	require.Same(t, outer, callbackTx)
	require.Nil(t, sess.Session.GetTransaction())
	require.Nil(t, sess.implicit)
}

func TestImplicitCleanupStoredProcedureNestedScopesKeepMarkerIdentity(t *testing.T) {
	sess, ctx, outer := newFrontendImplicitSession(t)
	outerCalls, innerCalls := 0, 0
	sess.RegisterImplicitTransactionCleanup(ctx, outer, func() error {
		outerCalls++
		return nil
	})

	endOuter := sess.beginProcedureTransactionScope()
	ctx.SetTransaction(nil)

	innerTx, err := sess.StartTransaction(ctx, sql.ReadWrite)
	require.NoError(t, err)
	inner := innerTx.(*Transaction)
	ctx.SetTransaction(inner)
	sess.RegisterImplicitTransactionCleanup(ctx, inner, func() error {
		innerCalls++
		return nil
	})

	endInner := sess.beginProcedureTransactionScope()
	ctx.SetTransaction(nil)
	require.Nil(t, sess.Session.GetTransaction())
	require.Nil(t, sess.implicit)

	// A child transaction created inside the nested scope is cleaned before the
	// nested outer marker is restored.
	childTx, err := sess.StartTransaction(ctx, sql.ReadWrite)
	require.NoError(t, err)
	child := childTx.(*Transaction)
	ctx.SetTransaction(child)
	ctx.SetTransaction(inner)
	require.Same(t, inner, sess.Session.GetTransaction())
	require.Same(t, inner, sess.implicit.tx)
	require.Zero(t, innerCalls, "restoring the nested outer marker must not clean it up")

	endInner()
	ctx.SetTransaction(outer)
	require.Same(t, outer, sess.Session.GetTransaction())
	require.Same(t, outer, sess.implicit.tx)
	require.Equal(t, 1, innerCalls, "the nested marker is cleaned before restoring the outer marker")
	require.Zero(t, outerCalls)

	endOuter()
	ctx.SetTransaction(nil)
	require.Equal(t, 1, outerCalls)
	require.Nil(t, sess.Session.GetTransaction())
	require.Nil(t, sess.implicit)
}

func TestImplicitCleanupOnlyArmsForFrontendOrigin(t *testing.T) {
	origins := []mycontext.QueryOriginKind{
		mycontext.UnknownQueryOrigin,
		mycontext.InternalQueryOrigin,
		mycontext.MySQLReplicationQueryOrigin,
		mycontext.PostgresReplicationQueryOrigin,
		mycontext.MaintenanceQueryOrigin,
		mycontext.RecoveryQueryOrigin,
	}
	for _, origin := range origins {
		t.Run(originName(origin), func(t *testing.T) {
			sess := NewSession(memory.NewSession(sql.NewBaseSession(), nil), nil)
			base := context.Background()
			if origin != mycontext.UnknownQueryOrigin {
				base = mycontext.WithQueryOrigin(base, origin)
			}
			ctx := sql.NewContext(base, sql.WithSession(sess))
			tx, err := sess.StartTransaction(ctx, sql.ReadWrite)
			require.NoError(t, err)
			require.Nil(t, sess.implicit)
			ctx.SetTransaction(tx)
			sess.Session.SetTransaction(nil)
		})
	}
}

func TestSetAutocommitZeroDisarmsImplicitCleanupOnce(t *testing.T) {
	sess, ctx, transaction := newFrontendImplicitSession(t)
	callbackCalls := 0
	sess.RegisterImplicitTransactionCleanup(ctx, transaction, func() error {
		callbackCalls++
		return nil
	})

	require.NoError(t, sess.SetSessionVariable(ctx, sql.AutoCommitSessionVar, int8(0)))
	require.Equal(t, 1, callbackCalls)
	require.Nil(t, sess.implicit)
	require.Nil(t, sess.Session.GetTransaction())
	ctx.SetTransaction(nil)
	require.Equal(t, 1, callbackCalls)
}

func originName(origin mycontext.QueryOriginKind) string {
	switch origin {
	case mycontext.UnknownQueryOrigin:
		return "unknown"
	case mycontext.InternalQueryOrigin:
		return "internal"
	case mycontext.MySQLReplicationQueryOrigin:
		return "mysql-replication"
	case mycontext.PostgresReplicationQueryOrigin:
		return "postgres-replication"
	case mycontext.MaintenanceQueryOrigin:
		return "maintenance"
	case mycontext.RecoveryQueryOrigin:
		return "recovery"
	default:
		return "origin"
	}
}
