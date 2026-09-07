package backend

import (
	"context"
	stdsql "database/sql"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestSessionBeginDuckLakeOperationNoopsWithoutProvider(t *testing.T) {
	// A nil provider/session must always return an idempotent no-op release.
	var nilSession *Session
	release := nilSession.BeginDuckLakeOperation(mycontext.WithFrontendQuery(context.Background()))
	release()
	release()
	session := &Session{}
	release = session.BeginDuckLakeOperation(mycontext.WithFrontendQuery(context.Background()))
	release()

	// Keep the forwarding contract compile-checked even when no runtime is
	// configured in this package-level test.
	var _ interface {
		BeginDuckLakeOperation(context.Context) func()
	} = (*Session)(nil)
}

func TestSessionEndRollsBackLogicalImplicitTransactionOnce(t *testing.T) {
	session, ctx, transaction := newFrontendImplicitSession(t)
	var callbackCalls atomic.Int32
	require.True(t, session.HasImplicitTransactionCleanup(transaction))
	session.RegisterImplicitTransactionCleanup(ctx, transaction, func() error {
		callbackCalls.Add(1)
		return nil
	})

	// SessionManager invokes this callback when COM_RESET_CONNECTION replaces a
	// GMS session. A second invocation can occur during disconnect cleanup and
	// must not run the operation-release callback twice.
	session.SessionEnd()
	session.SessionEnd()

	require.Equal(t, int32(1), callbackCalls.Load())
	require.Nil(t, session.Session.GetTransaction())
	require.Nil(t, session.implicit)
}

func TestSessionEndRollsBackPhysicalTransactionAndKeepsConnection(t *testing.T) {
	provider := catalog.NewInMemoryDBProvider()
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	session := NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)
	ctx := sql.NewContext(
		mycontext.WithFrontendQuery(context.Background()),
		sql.WithSession(session),
	)
	ctx.SetIgnoreAutoCommit(true)
	transactionValue, err := session.StartTransaction(ctx, sql.ReadWrite)
	require.NoError(t, err)
	transaction, ok := transactionValue.(*Transaction)
	require.True(t, ok)
	require.NotNil(t, transaction.tx)
	ctx.SetTransaction(transaction)

	ownerBefore, txBefore := session.GetTxnBinding()
	require.NotNil(t, ownerBefore)
	require.Same(t, transaction.tx, txBefore)
	var callbackCalls atomic.Int32
	var callbackSuccess atomic.Bool
	require.True(t, session.RegisterPostPhysicalTransactionCleanup(nil, transaction.tx, func(success bool) {
		callbackCalls.Add(1)
		callbackSuccess.Store(success)
	}))

	session.SessionEnd()

	require.Equal(t, int32(1), callbackCalls.Load())
	require.False(t, callbackSuccess.Load())
	require.Nil(t, session.Session.GetTransaction())
	ownerAfter, txAfter := session.GetTxnBinding()
	require.Same(t, ownerBefore, ownerAfter, "SessionEnd must not close the generic connection")
	require.Nil(t, txAfter)
}

func TestSessionCloseTxnIfCompletesPhysicalPostCleanup(t *testing.T) {
	provider := catalog.NewInMemoryDBProvider()
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	session := NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)

	tx, err := session.GetCatalogTxn(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, tx)

	var callbackCalls atomic.Int32
	var callbackSuccess atomic.Bool
	require.True(t, session.RegisterPostPhysicalTransactionCleanup(nil, tx, func(success bool) {
		callbackCalls.Add(1)
		callbackSuccess.Store(success)
	}))

	// CloseTxnIf is used when the protocol has lost a trustworthy owner and
	// cannot run the normal commit/rollback finalizer. It must still release the
	// callback associated with this exact physical transaction.
	session.CloseTxnIf(tx)
	require.Equal(t, int32(1), callbackCalls.Load())
	require.False(t, callbackSuccess.Load())
	require.Nil(t, session.TryGetTxnForRelease())

	// The raw transaction itself is still owned by the caller after an
	// identity-only mapping removal; finish it so the test does not hold a
	// database/sql connection through provider teardown.
	require.NoError(t, tx.Rollback())
}

func TestSessionCloseTxnIfDoesNotCompleteReplacementCallback(t *testing.T) {
	provider := catalog.NewInMemoryDBProvider()
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	session := NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)

	first, err := session.GetCatalogTxn(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, first)
	var callbackCalls atomic.Int32
	require.True(t, session.RegisterPostPhysicalTransactionCleanup(nil, first, func(bool) {
		callbackCalls.Add(1)
	}))

	// A stale callback carrying another transaction must not remove the current
	// mapping or complete the current transaction's callback.
	session.CloseTxnIf(new(stdsql.Tx))
	require.Same(t, first, session.TryGetTxnForRelease())
	require.Zero(t, callbackCalls.Load())

	session.CloseTxnIf(first)
	require.Equal(t, int32(1), callbackCalls.Load())
	require.Nil(t, session.TryGetTxnForRelease())
	require.NoError(t, first.Rollback())
}

func TestPostPhysicalCleanupLateRegistrationSeesCompletion(t *testing.T) {
	provider := catalog.NewInMemoryDBProvider()
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	session := NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)

	tx, err := session.GetCatalogTxn(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, tx)

	// Model pool teardown/finalization winning the race before COPY has
	// registered its deferred operation callback.
	session.completePhysicalPostCleanup(tx, false)
	var calls atomic.Int32
	var outcome atomic.Bool
	require.True(t, session.RegisterPostPhysicalTransactionCleanup(nil, tx, func(success bool) {
		calls.Add(1)
		outcome.Store(success)
	}))
	require.Equal(t, int32(1), calls.Load())
	require.False(t, outcome.Load())

	// The mapping is still owned by the test session; finish the raw handle so
	// provider teardown does not retain a database/sql connection.
	require.NoError(t, tx.Rollback())
}

func TestPostLogicalCleanupLateRegistrationSeesCompletion(t *testing.T) {
	transaction := &Transaction{}
	transaction.completePostCleanup(false)

	var calls atomic.Int32
	var outcome atomic.Bool
	require.True(t, transaction.registerPostCleanup(func(success bool) {
		calls.Add(1)
		outcome.Store(success)
	}))
	require.Equal(t, int32(1), calls.Load())
	require.False(t, outcome.Load())
}

func TestCloseConnIfBindingCompletesCallbackWhenRollbackAlreadyDone(t *testing.T) {
	provider := catalog.NewInMemoryDBProvider()
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	session := NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)

	started, err := session.GetCatalogTxn(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, started)
	owner, tx := session.GetTxnBindingForRelease()
	require.NotNil(t, owner)
	require.NotNil(t, tx)
	var calls atomic.Int32
	require.True(t, session.RegisterPostPhysicalTransactionCleanup(nil, tx, func(success bool) {
		require.False(t, success)
		calls.Add(1)
	}))

	// Force CloseConnIfBinding to observe an already-inactive driver
	// transaction. The pool still removes the exact mapping, but returns the
	// driver's ErrTxDone; callback completion must follow the mapping removal,
	// not the returned close error.
	require.NoError(t, tx.Rollback())
	_ = session.CloseConnIfBinding(owner, tx)
	require.Equal(t, int32(1), calls.Load())
	require.Nil(t, session.TryGetTxnForRelease())
}

func TestProviderCloseCompletesPhysicalPostCleanup(t *testing.T) {
	provider := catalog.NewInMemoryDBProvider()
	session := NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)
	started, err := session.GetCatalogTxn(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, started)
	_, tx := session.GetTxnBindingForRelease()
	require.NotNil(t, tx)

	var calls atomic.Int32
	require.True(t, session.RegisterPostPhysicalTransactionCleanup(nil, tx, func(success bool) {
		require.False(t, success)
		calls.Add(1)
	}))

	// Provider teardown finalizes through ConnectionPool.closeLocked rather
	// than Session.CloseTxn; the pool completion bridge must still resolve the
	// protocol callback exactly once.
	require.NoError(t, provider.Close())
	require.Equal(t, int32(1), calls.Load())
}

func TestSessionCommitTxnReportsStaleBindingAndFailureOutcome(t *testing.T) {
	provider := catalog.NewInMemoryDBProvider()
	t.Cleanup(func() { _ = provider.Close() })
	session := NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)

	tx, err := session.GetCatalogTxn(context.Background(), nil)
	require.NoError(t, err)
	var calls atomic.Int32
	var success atomic.Bool
	require.True(t, session.RegisterPostPhysicalTransactionCleanup(nil, tx, func(ok bool) {
		calls.Add(1)
		success.Store(ok)
	}))

	// Retire the binding through the normal session close path, then deliver a
	// delayed commit callback for the old physical transaction. The stale call
	// must be visible to its caller and must not be reported as success.
	require.NoError(t, tx.Rollback())
	session.CloseTxn()
	require.Equal(t, int32(1), calls.Load())
	require.False(t, success.Load())

	err = session.CommitTxn(tx)
	require.Error(t, err)
	require.True(t, errors.Is(err, adapter.ErrTransactionBindingChanged))
	require.Equal(t, int32(1), calls.Load())
}
