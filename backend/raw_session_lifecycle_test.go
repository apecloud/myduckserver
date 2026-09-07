package backend

import (
	"context"
	stdsql "database/sql"
	"sync/atomic"
	"testing"

	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func newRawLifecycleSession(t *testing.T, id uint32) (*catalog.DatabaseProvider, *Session, *stdsql.Conn, *stdsql.Tx) {
	t.Helper()
	provider := catalog.NewInMemoryDBProvider()
	session := NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)
	session.SetConnectionId(id)
	tx, err := session.GetCatalogTxn(context.Background(), nil)
	require.NoError(t, err)
	owner, bound := session.GetTxnBindingForRelease()
	require.NotNil(t, owner)
	require.Same(t, tx, bound)
	return provider, session, owner, tx
}

// PostgreSQL transaction controls own a raw database/sql transaction without
// placing a GMS transaction in Session.GetTransaction. SessionEnd must still
// roll it back while retaining the generic pool connection for subsequent
// lifecycle cleanup.
func TestSessionEndRawTransactionRollsBackAndRetainsGenericConnection(t *testing.T) {
	provider, session, owner, tx := newRawLifecycleSession(t, 311)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })

	var callbacks atomic.Int32
	var success atomic.Bool
	require.True(t, session.RegisterPostPhysicalTransactionCleanup(nil, tx, func(ok bool) {
		callbacks.Add(1)
		success.Store(ok)
	}))
	// Registration on an active raw transaction must only arm the bridge. A
	// reversed pool-result branch would complete this callback immediately and
	// release a COPY/operation lease before the physical transaction ends.
	require.Zero(t, callbacks.Load())

	// Do not call SetTransaction: this is the raw PostgreSQL path.
	session.SessionEnd()

	require.Equal(t, int32(1), callbacks.Load())
	require.False(t, success.Load())
	currentConn, currentTx := session.GetTxnBindingForRelease()
	require.Same(t, owner, currentConn)
	require.Nil(t, currentTx)

	// The generic connection remains usable after raw rollback and is not
	// replaced merely because the transaction binding was finalized.
	conn, err := session.GetCatalogConn(context.Background())
	require.NoError(t, err)
	require.Same(t, owner, conn)
	var got int
	require.NoError(t, conn.QueryRowContext(context.Background(), "SELECT 1").Scan(&got))
	require.Equal(t, 1, got)
}

// A stale SessionEnd may run after another lifecycle path has already removed
// the raw binding and installed a replacement generic connection. It must
// finish only its captured transaction and never close the replacement.
func TestSessionEndRawBindingMismatchKeepsReplacementConnection(t *testing.T) {
	provider, session, owner, tx := newRawLifecycleSession(t, 312)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })

	var callbacks atomic.Int32
	require.True(t, session.RegisterPostPhysicalTransactionCleanup(nil, tx, func(bool) {
		callbacks.Add(1)
	}))

	// Model an earlier finalizer that completed the raw transaction and removed
	// its pool mapping, but did not clear this stale Session object's snapshot.
	require.NoError(t, tx.Rollback())
	provider.Pool().CloseTxn(session.ID())
	require.NoError(t, provider.Pool().CloseConn(session.ID()))
	replacement, err := provider.Pool().GetConn(context.Background(), session.ID())
	require.NoError(t, err)
	require.NotNil(t, replacement)
	require.NotSame(t, owner, replacement)
	require.Equal(t, int32(1), callbacks.Load())

	session.SessionEnd()

	currentConn, currentTx := provider.Pool().GetTxnBindingForRelease(session.ID())
	require.Same(t, replacement, currentConn)
	require.Nil(t, currentTx)
	require.Equal(t, int32(1), callbacks.Load(), "stale SessionEnd must not complete the callback twice")
}

func TestPoolBridgeDrainsSessionCallbacksBeforeRepanic(t *testing.T) {
	provider, session, owner, tx := newRawLifecycleSession(t, 313)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })

	var firstCalls atomic.Int32
	var secondCalls atomic.Int32
	require.True(t, session.RegisterPostPhysicalTransactionCleanup(nil, tx, func(bool) {
		firstCalls.Add(1)
		panic("pool bridge cleanup panic")
	}))
	require.True(t, session.RegisterPostPhysicalTransactionCleanup(nil, tx, func(bool) {
		secondCalls.Add(1)
	}))

	require.PanicsWithValue(t, "pool bridge cleanup panic", func() {
		_, _, _ = session.RollbackTxn(tx)
	})
	require.Equal(t, int32(1), firstCalls.Load())
	require.Equal(t, int32(1), secondCalls.Load())
	currentConn, currentTx := provider.Pool().GetTxnBindingForRelease(session.ID())
	require.Same(t, owner, currentConn)
	require.Nil(t, currentTx)

	var lateCalls atomic.Int32
	require.True(t, session.RegisterPostPhysicalTransactionCleanup(nil, tx, func(success bool) {
		require.False(t, success)
		lateCalls.Add(1)
	}))
	require.Equal(t, int32(1), lateCalls.Load())
}
