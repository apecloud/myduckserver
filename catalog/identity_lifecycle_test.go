package catalog

import (
	"database/sql"
	"sync/atomic"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

// CloseTxnAndGet must resolve the binding at the point it acquires the
// release/session section. A caller may have captured an older snapshot before
// a replacement transaction was installed; the replacement is the only entry
// that this close operation is allowed to remove and complete.
func TestCloseTxnAndGetUsesCurrentReplacementAndReturnsNilWhenAbsent(t *testing.T) {
	p := NewConnectionPool(nil, nil, "memory")
	const sessionID uint32 = 301
	first := new(sql.Tx)
	replacement := new(sql.Tx)

	var firstCompletions atomic.Int32
	var replacementCompletions atomic.Int32
	p.txns.Store(sessionID, first)
	require.True(t, p.RegisterTransactionCompletion(first, func(bool) {
		firstCompletions.Add(1)
	}))
	// This is the stale read that the old lookup-then-close sequence could use.
	staleSnapshot := p.TryGetTxnForRelease(sessionID)
	require.Same(t, first, staleSnapshot)

	p.txns.Store(sessionID, replacement)
	require.True(t, p.RegisterTransactionCompletion(replacement, func(success bool) {
		require.False(t, success)
		replacementCompletions.Add(1)
	}))

	removed := p.CloseTxnAndGet(sessionID)
	require.Same(t, replacement, removed)
	require.Zero(t, firstCompletions.Load(), "the stale snapshot must not be completed")
	require.Equal(t, int32(1), replacementCompletions.Load())
	require.Nil(t, p.TryGetTxnForRelease(sessionID))

	// A nil result is an observation, not permission to remove a later binding.
	require.Nil(t, p.CloseTxnAndGet(sessionID))
	require.Nil(t, p.TryGetTxnForRelease(sessionID))
}

// A malformed/missing physical owner is a binding error, not a terminal
// transaction event. The completion callback must stay pending until a later
// finalizer can remove the exact mapping.
func TestRollbackTxnBindingErrorLeavesCompletionPending(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	p := NewConnectionPool(nil, db, "memory")
	p.registerMySQLUDFsOnce.Do(func() {})
	t.Cleanup(func() { _ = db.Close() })
	const sessionID uint32 = 302
	mock.ExpectBegin()
	tx, err := p.GetTxn(nil, sessionID, "", nil)
	require.NoError(t, err)
	owner, bound := p.GetTxnBindingForRelease(sessionID)
	require.NotNil(t, owner)
	require.Same(t, tx, bound)

	var completions atomic.Int32
	require.True(t, p.RegisterTransactionCompletion(tx, func(success bool) {
		require.False(t, success)
		completions.Add(1)
	}))

	// Corrupt only the owner side. Rollback must fail closed and leave tx mapped
	// so its deferred operation lease cannot be released early.
	p.txnConns.Delete(sessionID)
	conn, inactive, err := p.RollbackTxn(sessionID, tx)
	require.Nil(t, conn)
	require.False(t, inactive)
	require.ErrorContains(t, err, "physical owner")
	require.Zero(t, completions.Load())
	require.Same(t, tx, p.TryGetTxnForRelease(sessionID))

	// Repair the binding and let the real finalizer close it.
	p.txnConns.Store(sessionID, owner)
	mock.ExpectRollback()
	conn, inactive, err = p.RollbackTxn(sessionID, tx)
	require.Same(t, owner, conn)
	require.True(t, inactive)
	require.NoError(t, err)
	require.Equal(t, int32(1), completions.Load())
	require.Nil(t, p.TryGetTxnForRelease(sessionID))
	mock.ExpectClose()
	require.NoError(t, p.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}
