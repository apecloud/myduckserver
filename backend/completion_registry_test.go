package backend

import (
	stdsql "database/sql"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newPostPhysicalRegistrySessionForTest() *Session {
	return &Session{postPhysical: make(map[*stdsql.Tx]*postTransactionCleanup)}
}

func TestPostPhysicalTombstonesBoundedAndPendingRetained(t *testing.T) {
	sess := newPostPhysicalRegistrySessionForTest()

	pendingTx := new(stdsql.Tx)
	pendingState := &postTransactionCleanup{}
	sess.postPhysical[pendingTx] = pendingState
	var pendingCalls atomic.Int32
	require.True(t, pendingState.register(func(bool) {
		pendingCalls.Add(1)
	}))

	completed := make([]*stdsql.Tx, maxPostPhysicalTombstones+8)
	for i := range completed {
		completed[i] = new(stdsql.Tx)
		sess.completePhysicalPostCleanup(completed[i], i%2 == 0)
	}

	sess.postPhysicalMu.Lock()
	mapLen := len(sess.postPhysical)
	orderLen := len(sess.postPhysicalOrder)
	_, pendingRetained := sess.postPhysical[pendingTx]
	_, oldestRetained := sess.postPhysical[completed[0]]
	_, newestRetained := sess.postPhysical[completed[len(completed)-1]]
	sess.postPhysicalMu.Unlock()

	require.LessOrEqual(t, mapLen, maxPostPhysicalTombstones+1)
	require.LessOrEqual(t, orderLen, maxPostPhysicalTombstones)
	require.True(t, pendingRetained, "pending callback state must never be evicted")
	require.False(t, oldestRetained, "old completed tombstones should be evicted")
	require.True(t, newestRetained)
	require.Zero(t, pendingCalls.Load())
}

func TestPostPhysicalTTLAndLateRegistration(t *testing.T) {
	sess := newPostPhysicalRegistrySessionForTest()

	recent := new(stdsql.Tx)
	sess.completePhysicalPostCleanup(recent, true)
	var recentSuccess atomic.Bool
	require.True(t, sess.RegisterPostPhysicalTransactionCleanup(nil, recent, func(success bool) {
		recentSuccess.Store(success)
	}))
	require.True(t, recentSuccess.Load(), "a recent tombstone must preserve its outcome")

	expired := new(stdsql.Tx)
	sess.completePhysicalPostCleanup(expired, true)
	sess.postPhysicalMu.Lock()
	state := sess.postPhysical[expired]
	sess.postPhysicalMu.Unlock()
	require.NotNil(t, state)
	state.mu.Lock()
	state.completedAt = time.Now().Add(-postPhysicalTombstoneTTL - time.Second)
	state.mu.Unlock()

	var expiredCalls atomic.Int32
	require.False(t, sess.RegisterPostPhysicalTransactionCleanup(nil, expired, func(success bool) {
		expiredCalls.Add(1)
		require.False(t, success)
	}))
	require.Zero(t, expiredCalls.Load(), "false registration leaves fallback release to the caller")

	sess.postPhysicalMu.Lock()
	_, stillRetained := sess.postPhysical[expired]
	sess.postPhysicalMu.Unlock()
	require.False(t, stillRetained)
}

func TestPostPhysicalRegistrationWithoutFinalizerFailsClosed(t *testing.T) {
	sess := newPostPhysicalRegistrySessionForTest()
	tx := new(stdsql.Tx)
	var calls atomic.Int32
	require.False(t, sess.RegisterPostPhysicalTransactionCleanup(nil, tx, func(success bool) {
		calls.Add(1)
		require.False(t, success)
	}))
	// A false registration result leaves the caller responsible for fallback
	// release; the callback itself must not be invoked as well.
	require.Zero(t, calls.Load())
	sess.postPhysicalMu.Lock()
	_, retained := sess.postPhysical[tx]
	sess.postPhysicalMu.Unlock()
	require.False(t, retained)
}

func TestPostTransactionCleanupDrainsCallbacksBeforeRepanic(t *testing.T) {
	state := new(postTransactionCleanup)
	var firstCalls atomic.Int32
	var secondCalls atomic.Int32
	require.True(t, state.register(func(bool) {
		firstCalls.Add(1)
		panic("post-transaction cleanup panic")
	}))
	require.True(t, state.register(func(bool) {
		secondCalls.Add(1)
	}))

	require.PanicsWithValue(t, "post-transaction cleanup panic", func() {
		state.complete(false)
	})
	require.Equal(t, int32(1), firstCalls.Load())
	require.Equal(t, int32(1), secondCalls.Load())
	require.True(t, state.isCompleted())
}

func TestPostPhysicalTombstoneWaitsForCallbackDrain(t *testing.T) {
	tx := new(stdsql.Tx)
	state := new(postTransactionCleanup)
	sess := &Session{postPhysical: map[*stdsql.Tx]*postTransactionCleanup{tx: state}}

	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	defer func() {
		select {
		case <-releaseCallback:
		default:
			close(releaseCallback)
		}
	}()
	require.True(t, state.register(func(bool) {
		close(callbackStarted)
		<-releaseCallback
	}))

	completionDone := make(chan struct{})
	go func() {
		defer close(completionDone)
		sess.completePhysicalPostCleanup(tx, true)
	}()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("physical completion callback did not start")
	}

	var lateCalls atomic.Int32
	require.True(t, sess.RegisterPostPhysicalTransactionCleanup(nil, tx, func(success bool) {
		require.True(t, success)
		lateCalls.Add(1)
	}))
	require.Equal(t, int32(1), lateCalls.Load())

	// The terminal outcome is visible to late registration, but the original
	// callback batch is still in flight. It is not a pruneable tombstone yet.
	sess.postPhysicalMu.Lock()
	sess.prunePostPhysicalCompletionsLocked(time.Now().Add(postPhysicalTombstoneTTL + time.Second))
	_, retained := sess.postPhysical[tx]
	orderLenBeforeDrain := len(sess.postPhysicalOrder)
	sess.postPhysicalMu.Unlock()
	require.True(t, retained)
	require.Zero(t, orderLenBeforeDrain)

	close(releaseCallback)
	select {
	case <-completionDone:
	case <-time.After(time.Second):
		t.Fatal("physical completion did not finish")
	}

	sess.postPhysicalMu.Lock()
	recorded := sess.postPhysical[tx]
	orderLenAfterDrain := len(sess.postPhysicalOrder)
	sess.postPhysicalMu.Unlock()
	require.Same(t, state, recorded)
	completed, _ := state.completionSnapshot()
	require.True(t, completed)
	require.Equal(t, 1, orderLenAfterDrain)
}
