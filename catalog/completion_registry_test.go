package catalog

import (
	stdsql "database/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newCompletionRegistryPoolForTest() *ConnectionPool {
	return &ConnectionPool{
		transactionCompletions: make(map[*stdsql.Tx]*transactionCompletion),
		activeTransactions:     make(map[*stdsql.Tx]struct{}),
	}
}

func finishTransactionForTest(p *ConnectionPool, tx *stdsql.Tx, success bool) {
	p.finishTransactionWithOutcomeDeferred(tx, success).run()
}

func TestTransactionCompletionTombstonesBoundedAndPendingRetained(t *testing.T) {
	p := newCompletionRegistryPoolForTest()

	pendingTx := new(stdsql.Tx)
	p.markTransactionActive(pendingTx)
	var pendingCalls atomic.Int32
	require.True(t, p.RegisterTransactionCompletion(pendingTx, func(bool) {
		pendingCalls.Add(1)
	}))

	completed := make([]*stdsql.Tx, maxTransactionCompletionTombstones+8)
	for i := range completed {
		completed[i] = new(stdsql.Tx)
		finishTransactionForTest(p, completed[i], i%2 == 0)
	}

	p.transactionCompletionMu.Lock()
	mapLen := len(p.transactionCompletions)
	orderLen := len(p.transactionCompletionOrder)
	_, pendingRetained := p.transactionCompletions[pendingTx]
	_, oldestRetained := p.transactionCompletions[completed[0]]
	_, newestRetained := p.transactionCompletions[completed[len(completed)-1]]
	p.transactionCompletionMu.Unlock()

	require.LessOrEqual(t, mapLen, maxTransactionCompletionTombstones+1)
	require.LessOrEqual(t, orderLen, maxTransactionCompletionTombstones)
	require.True(t, pendingRetained, "pending callback state must never be evicted")
	require.False(t, oldestRetained, "old completed tombstones should be evicted")
	require.True(t, newestRetained)
	require.Zero(t, pendingCalls.Load())
}

func TestTransactionCompletionTTLAndLateRegistration(t *testing.T) {
	p := newCompletionRegistryPoolForTest()

	recent := new(stdsql.Tx)
	finishTransactionForTest(p, recent, true)
	var recentSuccess atomic.Bool
	require.True(t, p.RegisterTransactionCompletion(recent, func(success bool) {
		recentSuccess.Store(success)
	}))
	require.True(t, recentSuccess.Load(), "a recent tombstone must preserve its outcome")

	expired := new(stdsql.Tx)
	finishTransactionForTest(p, expired, true)
	p.transactionCompletionMu.Lock()
	state := p.transactionCompletions[expired]
	p.transactionCompletionMu.Unlock()
	require.NotNil(t, state)
	state.mu.Lock()
	state.completedAt = time.Now().Add(-transactionCompletionTombstoneTTL - time.Second)
	state.mu.Unlock()

	var expiredCalls atomic.Int32
	require.False(t, p.RegisterTransactionCompletion(expired, func(success bool) {
		expiredCalls.Add(1)
		require.False(t, success)
	}))
	require.Equal(t, int32(1), expiredCalls.Load(), "expired registration must fail closed and resolve fallback")

	p.transactionCompletionMu.Lock()
	_, stillRetained := p.transactionCompletions[expired]
	p.transactionCompletionMu.Unlock()
	require.False(t, stillRetained)
}

func TestClearCompletedTransactionCompletionsKeepsPending(t *testing.T) {
	p := newCompletionRegistryPoolForTest()
	completed := new(stdsql.Tx)
	finishTransactionForTest(p, completed, false)
	pending := new(stdsql.Tx)
	p.markTransactionActive(pending)
	require.True(t, p.RegisterTransactionCompletion(pending, func(bool) {}))

	p.clearCompletedTransactionCompletions()

	p.transactionCompletionMu.Lock()
	_, completedRetained := p.transactionCompletions[completed]
	_, pendingRetained := p.transactionCompletions[pending]
	orderLen := len(p.transactionCompletionOrder)
	p.transactionCompletionMu.Unlock()
	require.False(t, completedRetained)
	require.True(t, pendingRetained)
	require.Zero(t, orderLen)
}

func TestTransactionCompletionRegisterAndFinishRace(t *testing.T) {
	p := newCompletionRegistryPoolForTest()
	tx := new(stdsql.Tx)
	p.markTransactionActive(tx)
	start := make(chan struct{})
	var calls atomic.Int32
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		p.RegisterTransactionCompletion(tx, func(bool) {
			calls.Add(1)
		})
	}()
	go func() {
		defer wg.Done()
		<-start
		finishTransactionForTest(p, tx, false)
	}()
	close(start)
	wg.Wait()
	require.Equal(t, int32(1), calls.Load())
}

func TestTransactionCompletionTombstoneWaitsForFinishedHook(t *testing.T) {
	p := newCompletionRegistryPoolForTest()
	tx := new(stdsql.Tx)
	p.markTransactionActive(tx)

	finishedStarted := make(chan struct{})
	releaseFinished := make(chan struct{})
	defer func() {
		select {
		case <-releaseFinished:
		default:
			close(releaseFinished)
		}
	}()
	p.SetTransactionLifecycleHooks(nil, func(*stdsql.Tx) {
		close(finishedStarted)
		<-releaseFinished
	})

	dispatch := p.finishTransactionWithOutcomeDeferred(tx, true)
	require.NotNil(t, dispatch)
	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		dispatch.run()
	}()

	select {
	case <-finishedStarted:
	case <-time.After(time.Second):
		t.Fatal("transaction finished hook did not start")
	}

	var lateCalls atomic.Int32
	require.True(t, p.RegisterTransactionCompletion(tx, func(success bool) {
		require.True(t, success)
		lateCalls.Add(1)
	}))
	require.Equal(t, int32(1), lateCalls.Load())

	// dispatchComplete is already true, but the owner has not completed its
	// finished hook. Neither TTL pruning nor lifecycle tombstone clearing may
	// remove this in-flight state.
	p.transactionCompletionMu.Lock()
	p.pruneTransactionCompletionsLocked(time.Now().Add(transactionCompletionTombstoneTTL + time.Second))
	_, retainedAfterPrune := p.transactionCompletions[tx]
	orderLenBeforeFinished := len(p.transactionCompletionOrder)
	p.transactionCompletionMu.Unlock()
	require.True(t, retainedAfterPrune)
	require.Zero(t, orderLenBeforeFinished)

	p.clearCompletedTransactionCompletions()
	p.transactionCompletionMu.Lock()
	_, retainedAfterClear := p.transactionCompletions[tx]
	p.transactionCompletionMu.Unlock()
	require.True(t, retainedAfterClear)

	close(releaseFinished)
	select {
	case <-dispatchDone:
	case <-time.After(time.Second):
		t.Fatal("transaction completion dispatch did not finish")
	}

	p.transactionCompletionMu.Lock()
	state := p.transactionCompletions[tx]
	orderLenAfterFinished := len(p.transactionCompletionOrder)
	p.transactionCompletionMu.Unlock()
	require.NotNil(t, state)
	recorded, _ := state.completionSnapshot()
	require.True(t, recorded)
	require.Equal(t, 1, orderLenAfterFinished)
}

func TestTransactionCompletionDuplicateFinishHasSingleOwner(t *testing.T) {
	p := newCompletionRegistryPoolForTest()
	tx := new(stdsql.Tx)
	p.markTransactionActive(tx)

	var callbackCalls atomic.Int32
	var finishedCalls atomic.Int32
	p.SetTransactionLifecycleHooks(nil, func(*stdsql.Tx) {
		finishedCalls.Add(1)
	})
	require.True(t, p.RegisterTransactionCompletion(tx, func(success bool) {
		require.True(t, success)
		callbackCalls.Add(1)
	}))

	owner := p.finishTransactionWithOutcomeDeferred(tx, true)
	duplicate := p.finishTransactionWithOutcomeDeferred(tx, false)
	require.NotNil(t, owner)
	require.Nil(t, duplicate)
	owner.run()

	require.Equal(t, int32(1), callbackCalls.Load())
	require.Equal(t, int32(1), finishedCalls.Load())
}
