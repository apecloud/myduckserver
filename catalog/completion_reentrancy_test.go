package catalog

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

const completionReentryTimeout = 3 * time.Second

type completionObservation struct {
	calls   atomic.Int32
	outcome atomic.Int32
}

func registerReentrantCompletion(
	t *testing.T,
	p *ConnectionPool,
	id uint32,
	tx *stdsql.Tx,
	beforeReturn func() bool,
) *completionObservation {
	t.Helper()
	observation := new(completionObservation)
	require.True(t, p.RegisterTransactionCompletion(tx, func(success bool) {
		// Re-enter both the lifecycle release gate and the same session mutex. A
		// callback dispatched by a *Locked helper would deadlock here.
		_, _ = p.GetTxnBindingForRelease(id)
		if beforeReturn != nil && !beforeReturn() {
			observation.outcome.Store(-2)
		} else if success {
			observation.outcome.Store(1)
		} else {
			observation.outcome.Store(-1)
		}
		observation.calls.Add(1)
	}))
	return observation
}

func runCompletionFinalizer(t *testing.T, call func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- call()
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(completionReentryTimeout):
		t.Fatal("transaction completion callback did not return after pool re-entry")
	}
}

func requireCompletionObservation(t *testing.T, observation *completionObservation, wantOutcome int32) {
	t.Helper()
	require.Equal(t, int32(1), observation.calls.Load(), "completion must run exactly once before the finalizer returns")
	require.Equal(t, wantOutcome, observation.outcome.Load())
}

func newReentrantTransactionPool(
	t *testing.T,
	id uint32,
) (*ConnectionPool, *stdsql.DB, sqlmock.Sqlmock, *stdsql.Conn, *stdsql.Tx) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	p := NewConnectionPool(nil, db, "memory")
	p.registerMySQLUDFsOnce.Do(func() {})
	mock.ExpectBegin()
	tx, err := p.GetTxn(context.Background(), id, "", nil)
	require.NoError(t, err)
	conn, bound := p.GetTxnBindingForRelease(id)
	require.NotNil(t, conn)
	require.Same(t, tx, bound)
	return p, db, mock, conn, tx
}

func TestConnectionPoolMappingFinalizersDispatchCompletionAfterUnlock(t *testing.T) {
	t.Run("CloseTxnAndGet", func(t *testing.T) {
		p := NewConnectionPool(nil, nil, "memory")
		const id uint32 = 401
		tx := new(stdsql.Tx)
		p.txns.Store(id, tx)
		p.markTransactionActive(tx)
		observation := registerReentrantCompletion(t, p, id, tx, nil)

		var removed *stdsql.Tx
		runCompletionFinalizer(t, func() error {
			removed = p.CloseTxnAndGet(id)
			return nil
		})
		require.Same(t, tx, removed)
		requireCompletionObservation(t, observation, -1)
	})

	t.Run("CloseTxnIf", func(t *testing.T) {
		p := NewConnectionPool(nil, nil, "memory")
		const id uint32 = 402
		tx := new(stdsql.Tx)
		p.txns.Store(id, tx)
		p.markTransactionActive(tx)
		observation := registerReentrantCompletion(t, p, id, tx, nil)

		runCompletionFinalizer(t, func() error {
			p.CloseTxnIf(id, tx)
			return nil
		})
		requireCompletionObservation(t, observation, -1)
	})
}

func TestConnectionPoolDriverFinalizersDispatchCompletionAfterUnlock(t *testing.T) {
	t.Run("CloseConn", func(t *testing.T) {
		const id uint32 = 409
		p, _, mock, _, tx := newReentrantTransactionPool(t, id)
		observation := registerReentrantCompletion(t, p, id, tx, nil)
		mock.ExpectRollback()
		mock.ExpectClose()

		runCompletionFinalizer(t, func() error {
			return p.CloseConn(id)
		})
		requireCompletionObservation(t, observation, -1)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("CloseConnIf", func(t *testing.T) {
		const id uint32 = 410
		p, _, mock, conn, tx := newReentrantTransactionPool(t, id)
		observation := registerReentrantCompletion(t, p, id, tx, nil)
		mock.ExpectRollback()
		mock.ExpectClose()

		runCompletionFinalizer(t, func() error {
			return p.CloseConnIf(id, conn)
		})
		requireCompletionObservation(t, observation, -1)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("CommitTxnWithOutcome", func(t *testing.T) {
		const id uint32 = 403
		p, _, mock, _, tx := newReentrantTransactionPool(t, id)
		observation := registerReentrantCompletion(t, p, id, tx, nil)
		mock.ExpectCommit()

		runCompletionFinalizer(t, func() error {
			matched, err := p.CommitTxnWithOutcome(id, tx)
			if !matched {
				return fmt.Errorf("commit did not match the current transaction")
			}
			return err
		})
		requireCompletionObservation(t, observation, 1)
		mock.ExpectClose()
		require.NoError(t, p.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("RollbackTxn", func(t *testing.T) {
		const id uint32 = 404
		p, _, mock, _, tx := newReentrantTransactionPool(t, id)
		observation := registerReentrantCompletion(t, p, id, tx, nil)
		mock.ExpectRollback()

		runCompletionFinalizer(t, func() error {
			_, inactive, err := p.RollbackTxn(id, tx)
			if !inactive {
				return fmt.Errorf("rollback did not prove the transaction inactive")
			}
			return err
		})
		requireCompletionObservation(t, observation, -1)
		mock.ExpectClose()
		require.NoError(t, p.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("RollbackTxnWithCleanup", func(t *testing.T) {
		const id uint32 = 405
		p, _, mock, _, tx := newReentrantTransactionPool(t, id)
		var cleanupDone atomic.Bool
		observation := registerReentrantCompletion(t, p, id, tx, cleanupDone.Load)
		mock.ExpectRollback()

		runCompletionFinalizer(t, func() error {
			inactive, err := p.RollbackTxnWithCleanup(id, tx, func(*stdsql.Conn) error {
				cleanupDone.Store(true)
				return nil
			})
			if !inactive {
				return fmt.Errorf("rollback did not prove the transaction inactive")
			}
			return err
		})
		requireCompletionObservation(t, observation, -1)
		mock.ExpectClose()
		require.NoError(t, p.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("RollbackTxnWithCleanup late registration", func(t *testing.T) {
		const id uint32 = 408
		p, _, mock, _, tx := newReentrantTransactionPool(t, id)
		var cleanupDone atomic.Bool
		observation := new(completionObservation)
		var registered bool
		mock.ExpectRollback()

		runCompletionFinalizer(t, func() error {
			inactive, err := p.RollbackTxnWithCleanup(id, tx, func(*stdsql.Conn) error {
				registered = p.RegisterTransactionCompletion(tx, func(success bool) {
					// Registration happens after the terminal outcome is recorded but
					// before the pool/session locks are released. It must join the
					// deferred batch instead of running inline under those locks.
					_, _ = p.GetTxnBindingForRelease(id)
					if !cleanupDone.Load() {
						observation.outcome.Store(-2)
					} else if success {
						observation.outcome.Store(1)
					} else {
						observation.outcome.Store(-1)
					}
					observation.calls.Add(1)
				})
				cleanupDone.Store(true)
				return nil
			})
			if !inactive {
				return fmt.Errorf("rollback did not prove the transaction inactive")
			}
			return err
		})
		require.True(t, registered)
		requireCompletionObservation(t, observation, -1)
		mock.ExpectClose()
		require.NoError(t, p.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("CloseConnIfBinding", func(t *testing.T) {
		const id uint32 = 406
		p, _, mock, conn, tx := newReentrantTransactionPool(t, id)
		observation := registerReentrantCompletion(t, p, id, tx, nil)
		mock.ExpectRollback()
		mock.ExpectClose()

		runCompletionFinalizer(t, func() error {
			return p.CloseConnIfBinding(id, conn, tx)
		})
		requireCompletionObservation(t, observation, -1)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestConnectionPoolGenerationFinalizersDispatchInOrderAfterUnlock(t *testing.T) {
	for _, reset := range []bool{false, true} {
		name := "Close"
		if reset {
			name = "Reset"
		}
		t.Run(name, func(t *testing.T) {
			const id uint32 = 407
			p, _, mock, _, tx := newReentrantTransactionPool(t, id)
			var orderMu sync.Mutex
			order := make([]string, 0, 2)
			p.SetTransactionLifecycleHooks(nil, func(*stdsql.Tx) {
				_, _ = p.GetTxnBindingForRelease(id)
				orderMu.Lock()
				order = append(order, "finished")
				orderMu.Unlock()
			})
			require.True(t, p.RegisterTransactionCompletion(tx, func(bool) {
				_, _ = p.GetTxnBindingForRelease(id)
				orderMu.Lock()
				order = append(order, "callback")
				orderMu.Unlock()
			}))
			mock.ExpectRollback()
			mock.ExpectClose()

			var replacementDB *stdsql.DB
			var replacementMock sqlmock.Sqlmock
			if reset {
				var err error
				replacementDB, replacementMock, err = sqlmock.New()
				require.NoError(t, err)
			}
			runCompletionFinalizer(t, func() error {
				if reset {
					return p.Reset(nil, replacementDB)
				}
				return p.Close()
			})

			orderMu.Lock()
			gotOrder := append([]string(nil), order...)
			orderMu.Unlock()
			require.Equal(t, []string{"callback", "finished"}, gotOrder)
			require.NoError(t, mock.ExpectationsWereMet())

			if reset {
				replacementMock.ExpectClose()
				require.NoError(t, p.Close())
				require.NoError(t, replacementMock.ExpectationsWereMet())
			}
		})
	}
}

func TestTransactionCompletionBatchDrainsAfterCallbackPanic(t *testing.T) {
	var eventsMu sync.Mutex
	events := make([]string, 0, 4)
	appendEvent := func(event string) {
		eventsMu.Lock()
		events = append(events, event)
		eventsMu.Unlock()
	}

	batch := transactionCompletionBatch{dispatches: []*transactionCompletionDispatch{
		{
			callbacks: []func(bool){func(bool) {
				appendEvent("first-callback")
				panic("completion panic")
			}},
			finished: func(*stdsql.Tx) { appendEvent("first-finished") },
		},
		{
			callbacks: []func(bool){func(bool) { appendEvent("second-callback") }},
			finished:  func(*stdsql.Tx) { appendEvent("second-finished") },
		},
	}}

	require.PanicsWithValue(t, "completion panic", batch.run)
	eventsMu.Lock()
	gotEvents := append([]string(nil), events...)
	eventsMu.Unlock()
	require.Equal(t, []string{
		"first-callback",
		"first-finished",
		"second-callback",
		"second-finished",
	}, gotEvents)
	require.Empty(t, batch.dispatches)
}

func TestTransactionCompletionBatchDrainsAfterFinishedHookPanic(t *testing.T) {
	var eventsMu sync.Mutex
	events := make([]string, 0, 4)
	appendEvent := func(event string) {
		eventsMu.Lock()
		events = append(events, event)
		eventsMu.Unlock()
	}

	batch := transactionCompletionBatch{dispatches: []*transactionCompletionDispatch{
		{
			callbacks: []func(bool){func(bool) { appendEvent("first-callback") }},
			finished: func(*stdsql.Tx) {
				appendEvent("first-finished")
				panic("finished hook panic")
			},
		},
		{
			callbacks: []func(bool){func(bool) { appendEvent("second-callback") }},
			finished:  func(*stdsql.Tx) { appendEvent("second-finished") },
		},
	}}

	require.PanicsWithValue(t, "finished hook panic", batch.run)
	eventsMu.Lock()
	gotEvents := append([]string(nil), events...)
	eventsMu.Unlock()
	require.Equal(t, []string{
		"first-callback",
		"first-finished",
		"second-callback",
		"second-finished",
	}, gotEvents)
	require.Empty(t, batch.dispatches)
}
