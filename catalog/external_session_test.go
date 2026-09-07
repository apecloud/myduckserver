package catalog

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func newExternalSessionTestPool(
	t *testing.T,
) (*ConnectionPool, *stdsql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	p := NewConnectionPool(nil, db, "memory")
	// UDF registration is DuckDB-specific and orthogonal to these lifecycle
	// tests. Mark it complete so sqlmock sees only the driver calls under test.
	p.registerMySQLUDFsOnce.Do(func() {})
	return p, db, mock
}

func TestExternalSessionExecutionLeaseUsesExactConnection(t *testing.T) {
	p, _, mock := newExternalSessionTestPool(t)
	var initializerCalls atomic.Int32
	p.SetConnectionInitializer(func(_ context.Context, conn *stdsql.Conn) error {
		require.NotNil(t, conn)
		initializerCalls.Add(1)
		return nil
	})

	session, err := p.OpenExternalSession(context.Background())
	require.NoError(t, err)
	require.Equal(t, int32(1), initializerCalls.Load())

	conn, tx, release, err := session.ExecutionLease(context.Background())
	require.NoError(t, err)
	require.Same(t, session.conn, conn)
	require.Nil(t, tx)
	require.Equal(t, int32(2), initializerCalls.Load())
	release()
	release()

	var cleanupCalls atomic.Int32
	require.NoError(t, session.SetCleanupHook(func() { cleanupCalls.Add(1) }))
	mock.ExpectClose()
	require.NoError(t, session.Close())
	require.NoError(t, session.Close())
	require.Equal(t, int32(1), cleanupCalls.Load())

	_, _, _, err = session.ExecutionLease(context.Background())
	require.ErrorIs(t, err, ErrExternalSessionClosed)
	require.Equal(t, int32(2), initializerCalls.Load(), "closed handles must fail before initialization")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExternalSessionAdmissionFailsBeforeDriverWork(t *testing.T) {
	t.Run("Open rechecks after lifecycle admission", func(t *testing.T) {
		p, db, _ := newExternalSessionTestPool(t)
		admissionErr := errors.New("external admission rejected")
		var guardCalls atomic.Int32
		var initializerCalls atomic.Int32
		p.SetConnectionInitializer(func(context.Context, *stdsql.Conn) error {
			initializerCalls.Add(1)
			return nil
		})
		p.SetConnectionAdmissionGuard(func(context.Context) error {
			if guardCalls.Add(1) == 2 {
				return admissionErr
			}
			return nil
		})

		session, err := p.OpenExternalSession(context.Background())
		require.Nil(t, session)
		require.ErrorIs(t, err, admissionErr)
		require.Equal(t, int32(2), guardCalls.Load())
		require.Zero(t, initializerCalls.Load())
		require.Zero(t, db.Stats().InUse)
	})

	t.Run("Lease rejects before initializer", func(t *testing.T) {
		p, _, mock := newExternalSessionTestPool(t)
		var initializerCalls atomic.Int32
		p.SetConnectionInitializer(func(context.Context, *stdsql.Conn) error {
			initializerCalls.Add(1)
			return nil
		})
		session, err := p.OpenExternalSession(context.Background())
		require.NoError(t, err)
		require.Equal(t, int32(1), initializerCalls.Load())

		admissionErr := errors.New("generation is unavailable")
		p.SetConnectionAdmissionGuard(func(context.Context) error { return admissionErr })
		_, _, _, err = session.ExecutionLease(context.Background())
		require.ErrorIs(t, err, admissionErr)
		require.Equal(t, int32(1), initializerCalls.Load())

		p.SetConnectionAdmissionGuard(nil)
		mock.ExpectClose()
		require.NoError(t, session.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestExternalSessionTransactionIdentityAndLifecycleHooks(t *testing.T) {
	p, _, mock := newExternalSessionTestPool(t)
	var admissionCalls atomic.Int32
	var admittedTransactions atomic.Int32
	var releasedAdmissions atomic.Int32
	var finishedCalls atomic.Int32
	var finishedMu sync.Mutex
	var finished []*stdsql.Tx
	p.SetTransactionLifecycleHooks(func() func(*stdsql.Tx) {
		admissionCalls.Add(1)
		return func(tx *stdsql.Tx) {
			if tx == nil {
				releasedAdmissions.Add(1)
				return
			}
			admittedTransactions.Add(1)
		}
	}, func(tx *stdsql.Tx) {
		finishedCalls.Add(1)
		finishedMu.Lock()
		finished = append(finished, tx)
		finishedMu.Unlock()
	})

	session, err := p.OpenExternalSession(context.Background())
	require.NoError(t, err)
	mock.ExpectBegin()
	tx, err := session.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, tx)
	require.Equal(t, int32(1), admissionCalls.Load())
	require.Equal(t, int32(1), admittedTransactions.Load())

	wrong := new(stdsql.Tx)
	require.ErrorIs(t, session.Commit(wrong), ErrExternalSessionTransactionMismatch)
	require.Zero(t, finishedCalls.Load())
	conn, leasedTx, release, err := session.ExecutionLease(context.Background())
	require.NoError(t, err)
	require.Same(t, session.conn, conn)
	require.Same(t, tx, leasedTx)
	release()

	var commitCompletions atomic.Int32
	require.True(t, p.RegisterTransactionCompletion(tx, func(success bool) {
		require.True(t, success)
		commitCompletions.Add(1)
	}))
	mock.ExpectCommit()
	require.NoError(t, session.Commit(tx))
	require.Equal(t, int32(1), commitCompletions.Load())
	require.Equal(t, int32(1), finishedCalls.Load())

	mock.ExpectBegin()
	tx, err = session.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, int32(2), admissionCalls.Load())
	require.Equal(t, int32(2), admittedTransactions.Load())
	require.Zero(t, releasedAdmissions.Load())

	var rollbackCompletions atomic.Int32
	require.True(t, p.RegisterTransactionCompletion(tx, func(success bool) {
		require.False(t, success)
		rollbackCompletions.Add(1)
	}))
	mock.ExpectRollback()
	var cleanupConn *stdsql.Conn
	require.NoError(t, session.Rollback(tx, func(conn *stdsql.Conn) error {
		cleanupConn = conn
		return nil
	}))
	require.Same(t, session.conn, cleanupConn)
	require.Equal(t, int32(1), rollbackCompletions.Load())
	require.Equal(t, int32(2), finishedCalls.Load())

	finishedMu.Lock()
	require.Equal(t, 2, len(finished))
	finishedMu.Unlock()
	mock.ExpectClose()
	require.NoError(t, session.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExternalSessionResetWaitsForLeaseAndInvalidatesHandle(t *testing.T) {
	p, _, oldMock := newExternalSessionTestPool(t)
	var initializerCalls atomic.Int32
	p.SetConnectionInitializer(func(context.Context, *stdsql.Conn) error {
		initializerCalls.Add(1)
		return nil
	})
	session, err := p.OpenExternalSession(context.Background())
	require.NoError(t, err)
	oldGeneration := session.generation
	var cleanupCalls atomic.Int32
	require.NoError(t, session.SetCleanupHook(func() { cleanupCalls.Add(1) }))

	_, _, release, err := session.ExecutionLease(context.Background())
	require.NoError(t, err)
	require.Equal(t, int32(2), initializerCalls.Load())

	newDB, newMock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = newDB.Close() })
	oldMock.ExpectClose()
	resetDone := make(chan error, 1)
	go func() { resetDone <- p.Reset(nil, newDB) }()
	waitForPoolShutdown(t, p)
	select {
	case err := <-resetDone:
		t.Fatalf("Reset completed before the execution lease released: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	release()
	select {
	case err := <-resetDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Reset did not finish after the execution lease released")
	}
	require.Equal(t, int32(1), cleanupCalls.Load())
	require.NoError(t, oldMock.ExpectationsWereMet())

	_, _, _, err = session.ExecutionLease(context.Background())
	require.ErrorIs(t, err, ErrExternalSessionStale)
	require.Equal(t, int32(2), initializerCalls.Load(), "stale handles must fail before initialization")
	var lateCleanupCalls atomic.Int32
	err = session.SetCleanupHook(func() { lateCleanupCalls.Add(1) })
	require.ErrorIs(t, err, ErrExternalSessionStale)
	require.Equal(t, int32(1), lateCleanupCalls.Load())

	// Reset owns a fresh UDF-registration generation. Keep this sqlmock-focused
	// test independent of DuckDB's UDF implementation.
	p.registerMySQLUDFsOnce.Do(func() {})
	newSession, err := p.OpenExternalSession(context.Background())
	require.NoError(t, err)
	require.Equal(t, oldGeneration+1, newSession.generation)
	newMock.ExpectClose()
	require.NoError(t, newSession.Close())
	require.NoError(t, p.Close())
	require.NoError(t, newMock.ExpectationsWereMet())
}

func TestExternalSessionPoolCloseDrainsLeaseThenCleansBeforeRollback(t *testing.T) {
	p, _, mock := newExternalSessionTestPool(t)
	session, err := p.OpenExternalSession(context.Background())
	require.NoError(t, err)
	mock.ExpectBegin()
	tx, err := session.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	conn := session.conn
	var cleanupComplete atomic.Bool
	hookErr := make(chan error, 1)
	require.NoError(t, session.SetCleanupHook(func() {
		err := conn.Raw(func(any) error {
			cleanupComplete.Store(true)
			return nil
		})
		hookErr <- err
	}))
	conn, leasedTx, release, err := session.ExecutionLease(context.Background())
	require.NoError(t, err)
	require.Same(t, tx, leasedTx)
	require.Same(t, session.conn, conn)
	var completionCalls atomic.Int32
	require.True(t, p.RegisterTransactionCompletion(tx, func(success bool) {
		require.False(t, success)
		require.True(t, cleanupComplete.Load(), "cleanup must finish before transaction completion")
		completionCalls.Add(1)
	}))

	mock.ExpectRollback()
	mock.ExpectClose()
	closeDone := make(chan error, 1)
	go func() { closeDone <- p.Close() }()
	waitForPoolShutdown(t, p)
	select {
	case err := <-closeDone:
		t.Fatalf("Close completed before the execution lease released: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	release()

	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not finish after the execution lease released")
	}
	require.NoError(t, <-hookErr)
	require.True(t, cleanupComplete.Load())
	require.Equal(t, int32(1), completionCalls.Load())
	require.NoError(t, mock.ExpectationsWereMet())
	_, _, _, err = session.ExecutionLease(context.Background())
	require.ErrorIs(t, err, ErrExternalSessionStale)
}

func TestExternalSessionCleanupPanicDoesNotSkipLaterSessions(t *testing.T) {
	p, _, mock := newExternalSessionTestPool(t)
	first, err := p.OpenExternalSession(context.Background())
	require.NoError(t, err)
	second, err := p.OpenExternalSession(context.Background())
	require.NoError(t, err)

	var firstCalls atomic.Int32
	var secondCalls atomic.Int32
	require.NoError(t, first.SetCleanupHook(func() {
		firstCalls.Add(1)
		panic("first cleanup failed")
	}))
	require.NoError(t, second.SetCleanupHook(func() { secondCalls.Add(1) }))
	mock.ExpectClose()
	mock.ExpectClose()
	err = p.Close()
	require.ErrorContains(t, err, "first cleanup failed")
	require.Equal(t, int32(1), firstCalls.Load())
	require.Equal(t, int32(1), secondCalls.Load())
	require.NoError(t, mock.ExpectationsWereMet())
	require.Nil(t, first.conn)
	require.Nil(t, second.conn)
}

func TestExternalSessionConcurrentCloseWaitsForCleanup(t *testing.T) {
	p, _, mock := newExternalSessionTestPool(t)
	session, err := p.OpenExternalSession(context.Background())
	require.NoError(t, err)
	hookStarted := make(chan struct{})
	releaseHook := make(chan struct{})
	var hookCalls atomic.Int32
	require.NoError(t, session.SetCleanupHook(func() {
		hookCalls.Add(1)
		close(hookStarted)
		<-releaseHook
	}))
	mock.ExpectClose()

	firstDone := make(chan error, 1)
	go func() { firstDone <- session.Close() }()
	select {
	case <-hookStarted:
	case <-time.After(time.Second):
		t.Fatal("cleanup hook did not start")
	}

	secondDone := make(chan error, 1)
	go func() { secondDone <- session.Close() }()
	select {
	case err := <-secondDone:
		t.Fatalf("concurrent Close returned while cleanup was still running: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseHook)
	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)
	require.Equal(t, int32(1), hookCalls.Load())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExternalSessionTeardownWaitsForCompletionDispatch(t *testing.T) {
	p, _, mock := newExternalSessionTestPool(t)
	session, err := p.OpenExternalSession(context.Background())
	require.NoError(t, err)
	mock.ExpectBegin()
	tx, err := session.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	var callbackStarted atomic.Bool
	callbackRelease := make(chan struct{})
	require.True(t, p.RegisterTransactionCompletion(tx, func(bool) {
		callbackStarted.Store(true)
		<-callbackRelease
	}))
	_, _, leaseRelease, err := session.ExecutionLease(context.Background())
	require.NoError(t, err)
	mock.ExpectRollback()
	mock.ExpectClose()

	closeDone := make(chan error, 1)
	go func() { closeDone <- p.Close() }()
	waitForPoolShutdown(t, p)
	leaseRelease()
	select {
	case <-closeDone:
		t.Fatal("pool Close returned before completion dispatch was released")
	case <-time.After(100 * time.Millisecond):
	}
	// The callback runs after the pool writer unlocks. A concurrent release-side
	// Close must still wait for the teardown completion signal, rather than
	// returning while the terminal callback is in flight.
	secondDone := make(chan error, 1)
	go func() { secondDone <- session.Close() }()
	select {
	case err := <-secondDone:
		t.Fatalf("external Close returned while completion callback was running: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.Eventually(t, callbackStarted.Load, time.Second, time.Millisecond)
	close(callbackRelease)
	require.NoError(t, <-closeDone)
	require.NoError(t, <-secondDone)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExternalSessionInitializerFailureRetiresHandle(t *testing.T) {
	p, _, mock := newExternalSessionTestPool(t)
	initializerErr := errors.New("initializer failed")
	var calls atomic.Int32
	p.SetConnectionInitializer(func(context.Context, *stdsql.Conn) error {
		if calls.Add(1) == 2 {
			return initializerErr
		}
		return nil
	})
	session, err := p.OpenExternalSession(context.Background())
	require.NoError(t, err)
	mock.ExpectClose()
	_, _, _, err = session.ExecutionLease(context.Background())
	require.ErrorIs(t, err, initializerErr)
	require.Equal(t, int32(2), calls.Load())
	_, _, _, err = session.ExecutionLease(context.Background())
	require.ErrorIs(t, err, ErrExternalSessionClosed)
	require.NoError(t, mock.ExpectationsWereMet())
}

type panicBeginConnector struct {
	closes     *atomic.Int32
	closeErr   error
	closePanic bool
}

func (c panicBeginConnector) Connect(context.Context) (driver.Conn, error) {
	return &panicBeginConn{closes: c.closes, closeErr: c.closeErr, closePanic: c.closePanic}, nil
}

func (c panicBeginConnector) Driver() driver.Driver {
	return panicBeginDriver{closes: c.closes, closeErr: c.closeErr, closePanic: c.closePanic}
}

type panicBeginDriver struct {
	closes     *atomic.Int32
	closeErr   error
	closePanic bool
}

func (d panicBeginDriver) Open(string) (driver.Conn, error) {
	return &panicBeginConn{closes: d.closes, closeErr: d.closeErr, closePanic: d.closePanic}, nil
}

type panicBeginConn struct {
	closes     *atomic.Int32
	closeErr   error
	closePanic bool
}

func (*panicBeginConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare unsupported")
}
func (c *panicBeginConn) Close() error {
	if c.closes != nil {
		c.closes.Add(1)
	}
	if c.closePanic {
		panic("close panic")
	}
	return c.closeErr
}
func (*panicBeginConn) Begin() (driver.Tx, error) { panic("begin panic") }
func (*panicBeginConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	panic("begin tx panic")
}

func TestExternalSessionBeginPanicRetiresHandle(t *testing.T) {
	var physicalCloses atomic.Int32
	db := stdsql.OpenDB(panicBeginConnector{closes: &physicalCloses})
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	p := NewConnectionPool(nil, db, "memory")
	p.registerMySQLUDFsOnce.Do(func() {})
	session, err := p.OpenExternalSession(context.Background())
	require.NoError(t, err)
	var cleanupCalls atomic.Int32
	require.NoError(t, session.SetCleanupHook(func() { cleanupCalls.Add(1) }))

	beginDone := make(chan error, 1)
	go func() {
		_, beginErr := session.BeginTx(context.Background(), nil)
		beginDone <- beginErr
	}()
	select {
	case err = <-beginDone:
		require.ErrorContains(t, err, "begin transaction panicked")
	case <-time.After(time.Second):
		t.Fatal("BeginTx did not return after the driver panicked")
	}
	require.Equal(t, int32(1), cleanupCalls.Load())
	require.Eventually(t, func() bool {
		return physicalCloses.Load() == 1
	}, time.Second, time.Millisecond)
	_, _, _, err = session.ExecutionLease(context.Background())
	require.ErrorIs(t, err, ErrExternalSessionGenerationPoisoned)
	_, err = p.OpenExternalSession(context.Background())
	require.ErrorIs(t, err, ErrExternalSessionGenerationPoisoned)

	// database/sql cannot release the Conn read lock lost by its panic path.
	// The exact connection must remain checked out rather than becoming reusable.
	acquireCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = db.Conn(acquireCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	replacementDB, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = replacementDB.Close() })
	resetDone := make(chan error, 1)
	go func() { resetDone <- p.Reset(nil, replacementDB) }()
	select {
	case err = <-resetDone:
		require.ErrorIs(t, err, ErrExternalSessionGenerationPoisoned)
	case <-time.After(time.Second):
		t.Fatal("pool Reset waited for a quarantined external connection")
	}
	require.Same(t, db, p.DB, "Reset must not install a new in-process generation after a driver panic")

	closeDone := make(chan error, 1)
	go func() { closeDone <- p.Close() }()
	select {
	case err = <-closeDone:
		require.ErrorIs(t, err, ErrExternalSessionGenerationPoisoned)
	case <-time.After(time.Second):
		t.Fatal("pool Close waited for a quarantined external connection")
	}
	require.Equal(t, int32(1), physicalCloses.Load())
}

func TestExternalSessionBeginPanicPhysicalCloseFailureIsBounded(t *testing.T) {
	tests := []struct {
		name          string
		closeErr      error
		closePanic    bool
		expectedError string
	}{
		{
			name:          "driver ErrBadConn",
			closeErr:      driver.ErrBadConn,
			expectedError: driver.ErrBadConn.Error(),
		},
		{
			name:          "driver close panic",
			closePanic:    true,
			expectedError: "driver operation panicked: close panic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var physicalCloses atomic.Int32
			db := stdsql.OpenDB(panicBeginConnector{
				closes:     &physicalCloses,
				closeErr:   tt.closeErr,
				closePanic: tt.closePanic,
			})
			t.Cleanup(func() { _ = db.Close() })
			db.SetMaxOpenConns(1)
			p := NewConnectionPool(nil, db, "memory")
			p.registerMySQLUDFsOnce.Do(func() {})
			session, err := p.OpenExternalSession(context.Background())
			require.NoError(t, err)

			beginDone := make(chan error, 1)
			go func() {
				_, beginErr := session.BeginTx(context.Background(), nil)
				beginDone <- beginErr
			}()
			select {
			case err = <-beginDone:
				require.ErrorContains(t, err, "begin transaction panicked")
				require.ErrorContains(t, err, tt.expectedError)
			case <-time.After(time.Second):
				t.Fatal("BeginTx waited for database/sql retirement after driver Close failed")
			}
			require.Equal(t, int32(1), physicalCloses.Load())

			closeDone := make(chan error, 1)
			go func() { closeDone <- p.Close() }()
			select {
			case err = <-closeDone:
				require.ErrorIs(t, err, ErrExternalSessionGenerationPoisoned)
			case <-time.After(time.Second):
				t.Fatal("pool Close waited after driver Close failed")
			}
			require.Equal(t, int32(1), physicalCloses.Load())
		})
	}
}

func TestExternalSessionBeginPanicPoisonsBeforeCleanupCompletes(t *testing.T) {
	var physicalCloses atomic.Int32
	db := stdsql.OpenDB(panicBeginConnector{closes: &physicalCloses})
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	p := NewConnectionPool(nil, db, "memory")
	p.registerMySQLUDFsOnce.Do(func() {})
	session, err := p.OpenExternalSession(context.Background())
	require.NoError(t, err)

	hookStarted := make(chan struct{})
	releaseHook := make(chan struct{})
	require.NoError(t, session.SetCleanupHook(func() {
		close(hookStarted)
		<-releaseHook
	}))
	beginDone := make(chan error, 1)
	go func() {
		_, beginErr := session.BeginTx(context.Background(), nil)
		beginDone <- beginErr
	}()
	select {
	case <-hookStarted:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not start after the BeginTx driver panic")
	}

	openCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = p.OpenExternalSession(openCtx)
	require.ErrorIs(t, err, ErrExternalSessionGenerationPoisoned)
	close(releaseHook)
	select {
	case err = <-beginDone:
		require.ErrorContains(t, err, "begin transaction panicked")
	case <-time.After(time.Second):
		t.Fatal("BeginTx did not finish after cleanup was released")
	}
	require.Equal(t, int32(1), physicalCloses.Load())
	require.ErrorIs(t, p.Close(), ErrExternalSessionGenerationPoisoned)
}

type panicTransactionConnector struct {
	operation string
	closes    *atomic.Int32
}

func (c panicTransactionConnector) Connect(context.Context) (driver.Conn, error) {
	return &panicTransactionConn{operation: c.operation, closes: c.closes}, nil
}

func (c panicTransactionConnector) Driver() driver.Driver {
	return panicTransactionDriver{operation: c.operation, closes: c.closes}
}

type panicTransactionDriver struct {
	operation string
	closes    *atomic.Int32
}

func (d panicTransactionDriver) Open(string) (driver.Conn, error) {
	return &panicTransactionConn{operation: d.operation, closes: d.closes}, nil
}

type panicTransactionConn struct {
	operation string
	closes    *atomic.Int32
}

func (*panicTransactionConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare unsupported")
}

func (c *panicTransactionConn) Close() error {
	if c.closes != nil {
		c.closes.Add(1)
	}
	return nil
}

func (c *panicTransactionConn) Begin() (driver.Tx, error) {
	return &panicTransactionTx{operation: c.operation}, nil
}

func (c *panicTransactionConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return &panicTransactionTx{operation: c.operation}, nil
}

type panicTransactionTx struct {
	operation string
}

func (tx *panicTransactionTx) Commit() error {
	if tx.operation == "commit" {
		panic("commit panic")
	}
	return nil
}

func (tx *panicTransactionTx) Rollback() error {
	if tx.operation == "rollback" {
		panic("rollback panic")
	}
	return nil
}

func TestExternalSessionTransactionPanicQuarantinesConnection(t *testing.T) {
	tests := []struct {
		name          string
		driverPanic   string
		terminalError string
		terminal      func(*ExternalSession, *stdsql.Tx) error
	}{
		{
			name:          "commit",
			driverPanic:   "commit",
			terminalError: "commit transaction panicked",
			terminal:      func(session *ExternalSession, tx *stdsql.Tx) error { return session.Commit(tx) },
		},
		{
			name:          "rollback",
			driverPanic:   "rollback",
			terminalError: "rollback transaction panicked",
			terminal: func(session *ExternalSession, tx *stdsql.Tx) error {
				return session.Rollback(tx, nil)
			},
		},
		{
			name:          "close with active transaction",
			driverPanic:   "rollback",
			terminalError: "rollback transaction panicked",
			terminal:      func(session *ExternalSession, _ *stdsql.Tx) error { return session.Close() },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var physicalCloses atomic.Int32
			db := stdsql.OpenDB(panicTransactionConnector{
				operation: tt.driverPanic,
				closes:    &physicalCloses,
			})
			t.Cleanup(func() { _ = db.Close() })
			db.SetMaxOpenConns(1)
			p := NewConnectionPool(nil, db, "memory")
			p.registerMySQLUDFsOnce.Do(func() {})

			var finishedCalls atomic.Int32
			p.SetTransactionLifecycleHooks(nil, func(*stdsql.Tx) { finishedCalls.Add(1) })
			session, err := p.OpenExternalSession(context.Background())
			require.NoError(t, err)
			var cleanupCalls atomic.Int32
			require.NoError(t, session.SetCleanupHook(func() { cleanupCalls.Add(1) }))
			tx, err := session.BeginTx(context.Background(), nil)
			require.NoError(t, err)
			var completionCalls atomic.Int32
			require.True(t, p.RegisterTransactionCompletion(tx, func(success bool) {
				require.False(t, success)
				completionCalls.Add(1)
			}))

			terminalDone := make(chan error, 1)
			go func() { terminalDone <- tt.terminal(session, tx) }()
			select {
			case err = <-terminalDone:
				require.ErrorContains(t, err, tt.terminalError)
			case <-time.After(time.Second):
				t.Fatal("transaction terminal operation did not return after the driver panicked")
			}
			require.Equal(t, int32(1), cleanupCalls.Load())
			require.Equal(t, int32(1), completionCalls.Load())
			require.Equal(t, int32(1), finishedCalls.Load())
			require.Eventually(t, func() bool {
				return physicalCloses.Load() == 1
			}, time.Second, time.Millisecond)

			_, _, _, err = session.ExecutionLease(context.Background())
			require.ErrorIs(t, err, ErrExternalSessionGenerationPoisoned)
			acquireCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			_, err = db.Conn(acquireCtx)
			require.ErrorIs(t, err, context.DeadlineExceeded)

			closeDone := make(chan error, 1)
			go func() { closeDone <- p.Close() }()
			select {
			case err = <-closeDone:
				require.ErrorIs(t, err, ErrExternalSessionGenerationPoisoned)
			case <-time.After(time.Second):
				t.Fatal("pool Close waited for a quarantined external connection")
			}
			require.Equal(t, int32(1), physicalCloses.Load())
		})
	}
}

func TestExternalSessionSetCleanupHookAdmittedBeforeShutdownWriter(t *testing.T) {
	p, _, mock := newExternalSessionTestPool(t)
	session, err := p.OpenExternalSession(context.Background())
	require.NoError(t, err)
	_, _, release, err := session.ExecutionLease(context.Background())
	require.NoError(t, err)
	closeDone := make(chan error, 1)
	go func() { closeDone <- p.Close() }()
	waitForPoolShutdown(t, p)
	var hookCalls atomic.Int32
	setDone := make(chan error, 1)
	go func() {
		setDone <- session.SetCleanupHook(func() { hookCalls.Add(1) })
	}()
	select {
	case err := <-setDone:
		t.Fatalf("cleanup hook registration unexpectedly completed while lease was held: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	mock.ExpectClose()
	release()
	require.NoError(t, <-setDone)
	require.NoError(t, <-closeDone)
	require.Equal(t, int32(1), hookCalls.Load())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExternalSessionUnexpectedRollbackErrorRetiresHandle(t *testing.T) {
	p, _, mock := newExternalSessionTestPool(t)
	var finishedCalls atomic.Int32
	p.SetTransactionLifecycleHooks(nil, func(*stdsql.Tx) { finishedCalls.Add(1) })
	session, err := p.OpenExternalSession(context.Background())
	require.NoError(t, err)
	var cleanupCalls atomic.Int32
	require.NoError(t, session.SetCleanupHook(func() { cleanupCalls.Add(1) }))
	mock.ExpectBegin()
	tx, err := session.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	rollbackErr := errors.New("rollback state is unknown")
	mock.ExpectRollback().WillReturnError(rollbackErr)
	mock.ExpectClose()
	err = session.Rollback(tx, nil)
	require.ErrorIs(t, err, rollbackErr)
	require.Equal(t, int32(1), cleanupCalls.Load())
	require.Equal(t, int32(1), finishedCalls.Load())
	require.NoError(t, mock.ExpectationsWereMet())

	_, _, _, err = session.ExecutionLease(context.Background())
	require.ErrorIs(t, err, ErrExternalSessionClosed)
}
