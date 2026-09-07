package catalog

import (
	"context"
	stdsql "database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/apecloud/myduckserver/configuration"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/stretchr/testify/require"
)

// newMalformedGenerationProvider creates a real provider/pool pairing and then
// corrupts only the logical transaction marker. The sql.Tx and its physical
// owner remain live, which is the state pool teardown must preserve.
func newMalformedGenerationProvider(t *testing.T, sessionID uint32) (*DatabaseProvider, *ConnectionPool, *stdsql.Tx, *stdsql.Conn, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	p := NewConnectionPool(nil, db, "memory")
	p.registerMySQLUDFsOnce.Do(func() {})
	provider := &DatabaseProvider{
		mu:      &sync.RWMutex{},
		pool:    p,
		storage: db,
		duckLake: &duckLakeRuntime{config: configuration.DuckLakeConfig{
			MetadataPath: "/tmp/task77/catalog.ducklake",
			DataPath:     "/tmp/task77/data",
		}},
	}
	p.SetTransactionLifecycleHooksWithError(provider.beginDuckLakeTransactionWithError, provider.untrackDuckLakeTransaction)
	p.SetTransactionAdmissionGuard(provider.duckLakeGenerationPoisonError)
	p.SetConnectionAdmissionGuard(provider.duckLakeConnectionAdmissionError)

	mock.ExpectBegin()
	tx, err := p.GetTxn(context.Background(), sessionID, "", nil)
	require.NoError(t, err)
	ownerEntry, ok := p.txnConns.Load(sessionID)
	require.True(t, ok)
	owner, ok := ownerEntry.(*stdsql.Conn)
	require.True(t, ok)
	require.NotNil(t, owner)

	p.txns.Store(sessionID, "malformed transaction marker")
	t.Cleanup(func() {
		// A failed teardown intentionally leaves the owner available for an
		// explicit recovery. Keep cleanup bounded even when an assertion above
		// aborts the test before the normal repair below.
		p.txns.Store(sessionID, tx)
		_ = tx.Rollback()
		p.CloseTxn(sessionID)
		_ = owner.Close()
		_ = db.Close()
	})
	return provider, p, tx, owner, mock
}

func newPoisonedAdmissionProvider(t *testing.T) (*DatabaseProvider, *ConnectionPool, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	p := NewConnectionPool(nil, db, "memory")
	provider := &DatabaseProvider{
		mu:      &sync.RWMutex{},
		pool:    p,
		storage: db,
		duckLake: &duckLakeRuntime{config: configuration.DuckLakeConfig{
			MetadataPath: "/tmp/task77/catalog.ducklake",
			DataPath:     "/tmp/task77/data",
		}},
	}
	p.SetTransactionLifecycleHooksWithError(provider.beginDuckLakeTransactionWithError, provider.untrackDuckLakeTransaction)
	p.SetTransactionAdmissionGuard(provider.duckLakeGenerationPoisonError)
	p.SetConnectionAdmissionGuard(provider.duckLakeConnectionAdmissionError)
	provider.poisonDuckLakeGeneration(errors.New("malformed generation for admission test"))
	t.Cleanup(func() { _ = db.Close() })
	return provider, p, mock
}

func TestDuckLakeProviderCloseMalformedTransactionReturnsBoundedError(t *testing.T) {
	const sessionID = uint32(184)
	provider, pool, tx, owner, mock := newMalformedGenerationProvider(t, sessionID)
	operationRelease := provider.BeginDuckLakeOperationForOwner(
		mycontext.WithFrontendQuery(context.Background()), tx,
	)
	t.Cleanup(operationRelease)

	closeDone := make(chan error, 1)
	go func() { closeDone <- provider.Close() }()
	select {
	case closeErr := <-closeDone:
		require.ErrorContains(t, closeErr, "active transaction mapping is invalid")
	case <-time.After(time.Second):
		t.Fatal("provider Close hung on a malformed transaction mapping")
	}

	// The unknown physical owner and malformed marker are deliberately retained;
	// closing either would risk blocking database/sql on an untyped live owner.
	currentOwner, ok := pool.txnConns.Load(sessionID)
	require.True(t, ok)
	require.Same(t, owner, currentOwner)
	currentMarker, ok := pool.txns.Load(sessionID)
	require.True(t, ok)
	require.Equal(t, "malformed transaction marker", currentMarker)
	require.Equal(t, 1, duckLakeOperationCount(provider))
	require.ErrorIs(t, provider.duckLakeGenerationPoisonError(), errDuckLakeGenerationPoisoned)

	// Repair the marker and finish the owner explicitly so sqlmock can verify
	// that no implicit/unknown-owner rollback was attempted by provider.Close.
	pool.txns.Store(sessionID, tx)
	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	pool.CloseTxn(sessionID)
	operationRelease()
	require.Zero(t, duckLakeOperationCount(provider))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDuckLakeProviderRestartMalformedTransactionReturnsBoundedError(t *testing.T) {
	const sessionID = uint32(185)
	provider, pool, tx, owner, mock := newMalformedGenerationProvider(t, sessionID)
	operationRelease := provider.BeginDuckLakeOperationForOwner(
		mycontext.WithFrontendQuery(context.Background()), tx,
	)
	t.Cleanup(operationRelease)

	restartDone := make(chan error, 1)
	go func() { restartDone <- provider.Restart(true) }()
	select {
	case restartErr := <-restartDone:
		require.ErrorContains(t, restartErr, "active transaction mapping is invalid")
	case <-time.After(time.Second):
		t.Fatal("provider Restart hung on a malformed transaction mapping")
	}

	currentOwner, ok := pool.txnConns.Load(sessionID)
	require.True(t, ok)
	require.Same(t, owner, currentOwner)
	currentMarker, ok := pool.txns.Load(sessionID)
	require.True(t, ok)
	require.Equal(t, "malformed transaction marker", currentMarker)
	require.Equal(t, 1, duckLakeOperationCount(provider))
	require.ErrorIs(t, provider.duckLakeGenerationPoisonError(), errDuckLakeGenerationPoisoned)

	pool.txns.Store(sessionID, tx)
	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	pool.CloseTxn(sessionID)
	operationRelease()
	require.Zero(t, duckLakeOperationCount(provider))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDuckLakePoisonedGenerationRejectsNewPhysicalTransaction(t *testing.T) {
	provider, pool, mock := newPoisonedAdmissionProvider(t)

	_, err := pool.GetTxn(context.Background(), 771, "", nil)
	require.ErrorIs(t, err, errDuckLakeGenerationPoisoned)
	_, mapped := pool.txns.Load(uint32(771))
	require.False(t, mapped, "poisoned generation must not create a transaction mapping")
	require.NoError(t, mock.ExpectationsWereMet())
	_ = provider
}

func TestDuckLakePoisonedGenerationRejectsConnectionAndSnapshotAdmission(t *testing.T) {
	provider, pool, mock := newPoisonedAdmissionProvider(t)
	var initializerCalls atomic.Int32
	pool.SetConnectionInitializer(func(context.Context, *stdsql.Conn) error {
		initializerCalls.Add(1)
		return nil
	})
	ctx := mycontext.WithFrontendQuery(context.Background())

	conn, err := pool.GetConn(ctx, 772)
	require.Nil(t, conn)
	require.ErrorIs(t, err, errDuckLakeGenerationPoisoned)
	_, mapped := pool.conns.Load(uint32(772))
	require.False(t, mapped, "poisoned connection admission must not install a mapping")

	snapshotConn, snapshotTx, release, err := pool.GetExecutionSnapshotLease(ctx, 773, "", false)
	require.Nil(t, snapshotConn)
	require.Nil(t, snapshotTx)
	require.NotNil(t, release)
	release()
	require.ErrorIs(t, err, errDuckLakeGenerationPoisoned)
	_, mapped = pool.conns.Load(uint32(773))
	require.False(t, mapped, "poisoned snapshot admission must not install a mapping")

	require.Empty(t, pool.CurrentSchema(772))
	require.Empty(t, pool.CurrentCatalog(772))
	require.Zero(t, initializerCalls.Load(), "poison must be reported before connection initialization")
	require.NoError(t, mock.ExpectationsWereMet())
	_ = provider
}

func TestDuckLakePoisonedGenerationRejectsDirectConnectionInitialization(t *testing.T) {
	provider, pool, mock := newPoisonedAdmissionProvider(t)
	conn, err := pool.DB.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	err = provider.InitializeConnection(mycontext.WithFrontendQuery(context.Background()), conn)
	require.ErrorIs(t, err, errDuckLakeGenerationPoisoned)
	require.ErrorIs(
		t,
		provider.duckLakeConnectionAdmissionError(mycontext.WithRecoveryQuery(context.Background())),
		errDuckLakeGenerationPoisoned,
	)
	require.ErrorIs(
		t,
		provider.duckLakeConnectionAdmissionError(
			WithDuckLakeCleanupLease(mycontext.WithMaintenanceQuery(context.Background())),
		),
		errDuckLakeGenerationPoisoned,
	)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDuckLakeGenerationTransitionConnectionAdmission(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	pool := NewConnectionPool(nil, db, "memory")
	pool.registerMySQLUDFsOnce.Do(func() {})
	provider := &DatabaseProvider{
		mu:      &sync.RWMutex{},
		pool:    pool,
		storage: db,
		duckLake: &duckLakeRuntime{config: configuration.DuckLakeConfig{
			MetadataPath: "/tmp/task77/catalog.ducklake",
			DataPath:     "/tmp/task77/data",
		}},
	}
	pool.SetConnectionAdmissionGuard(provider.duckLakeConnectionAdmissionError)
	var initializerCalls atomic.Int32
	pool.SetConnectionInitializer(func(context.Context, *stdsql.Conn) error {
		initializerCalls.Add(1)
		return nil
	})

	provider.duckLakeTxnMu.Lock()
	provider.ensureDuckLakeTxnStateLocked()
	provider.duckLakeCleanupActive = true
	provider.duckLakeTxnMu.Unlock()
	defer func() {
		provider.duckLakeTxnMu.Lock()
		provider.duckLakeCleanupActive = false
		provider.duckLakeTxnCond.Broadcast()
		provider.duckLakeTxnMu.Unlock()
		_ = db.Close()
	}()

	frontendCtx := mycontext.WithFrontendQuery(context.Background())
	maintenanceCtx := mycontext.WithMaintenanceQuery(context.Background())
	recoveryCtx := mycontext.WithRecoveryQuery(context.Background())
	cleanupCtx := WithDuckLakeCleanupLease(maintenanceCtx)

	require.ErrorIs(t, provider.duckLakeConnectionAdmissionError(frontendCtx), errDuckLakeGenerationTransition)
	require.ErrorIs(t, provider.duckLakeConnectionAdmissionError(maintenanceCtx), errDuckLakeGenerationTransition)
	require.ErrorIs(
		t,
		provider.duckLakeConnectionAdmissionError(WithDuckLakeCleanupLease(frontendCtx)),
		errDuckLakeGenerationTransition,
	)
	for _, origin := range []mycontext.QueryOriginKind{
		mycontext.UnknownQueryOrigin,
		mycontext.InternalQueryOrigin,
		mycontext.MySQLReplicationQueryOrigin,
		mycontext.PostgresReplicationQueryOrigin,
	} {
		require.ErrorIs(
			t,
			provider.duckLakeConnectionAdmissionError(mycontext.WithQueryOrigin(context.Background(), origin)),
			errDuckLakeGenerationTransition,
		)
	}
	require.NoError(t, provider.duckLakeConnectionAdmissionError(recoveryCtx))
	require.NoError(t, provider.duckLakeConnectionAdmissionError(cleanupCtx))

	conn, err := pool.GetConn(frontendCtx, 774)
	require.Nil(t, conn)
	require.ErrorIs(t, err, errDuckLakeGenerationTransition)
	_, mapped := pool.conns.Load(uint32(774))
	require.False(t, mapped)

	snapshotConn, snapshotTx, release, err := pool.GetExecutionSnapshotLease(frontendCtx, 775, "", false)
	require.Nil(t, snapshotConn)
	require.Nil(t, snapshotTx)
	require.NotNil(t, release)
	release()
	require.ErrorIs(t, err, errDuckLakeGenerationTransition)
	_, mapped = pool.conns.Load(uint32(775))
	require.False(t, mapped)

	recoveryConn, err := pool.GetConn(recoveryCtx, 776)
	require.NoError(t, err)
	require.NotNil(t, recoveryConn)

	cleanupConn, err := pool.GetConn(cleanupCtx, 776)
	require.NoError(t, err)
	require.Same(t, recoveryConn, cleanupConn)

	require.ErrorIs(
		t,
		provider.InitializeConnection(frontendCtx, recoveryConn),
		errDuckLakeGenerationTransition,
	)
	require.ErrorIs(
		t,
		provider.EnsureDuckLakeConnectionWithBinding(frontendCtx, recoveryConn, nil),
		errDuckLakeGenerationTransition,
	)
	require.ErrorIs(
		t,
		provider.EnsureDuckLakeConnectionWithBinding(frontendCtx, recoveryConn, new(stdsql.Tx)),
		errDuckLakeGenerationTransition,
		"a transaction pointer is not an admission token during transition",
	)
	require.Empty(t, pool.CurrentSchema(774))
	require.Empty(t, pool.CurrentCatalog(774))
	require.Equal(t, int32(2), initializerCalls.Load())
	require.NoError(t, pool.CloseConn(776))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConnectionAdmissionGuardRechecksAfterLifecycleAdmission(t *testing.T) {
	testAdmission := func(t *testing.T, acquire func(*ConnectionPool) error) {
		t.Helper()
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		pool := NewConnectionPool(nil, db, "memory")
		pool.registerMySQLUDFsOnce.Do(func() {})
		var guardCalls atomic.Int32
		var initializerCalls atomic.Int32
		pool.SetConnectionAdmissionGuard(func(context.Context) error {
			if guardCalls.Add(1) == 2 {
				return errDuckLakeGenerationTransition
			}
			return nil
		})
		pool.SetConnectionInitializer(func(context.Context, *stdsql.Conn) error {
			initializerCalls.Add(1)
			return nil
		})

		require.ErrorIs(t, acquire(pool), errDuckLakeGenerationTransition)
		require.Equal(t, int32(2), guardCalls.Load())
		require.Zero(t, initializerCalls.Load())
		_, mapped := pool.conns.Load(uint32(778))
		require.False(t, mapped)
		require.NoError(t, mock.ExpectationsWereMet())
		_ = db.Close()
	}

	t.Run("connection", func(t *testing.T) {
		testAdmission(t, func(pool *ConnectionPool) error {
			conn, err := pool.GetConn(mycontext.WithFrontendQuery(context.Background()), 778)
			require.Nil(t, conn)
			return err
		})
	})
	t.Run("execution snapshot", func(t *testing.T) {
		testAdmission(t, func(pool *ConnectionPool) error {
			conn, tx, release, err := pool.GetExecutionSnapshotLease(
				mycontext.WithFrontendQuery(context.Background()), 778, "", false,
			)
			require.Nil(t, conn)
			require.Nil(t, tx)
			require.NotNil(t, release)
			release()
			return err
		})
	})
}

func TestDuckLakeGenerationTransitionRejectsActiveTransactionExecutionButAllowsReleaseLookup(t *testing.T) {
	pool := NewConnectionPool(nil, nil, "memory")
	provider := &DatabaseProvider{duckLake: &duckLakeRuntime{config: configuration.DuckLakeConfig{
		MetadataPath: "/tmp/task77/catalog.ducklake",
		DataPath:     "/tmp/task77/data",
	}}}
	pool.SetConnectionAdmissionGuard(provider.duckLakeConnectionAdmissionError)

	const sessionID uint32 = 779
	conn := new(stdsql.Conn)
	tx := new(stdsql.Tx)
	pool.conns.Store(sessionID, conn)
	pool.txns.Store(sessionID, tx)
	pool.txnConns.Store(sessionID, conn)

	provider.duckLakeTxnMu.Lock()
	provider.ensureDuckLakeTxnStateLocked()
	provider.duckLakeCleanupActive = true
	provider.duckLakeTxnMu.Unlock()
	defer func() {
		provider.duckLakeTxnMu.Lock()
		provider.duckLakeCleanupActive = false
		provider.duckLakeTxnCond.Broadcast()
		provider.duckLakeTxnMu.Unlock()
	}()

	gotConn, gotTx, err := pool.GetTxnWithBinding(
		mycontext.WithFrontendQuery(context.Background()),
		sessionID,
		"",
		nil,
	)
	require.Nil(t, gotConn)
	require.Nil(t, gotTx)
	require.ErrorIs(t, err, errDuckLakeGenerationTransition)

	releaseConn, releaseTx := pool.GetTxnBindingForRelease(sessionID)
	require.Same(t, conn, releaseConn)
	require.Same(t, tx, releaseTx)
}

func TestActiveTransactionAdmissionGuardRechecksAfterLifecycleAdmission(t *testing.T) {
	pool := NewConnectionPool(nil, nil, "memory")
	const sessionID uint32 = 780
	conn := new(stdsql.Conn)
	tx := new(stdsql.Tx)
	pool.conns.Store(sessionID, conn)
	pool.txns.Store(sessionID, tx)
	pool.txnConns.Store(sessionID, conn)

	var guardCalls atomic.Int32
	pool.SetConnectionAdmissionGuard(func(context.Context) error {
		if guardCalls.Add(1) == 2 {
			return errDuckLakeGenerationTransition
		}
		return nil
	})

	gotConn, gotTx, err := pool.GetTxnWithBinding(context.Background(), sessionID, "", nil)
	require.Nil(t, gotConn)
	require.Nil(t, gotTx)
	require.ErrorIs(t, err, errDuckLakeGenerationTransition)
	require.Equal(t, int32(2), guardCalls.Load())

	releaseConn, releaseTx := pool.GetTxnBindingForRelease(sessionID)
	require.Same(t, conn, releaseConn)
	require.Same(t, tx, releaseTx)
}

func TestDuckLakePoisonedGenerationRejectsOperationAndCleanup(t *testing.T) {
	provider, pool, mock := newPoisonedAdmissionProvider(t)

	release, err := provider.BeginDuckLakeOperationForOwnerWithError(
		mycontext.WithFrontendQuery(context.Background()), "owner",
	)
	require.ErrorIs(t, err, errDuckLakeGenerationPoisoned)
	release()
	require.Zero(t, duckLakeOperationCount(provider))

	conn, err := pool.DB.Conn(context.Background())
	require.NoError(t, err)
	cleanupErr := provider.CleanupDuckLakeOrphansOnConn(
		mycontext.WithMaintenanceQuery(context.Background()), conn,
	)
	require.ErrorIs(t, cleanupErr, errDuckLakeGenerationPoisoned)
	require.NoError(t, conn.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDuckLakePoisonedGenerationRestartFailsClosed(t *testing.T) {
	provider, pool, mock := newPoisonedAdmissionProvider(t)

	restartDone := make(chan error, 1)
	go func() { restartDone <- provider.Restart(true) }()
	select {
	case restartErr := <-restartDone:
		require.ErrorIs(t, restartErr, errDuckLakeGenerationPoisoned)
	case <-time.After(time.Second):
		t.Fatal("Restart hung while generation was poisoned")
	}
	var transactionMappings int
	pool.txns.Range(func(_, _ any) bool {
		transactionMappings++
		return true
	})
	require.Zero(t, transactionMappings)
	require.NoError(t, mock.ExpectationsWereMet())
}
