package catalog

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/apecloud/myduckserver/testutil"
	gmsql "github.com/dolthub/go-mysql-server/sql"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

func TestConnectionPoolRegistersMySQLUDFsOnce(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	pool := NewConnectionPool(connector, db, "memory")
	activeConnector := connector
	t.Cleanup(func() {
		require.NoError(t, pool.Close())
		require.NoError(t, activeConnector.Close())
	})

	conn1, err := pool.GetConn(context.Background(), 1)
	require.NoError(t, err)
	assertMySQLUDFsCallable(t, conn1)

	// Keep conn1 checked out so database/sql must open another physical
	// connection. DuckDB scalar UDFs belong to the database, not a connection.
	conn2, err := pool.GetConn(context.Background(), 2)
	require.NoError(t, err)
	assertMySQLUDFsCallable(t, conn2)
	require.NoError(t, pool.CloseConn(1))
	require.NoError(t, pool.CloseConn(2))

	conn3, err := pool.GetConn(context.Background(), 3)
	require.NoError(t, err)
	assertMySQLUDFsCallable(t, conn3)

	connector2, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	require.NoError(t, pool.Reset(connector2, stdsql.OpenDB(connector2)))
	require.NoError(t, connector.Close())
	activeConnector = connector2

	conn3, err = pool.GetConn(context.Background(), 3)
	require.NoError(t, err)
	assertMySQLUDFsCallable(t, conn3)
}

func TestConnectionPoolCurrentCatalogBeforeConnection(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	pool := NewConnectionPool(connector, stdsql.OpenDB(connector), "memory")
	t.Cleanup(func() {
		require.NoError(t, pool.Close())
		require.NoError(t, connector.Close())
	})

	// Newer GMS resolves a fully qualified database before the session needs a
	// DuckDB connection. Keep that first catalog lookup anchored to the provider.
	require.Equal(t, "memory", pool.CurrentCatalog(42))

	_, err = pool.GetConn(context.Background(), 42)
	require.NoError(t, err)
	require.Equal(t, "memory", pool.CurrentCatalog(42))

	conn, err := pool.GetConn(context.Background(), 42)
	require.NoError(t, err)
	_, err = conn.ExecContext(context.Background(), "ATTACH ':memory:' AS selected; USE selected")
	require.NoError(t, err)
	require.Equal(t, "selected", pool.CurrentCatalog(42))

	require.NoError(t, pool.CloseConn(42))
	require.Empty(t, pool.CurrentCatalog(42), "closed sessions must not look uninitialized")

	broken, err := pool.GetConn(context.Background(), 43)
	require.NoError(t, err)
	require.ErrorIs(t, broken.Raw(func(any) error { return driver.ErrBadConn }), driver.ErrBadConn)
	require.Empty(t, pool.CurrentCatalog(43), "broken connections must not use the default fallback")
}

func TestDatabaseProviderDefaultTimeZoneAppliesToEveryConnection(t *testing.T) {
	prov, err := NewDBProvider("UTC", t.TempDir(), "myduck")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, prov.Close())
	})

	ctx := context.Background()
	conn1, err := prov.Storage().Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn1.Close())
	})
	conn2, err := prov.Storage().Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn2.Close())
	})

	for _, conn := range []*stdsql.Conn{conn1, conn2} {
		var timeZone string
		require.NoError(t, conn.QueryRowContext(ctx, "SELECT current_setting('TimeZone')").Scan(&timeZone))
		require.Equal(t, "UTC", timeZone)
	}
}

func TestTransactionOutlivesRequestContext(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	pool := NewConnectionPool(connector, db, "memory")
	t.Cleanup(func() {
		require.NoError(t, pool.Close())
		require.NoError(t, connector.Close())
	})

	requestCtx, cancel := context.WithCancel(context.Background())
	tx, err := pool.GetTxn(requestCtx, 1, "", nil)
	require.NoError(t, err)
	cancel()

	// MySQL autocommit=0 transactions span multiple protocol requests. Ending
	// one request must not roll back the session transaction underneath it.
	require.NoError(t, tx.Commit())
	pool.CloseTxn(1)

	conn, err := pool.GetConn(context.Background(), 1)
	require.NoError(t, err)
	var got int
	require.NoError(t, conn.QueryRowContext(context.Background(), "SELECT 1").Scan(&got))
	require.Equal(t, 1, got)
}

func TestConnectionPoolInitializerSkipsActiveTransaction(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	pool := NewConnectionPool(connector, db, "memory")
	t.Cleanup(func() {
		require.NoError(t, pool.Close())
		require.NoError(t, connector.Close())
	})

	var origins []mycontext.QueryOriginKind
	pool.SetConnectionInitializer(func(ctx context.Context, _ *stdsql.Conn) error {
		origins = append(origins, mycontext.QueryOrigin(ctx))
		return nil
	})
	frontendCtx := mycontext.WithFrontendQuery(context.Background())

	// A fresh acquisition and a reuse without a transaction both run the
	// initializer, so a changed origin can be handled at the next boundary.
	_, err = pool.GetConn(frontendCtx, 1)
	require.NoError(t, err)
	_, err = pool.GetConn(frontendCtx, 1)
	require.NoError(t, err)
	require.Equal(t, []mycontext.QueryOriginKind{
		mycontext.FrontendQueryOrigin,
		mycontext.FrontendQueryOrigin,
	}, origins)

	// GetTxn must initialize before BeginTx, but subsequent acquisitions keep
	// transaction-scoped connection state stable until the transaction closes.
	tx, err := pool.GetTxn(frontendCtx, 1, "", nil)
	require.NoError(t, err)
	_, err = pool.GetConn(mycontext.WithQueryOrigin(context.Background(), mycontext.MySQLReplicationQueryOrigin), 1)
	require.NoError(t, err)
	require.Len(t, origins, 3)
	require.Equal(t, mycontext.FrontendQueryOrigin, origins[2])
	require.NoError(t, tx.Rollback())
	pool.CloseTxn(1)

	// Once the transaction is gone, the next origin is observed normally.
	_, err = pool.GetConn(mycontext.WithQueryOrigin(context.Background(), mycontext.MySQLReplicationQueryOrigin), 1)
	require.NoError(t, err)
	require.Equal(t, []mycontext.QueryOriginKind{
		mycontext.FrontendQueryOrigin,
		mycontext.FrontendQueryOrigin,
		mycontext.FrontendQueryOrigin,
		mycontext.MySQLReplicationQueryOrigin,
	}, origins)
}

func TestGetConnForSchemaDoesNotSwitchActiveTransaction(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	pool := NewConnectionPool(connector, db, "memory")
	t.Cleanup(func() {
		require.NoError(t, pool.Close())
		require.NoError(t, connector.Close())
	})

	conn, err := pool.GetConn(context.Background(), 77)
	require.NoError(t, err)
	tx, err := pool.GetTxn(context.Background(), 77, "", nil)
	require.NoError(t, err)

	// The requested schema does not exist. While the transaction owns the
	// connection, GetConnForSchema must return it without issuing USE through
	// the separate *sql.Conn surface.
	got, err := pool.GetConnForSchema(context.Background(), 77, "schema_that_does_not_exist")
	require.NoError(t, err)
	require.Same(t, conn, got)
	require.NoError(t, tx.Rollback())
	pool.CloseTxn(77)
}

func TestCloseConnRollsBackTransaction(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	pool := NewConnectionPool(connector, db, "memory")
	t.Cleanup(func() {
		require.NoError(t, pool.Close())
		require.NoError(t, connector.Close())
	})

	conn, err := pool.GetConn(context.Background(), 1)
	require.NoError(t, err)
	_, err = conn.ExecContext(context.Background(), "CREATE TABLE close_rollback (id INTEGER)")
	require.NoError(t, err)
	tx, err := pool.GetTxn(context.Background(), 1, "", nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(context.Background(), "INSERT INTO close_rollback VALUES (1)")
	require.NoError(t, err)

	require.NoError(t, pool.CloseConn(1))
	require.Nil(t, pool.TryGetTxn(1))

	conn, err = pool.GetConn(context.Background(), 2)
	require.NoError(t, err)
	var count int
	require.NoError(t, conn.QueryRowContext(context.Background(), "SELECT count(*) FROM close_rollback").Scan(&count))
	require.Zero(t, count)
}

func TestConnectionPoolStaleFinalizerCannotCommitReplacement(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	pool := NewConnectionPool(connector, db, "memory")
	t.Cleanup(func() {
		require.NoError(t, pool.Close())
		require.NoError(t, connector.Close())
	})

	first, err := pool.GetTxn(context.Background(), 91, "", nil)
	require.NoError(t, err)
	require.NoError(t, first.Rollback())
	pool.CloseTxn(91)
	second, err := pool.GetTxn(context.Background(), 91, "", nil)
	require.NoError(t, err)

	// A delayed callback for the old transaction must not finalize the new one.
	require.NoError(t, pool.CommitTxn(91, first))
	require.Same(t, second, pool.TryGetTxn(91))
	require.NoError(t, second.Rollback())
	pool.CloseTxn(91)
}

func TestConnectionPoolRollbackErrorEvictsPhysicalOwner(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	pool := NewConnectionPool(nil, db, "memory")
	pool.registerMySQLUDFsOnce.Do(func() {})
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectBegin()
	tx, err := pool.GetTxn(context.Background(), 92, "", nil)
	require.NoError(t, err)
	rollbackErr := errors.New("driver rollback state is unknown")
	mock.ExpectRollback().WillReturnError(rollbackErr)

	_, inactive, err := pool.RollbackTxn(92, tx)
	require.False(t, inactive)
	require.ErrorIs(t, err, rollbackErr)
	require.Nil(t, pool.TryGetTxn(92))
	_, stillMapped := pool.conns.Load(uint32(92))
	require.False(t, stillMapped, "rollback errors must evict the physical owner")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConnectionPoolActiveTransactionDoesNotFallbackToSessionConnection(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	p := NewConnectionPool(nil, db, "memory")
	p.registerMySQLUDFsOnce.Do(func() {})
	t.Cleanup(func() { _ = db.Close() })

	const sessionID = uint32(93)
	mock.ExpectBegin()
	tx, err := p.GetTxn(context.Background(), sessionID, "", nil)
	require.NoError(t, err)
	connEntry, ok := p.conns.Load(sessionID)
	require.True(t, ok)
	require.NotNil(t, connEntry)

	// Simulate a partially lost transaction-owner mapping while the generic
	// session connection is still present. That generic entry is not proof of
	// ownership and must never be used for active transaction work.
	p.txnConns.Delete(sessionID)
	owner, bound := p.GetTxnBinding(sessionID)
	require.Nil(t, owner)
	require.Same(t, tx, bound)

	// Every connection-facing lifecycle path must reject the ownerless active
	// transaction instead of falling back to the generic session connection.
	conn, err := p.GetConn(context.Background(), sessionID)
	require.ErrorContains(t, err, "no physical owner")
	require.Nil(t, conn)
	conn, err = p.GetConnForSchema(context.Background(), sessionID, "main")
	require.ErrorContains(t, err, "no physical owner")
	require.Nil(t, conn)
	gotTx, err := p.GetTxn(context.Background(), sessionID, "", nil)
	require.ErrorContains(t, err, "no physical owner")
	require.Nil(t, gotTx)
	require.Empty(t, p.CurrentSchema(sessionID))
	require.Empty(t, p.CurrentCatalog(sessionID))

	snapshotConn, snapshotTx, err := p.GetExecutionSnapshot(
		context.Background(), sessionID, "", false,
	)
	require.ErrorContains(t, err, "no physical owner")
	require.Nil(t, snapshotConn)
	require.Same(t, tx, snapshotTx)

	mock.ExpectRollback()
	mock.ExpectClose()
	require.NoError(t, p.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConnectionPoolCloseConnSweepsOwnerWhenGenericMappingIsAbsent(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	p := NewConnectionPool(nil, db, "memory")
	p.registerMySQLUDFsOnce.Do(func() {})
	t.Cleanup(func() { _ = db.Close() })

	const sessionID = uint32(99)
	mock.ExpectBegin()
	_, err = p.GetTxn(context.Background(), sessionID, "", nil)
	require.NoError(t, err)
	ownerEntry, ok := p.txnConns.Load(sessionID)
	require.True(t, ok)
	owner, ok := ownerEntry.(*stdsql.Conn)
	require.True(t, ok)
	require.NotNil(t, owner)

	// A failed reconnect/identity-safe close can remove the generic session map
	// while the active transaction still owns its physical connection. An
	// unconditional session close must roll back and retire that owner instead
	// of only deleting the logical transaction marker.
	p.conns.Delete(sessionID)
	mock.ExpectRollback()
	mock.ExpectClose()
	require.NoError(t, p.CloseConn(sessionID))

	_, present := p.conns.Load(sessionID)
	require.False(t, present)
	_, present = p.txns.Load(sessionID)
	require.False(t, present)
	_, present = p.txnConns.Load(sessionID)
	require.False(t, present)
	require.Nil(t, p.TryGetTxn(sessionID))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConnectionPoolCloseSweepsOwnerWithoutGenericMapping(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	p := NewConnectionPool(nil, db, "memory")
	p.registerMySQLUDFsOnce.Do(func() {})
	t.Cleanup(func() { _ = db.Close() })

	const sessionID = uint32(100)
	mock.ExpectBegin()
	_, err = p.GetTxn(context.Background(), sessionID, "", nil)
	require.NoError(t, err)
	ownerEntry, ok := p.txnConns.Load(sessionID)
	require.True(t, ok)
	owner, ok := ownerEntry.(*stdsql.Conn)
	require.True(t, ok)
	require.NotNil(t, owner)
	p.conns.Delete(sessionID)

	// closeLocked must sweep txnConns independently of conns. Otherwise this
	// checked-out owner survives pool teardown and can retain an open driver
	// transaction.
	mock.ExpectRollback()
	mock.ExpectClose()
	require.NoError(t, p.Close())
	_, present := p.conns.Load(sessionID)
	require.False(t, present)
	_, present = p.txns.Load(sessionID)
	require.False(t, present)
	_, present = p.txnConns.Load(sessionID)
	require.False(t, present)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConnectionPoolActiveTransactionUsesPhysicalOwnerOverReplacementSessionConn(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	p := NewConnectionPool(nil, db, "memory")
	p.registerMySQLUDFsOnce.Do(func() {})
	t.Cleanup(func() { _ = db.Close() })

	const sessionID = uint32(94)
	mock.ExpectBegin()
	tx, err := p.GetTxn(context.Background(), sessionID, "", nil)
	require.NoError(t, err)
	ownerEntry, ok := p.txnConns.Load(sessionID)
	require.True(t, ok)
	owner, ok := ownerEntry.(*stdsql.Conn)
	require.True(t, ok)
	require.NotNil(t, owner)

	// Simulate a stale generic session mapping left behind by a reconnect. The
	// active transaction must continue to use the owner captured at BeginTx.
	replacement, err := db.Conn(context.Background())
	require.NoError(t, err)
	p.conns.Store(sessionID, replacement)
	got, err := p.GetConn(context.Background(), sessionID)
	require.NoError(t, err)
	require.Same(t, owner, got)
	got, err = p.GetConnForSchema(context.Background(), sessionID, "schema_that_does_not_exist")
	require.NoError(t, err)
	require.Same(t, owner, got)

	// Restore the normal map before pool teardown and release the synthetic
	// replacement separately; it is not part of the transaction binding.
	p.conns.Store(sessionID, owner)
	require.NoError(t, replacement.Close())
	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	p.CloseTxn(sessionID)
	mock.ExpectClose()
	mock.ExpectClose()
	require.NoError(t, p.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConnectionPoolMalformedMappingsFailClosed(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	p := NewConnectionPool(connector, db, "memory")

	// A malformed generic entry must never be treated as an uninitialized
	// session (which would re-open a second connection) or panic a metadata
	// lookup. CurrentCatalog/CurrentSchema have no error return, so they fail
	// closed with an empty value; connection acquisition reports the corruption.
	const sessionID = uint32(95)
	p.conns.Store(sessionID, "not-a-connection")
	require.NotPanics(t, func() {
		require.Empty(t, p.CurrentSchema(sessionID))
		require.Empty(t, p.CurrentCatalog(sessionID))
	})
	_, err = p.GetConn(context.Background(), sessionID)
	require.ErrorContains(t, err, "connection mapping is invalid")

	// A malformed active transaction mapping is likewise not idle and must not
	// fall back to the generic connection path. CloseTxn can discard the poisoned
	// marker without attempting a type-unsafe rollback.
	const poisonedID = uint32(96)
	p.txns.Store(poisonedID, "not-a-transaction")
	p.txnConns.Store(poisonedID, "not-a-connection")
	require.Nil(t, p.TryGetTxn(poisonedID))
	_, err = p.GetConn(context.Background(), poisonedID)
	require.ErrorContains(t, err, "active transaction mapping is invalid")
	_, _, err = p.GetExecutionSnapshot(context.Background(), poisonedID, "", false)
	require.ErrorContains(t, err, "active transaction mapping is invalid")
	p.CloseTxn(poisonedID)
	_, stillPoisoned := p.txns.Load(poisonedID)
	require.False(t, stillPoisoned)

	// Close helpers return a concrete error for malformed generic entries and
	// never close an unrelated caller-supplied connection.
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	const closeID = uint32(97)
	p.conns.Store(closeID, "not-a-connection")
	require.ErrorContains(t, p.CloseConnIf(closeID, conn), "connection mapping is invalid")
	require.ErrorContains(t, p.CloseConnIfBinding(closeID, conn, nil), "connection mapping is invalid")
	require.NoError(t, conn.Close())

	// Pool teardown remains panic-free even when a map was poisoned. It reports
	// the malformed entry so the caller can surface the lifecycle corruption.
	const teardownID = uint32(98)
	p.conns.Store(teardownID, "not-a-connection")
	require.ErrorContains(t, p.Close(), "connection mapping is invalid")
	require.NoError(t, connector.Close())
}

func TestConnectionPoolMalformedTransactionDoesNotCloseLiveOwner(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	p := NewConnectionPool(nil, db, "memory")
	p.registerMySQLUDFsOnce.Do(func() {})

	const sessionID = uint32(109)
	mock.ExpectBegin()
	tx, err := p.GetTxn(context.Background(), sessionID, "", nil)
	require.NoError(t, err)
	ownerEntry, ok := p.txnConns.Load(sessionID)
	require.True(t, ok)
	owner, ok := ownerEntry.(*stdsql.Conn)
	require.True(t, ok)
	require.NotNil(t, owner)

	// Replace only the logical marker. The physical transaction is still live,
	// so closing its owner would wait forever on database/sql's close mutex.
	p.txns.Store(sessionID, "corrupt transaction marker")
	closeDone := make(chan error, 1)
	go func() { closeDone <- p.CloseConn(sessionID) }()
	select {
	case closeErr := <-closeDone:
		require.ErrorContains(t, closeErr, "active transaction mapping is invalid")
	case <-time.After(time.Second):
		t.Fatal("CloseConn hung while transaction mapping was malformed")
	}
	currentOwner, ok := p.txnConns.Load(sessionID)
	require.True(t, ok)
	require.Same(t, owner, currentOwner)
	currentConn, ok := p.conns.Load(sessionID)
	require.True(t, ok)
	require.Same(t, owner, currentConn)

	// Restore a typed marker so the test can finish the live transaction and
	// release the sqlmock connection normally.
	p.txns.Store(sessionID, tx)
	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	p.CloseTxn(sessionID)
	mock.ExpectClose()
	require.NoError(t, p.CloseConn(sessionID))
	require.NoError(t, mock.ExpectationsWereMet())
	_ = db.Close()
}

func TestConnectionPoolCloseReportsMalformedTransactionWithoutBlocking(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	p := NewConnectionPool(nil, db, "memory")
	p.registerMySQLUDFsOnce.Do(func() {})

	const sessionID = uint32(110)
	mock.ExpectBegin()
	tx, err := p.GetTxn(context.Background(), sessionID, "", nil)
	require.NoError(t, err)
	ownerEntry, ok := p.txnConns.Load(sessionID)
	require.True(t, ok)
	owner, ok := ownerEntry.(*stdsql.Conn)
	require.True(t, ok)

	p.txns.Store(sessionID, "corrupt transaction marker")
	closeDone := make(chan error, 1)
	go func() { closeDone <- p.Close() }()
	select {
	case closeErr := <-closeDone:
		require.ErrorContains(t, closeErr, "active transaction mapping is invalid")
	case <-time.After(time.Second):
		t.Fatal("pool Close hung while transaction mapping was malformed")
	}
	currentOwner, ok := p.txnConns.Load(sessionID)
	require.True(t, ok)
	require.Same(t, owner, currentOwner)

	// The failed teardown leaves the live owner available for an explicit
	// recovery, after which a normal close can retire it.
	p.txns.Store(sessionID, tx)
	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	p.CloseTxn(sessionID)
	mock.ExpectClose()
	require.NoError(t, p.CloseConn(sessionID))
	require.NoError(t, mock.ExpectationsWereMet())
	_ = db.Close()
}

func TestConnectionPoolSingleConnectionReleaseDoesNotDeadlockOtherSession(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	p := NewConnectionPool(connector, db, "memory")
	t.Cleanup(func() {
		require.NoError(t, p.Close())
		require.NoError(t, connector.Close())
	})

	conn1, err := p.GetConn(context.Background(), 101)
	require.NoError(t, err)
	tx, err := p.GetTxn(context.Background(), 101, "", nil)
	require.NoError(t, err)

	acquired := make(chan *stdsql.Conn, 1)
	acquireErr := make(chan error, 1)
	go func() {
		conn2, getErr := p.GetConn(context.Background(), 102)
		if getErr != nil {
			acquireErr <- getErr
			return
		}
		acquired <- conn2
	}()
	select {
	case <-acquired:
		t.Fatal("second session acquired the only connection before release")
	case <-time.After(20 * time.Millisecond):
	}

	require.NoError(t, tx.Rollback())
	p.CloseTxn(101)
	require.NoError(t, p.CloseConn(101))
	select {
	case err := <-acquireErr:
		require.NoError(t, err)
	case conn2 := <-acquired:
		require.NotNil(t, conn2)
		require.NoError(t, p.CloseConn(102))
	case <-time.After(time.Second):
		t.Fatal("second session remained blocked after first session release")
	}
	_ = conn1
}

func TestConnectionPoolCloseUnblocksPendingConnectionAcquire(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	p := NewConnectionPool(connector, db, "memory")

	// Keep the sole physical connection checked out. The second acquisition
	// enters database/sql's pending request queue while the pool read lock is
	// held, which is the shutdown deadlock regression this test guards.
	_, err = p.GetConn(context.Background(), 111)
	require.NoError(t, err)

	acquireDone := make(chan error, 1)
	go func() {
		_, acquireErr := p.GetConn(context.Background(), 112)
		acquireDone <- acquireErr
	}()
	// Wait after starting the goroutine so this test does not race the setup
	// connection with the pending request.
	deadline := time.Now().Add(time.Second)
	for db.Stats().WaitCount < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	require.GreaterOrEqual(t, db.Stats().WaitCount, int64(1), "second acquisition did not reach database/sql wait queue")

	closeDone := make(chan error, 1)
	go func() { closeDone <- p.Close() }()
	select {
	case closeErr := <-closeDone:
		require.NoError(t, closeErr)
	case <-time.After(time.Second):
		t.Fatal("pool close remained blocked behind a pending connection acquire")
	}
	select {
	case acquireErr := <-acquireDone:
		require.Error(t, acquireErr)
	case <-time.After(time.Second):
		t.Fatal("pending connection acquire was not canceled by pool close")
	}
}

func TestConnectionPoolExecutionSnapshotLeaseBlocksClose(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	p := NewConnectionPool(connector, db, "memory")

	conn, tx, release, err := p.GetExecutionSnapshotLease(
		context.Background(), 113, "", true,
	)
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.Nil(t, tx)
	require.NotNil(t, release)
	t.Cleanup(release)

	closeDone := make(chan error, 1)
	go func() { closeDone <- p.Close() }()
	waitForPoolShutdown(t, p)

	// Shutdown is announced, but the physical generation must remain alive while
	// the snapshot holder still owns its lease.
	select {
	case closeErr := <-closeDone:
		t.Fatalf("pool closed before snapshot lease release: %v", closeErr)
	case <-time.After(100 * time.Millisecond):
	}
	_, mapped := p.conns.Load(uint32(113))
	require.True(t, mapped, "shutdown must not retire a leased connection")

	// Release is intentionally safe to call more than once. The first call lets
	// teardown proceed; the second must not unlock the lifecycle gate again.
	release()
	release()
	select {
	case closeErr := <-closeDone:
		require.NoError(t, closeErr)
	case <-time.After(time.Second):
		t.Fatal("pool close did not finish after snapshot lease release")
	}
	_, mapped = p.conns.Load(uint32(113))
	require.False(t, mapped)
	require.NoError(t, connector.Close())
}

func TestConnectionPoolExecutionSnapshotLeaseBlocksReset(t *testing.T) {
	oldConnector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	oldDB := stdsql.OpenDB(oldConnector)
	p := NewConnectionPool(oldConnector, oldDB, "memory")

	conn, tx, release, err := p.GetExecutionSnapshotLease(
		context.Background(), 114, "", true,
	)
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.Nil(t, tx)
	t.Cleanup(release)

	newConnector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	newDB := stdsql.OpenDB(newConnector)
	resetDone := make(chan error, 1)
	go func() { resetDone <- p.Reset(newConnector, newDB) }()
	waitForPoolShutdown(t, p)

	select {
	case resetErr := <-resetDone:
		t.Fatalf("pool reset retired a leased generation: %v", resetErr)
	case <-time.After(100 * time.Millisecond):
	}
	_, mapped := p.conns.Load(uint32(114))
	require.True(t, mapped, "reset must wait for the leased connection")

	release()
	release()
	select {
	case resetErr := <-resetDone:
		require.NoError(t, resetErr)
	case <-time.After(time.Second):
		t.Fatal("pool reset did not finish after snapshot lease release")
	}

	// The replacement generation is usable after the old leased generation has
	// drained.
	newConn, err := p.GetConn(context.Background(), 115)
	require.NoError(t, err)
	var got int
	require.NoError(t, newConn.QueryRowContext(context.Background(), "SELECT 1").Scan(&got))
	require.Equal(t, 1, got)
	require.NoError(t, p.CloseConn(115))
	require.NoError(t, p.Close())
	require.NoError(t, oldConnector.Close())
	require.NoError(t, newConnector.Close())
}

func TestConnectionPoolResetUnblocksPendingConnectionAcquire(t *testing.T) {
	oldConnector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	oldDB := stdsql.OpenDB(oldConnector)
	oldDB.SetMaxOpenConns(1)
	p := NewConnectionPool(oldConnector, oldDB, "memory")

	_, err = p.GetConn(context.Background(), 121)
	require.NoError(t, err)
	acquireDone := make(chan error, 1)
	go func() {
		_, acquireErr := p.GetConn(context.Background(), 122)
		acquireDone <- acquireErr
	}()
	deadline := time.Now().Add(time.Second)
	for oldDB.Stats().WaitCount < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	require.GreaterOrEqual(t, oldDB.Stats().WaitCount, int64(1), "second acquisition did not reach database/sql wait queue")

	newConnector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	newDB := stdsql.OpenDB(newConnector)
	resetDone := make(chan error, 1)
	go func() { resetDone <- p.Reset(newConnector, newDB) }()
	select {
	case resetErr := <-resetDone:
		require.NoError(t, resetErr)
	case <-time.After(time.Second):
		t.Fatal("pool reset remained blocked behind a pending connection acquire")
	}
	select {
	case acquireErr := <-acquireDone:
		require.Error(t, acquireErr)
	case <-time.After(time.Second):
		t.Fatal("pending connection acquire was not canceled by pool reset")
	}

	// Reset must leave the replacement generation usable after the old DB was
	// pre-closed to release the blocked request.
	conn, err := p.GetConn(context.Background(), 123)
	require.NoError(t, err)
	var got int
	require.NoError(t, conn.QueryRowContext(context.Background(), "SELECT 1").Scan(&got))
	require.Equal(t, 1, got)
	require.NoError(t, p.CloseConn(123))
	require.NoError(t, p.Close())
}

func TestConnectionPoolCloseRollsBackActiveTransactionAfterPreClose(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	p := NewConnectionPool(connector, db, "memory")
	conn, err := p.GetConn(context.Background(), 131)
	require.NoError(t, err)
	_, err = conn.ExecContext(context.Background(), "CREATE TABLE close_preclose_tx (id INTEGER)")
	require.NoError(t, err)
	tx, err := p.GetTxn(context.Background(), 131, "", nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(context.Background(), "INSERT INTO close_preclose_tx VALUES (1)")
	require.NoError(t, err)

	require.NoError(t, p.Close())
	_, present := p.txns.Load(uint32(131))
	require.False(t, present)
}

func TestConnectionPoolShutdownAllowsReleaseWhileSchemaReadIsBlocked(t *testing.T) {
	driverState := newBlockingSchemaDriver()
	db := stdsql.OpenDB(driverState)
	db.SetMaxOpenConns(2)
	p := NewConnectionPool(nil, db, "memory")
	p.registerMySQLUDFsOnce.Do(func() {})

	conn1, err := p.GetConn(context.Background(), 141)
	require.NoError(t, err)
	conn2, err := p.GetConn(context.Background(), 142)
	require.NoError(t, err)
	require.NotNil(t, conn1)
	require.NotNil(t, conn2)

	schemaDone := make(chan struct{})
	go func() {
		_ = p.CurrentSchema(141)
		close(schemaDone)
	}()
	select {
	case <-driverState.schemaStarted:
	case <-time.After(time.Second):
		t.Fatal("schema query did not start")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- p.Close() }()
	waitForPoolShutdown(t, p)

	// Close is waiting for the blocked schema reader. A session release must
	// still be admitted while that writer is pending; an RWMutex would block it
	// behind the queued writer and leave both goroutines waiting.
	releaseDone := make(chan error, 1)
	go func() { releaseDone <- p.CloseConn(142) }()
	select {
	case releaseErr := <-releaseDone:
		require.NoError(t, releaseErr)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("connection release remained blocked by pending pool shutdown")
	}

	close(driverState.releaseSchema)
	select {
	case <-schemaDone:
	case <-time.After(time.Second):
		t.Fatal("schema reader did not finish after release")
	}
	select {
	case closeErr := <-closeDone:
		require.NoError(t, closeErr)
	case <-time.After(time.Second):
		t.Fatal("pool close did not finish after schema reader")
	}
}

func TestConnectionPoolResetAllowsReleaseWhileSchemaReadIsBlocked(t *testing.T) {
	oldDriver := newBlockingSchemaDriver()
	oldDB := stdsql.OpenDB(oldDriver)
	oldDB.SetMaxOpenConns(2)
	p := NewConnectionPool(nil, oldDB, "memory")
	p.registerMySQLUDFsOnce.Do(func() {})

	_, err := p.GetConn(context.Background(), 151)
	require.NoError(t, err)
	_, err = p.GetConn(context.Background(), 152)
	require.NoError(t, err)

	schemaDone := make(chan struct{})
	go func() {
		_ = p.CurrentSchema(151)
		close(schemaDone)
	}()
	select {
	case <-oldDriver.schemaStarted:
	case <-time.After(time.Second):
		t.Fatal("schema query did not start")
	}

	newDriver := newBlockingSchemaDriver()
	newDB := stdsql.OpenDB(newDriver)
	resetDone := make(chan error, 1)
	go func() { resetDone <- p.Reset(nil, newDB) }()
	waitForPoolShutdown(t, p)

	releaseDone := make(chan error, 1)
	go func() { releaseDone <- p.CloseConn(152) }()
	select {
	case releaseErr := <-releaseDone:
		require.NoError(t, releaseErr)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("connection release remained blocked by pending pool reset")
	}

	close(oldDriver.releaseSchema)
	select {
	case <-schemaDone:
	case <-time.After(time.Second):
		t.Fatal("schema reader did not finish after release")
	}
	select {
	case resetErr := <-resetDone:
		require.NoError(t, resetErr)
	case <-time.After(time.Second):
		t.Fatal("pool reset did not finish after schema reader")
	}

	p.registerMySQLUDFsOnce.Do(func() {})
	_, err = p.GetConn(context.Background(), 153)
	require.NoError(t, err)
	require.NoError(t, p.CloseConn(153))
	require.NoError(t, p.Close())
}

func TestConnectionPoolReleaseBindingSnapshotAllowsShutdownProgress(t *testing.T) {
	driverState := newBlockingSchemaDriver()
	db := stdsql.OpenDB(driverState)
	db.SetMaxOpenConns(3)
	p := NewConnectionPool(nil, db, "memory")
	p.registerMySQLUDFsOnce.Do(func() {})

	// Keep one reader blocked in a driver call while the second session is the
	// release target. The release snapshot must be admitted after shutdown has
	// been announced; using the ordinary lifecycle read lock here would queue it
	// behind Close and deadlock the pool teardown.
	_, err := p.GetConn(context.Background(), 161)
	require.NoError(t, err)
	releaseConn, err := p.GetConn(context.Background(), 162)
	require.NoError(t, err)
	require.NotNil(t, releaseConn)

	schemaDone := make(chan struct{})
	go func() {
		_ = p.CurrentSchema(161)
		close(schemaDone)
	}()
	select {
	case <-driverState.schemaStarted:
	case <-time.After(time.Second):
		t.Fatal("schema query did not start")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- p.Close() }()
	waitForPoolShutdown(t, p)
	ordinarySnapshotDone := make(chan struct{})
	go func() {
		_, _ = p.GetTxnBinding(162)
		close(ordinarySnapshotDone)
	}()
	select {
	case <-ordinarySnapshotDone:
		t.Fatal("ordinary binding snapshot entered after pool shutdown")
	case <-time.After(100 * time.Millisecond):
	}

	releaseSnapshotDone := make(chan error, 1)
	go func() {
		conn, tx := p.GetTxnBindingForRelease(162)
		if conn == nil || tx != nil {
			releaseSnapshotDone <- fmt.Errorf("unexpected release binding conn=%v tx=%v", conn, tx)
			return
		}
		releaseSnapshotDone <- p.CloseConnIfBinding(162, conn, nil)
	}()
	select {
	case releaseErr := <-releaseSnapshotDone:
		require.NoError(t, releaseErr)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("release binding snapshot remained blocked by pending pool shutdown")
	}

	close(driverState.releaseSchema)
	select {
	case <-schemaDone:
	case <-time.After(time.Second):
		t.Fatal("schema reader did not finish after release")
	}
	select {
	case closeErr := <-closeDone:
		require.NoError(t, closeErr)
	case <-time.After(time.Second):
		t.Fatal("pool close did not finish after schema reader")
	}
	select {
	case <-ordinarySnapshotDone:
	case <-time.After(time.Second):
		t.Fatal("ordinary binding snapshot did not resume after pool shutdown")
	}
}

func waitForPoolShutdown(t *testing.T, p *ConnectionPool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		p.lifecycleMu.mu.Lock()
		shuttingDown := p.lifecycleMu.shuttingDown
		p.lifecycleMu.mu.Unlock()
		if shuttingDown {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("pool lifecycle did not announce shutdown")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestFirstMySQLQueryAfterServerStart(t *testing.T) {
	originalWorkingDir, err := os.Getwd()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Chdir(originalWorkingDir))
	}()

	testDir := testutil.CreateTestDir(t)
	testEnv := testutil.NewTestEnv()
	require.NoError(t, testutil.StartDuckSqlServer(t, testDir, nil, testEnv))
	t.Cleanup(func() {
		require.NoError(t, testEnv.MyDuckServer.Close())
		testutil.StopDuckSqlServer(t, testEnv.DuckProcess)
	})

	var got int
	require.NoError(t, testEnv.MyDuckServer.QueryRowContext(context.Background(), "SELECT 1").Scan(&got))
	require.Equal(t, 1, got)
}

func assertMySQLUDFsCallable(t *testing.T, conn *stdsql.Conn) {
	t.Helper()

	var randValue float64
	var randomBytesLength int
	var vector []byte
	err := conn.QueryRowContext(
		context.Background(),
		"SELECT mysql_rand(1), octet_length(mysql_random_bytes(3)), string_to_vector('[1.5,-2]', 2)",
	).Scan(&randValue, &randomBytesLength, &vector)
	require.NoError(t, err)
	require.Equal(t, mysqlRandFromSeed(1), randValue)
	require.Equal(t, 3, randomBytesLength)
	decoded, err := gmsql.DecodeVector(vector)
	require.NoError(t, err)
	require.Equal(t, []float32{1.5, -2}, decoded)
}

// blockingSchemaDriver gives the shutdown regression a deterministic blocked
// driver query and a separate connection that can be released while Close is
// waiting for the blocked reader.
type blockingSchemaDriver struct {
	mu            sync.Mutex
	nextID        int
	schemaStarted chan struct{}
	releaseSchema chan struct{}
	startedOnce   sync.Once
	connections   map[int]*blockingSchemaConn
}

func newBlockingSchemaDriver() *blockingSchemaDriver {
	return &blockingSchemaDriver{
		schemaStarted: make(chan struct{}),
		releaseSchema: make(chan struct{}),
		connections:   make(map[int]*blockingSchemaConn),
	}
}

func (d *blockingSchemaDriver) Connect(context.Context) (driver.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	id := d.nextID
	d.nextID++
	conn := &blockingSchemaConn{driver: d, id: id}
	d.connections[id] = conn
	return conn, nil
}

func (d *blockingSchemaDriver) Driver() driver.Driver { return d }

func (d *blockingSchemaDriver) Open(string) (driver.Conn, error) {
	return d.Connect(context.Background())
}

type blockingSchemaConn struct {
	driver *blockingSchemaDriver
	id     int
	mu     sync.Mutex
	closed bool
}

func (c *blockingSchemaConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare is unsupported")
}

func (c *blockingSchemaConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *blockingSchemaConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("transactions are unsupported")
}

func (c *blockingSchemaConn) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if query != "SELECT CURRENT_SCHEMA" {
		return nil, fmt.Errorf("unexpected query %q", query)
	}
	c.driver.startedOnce.Do(func() { close(c.driver.schemaStarted) })
	select {
	case <-c.driver.releaseSchema:
		return &blockingSchemaRows{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type blockingSchemaRows struct {
	read bool
}

func (*blockingSchemaRows) Columns() []string { return []string{"current_schema"} }

func (r *blockingSchemaRows) Close() error { return nil }

func (r *blockingSchemaRows) Next(dest []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	dest[0] = "main"
	return nil
}
