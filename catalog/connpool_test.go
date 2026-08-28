package catalog

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"os"
	"testing"

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
