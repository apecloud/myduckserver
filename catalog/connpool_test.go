package catalog

import (
	"context"
	stdsql "database/sql"
	"os"
	"testing"

	"github.com/apecloud/myduckserver/testutil"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

func TestConnectionPoolRegistersMySQLUDFsOnce(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	pool := NewConnectionPool(connector, db)
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
	err := conn.QueryRowContext(
		context.Background(),
		"SELECT mysql_rand(1), octet_length(mysql_random_bytes(3))",
	).Scan(&randValue, &randomBytesLength)
	require.NoError(t, err)
	require.Equal(t, mysqlRandFromSeed(1), randValue)
	require.Equal(t, 3, randomBytesLength)
}
