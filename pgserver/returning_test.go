package pgserver

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/apecloud/myduckserver/testutil"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestInsertReturningWireProtocols(t *testing.T) {
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

	_, err = testEnv.MyDuckServer.Exec("CREATE DATABASE returning_wire")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("USE returning_wire")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("CREATE TABLE returning_rows (id INT PRIMARY KEY, name VARCHAR(32) NOT NULL)")
	require.NoError(t, err)

	t.Run("mysql text", func(t *testing.T) {
		requireReturningWireRow(t,
			testEnv.MyDuckServer.QueryRow("INSERT INTO returning_rows VALUES (1, 'mysql text') RETURNING id, name"),
			1, "mysql text")
	})

	t.Run("mysql prepared boundary", func(t *testing.T) {
		stmt, err := testEnv.MyDuckServer.Prepare("INSERT INTO returning_rows VALUES (?, ?) RETURNING id, name")
		require.NoError(t, err)
		defer stmt.Close()
		err = stmt.QueryRow(2, "mysql prepared").Scan(new(int32), new(string))
		require.ErrorContains(t, err, "incorrect argument count for command: have 0 want 2")
	})

	ctx := context.Background()
	pgURL := "postgresql://postgres@127.0.0.1:" + strconv.Itoa(testEnv.DuckPgPort) + "/postgres"
	t.Run("postgres simple", func(t *testing.T) {
		config, err := pgx.ParseConfig(pgURL)
		require.NoError(t, err)
		config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		conn, err := pgx.ConnectConfig(ctx, config)
		require.NoError(t, err)
		defer conn.Close(ctx)
		requireReturningWireRow(t,
			conn.QueryRow(ctx, "INSERT INTO returning_wire.returning_rows VALUES (3, 'postgres simple') RETURNING id, name"),
			3, "postgres simple")
	})

	t.Run("postgres prepared", func(t *testing.T) {
		conn, err := pgx.Connect(ctx, pgURL)
		require.NoError(t, err)
		defer conn.Close(ctx)
		_, err = conn.Prepare(ctx, "returning_wire_prepared", "INSERT INTO returning_wire.returning_rows VALUES ($1, $2) RETURNING id, name")
		require.NoError(t, err)
		requireReturningWireRow(t,
			conn.QueryRow(ctx, "returning_wire_prepared", int32(4), "postgres prepared"),
			4, "postgres prepared")
	})

	var count int
	err = testEnv.MyDuckServer.QueryRow("SELECT count(*) FROM returning_rows").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 3, count)
}

type returningWireRowScanner interface {
	Scan(dest ...any) error
}

func requireReturningWireRow(t *testing.T, row returningWireRowScanner, expectedID int32, expectedName string) {
	t.Helper()
	var id int32
	var name string
	require.NoError(t, row.Scan(&id, &name))
	require.Equal(t, expectedID, id)
	require.Equal(t, expectedName, name)
}
