package pgserver

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/apecloud/myduckserver/testutil"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// A simple Query message containing multiple statements is one PostgreSQL
// transaction scope. The first write must not survive a later statement error.
func TestPostgresSimpleQueryImplicitTransactionRollsBackBatch(t *testing.T) {
	ctx, conn := newImplicitTransactionTestConnection(t, pgx.QueryExecModeSimpleProtocol)

	_, err := conn.Exec(ctx, "INSERT INTO implicit_batch.implicit_batch_rows VALUES (2); INSERT INTO implicit_batch.implicit_batch_rows (missing) VALUES (3)")
	require.Error(t, err)

	var count int
	require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM implicit_batch.implicit_batch_rows").Scan(&count))
	require.Equal(t, 1, count)
}

// pgx.Batch uses the extended protocol and emits one Sync after all queued
// statements. The server must retain the same implicit transaction across the
// whole exchange so an error in a later statement rolls back earlier writes.
func TestPostgresExtendedBatchImplicitTransactionRollsBack(t *testing.T) {
	ctx, conn := newImplicitTransactionTestConnection(t, pgx.QueryExecModeExec)

	var batch pgx.Batch
	batch.Queue("INSERT INTO implicit_batch.implicit_batch_rows VALUES (2)")
	batch.Queue("INSERT INTO implicit_batch.implicit_batch_rows (missing) VALUES (3)")
	results := conn.SendBatch(ctx, &batch)
	_, err := results.Exec()
	require.NoError(t, err)
	_, err = results.Exec()
	require.Error(t, err)
	// The second Exec has consumed the server error and closed the result
	// reader. Close is still required by pgx before reusing the connection.
	_ = results.Close()

	var count int
	require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM implicit_batch.implicit_batch_rows").Scan(&count))
	require.Equal(t, 1, count)

	// A subsequent clean extended exchange must start a fresh implicit scope
	// and commit it when its Sync arrives.
	var clean pgx.Batch
	clean.Queue("INSERT INTO implicit_batch.implicit_batch_rows VALUES (4)")
	cleanResults := conn.SendBatch(ctx, &clean)
	_, err = cleanResults.Exec()
	require.NoError(t, err)
	require.NoError(t, cleanResults.Close())
	require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM implicit_batch.implicit_batch_rows").Scan(&count))
	require.Equal(t, 2, count)
}

// An explicit BEGIN must promote the connection to PostgreSQL's sticky
// transaction-block state. A later statement error is not an implicit-scope
// error: the block remains failed until the client issues ROLLBACK.
func TestPostgresExplicitTransactionRemainsFailedUntilRollback(t *testing.T) {
	ctx, conn := newImplicitTransactionTestConnection(t, pgx.QueryExecModeSimpleProtocol)

	_, err := conn.Exec(ctx, "BEGIN; INSERT INTO implicit_batch.implicit_batch_rows VALUES (2); INSERT INTO implicit_batch.implicit_batch_rows (missing) VALUES (3)")
	require.Error(t, err)

	_, err = conn.Exec(ctx, "SELECT count(*) FROM implicit_batch.implicit_batch_rows")
	require.Error(t, err)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "25P02", pgErr.Code)

	_, err = conn.Exec(ctx, "ROLLBACK")
	require.NoError(t, err)

	var count int
	require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM implicit_batch.implicit_batch_rows").Scan(&count))
	require.Equal(t, 1, count)
}

func newImplicitTransactionTestConnection(t *testing.T, mode pgx.QueryExecMode) (context.Context, *pgx.Conn) {
	t.Helper()

	originalWorkingDir, err := os.Getwd()
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Chdir(originalWorkingDir)) }()

	testDir := testutil.CreateTestDir(t)
	testEnv := testutil.NewTestEnv()
	require.NoError(t, testutil.StartDuckSqlServer(t, testDir, nil, testEnv))
	t.Cleanup(func() {
		require.NoError(t, testEnv.MyDuckServer.Close())
		testutil.StopDuckSqlServer(t, testEnv.DuckProcess)
	})

	_, err = testEnv.MyDuckServer.Exec("CREATE DATABASE implicit_batch")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("CREATE TABLE implicit_batch.implicit_batch_rows (id INTEGER)")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("INSERT INTO implicit_batch.implicit_batch_rows VALUES (1)")
	require.NoError(t, err)

	ctx := context.Background()
	config, err := pgx.ParseConfig("postgresql://postgres@127.0.0.1:" + strconv.Itoa(testEnv.DuckPgPort) + "/postgres")
	require.NoError(t, err)
	config.DefaultQueryExecMode = mode
	conn, err := pgx.ConnectConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(ctx) })
	return ctx, conn
}
