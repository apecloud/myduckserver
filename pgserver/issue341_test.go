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

// TestIssue341PgAuthRejectsWrongPassword checks
// https://github.com/apecloud/myduckserver/issues/341 :
// --superuser-password enables SCRAM; a wrong password must not connect.
func TestIssue341PgAuthRejectsWrongPassword(t *testing.T) {
	originalWorkingDir, err := os.Getwd()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.Chdir(originalWorkingDir))
	})

	testDir := testutil.CreateTestDir(t)
	testEnv := testutil.NewTestEnv()
	testEnv.SuperuserPassword = "secret"
	testEnv.ExtraArgs = []string{"--superuser-password=secret"}
	err = testutil.StartDuckSqlServer(t, testDir, nil, testEnv)
	require.NoError(t, err)
	t.Cleanup(func() {
		if testEnv.DuckProcess != nil {
			testutil.StopDuckSqlServer(t, testEnv.DuckProcess)
		}
	})

	port := strconv.Itoa(testEnv.DuckPgPort)
	_, err = pgx.Connect(context.Background(), "postgresql://postgres:wrong@127.0.0.1:"+port+"/postgres")
	require.Error(t, err, "wrong password must be rejected")
	_, err = pgx.Connect(context.Background(), "postgresql://missing_role:secret@127.0.0.1:"+port+"/postgres")
	require.Error(t, err, "unknown role must be rejected")

	db, err := pgx.Connect(context.Background(), "postgresql://postgres:secret@127.0.0.1:"+port+"/postgres")
	require.NoError(t, err, "correct password must connect")
	defer db.Close(context.Background())

	var extendedValue int
	err = db.QueryRow(context.Background(), "SELECT $1::int + 1", 41).Scan(&extendedValue)
	require.NoError(t, err)
	require.Equal(t, 42, extendedValue)

	simpleConfig, err := pgx.ParseConfig("postgresql://postgres:secret@127.0.0.1:" + port + "/postgres")
	require.NoError(t, err)
	simpleConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	simple, err := pgx.ConnectConfig(context.Background(), simpleConfig)
	require.NoError(t, err)
	var simpleValue int
	require.NoError(t, simple.QueryRow(context.Background(), "SELECT 42").Scan(&simpleValue))
	require.Equal(t, 42, simpleValue)
	require.NoError(t, simple.Close(context.Background()))

	require.NoError(t, db.Close(context.Background()))
	require.NoError(t, testEnv.MyDuckServer.Close())

	testutil.StopDuckSqlServer(t, testEnv.DuckProcess)
	testEnv.DuckProcess = nil
	require.NoError(t, testutil.StartDuckSqlServer(t, testDir, nil, testEnv))

	db, err = pgx.Connect(context.Background(), "postgresql://postgres:secret@127.0.0.1:"+port+"/postgres")
	require.NoError(t, err, "correct password must connect after restart")
	defer db.Close(context.Background())
	require.NoError(t, db.QueryRow(context.Background(), "SELECT 42").Scan(&extendedValue))
	require.Equal(t, 42, extendedValue)
}
