package main

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/apecloud/myduckserver/testutil"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestReadOnlyProtocols(t *testing.T) {
	testDir := testutil.CreateTestDir(t)
	testEnv := testutil.NewTestEnv()
	workingDir, err := os.Getwd()
	require.NoError(t, err)
	// testutil starts `go run .` from the parent of OriginalWorkingDir.
	testEnv.OriginalWorkingDir = filepath.Join(workingDir, "pgserver")
	testEnv.ExtraArgs = []string{"--read-only"}
	require.NoError(t, testutil.StartDuckSqlServer(t, testDir, nil, testEnv))
	defer testutil.StopDuckSqlServer(t, testEnv.DuckProcess)

	assertReadOnlyError := func(err error) {
		require.Error(t, err)
		require.Contains(t, strings.ToLower(err.Error()), "read only")
	}

	_, err = testEnv.MyDuckServer.Exec("SELECT 1")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("INSERT INTO missing_table VALUES (1)")
	assertReadOnlyError(err)
	_, err = testEnv.MyDuckServer.Exec("CREATE TABLE blocked_table (id INT)")
	assertReadOnlyError(err)
	_, err = testEnv.MyDuckServer.Exec("CREATE OR REPLACE TABLE blocked_replace (id INT)")
	assertReadOnlyError(err)

	ctx := context.Background()
	pgConn, err := pgx.Connect(ctx, "postgresql://postgres@127.0.0.1:"+strconv.Itoa(testEnv.DuckPgPort)+"/postgres")
	require.NoError(t, err)
	defer pgConn.Close(ctx)

	_, err = pgConn.Exec(ctx, "SELECT 1")
	require.NoError(t, err)
	_, err = pgConn.Exec(ctx, "INSERT INTO missing_table VALUES (1)")
	assertReadOnlyError(err)
	_, err = pgConn.Exec(ctx, "CREATE TABLE blocked_table (id INT)")
	assertReadOnlyError(err)
	_, err = pgConn.Exec(ctx, "CREATE OR REPLACE TABLE blocked_replace (id INT)")
	assertReadOnlyError(err)
}

func TestReadOnlyDefaultOff(t *testing.T) {
	testDir := testutil.CreateTestDir(t)
	testEnv := testutil.NewTestEnv()
	workingDir, err := os.Getwd()
	require.NoError(t, err)
	testEnv.OriginalWorkingDir = filepath.Join(workingDir, "pgserver")
	require.NoError(t, testutil.StartDuckSqlServer(t, testDir, nil, testEnv))
	defer testutil.StopDuckSqlServer(t, testEnv.DuckProcess)

	_, err = testEnv.MyDuckServer.Exec("CREATE DATABASE default_off_mysql")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("USE default_off_mysql")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("CREATE TABLE default_off_mysql (id INT)")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("INSERT INTO default_off_mysql VALUES (1)")
	require.NoError(t, err)

	ctx := context.Background()
	pgConn, err := pgx.Connect(ctx, "postgresql://postgres@127.0.0.1:"+strconv.Itoa(testEnv.DuckPgPort)+"/postgres")
	require.NoError(t, err)
	defer pgConn.Close(ctx)

	_, err = pgConn.Exec(ctx, "CREATE TABLE default_off_pg (id INT)")
	require.NoError(t, err)
	_, err = pgConn.Exec(ctx, "INSERT INTO default_off_pg VALUES (1)")
	require.NoError(t, err)
}
