package pgserver

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/storage"
	"github.com/apecloud/myduckserver/testutil"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestStatementLogTextRedactsLegacyObjectStorageConfig(t *testing.T) {
	config := &storage.ObjectStorageConfig{
		Provider:        "s3",
		Endpoint:        "https://s3.example.test:9443",
		Region:          "us-east-1",
		AccessKeyId:     "access-key-71",
		SecretAccessKey: "secret-71",
	}

	backup := statementLogText(ConvertedStatement{
		String:       "BACKUP DATABASE app TO 's3://bucket/app/'",
		BackupConfig: &BackupConfig{StorageConfig: config},
	})
	restore := statementLogText(ConvertedStatement{
		String:        "RESTORE DATABASE app FROM 's3://bucket/app/'",
		RestoreConfig: &RestoreConfig{StorageConfig: config},
	})

	require.Equal(t, catalog.RedactedSensitiveSQL, backup)
	require.Equal(t, catalog.RedactedSensitiveSQL, restore)
	require.NotContains(t, backup, config.Endpoint)
	require.NotContains(t, backup, config.AccessKeyId)
	require.NotContains(t, backup, config.SecretAccessKey)
	require.NotContains(t, restore, config.Endpoint)
	require.NotContains(t, restore, config.AccessKeyId)
	require.NotContains(t, restore, config.SecretAccessKey)
}

func TestNormalizePGWireValue(t *testing.T) {
	value, err := normalizePGWireValue(nil)
	require.NoError(t, err)
	require.Nil(t, value)

	value, err = normalizePGWireValue("plain text")
	require.NoError(t, err)
	require.Equal(t, "plain text", value)

	value, err = normalizePGWireValue(int64(42))
	require.NoError(t, err)
	require.Equal(t, int64(42), value)

	value, err = normalizePGWireValue(types.JSONDocument{Val: nil})
	require.NoError(t, err)
	require.Equal(t, "null", value)

	value, err = normalizePGWireValue(types.JSONDocument{Val: map[string]any{"key": "value"}})
	require.NoError(t, err)
	require.JSONEq(t, `{"key":"value"}`, value.(string))
}

func TestQueryRowLimitOnMySQLAndPostgresSessions(t *testing.T) {
	originalWorkingDir, err := os.Getwd()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Chdir(originalWorkingDir))
	}()

	testDir := testutil.CreateTestDir(t)
	testEnv := testutil.NewTestEnv()
	testEnv.ExtraArgs = []string{"--query-row-limit=2"}
	require.NoError(t, testutil.StartDuckSqlServer(t, testDir, nil, testEnv))
	t.Cleanup(func() {
		require.NoError(t, testEnv.MyDuckServer.Close())
		testutil.StopDuckSqlServer(t, testEnv.DuckProcess)
	})

	t.Run("mysql", func(t *testing.T) {
		ctx := context.Background()
		rows, err := testEnv.MyDuckServer.QueryContext(ctx, "SELECT 1 AS n UNION ALL SELECT 2 AS n ORDER BY n")
		require.NoError(t, err)
		var got []int
		for rows.Next() {
			var value int
			require.NoError(t, rows.Scan(&value))
			got = append(got, value)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.Equal(t, []int{1, 2}, got)

		err = mysqlQueryError(ctx, testEnv, "SELECT 1 AS n UNION ALL SELECT 2 AS n UNION ALL SELECT 3 AS n ORDER BY n")
		require.ErrorContains(t, err, "query returned more than the configured row limit of 2")

		stmt, err := testEnv.MyDuckServer.PrepareContext(ctx, "SELECT 1 AS n UNION ALL SELECT 2 AS n UNION ALL SELECT 3 AS n ORDER BY n")
		require.NoError(t, err)
		preparedRows, err := stmt.QueryContext(ctx)
		if err == nil {
			for preparedRows.Next() {
			}
			err = preparedRows.Err()
			require.NoError(t, preparedRows.Close())
		}
		require.ErrorContains(t, err, "query returned more than the configured row limit of 2")
		require.NoError(t, stmt.Close())

		var value int
		require.NoError(t, testEnv.MyDuckServer.QueryRowContext(ctx, "SELECT 42").Scan(&value))
		require.Equal(t, 42, value)
		require.NoError(t, testEnv.MyDuckServer.QueryRowContext(
			ctx,
			"SELECT 42 WHERE 3 IN (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) AND @@autocommit = 1",
		).Scan(&value))
		require.Equal(t, 42, value)
		_, err = testEnv.MyDuckServer.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS query_row_limit_mysql")
		require.NoError(t, err)
	})

	t.Run("postgres", func(t *testing.T) {
		ctx := context.Background()
		conn, err := pgx.Connect(ctx, "postgresql://postgres@127.0.0.1:"+strconv.Itoa(testEnv.DuckPgPort)+"/postgres")
		require.NoError(t, err)
		defer conn.Close(ctx)

		rows, err := conn.Query(ctx, "SELECT 1 AS n UNION ALL SELECT 2 AS n ORDER BY n")
		require.NoError(t, err)
		var got []int
		for rows.Next() {
			var value int
			require.NoError(t, rows.Scan(&value))
			got = append(got, value)
		}
		require.NoError(t, rows.Err())
		rows.Close()
		require.Equal(t, []int{1, 2}, got)

		rows, err = conn.Query(ctx, "SELECT 1 AS n UNION ALL SELECT 2 AS n UNION ALL SELECT 3 AS n ORDER BY n")
		if err == nil {
			for rows.Next() {
			}
			err = rows.Err()
			rows.Close()
		}
		require.ErrorContains(t, err, "query returned more than the configured row limit of 2")

		var value int
		require.NoError(t, conn.QueryRow(ctx, "SELECT 42").Scan(&value))
		require.Equal(t, 42, value)
		_, err = conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS query_row_limit_postgres")
		require.NoError(t, err)
	})
}

func mysqlQueryError(ctx context.Context, testEnv *testutil.TestEnv, query string) error {
	rows, err := testEnv.MyDuckServer.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
	}
	return rows.Err()
}

func TestPostgresDuckDBOnlyCreateOrReplaceTable(t *testing.T) {
	testDir := testutil.CreateTestDir(t)
	testEnv := testutil.NewTestEnv()
	require.NoError(t, testutil.StartDuckSqlServer(t, testDir, nil, testEnv))
	defer testutil.StopDuckSqlServer(t, testEnv.DuckProcess)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgresql://postgres@127.0.0.1:"+strconv.Itoa(testEnv.DuckPgPort)+"/postgres")
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS test_copy")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "SET search_path TO test_copy")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "CREATE OR REPLACE TABLE t (a int, b text, c float)")
	require.NoError(t, err)
}
