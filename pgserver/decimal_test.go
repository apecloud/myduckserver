package pgserver

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/apecloud/myduckserver/testutil"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

const decimalWireQuery = `
	SELECT
		CAST(12345678901234567890.123456789012345678 AS DECIMAL(38, 18)),
		CAST(NULL AS DECIMAL(10, 2)),
		CEIL(CAST(-1.5 AS DECIMAL(2, 1))),
		FLOOR(CAST(-1.5 AS DECIMAL(2, 1))),
		ROUND(CAST(1.235 AS DECIMAL(4, 3)), 2)`

const mysqlPreparedDecimalWireQuery = `
	SELECT
		CAST(? AS DECIMAL(38, 18)),
		CAST(NULL AS DECIMAL(10, 2)),
		CEIL(CAST(-1.5 AS DECIMAL(2, 1))),
		FLOOR(CAST(-1.5 AS DECIMAL(2, 1))),
		ROUND(CAST(1.235 AS DECIMAL(4, 3)), 2)`

const postgresPreparedDecimalWireQuery = `
	SELECT
		CAST($1 AS DECIMAL(38, 18)),
		CAST(NULL AS DECIMAL(10, 2)),
		CEIL(CAST(-1.5 AS DECIMAL(2, 1))),
		FLOOR(CAST(-1.5 AS DECIMAL(2, 1))),
		ROUND(CAST(1.235 AS DECIMAL(4, 3)), 2)`

func TestDecimalWireProtocols(t *testing.T) {
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

	t.Run("mysql text", func(t *testing.T) {
		requireMySQLDecimalRow(t, testEnv.MyDuckServer.QueryRow(decimalWireQuery))
	})

	t.Run("mysql prepared", func(t *testing.T) {
		stmt, err := testEnv.MyDuckServer.Prepare(mysqlPreparedDecimalWireQuery)
		require.NoError(t, err)
		defer stmt.Close()
		requireMySQLDecimalRow(t, stmt.QueryRow("12345678901234567890.123456789012345678"))
	})

	t.Run("mysql multi statement", func(t *testing.T) {
		db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%d)/?multiStatements=true", testEnv.DuckPort))
		require.NoError(t, err)
		defer db.Close()

		rows, err := db.Query(decimalWireQuery + "; " + decimalWireQuery)
		require.NoError(t, err)
		defer rows.Close()
		for resultSet := range 2 {
			require.True(t, rows.Next())
			requireMySQLDecimalRow(t, rows)
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
			require.Equal(t, resultSet == 0, rows.NextResultSet())
		}
	})

	t.Run("postgres extended", func(t *testing.T) {
		ctx := context.Background()
		conn, err := pgx.Connect(ctx, "postgresql://postgres@127.0.0.1:"+strconv.Itoa(testEnv.DuckPgPort)+"/postgres")
		require.NoError(t, err)
		defer conn.Close(ctx)
		requirePostgresDecimalRow(t, conn.QueryRow(ctx, decimalWireQuery))

		_, err = conn.Prepare(ctx, "decimal_wire_prepared", postgresPreparedDecimalWireQuery)
		require.NoError(t, err)
		requirePostgresDecimalRow(t, conn.QueryRow(
			ctx,
			"decimal_wire_prepared",
			"12345678901234567890.123456789012345678",
		))
	})

	t.Run("postgres simple", func(t *testing.T) {
		ctx := context.Background()
		config, err := pgx.ParseConfig("postgresql://postgres@127.0.0.1:" + strconv.Itoa(testEnv.DuckPgPort) + "/postgres")
		require.NoError(t, err)
		config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		conn, err := pgx.ConnectConfig(ctx, config)
		require.NoError(t, err)
		defer conn.Close(ctx)
		requirePostgresDecimalRow(t, conn.QueryRow(ctx, decimalWireQuery))
	})
}

type sqlRowScanner interface {
	Scan(dest ...any) error
}

func requireMySQLDecimalRow(t *testing.T, row sqlRowScanner) {
	t.Helper()
	var precise, rounded string
	var nullable sql.NullString
	var ceiling, floor int64
	err := row.Scan(&precise, &nullable, &ceiling, &floor, &rounded)
	require.NoError(t, err)
	require.Equal(t, "12345678901234567890.123456789012345678", precise)
	require.False(t, nullable.Valid)
	require.Equal(t, int64(-1), ceiling)
	require.Equal(t, int64(-2), floor)
	require.Equal(t, "1.24", rounded)
}

func requirePostgresDecimalRow(t *testing.T, row pgx.Row) {
	t.Helper()
	var precise, nullable, rounded pgtype.Numeric
	var ceiling, floor int64
	err := row.Scan(&precise, &nullable, &ceiling, &floor, &rounded)
	require.NoError(t, err)
	requirePGNumeric(t, precise, "12345678901234567890123456789012345678", -18)
	require.False(t, nullable.Valid)
	require.Equal(t, int64(-1), ceiling)
	require.Equal(t, int64(-2), floor)
	requirePGNumeric(t, rounded, "124", -2)
}

func requirePGNumeric(t *testing.T, actual pgtype.Numeric, coefficient string, exponent int32) {
	t.Helper()
	require.True(t, actual.Valid)
	require.False(t, actual.NaN)
	require.Equal(t, pgtype.Finite, actual.InfinityModifier)
	require.Equal(t, coefficient, actual.Int.String())
	require.Equal(t, exponent, actual.Exp)
}
