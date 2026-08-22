package pgserver

import (
	"context"
	"strconv"
	"testing"

	"github.com/apecloud/myduckserver/testutil"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

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
