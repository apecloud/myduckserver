package pgserver

import (
	"context"
	"strconv"
	"testing"

	"github.com/apecloud/myduckserver/testutil"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestIssue280RepeatedConnectMySQLDatabase reproduces
// https://github.com/apecloud/myduckserver/issues/280 :
// connect (database=mysql) -> insert -> disconnect, repeated.
func TestIssue280RepeatedConnectMySQLDatabase(t *testing.T) {
	testDir := testutil.CreateTestDir(t)
	testEnv := testutil.NewTestEnv()
	err := testutil.StartDuckSqlServer(t, testDir, nil, testEnv)
	require.NoError(t, err)
	defer testutil.StopDuckSqlServer(t, testEnv.DuckProcess)

	dsn := "postgresql://postgres@127.0.0.1:" + strconv.Itoa(testEnv.DuckPgPort) + "/mysql"
	for i := 0; i < 15; i++ {
		db, err := pgx.Connect(context.Background(), dsn)
		require.NoError(t, err, "connect failed on iteration %d", i)
		_, err = db.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS public.t280 (i INT)")
		require.NoError(t, err, "create failed on iteration %d", i)
		_, err = db.Exec(context.Background(), "INSERT INTO public.t280 VALUES (1)")
		require.NoError(t, err, "insert failed on iteration %d", i)
		require.NoError(t, db.Close(context.Background()), "close failed on iteration %d", i)
	}
}
