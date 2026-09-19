package pgserver

import (
	"context"
	"strconv"
	"testing"

	"github.com/apecloud/myduckserver/testutil"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestPGBackendPIDMatchesSession(t *testing.T) {
	env := testutil.NewTestEnv()
	require.NoError(t, testutil.StartDuckSqlServer(t, testutil.CreateTestDir(t), nil, env))
	defer testutil.StopDuckSqlServer(t, env.DuckProcess)
	ctx := context.Background()
	for _, mode := range []string{"simple_protocol", "cache_statement"} {
		t.Run(mode, func(t *testing.T) {
			dsn := "postgresql://postgres@127.0.0.1:" + strconv.Itoa(env.DuckPgPort) + "/postgres?default_query_exec_mode=" + mode
			first, err := pgx.Connect(ctx, dsn)
			require.NoError(t, err)
			defer first.Close(ctx)
			second, err := pgx.Connect(ctx, dsn)
			require.NoError(t, err)
			defer second.Close(ctx)
			for _, conn := range []*pgx.Conn{first, second} {
				for _, query := range []string{"SELECT pg_backend_pid()", " /* client */ SELECT pg_catalog.pg_backend_pid( );"} {
					for i := 0; i < 2; i++ {
						var pid int32
						require.NoError(t, conn.QueryRow(ctx, query).Scan(&pid))
						require.Positive(t, pid)
						require.Equal(t, conn.PgConn().PID(), uint32(pid))
					}
				}
				// The compatibility handler must not rewrite function names in literals.
				var literal string
				require.NoError(t, conn.QueryRow(ctx, `SELECT 'pg_backend_pid()'`).Scan(&literal))
				require.Equal(t, "pg_backend_pid()", literal)
				// Superset executes the business query on the same connection afterwards.
				var revenue int
				require.NoError(t, conn.QueryRow(ctx, `SELECT SUM(amount) FROM (VALUES (100), (200)) AS orders(amount)`).Scan(&revenue))
				require.Equal(t, 300, revenue)
			}
			require.NotEqual(t, first.PgConn().PID(), second.PgConn().PID(), "backend identity must distinguish concurrent sessions")
		})
	}
}
