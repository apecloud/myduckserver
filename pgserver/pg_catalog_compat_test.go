package pgserver

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/apecloud/myduckserver/testutil"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestPGCatalogMetabaseAndSupersetCompat(t *testing.T) {
	testDir := testutil.CreateTestDir(t)
	testEnv := testutil.NewTestEnv()
	require.NoError(t, testutil.StartDuckSqlServer(t, testDir, nil, testEnv))
	defer testutil.StopDuckSqlServer(t, testEnv.DuckProcess)

	_, err := testEnv.MyDuckServer.Exec("CREATE DATABASE app")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("CREATE TABLE app.pg_local (id INT, v VARCHAR(32))")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("INSERT INTO app.pg_local VALUES (1, 'local-pg')")
	require.NoError(t, err)

	ctx := context.Background()
	queryExecModes := []string{"cache_statement", "simple_protocol"}
	for _, mode := range queryExecModes {
		t.Run(mode, func(t *testing.T) {
			conn, err := pgx.Connect(ctx, "postgresql://postgres@127.0.0.1:"+strconv.Itoa(testEnv.DuckPgPort)+"/postgres?default_query_exec_mode="+mode)
			require.NoError(t, err)
			defer conn.Close(ctx)

			var nspname string
			require.NoError(t, conn.QueryRow(ctx, `SELECT nspname FROM pg_namespace WHERE nspname = 'app'`).Scan(&nspname))
			require.Equal(t, "app", nspname)

			var quoted string
			require.NoError(t, conn.QueryRow(ctx, `SELECT nspname FROM "pg_catalog"."pg_namespace" WHERE nspname = 'app'`).Scan(&quoted))
			require.Equal(t, "app", quoted)

			var relname, schema string
			require.NoError(t, conn.QueryRow(ctx, `
SELECT c.relname, n.nspname
FROM pg_catalog.pg_class AS c
INNER JOIN pg_catalog.pg_namespace AS n ON c.relnamespace = n.oid
WHERE c.relkind IN ('r', 'p', 'v', 'f', 'm')
  AND n.nspname = 'app'
  AND c.relname = 'pg_local'`).Scan(&relname, &schema))
			require.Equal(t, "pg_local", relname)
			require.Equal(t, "app", schema)

			var banner string
			require.NoError(t, conn.QueryRow(ctx, `select pg_catalog.version()`).Scan(&banner))
			require.True(t, strings.HasPrefix(banner, "PostgreSQL "), "got %q", banner)

			var duckVersion string
			require.NoError(t, conn.QueryRow(ctx, `select version()`).Scan(&duckVersion))
			require.NotEmpty(t, duckVersion)
			require.NotEqual(t, banner, duckVersion)
		})
	}
}
