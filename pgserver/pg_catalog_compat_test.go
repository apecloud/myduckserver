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

			_, err = conn.Exec(ctx, `BEGIN READ ONLY`)
			require.NoError(t, err)
			var one int
			require.NoError(t, conn.QueryRow(ctx, `SELECT 1`).Scan(&one))
			require.Equal(t, 1, one)
			_, err = conn.Exec(ctx, `COMMIT`)
			require.NoError(t, err)

			var tz string
			require.NoError(t, conn.QueryRow(ctx, `SHOW timezone`).Scan(&tz))
			require.NotEqual(t, "Local", tz)
			require.NotEmpty(t, tz)

			_, err = conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS app.pk_t (id INT PRIMARY KEY, v VARCHAR(32))`)
			require.NoError(t, err)

			rows, err := conn.Query(ctx, `
SELECT
    result.TABLE_SCHEM, result.TABLE_NAME, result.COLUMN_NAME, result.KEY_SEQ, result.PK_NAME
FROM (SELECT
          n.nspname AS TABLE_SCHEM,
          ct.relname AS TABLE_NAME,
          a.attname AS COLUMN_NAME,
          (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ,
          ci.relname AS PK_NAME,
          information_schema._pg_expandarray(i.indkey) AS KEYS,
          a.attnum AS A_ATTNUM,
          i.indnkeyatts AS KEY_COUNT
      FROM pg_catalog.pg_class ct
           JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)
           JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
           JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid)
           JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
      WHERE n.nspname = 'app' AND ct.relname = 'pk_t' AND i.indisprimary) result
WHERE result.A_ATTNUM = (result.KEYS).x AND result.KEY_SEQ <= result.KEY_COUNT
ORDER BY result.table_name, result.pk_name, result.key_seq`)
			require.NoError(t, err)
			defer rows.Close()
			for rows.Next() {
				var schem, name, col string
				var keySeq int64
				var pkName string
				require.NoError(t, rows.Scan(&schem, &name, &col, &keySeq, &pkName))
			}
			require.NoError(t, rows.Err())
		})
	}
}
