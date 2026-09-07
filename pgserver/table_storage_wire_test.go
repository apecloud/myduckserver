package pgserver

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/testutil"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestTableStorageSelectionWireDefaultOff(t *testing.T) {
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

	db := testEnv.MyDuckServer
	_, err = db.Exec("CREATE DATABASE storage_wire")
	require.NoError(t, err)
	_, err = db.Exec("USE storage_wire")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE mysql_object (id INT, payload VARCHAR(32)) ENGINE=DUCKLAKE")
	require.ErrorContains(t, err, "DuckLake service configuration is disabled")
	_, err = db.Exec("CREATE TABLE mysql_default (id INT)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE mysql_local (id INT) ENGINE=LOCAL")
	require.NoError(t, err)

	ctx := context.Background()
	pg, err := pgx.Connect(ctx, "postgresql://postgres@127.0.0.1:"+strconv.Itoa(testEnv.DuckPgPort)+"/postgres")
	require.NoError(t, err)
	t.Cleanup(func() { _ = pg.Close(ctx) })
	requireStorageKindPG(t, ctx, pg, "storage_wire", "mysql_default", catalog.TableStorageLocal)
	requireStorageKindPG(t, ctx, pg, "storage_wire", "mysql_local", catalog.TableStorageLocal)

	_, err = pg.Exec(ctx, "CREATE TABLE storage_wire.pg_object (id INTEGER) WITH (myduck_storage = 'object')")
	require.ErrorContains(t, err, "DuckLake service configuration is disabled")
	_, err = pg.Exec(ctx, "CREATE TABLE storage_wire.pg_default (id INTEGER)")
	require.NoError(t, err)
	requireStorageKindPG(t, ctx, pg, "storage_wire", "pg_default", catalog.TableStorageLocal)
}

func requireStorageKindPG(t *testing.T, ctx context.Context, db *pgx.Conn, catalogName, tableName string, want catalog.TableStorageKind) {
	t.Helper()
	var raw string
	// PostgreSQL exposes MyDuck's physical catalog as `myduck` and maps the
	// user database to the schema name.
	query := fmt.Sprintf("SELECT comment FROM duckdb_tables() WHERE database_name = 'myduck' AND schema_name = '%s' AND table_name = '%s'", catalogName, tableName)
	require.NoError(t, db.QueryRow(ctx, query).Scan(&raw))
	require.Equal(t, want, catalog.DecodeComment[catalog.ExtraTableInfo](raw).Meta.StorageKind())
}
