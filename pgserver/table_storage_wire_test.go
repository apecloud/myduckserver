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

func TestTableStorageSelectionWireMetadata(t *testing.T) {
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
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE mysql_default (id INT)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE mysql_local (id INT) ENGINE=LOCAL")
	require.NoError(t, err)

	ctx := context.Background()
	pg, err := pgx.Connect(ctx, "postgresql://postgres@127.0.0.1:"+strconv.Itoa(testEnv.DuckPgPort)+"/postgres")
	require.NoError(t, err)
	t.Cleanup(func() { _ = pg.Close(ctx) })
	requireStorageKindPG(t, ctx, pg, "storage_wire", "mysql_object", catalog.TableStorageObject)
	requireStorageKindPG(t, ctx, pg, "storage_wire", "mysql_default", catalog.TableStorageLocal)
	requireStorageKindPG(t, ctx, pg, "storage_wire", "mysql_local", catalog.TableStorageLocal)

	_, err = pg.Exec(ctx, "CREATE TABLE storage_wire.pg_object (id INTEGER) WITH (myduck_storage = 'object')")
	require.NoError(t, err)
	_, err = pg.Exec(ctx, "CREATE TABLE storage_wire.pg_default (id INTEGER)")
	require.NoError(t, err)
	_, err = pg.Prepare(ctx, "storage_wire_prepared_object", "CREATE TABLE storage_wire.pg_prepared_object (id INTEGER) WITH (myduck_storage = 'object')")
	require.NoError(t, err)
	_, err = pg.Exec(ctx, "storage_wire_prepared_object")
	require.NoError(t, err)
	requireStorageKindPG(t, ctx, pg, "storage_wire", "pg_object", catalog.TableStorageObject)
	requireStorageKindPG(t, ctx, pg, "storage_wire", "pg_default", catalog.TableStorageLocal)
	requireStorageKindPG(t, ctx, pg, "storage_wire", "pg_prepared_object", catalog.TableStorageObject)
}

func TestTableStorageSelectionIfNotExistsPreservesExistingTables(t *testing.T) {
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

	_, err = testEnv.MyDuckServer.Exec("CREATE DATABASE storage_if_not_exists")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("USE storage_if_not_exists")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("CREATE TABLE existing_local (id INT) ENGINE=LOCAL")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("CREATE TABLE existing_object (id INT) ENGINE=DUCKLAKE")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("ALTER TABLE existing_local COMMENT='local original'")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("ALTER TABLE existing_object COMMENT='object original'")
	require.NoError(t, err)

	ctx := context.Background()
	dsn := "postgresql://postgres@127.0.0.1:" + strconv.Itoa(testEnv.DuckPgPort) + "/postgres"
	simpleConfig, err := pgx.ParseConfig(dsn)
	require.NoError(t, err)
	simpleConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	simple, err := pgx.ConnectConfig(ctx, simpleConfig)
	require.NoError(t, err)
	t.Cleanup(func() { _ = simple.Close(ctx) })

	prepared, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = prepared.Close(ctx) })

	localBefore := readTableCommentPG(t, ctx, simple, "storage_if_not_exists", "existing_local")
	objectBefore := readTableCommentPG(t, ctx, simple, "storage_if_not_exists", "existing_object")
	require.Equal(t, catalog.TableStorageLocal, catalog.DecodeComment[catalog.ExtraTableInfo](localBefore).Meta.StorageKind())
	require.Equal(t, catalog.TableStorageObject, catalog.DecodeComment[catalog.ExtraTableInfo](objectBefore).Meta.StorageKind())

	// Simple protocol: an existing local table must not be rewritten by an
	// opposite requested selector.
	_, err = simple.Exec(ctx, "CREATE TABLE IF NOT EXISTS storage_if_not_exists.existing_local (id INTEGER) WITH (myduck_storage = 'object')")
	require.NoError(t, err)
	localAfter := readTableCommentPG(t, ctx, simple, "storage_if_not_exists", "existing_local")
	require.Equal(t, localBefore, localAfter)
	require.Equal(t, catalog.TableStorageLocal, catalog.DecodeComment[catalog.ExtraTableInfo](localAfter).Meta.StorageKind())

	// The simple path still persists an explicit selector for a new table.
	_, err = simple.Exec(ctx, "CREATE TABLE IF NOT EXISTS storage_if_not_exists.new_simple_object (id INTEGER) WITH (myduck_storage = 'object')")
	require.NoError(t, err)
	requireStorageKindPG(t, ctx, simple, "storage_if_not_exists", "new_simple_object", catalog.TableStorageObject)

	// Extended protocol: the prepared path must preserve an existing object
	// table when the request asks for local storage.
	_, err = prepared.Prepare(ctx, "if_not_exists_existing_object", "CREATE TABLE IF NOT EXISTS storage_if_not_exists.existing_object (id INTEGER) WITH (myduck_storage = 'local')")
	require.NoError(t, err)
	_, err = prepared.Exec(ctx, "if_not_exists_existing_object")
	require.NoError(t, err)
	objectAfter := readTableCommentPG(t, ctx, prepared, "storage_if_not_exists", "existing_object")
	require.Equal(t, objectBefore, objectAfter)
	require.Equal(t, catalog.TableStorageObject, catalog.DecodeComment[catalog.ExtraTableInfo](objectAfter).Meta.StorageKind())

	// The prepared path still persists an explicit local selector for a new
	// table.
	_, err = prepared.Prepare(ctx, "if_not_exists_new_local", "CREATE TABLE IF NOT EXISTS storage_if_not_exists.new_prepared_local (id INTEGER) WITH (myduck_storage = 'local')")
	require.NoError(t, err)
	_, err = prepared.Exec(ctx, "if_not_exists_new_local")
	require.NoError(t, err)
	requireStorageKindPG(t, ctx, prepared, "storage_if_not_exists", "new_prepared_local", catalog.TableStorageLocal)
}

func readTableCommentPG(t *testing.T, ctx context.Context, db *pgx.Conn, catalogName, tableName string) string {
	t.Helper()
	var raw string
	query := fmt.Sprintf("SELECT comment FROM duckdb_tables() WHERE database_name = 'myduck' AND schema_name = '%s' AND table_name = '%s'", catalogName, tableName)
	require.NoError(t, db.QueryRow(ctx, query).Scan(&raw))
	return raw
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
