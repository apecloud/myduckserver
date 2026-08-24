package pgserver

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/testutil"
	"github.com/jackc/pgx/v5"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

type managedCommentWireState struct {
	tableRaw       string
	generatedRaw   string
	tableComment   *catalog.Comment[catalog.ExtraTableInfo]
	generatedMeta  *catalog.Comment[catalog.MySQLType]
	primaryKeys    int64
	physicalChecks int64
	sequences      int64
	generatedExpr  string
}

func TestAlterTableCommentWireProtocols(t *testing.T) {
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

	db := testEnv.MyDuckServer
	_, err = db.Exec("CREATE DATABASE comment_wire")
	require.NoError(t, err)
	_, err = db.Exec("USE comment_wire")
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE comment_meta (
		id INT AUTO_INCREMENT PRIMARY KEY,
		required INT NOT NULL,
		source JSON,
		generated_v VECTOR(2) NOT NULL GENERATED ALWAYS AS (STRING_TO_VECTOR(source)) STORED,
		CONSTRAINT required_positive CHECK (required > 0)
	) COMMENT='before'`)
	require.NoError(t, err)

	ctx := context.Background()
	pgURL := "postgresql://postgres@127.0.0.1:" + strconv.Itoa(testEnv.DuckPgPort) + "/postgres"
	pgConfig, err := pgx.ParseConfig(pgURL)
	require.NoError(t, err)
	pgConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pgConn, err := pgx.ConnectConfig(ctx, pgConfig)
	require.NoError(t, err)

	initial := readManagedCommentWireState(t, ctx, pgConn, "comment_meta")
	requireManagedCommentWireState(t, initial, "before", catalog.ExtraTableInfo{}, catalog.MySQLType{})
	require.NotEmpty(t, initial.tableComment.Meta.Sequence)
	require.Equal(t, []int{0}, initial.tableComment.Meta.PkOrdinals)
	require.Len(t, initial.tableComment.Meta.Checks, 1)
	require.Equal(t, "required_positive", initial.tableComment.Meta.Checks[0].Name)
	require.Equal(t, "VECTOR", initial.generatedMeta.Meta.Name)
	require.Equal(t, uint32(2), initial.generatedMeta.Meta.Length)
	require.Equal(t, "STRING_TO_VECTOR(source)", strings.ReplaceAll(initial.generatedMeta.Meta.Generated, "`", ""))
	require.NotNil(t, initial.generatedMeta.Meta.Nullable)
	require.False(t, *initial.generatedMeta.Meta.Nullable)

	_, err = db.Exec("ALTER TABLE comment_meta COMMENT='after'")
	require.NoError(t, err)
	requireMySQLCommentMetadata(t, db, "after")
	after := readManagedCommentWireState(t, ctx, pgConn, "comment_meta")
	requireManagedCommentWireState(t, after, "after", initial.tableComment.Meta, initial.generatedMeta.Meta)
	require.NotEqual(t, initial.tableRaw, after.tableRaw)
	require.Equal(t, initial.generatedRaw, after.generatedRaw)

	_, err = db.Exec("ALTER TABLE comment_meta COMMENT=''")
	require.NoError(t, err)
	requireMySQLCommentMetadata(t, db, "")
	cleared := readManagedCommentWireState(t, ctx, pgConn, "comment_meta")
	requireManagedCommentWireState(t, cleared, "", initial.tableComment.Meta, initial.generatedMeta.Meta)
	require.NotEqual(t, after.tableRaw, cleared.tableRaw)
	require.Equal(t, initial.generatedRaw, cleared.generatedRaw)

	_, err = db.Exec("ALTER TABLE comment_meta COMMENT='final'")
	require.NoError(t, err)
	requireMySQLCommentMetadata(t, db, "final")
	final := readManagedCommentWireState(t, ctx, pgConn, "comment_meta")
	requireManagedCommentWireState(t, final, "final", initial.tableComment.Meta, initial.generatedMeta.Meta)
	require.Equal(t, initial.generatedRaw, final.generatedRaw)

	// Reopening the PostgreSQL client must reload the same persisted DuckDB
	// state. Provider-level clean reopen is covered by the root catalog test.
	require.NoError(t, pgConn.Close(ctx))
	pgConn, err = pgx.ConnectConfig(ctx, pgConfig)
	require.NoError(t, err)
	defer pgConn.Close(ctx)
	reopened := readManagedCommentWireState(t, ctx, pgConn, "comment_meta")
	require.Equal(t, final.tableRaw, reopened.tableRaw)
	require.Equal(t, final.generatedRaw, reopened.generatedRaw)
	requireManagedCommentWireState(t, reopened, "final", initial.tableComment.Meta, initial.generatedMeta.Meta)

	// PostgreSQL COMMENT ON TABLE bypasses the MySQL catalog interface and
	// overwrites the managed table comment. Keep this destructive boundary on a
	// disposable table so the supported cross-protocol path above stays intact.
	_, err = db.Exec(`CREATE TABLE pg_comment_boundary (
		id INT AUTO_INCREMENT PRIMARY KEY,
		required INT NOT NULL,
		source JSON,
		generated_v VECTOR(2) NOT NULL GENERATED ALWAYS AS (STRING_TO_VECTOR(source)) STORED,
		CONSTRAINT pg_required_positive CHECK (required > 0)
	) COMMENT='pg before'`)
	require.NoError(t, err)
	boundaryBefore := readManagedCommentWireState(t, ctx, pgConn, "pg_comment_boundary")
	require.Equal(t, "pg before", boundaryBefore.tableComment.Text)
	require.NotEmpty(t, boundaryBefore.tableComment.Meta.Sequence)
	require.Equal(t, []int{0}, boundaryBefore.tableComment.Meta.PkOrdinals)
	require.Len(t, boundaryBefore.tableComment.Meta.Checks, 1)

	_, err = pgConn.Exec(ctx, "COMMENT ON TABLE comment_wire.pg_comment_boundary IS 'postgres direct'")
	require.NoError(t, err)
	boundaryAfter := readManagedCommentWireState(t, ctx, pgConn, "pg_comment_boundary")
	require.Equal(t, "postgres direct", boundaryAfter.tableRaw)
	require.Equal(t, "postgres direct", boundaryAfter.tableComment.Text)
	require.Equal(t, catalog.ExtraTableInfo{}, boundaryAfter.tableComment.Meta)
	require.Equal(t, boundaryBefore.generatedRaw, boundaryAfter.generatedRaw)
	require.Equal(t, boundaryBefore.generatedMeta.Meta, boundaryAfter.generatedMeta.Meta)
	require.Equal(t, int64(1), boundaryAfter.primaryKeys)
	require.Equal(t, int64(2), boundaryAfter.sequences)
}

func readManagedCommentWireState(t *testing.T, ctx context.Context, conn *pgx.Conn, tableName string) managedCommentWireState {
	t.Helper()

	var state managedCommentWireState
	require.NoError(t, conn.QueryRow(ctx, `
		SELECT comment
		FROM duckdb_tables()
		WHERE database_name = 'myduck'
		  AND schema_name = 'comment_wire'
		  AND table_name = $1
	`, tableName).Scan(&state.tableRaw))
	require.NoError(t, conn.QueryRow(ctx, `
		SELECT comment, column_default
		FROM duckdb_columns()
		WHERE database_name = 'myduck'
		  AND schema_name = 'comment_wire'
		  AND table_name = $1
		  AND column_name = 'generated_v'
	`, tableName).Scan(&state.generatedRaw, &state.generatedExpr))
	require.NoError(t, conn.QueryRow(ctx, `
		SELECT count(*)
		FROM duckdb_constraints()
		WHERE database_name = 'myduck'
		  AND schema_name = 'comment_wire'
		  AND table_name = $1
		  AND constraint_type = 'PRIMARY KEY'
	`, tableName).Scan(&state.primaryKeys))
	require.NoError(t, conn.QueryRow(ctx, `
		SELECT count(*)
		FROM duckdb_constraints()
		WHERE database_name = 'myduck'
		  AND schema_name = 'comment_wire'
		  AND table_name = $1
		  AND constraint_type = 'CHECK'
	`, tableName).Scan(&state.physicalChecks))
	require.NoError(t, conn.QueryRow(ctx, `
		SELECT count(*)
		FROM duckdb_sequences()
		WHERE database_name = 'myduck'
		  AND schema_name = '__sys__'
		  AND sequence_name LIKE '__sys_table_seq_%'
	`).Scan(&state.sequences))

	state.tableComment = catalog.DecodeComment[catalog.ExtraTableInfo](state.tableRaw)
	state.generatedMeta = catalog.DecodeComment[catalog.MySQLType](state.generatedRaw)
	return state
}

func requireManagedCommentWireState(
	t *testing.T,
	state managedCommentWireState,
	text string,
	expectedTableMeta catalog.ExtraTableInfo,
	expectedGeneratedMeta catalog.MySQLType,
) {
	t.Helper()
	require.True(t, strings.HasPrefix(state.tableRaw, catalog.ManagedCommentPrefix))
	require.True(t, strings.HasPrefix(state.generatedRaw, catalog.ManagedCommentPrefix))
	require.Equal(t, text, state.tableComment.Text)
	if expectedTableMeta.Sequence != "" {
		require.Equal(t, expectedTableMeta, state.tableComment.Meta)
	}
	if expectedGeneratedMeta.Name != "" {
		require.Equal(t, expectedGeneratedMeta, state.generatedMeta.Meta)
	}
	require.Equal(t, int64(1), state.primaryKeys)
	// MyDuck keeps ALTER CHECK metadata in the managed table comment because
	// DuckDB cannot add that constraint shape after table creation.
	require.Zero(t, state.physicalChecks)
	require.Equal(t, int64(1), state.sequences)
	require.Contains(t, strings.ToLower(state.generatedExpr), "string_to_vector")
	require.Contains(t, state.generatedExpr, "2")
}

func requireMySQLCommentMetadata(t *testing.T, db *sqlx.DB, expectedComment string) {
	t.Helper()

	var actualComment string
	require.NoError(t, db.QueryRow(`
		SELECT table_comment
		FROM information_schema.tables
		WHERE table_schema = 'comment_wire'
		  AND table_name = 'comment_meta'
	`).Scan(&actualComment))
	require.Equal(t, expectedComment, actualComment)

	var tableName, createStatement string
	require.NoError(t, db.QueryRow("SHOW CREATE TABLE comment_meta").Scan(&tableName, &createStatement))
	require.Equal(t, "comment_meta", tableName)
	lowerCreate := strings.ToLower(createStatement)
	require.Contains(t, lowerCreate, "auto_increment")
	require.Contains(t, lowerCreate, "primary key")
	require.Contains(t, lowerCreate, "required_positive")
	require.Contains(t, lowerCreate, "generated always as")
	require.Contains(t, lowerCreate, "not null")
	if expectedComment == "" {
		require.NotContains(t, lowerCreate, "comment='")
	} else {
		require.Contains(t, createStatement, "COMMENT='"+expectedComment+"'")
	}

	var nullable, extra string
	require.NoError(t, db.QueryRow(`
		SELECT is_nullable, extra
		FROM information_schema.columns
		WHERE table_schema = 'comment_wire'
		  AND table_name = 'comment_meta'
		  AND column_name = 'generated_v'
	`).Scan(&nullable, &extra))
	require.Equal(t, "NO", nullable)
	require.Equal(t, "STORED GENERATED", strings.ToUpper(extra))
}
