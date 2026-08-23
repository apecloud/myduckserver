package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apecloud/myduckserver/testutil"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrdinaryQueryAuditProtocols(t *testing.T) {
	testDir := testutil.CreateTestDir(t)
	testEnv := testutil.NewTestEnv()
	workingDir, err := os.Getwd()
	require.NoError(t, err)
	testEnv.OriginalWorkingDir = filepath.Join(workingDir, "pgserver")
	require.NoError(t, testutil.StartDuckSqlServer(t, testDir, nil, testEnv))
	defer testutil.StopDuckSqlServer(t, testEnv.DuckProcess)

	mysqlSimple := "SELECT 'utf8mb4_uca1400_ai_ci' AS audit_mysql_simple UNION ALL SELECT '4302' UNION ALL SELECT '4303'"
	mysqlRows, err := testEnv.MyDuckServer.Query(mysqlSimple)
	requireSQLRows(t, mysqlRows, err, 3)

	mysqlPrepared := "SELECT ? AS audit_mysql_prepared UNION ALL SELECT 4305"
	stmt, err := testEnv.MyDuckServer.Prepare(mysqlPrepared)
	require.NoError(t, err)
	mysqlRows, err = stmt.Query(4304)
	requireSQLRows(t, mysqlRows, err, 2)
	require.NoError(t, stmt.Close())

	mysqlMulti := "SELECT 4310 AS audit_mysql_multi_first; SELECT 4311 AS audit_mysql_multi_second UNION ALL SELECT 4312"
	multiDB, err := sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%d)/?multiStatements=true", testEnv.DuckPort))
	require.NoError(t, err)
	defer multiDB.Close()
	mysqlRows, err = multiDB.Query(mysqlMulti)
	requireSQLResultSets(t, mysqlRows, err, []int{1, 2})

	ctx := context.Background()
	pgConfig, err := pgx.ParseConfig("postgresql://audit_pg_user@127.0.0.1:" + strconv.Itoa(testEnv.DuckPgPort) + "/postgres")
	require.NoError(t, err)
	pgConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pgConn, err := pgx.ConnectConfig(ctx, pgConfig)
	require.NoError(t, err)
	defer pgConn.Close(ctx)

	pgSimple := "SELECT generate_series AS audit_pg_simple FROM generate_series(1, 130)"
	requirePgQueryRows(t, ctx, pgConn, pgSimple, 130)

	pgPrepared := "SELECT $1::INTEGER AS audit_pg_prepared UNION ALL SELECT 4309"
	_, err = pgConn.Prepare(ctx, "audit_pg_prepared", pgPrepared)
	require.NoError(t, err)
	requirePgQueryRows(t, ctx, pgConn, "audit_pg_prepared", 2, 4308)

	pgShowSimple := "SHOW search_path"
	requirePgQueryRows(t, ctx, pgConn, pgShowSimple, 1)

	pgShowPrepared := "SHOW application_name"
	_, err = pgConn.Prepare(ctx, "audit_pg_show", pgShowPrepared)
	require.NoError(t, err)
	requirePgQueryRows(t, ctx, pgConn, "audit_pg_show", 1)

	pgPSQL := "select n.nspname as \"name\",\n  pg_catalog.pg_get_userbyid(n.nspowner) as \"owner\"\nfrom pg_catalog.pg_namespace n\nwhere n.nspname !~ '^pg_' and n.nspname <> 'information_schema'\norder by 1;"
	requirePgQueryRows(t, ctx, pgConn, pgPSQL, 1)

	pgWorkbench := "SELECT * FROM CURRENT_DATABASE();"
	_, err = pgConn.Exec(ctx, pgWorkbench)
	require.Error(t, err)

	require.NoError(t, testEnv.DuckLogFile.Sync())
	expectations := [][]string{
		{"audit=query", "protocol=mysql", "user=root", "rows=3", fmt.Sprintf("query=%q", mysqlSimple)},
		{"audit=query", "protocol=mysql", "user=root", "rows=2", fmt.Sprintf("query=%q", mysqlPrepared)},
		{"audit=query", "protocol=mysql", "user=root", "rows=1", fmt.Sprintf("query=%q", mysqlMulti)},
		{"audit=query", "protocol=mysql", "user=root", "rows=2", `query=" SELECT 4311 AS audit_mysql_multi_second UNION ALL SELECT 4312"`},
		{"audit=query", "protocol=postgres", "user=audit_pg_user", "rows=130", fmt.Sprintf("query=%q", pgSimple)},
		{"audit=query", "protocol=postgres", "user=audit_pg_user", "rows=2", fmt.Sprintf("query=%q", pgPrepared)},
		{"audit=query", "protocol=postgres", "user=audit_pg_user", "rows=1", fmt.Sprintf("query=%q", pgShowSimple)},
		{"audit=query", "protocol=postgres", "user=audit_pg_user", "rows=1", fmt.Sprintf("query=%q", pgShowPrepared)},
		{"audit=query", "protocol=postgres", "user=audit_pg_user", "rows=1", fmt.Sprintf("query=%q", pgPSQL)},
		{"audit=query", "protocol=postgres", "user=audit_pg_user", "rows=0", "error=", fmt.Sprintf("query=%q", pgWorkbench)},
	}

	var logContents string
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		contents, readErr := os.ReadFile(testEnv.DuckLogFilePath)
		assert.NoError(collect, readErr)
		logContents = string(contents)
		for _, expectation := range expectations {
			assertAuditLine(collect, logContents, expectation)
		}
		assert.Len(collect, auditLines(logContents), len(expectations))
	}, 3*time.Second, 25*time.Millisecond, "query audit records not found in %s:\n%s", testEnv.DuckLogFilePath, logContents)
}

func requireSQLRows(t *testing.T, rows *sql.Rows, err error, expected int) {
	t.Helper()
	require.NoError(t, err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, expected, count)
}

func requireSQLResultSets(t *testing.T, rows *sql.Rows, err error, expected []int) {
	t.Helper()
	require.NoError(t, err)
	defer rows.Close()

	actual := make([]int, 0, len(expected))
	for {
		count := 0
		for rows.Next() {
			count++
		}
		require.NoError(t, rows.Err())
		actual = append(actual, count)
		if !rows.NextResultSet() {
			break
		}
	}
	require.Equal(t, expected, actual)
}

func requirePgQueryRows(t *testing.T, ctx context.Context, conn *pgx.Conn, query string, expected int, args ...any) {
	t.Helper()
	rows, err := conn.Query(ctx, query, args...)
	require.NoError(t, err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, expected, count)
}

func assertAuditLine(t assert.TestingT, contents string, fields []string) {
	for _, line := range strings.Split(contents, "\n") {
		matches := true
		for _, field := range fields {
			if !strings.Contains(line, field) {
				matches = false
				break
			}
		}
		if matches {
			return
		}
	}
	assert.Fail(t, "query audit record not found", "required fields: %v", fields)
}

func auditLines(contents string) []string {
	var lines []string
	for _, line := range strings.Split(contents, "\n") {
		if strings.Contains(line, "audit=query") {
			lines = append(lines, line)
		}
	}
	return lines
}
