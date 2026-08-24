// Copyright 2024-2025 ApeCloud, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/apecloud/myduckserver/testutil"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestMySQLAutocommitOffSurvivesUse(t *testing.T) {
	testDir := testutil.CreateTestDir(t)
	testEnv := testutil.NewTestEnv()
	workingDir, err := os.Getwd()
	require.NoError(t, err)
	testEnv.OriginalWorkingDir = filepath.Join(workingDir, "pgserver")
	require.NoError(t, testutil.StartDuckSqlServer(t, testDir, nil, testEnv))
	t.Cleanup(func() {
		require.NoError(t, testEnv.MyDuckServer.Close())
		testutil.StopDuckSqlServer(t, testEnv.DuckProcess)
	})

	ctx := context.Background()
	conn, err := testEnv.MyDuckServer.DB.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	for _, query := range []string{
		"SET autocommit=0",
		"CREATE DATABASE task52_python",
		"USE task52_python",
		"CREATE TABLE tb1 (id INT PRIMARY KEY, value DOUBLE)",
		"INSERT INTO tb1 VALUES (1, 1.1)",
		"DROP DATABASE task52_python",
	} {
		_, err = conn.ExecContext(ctx, query)
		require.NoError(t, err, query)
	}
}

func TestMySQLTransactionAndConnectionBoundaries(t *testing.T) {
	testDir := testutil.CreateTestDir(t)
	testEnv := testutil.NewTestEnv()
	workingDir, err := os.Getwd()
	require.NoError(t, err)
	testEnv.OriginalWorkingDir = filepath.Join(workingDir, "pgserver")
	require.NoError(t, testutil.StartDuckSqlServer(t, testDir, nil, testEnv))
	t.Cleanup(func() {
		require.NoError(t, testEnv.MyDuckServer.Close())
		testutil.StopDuckSqlServer(t, testEnv.DuckProcess)
	})

	ctx := context.Background()
	newTable := func(t *testing.T, schema string) *mysqlWireSession {
		t.Helper()
		admin := newMySQLWireSession(t, ctx, testEnv.DuckPort)
		mustExecMySQL(t, ctx, admin,
			"SET autocommit=0",
			"CREATE DATABASE "+schema,
			"USE "+schema,
			"CREATE TABLE items (id INT PRIMARY KEY)",
		)
		t.Cleanup(func() {
			_, _ = admin.conn.ExecContext(ctx, "DROP DATABASE IF EXISTS "+schema)
		})
		return admin
	}

	t.Run("DDL failure leaves connection reusable", func(t *testing.T) {
		const schema = "task52_ddl_failure"
		owner := newMySQLWireSession(t, ctx, testEnv.DuckPort)
		observer := newMySQLWireSession(t, ctx, testEnv.DuckPort)
		mustExecMySQL(t, ctx, owner,
			"SET autocommit=0",
			"CREATE DATABASE "+schema,
			"USE "+schema,
			"CREATE TABLE items (id INT PRIMARY KEY)",
			"INSERT INTO items VALUES (1)",
		)
		t.Cleanup(func() {
			_, _ = owner.conn.ExecContext(ctx, "DROP DATABASE IF EXISTS "+schema)
		})

		_, err := owner.conn.ExecContext(ctx, "CREATE TABLE items (id INT PRIMARY KEY)")
		require.Error(t, err)
		requireMySQLInt(t, ctx, owner, "SELECT 1", 1)
		// MySQL commits the transaction before attempting DDL, even when the DDL fails.
		requireMySQLInt(t, ctx, observer, "SELECT count(*) FROM "+schema+".items", 1)

		mustExecMySQL(t, ctx, owner, "INSERT INTO items VALUES (2)", "ROLLBACK")
		requireMySQLInt(t, ctx, observer, "SELECT count(*) FROM "+schema+".items", 1)
	})

	t.Run("create and drop DDL is immediately visible", func(t *testing.T) {
		const schema = "task52_ddl_visibility"
		owner := newMySQLWireSession(t, ctx, testEnv.DuckPort)
		observer := newMySQLWireSession(t, ctx, testEnv.DuckPort)
		mustExecMySQL(t, ctx, owner, "SET autocommit=0", "CREATE DATABASE "+schema)
		t.Cleanup(func() {
			_, _ = owner.conn.ExecContext(ctx, "DROP DATABASE IF EXISTS "+schema)
		})
		requireMySQLInt(t, ctx, observer,
			"SELECT count(*) FROM information_schema.schemata WHERE schema_name='"+schema+"'", 1)

		mustExecMySQL(t, ctx, owner, "USE "+schema, "CREATE TABLE items (id INT PRIMARY KEY)")
		requireMySQLInt(t, ctx, observer, "SELECT count(*) FROM "+schema+".items", 0)

		mustExecMySQL(t, ctx, owner, "DROP TABLE items")
		_, err := observer.conn.ExecContext(ctx, "SELECT count(*) FROM "+schema+".items")
		require.Error(t, err)

		mustExecMySQL(t, ctx, owner, "DROP DATABASE "+schema)
		requireMySQLInt(t, ctx, observer,
			"SELECT count(*) FROM information_schema.schemata WHERE schema_name='"+schema+"'", 0)
	})

	t.Run("explicit commit and rollback", func(t *testing.T) {
		const schema = "task52_commit_rollback"
		newTable(t, schema)
		writer := newMySQLWireSession(t, ctx, testEnv.DuckPort)
		observer := newMySQLWireSession(t, ctx, testEnv.DuckPort)
		mustExecMySQL(t, ctx, writer, "USE "+schema, "SET autocommit=0", "INSERT INTO items VALUES (1)")
		requireMySQLInt(t, ctx, observer, "SELECT count(*) FROM "+schema+".items", 0)

		mustExecMySQL(t, ctx, writer, "COMMIT")
		requireMySQLInt(t, ctx, observer, "SELECT count(*) FROM "+schema+".items", 1)

		mustExecMySQL(t, ctx, writer, "INSERT INTO items VALUES (2)")
		requireMySQLInt(t, ctx, observer, "SELECT count(*) FROM "+schema+".items", 1)
		mustExecMySQL(t, ctx, writer, "ROLLBACK")
		requireMySQLInt(t, ctx, observer, "SELECT count(*) FROM "+schema+".items", 1)
	})

	t.Run("autocommit transitions commit at the MySQL boundary", func(t *testing.T) {
		const schema = "task52_autocommit_transition"
		newTable(t, schema)
		writer := newMySQLWireSession(t, ctx, testEnv.DuckPort)
		observer := newMySQLWireSession(t, ctx, testEnv.DuckPort)
		mustExecMySQL(t, ctx, writer, "USE "+schema)
		requireMySQLInt(t, ctx, writer, "SELECT @@autocommit", 1)

		mustExecMySQL(t, ctx, writer, "INSERT INTO items VALUES (1)", "SET autocommit=0")
		requireMySQLInt(t, ctx, writer, "SELECT @@autocommit", 0)
		mustExecMySQL(t, ctx, writer, "INSERT INTO items VALUES (2)")
		requireMySQLInt(t, ctx, observer, "SELECT count(*) FROM "+schema+".items", 1)

		mustExecMySQL(t, ctx, writer, "SET autocommit=1")
		requireMySQLInt(t, ctx, writer, "SELECT @@autocommit", 1)
		requireMySQLInt(t, ctx, observer, "SELECT count(*) FROM "+schema+".items", 2)

		mustExecMySQL(t, ctx, writer, "SET autocommit=0", "INSERT INTO items VALUES (3)")
		requireMySQLInt(t, ctx, observer, "SELECT count(*) FROM "+schema+".items", 2)
		mustExecMySQL(t, ctx, writer, "ROLLBACK", "SET autocommit=1", "INSERT INTO items VALUES (4)")
		requireMySQLInt(t, ctx, observer, "SELECT count(*) FROM "+schema+".items", 3)
	})

	t.Run("concurrent sessions keep isolated transactions", func(t *testing.T) {
		const schema = "task52_session_isolation"
		newTable(t, schema)
		sessionA := newMySQLWireSession(t, ctx, testEnv.DuckPort)
		sessionB := newMySQLWireSession(t, ctx, testEnv.DuckPort)
		mustExecMySQL(t, ctx, sessionA, "USE "+schema, "SET autocommit=0", "INSERT INTO items VALUES (1)")
		mustExecMySQL(t, ctx, sessionB, "USE "+schema, "SET autocommit=0")
		requireMySQLInt(t, ctx, sessionB, "SELECT count(*) FROM items WHERE id=1", 0)

		mustExecMySQL(t, ctx, sessionB, "INSERT INTO items VALUES (2)")
		requireMySQLInt(t, ctx, sessionA, "SELECT count(*) FROM items WHERE id=2", 0)
		mustExecMySQL(t, ctx, sessionA, "COMMIT")
		requireMySQLInt(t, ctx, sessionB, "SELECT count(*) FROM items WHERE id=1", 0)

		mustExecMySQL(t, ctx, sessionB, "ROLLBACK")
		requireMySQLInt(t, ctx, sessionB, "SELECT count(*) FROM items WHERE id=1", 1)
		mustExecMySQL(t, ctx, sessionB, "INSERT INTO items VALUES (2)", "COMMIT")
		requireMySQLInt(t, ctx, sessionA, "SELECT count(*) FROM items", 2)
	})

	t.Run("disconnect rollback does not affect another session", func(t *testing.T) {
		const schema = "task52_disconnect_rollback"
		newTable(t, schema)
		disconnecting := newMySQLWireSession(t, ctx, testEnv.DuckPort)
		survivor := newMySQLWireSession(t, ctx, testEnv.DuckPort)
		observer := newMySQLWireSession(t, ctx, testEnv.DuckPort)
		mustExecMySQL(t, ctx, disconnecting, "USE "+schema, "SET autocommit=0", "INSERT INTO items VALUES (1)")
		mustExecMySQL(t, ctx, survivor, "USE "+schema, "SET autocommit=0", "INSERT INTO items VALUES (2)")

		require.NoError(t, disconnecting.Close())
		requireMySQLInt(t, ctx, survivor, "SELECT count(*) FROM items WHERE id=2", 1)
		requireMySQLInt(t, ctx, survivor, "SELECT count(*) FROM items WHERE id=1", 0)
		mustExecMySQL(t, ctx, survivor, "COMMIT")
		requireMySQLInt(t, ctx, observer, "SELECT count(*) FROM "+schema+".items", 1)

		// Reusing the rolled-back key proves the disconnected transaction is gone.
		mustExecMySQL(t, ctx, observer, "INSERT INTO "+schema+".items VALUES (1)")
		requireMySQLInt(t, ctx, observer, "SELECT count(*) FROM "+schema+".items", 2)
	})
}

type mysqlWireSession struct {
	db   *sql.DB
	conn *sql.Conn
}

func newMySQLWireSession(t *testing.T, ctx context.Context, port int) *mysqlWireSession {
	t.Helper()
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%d)/", port))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	var one int
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT 1").Scan(&one))
	require.Equal(t, 1, one)
	session := &mysqlWireSession{db: db, conn: conn}
	t.Cleanup(func() {
		require.NoError(t, session.Close())
	})
	return session
}

func (s *mysqlWireSession) Close() error {
	var connErr, dbErr error
	if s.conn != nil {
		connErr = s.conn.Close()
		s.conn = nil
	}
	if s.db != nil {
		dbErr = s.db.Close()
		s.db = nil
	}
	return errors.Join(connErr, dbErr)
}

func mustExecMySQL(t *testing.T, ctx context.Context, session *mysqlWireSession, queries ...string) {
	t.Helper()
	for _, query := range queries {
		_, err := session.conn.ExecContext(ctx, query)
		require.NoError(t, err, query)
	}
}

func requireMySQLInt(t *testing.T, ctx context.Context, session *mysqlWireSession, query string, expected int) {
	t.Helper()
	var actual int
	require.NoError(t, session.conn.QueryRowContext(ctx, query).Scan(&actual), query)
	require.Equal(t, expected, actual, query)
}

func TestDMLWireResults(t *testing.T) {
	testDir := testutil.CreateTestDir(t)
	testEnv := testutil.NewTestEnv()
	workingDir, err := os.Getwd()
	require.NoError(t, err)
	testEnv.OriginalWorkingDir = filepath.Join(workingDir, "pgserver")
	require.NoError(t, testutil.StartDuckSqlServer(t, testDir, nil, testEnv))
	t.Cleanup(func() {
		require.NoError(t, testEnv.MyDuckServer.Close())
		testutil.StopDuckSqlServer(t, testEnv.DuckProcess)
	})

	var version string
	require.NoError(t, testEnv.MyDuckServer.QueryRow("SELECT @@version").Scan(&version))
	require.Equal(t, "8.0.23", version)

	_, err = testEnv.MyDuckServer.Exec("CREATE DATABASE task52_update")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("USE task52_update")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("CREATE TABLE tb1 (id INT PRIMARY KEY, value DOUBLE)")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("INSERT INTO tb1 VALUES (1, 1.1)")
	require.NoError(t, err)
	result, err := testEnv.MyDuckServer.Exec("UPDATE tb1 SET value=3.3 WHERE id=1")
	require.NoError(t, err)
	requireRowsAffected(t, result, 1)

	var value float64
	require.NoError(t, testEnv.MyDuckServer.QueryRow("SELECT value FROM tb1 WHERE id=1").Scan(&value))
	require.Equal(t, 3.3, value)

	stmt, err := testEnv.MyDuckServer.Prepare("UPDATE tb1 SET value=? WHERE id=?")
	require.NoError(t, err)
	defer stmt.Close()
	_, err = stmt.Exec(4.4, 1)
	require.ErrorContains(t, err, "incorrect argument count for command: have 0 want 2")
	require.NoError(t, testEnv.MyDuckServer.QueryRow("SELECT value FROM tb1 WHERE id=1").Scan(&value))
	require.Equal(t, 3.3, value)

	for range 2 {
		result, err = testEnv.MyDuckServer.Exec("UPDATE tb1 SET value=value+1 WHERE id=1")
		require.NoError(t, err)
		requireRowsAffected(t, result, 1)
	}

	require.NoError(t, testEnv.MyDuckServer.QueryRow("SELECT value FROM tb1 WHERE id=1").Scan(&value))
	require.InDelta(t, 5.3, value, 0.000001)

	_, err = testEnv.MyDuckServer.Exec("CREATE TABLE mysql_delete_rows (id INT PRIMARY KEY, value INT)")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("INSERT INTO mysql_delete_rows VALUES (1, 10), (2, 20), (3, 30)")
	require.NoError(t, err)

	result, err = testEnv.MyDuckServer.Exec("DELETE FROM mysql_delete_rows WHERE id=2")
	require.NoError(t, err)
	requireRowsAffected(t, result, 1)
	result, err = testEnv.MyDuckServer.Exec("DELETE FROM mysql_delete_rows WHERE id=2")
	require.NoError(t, err)
	requireRowsAffected(t, result, 0)
	_, err = testEnv.MyDuckServer.Exec("DELETE FROM mysql_delete_rows WHERE missing_column=1")
	require.Error(t, err)
	requireMySQLRows(t, testEnv.MyDuckServer, "SELECT id, value FROM mysql_delete_rows ORDER BY id", [][]int{{1, 10}, {3, 30}})

	_, err = testEnv.MyDuckServer.Exec("CREATE TABLE pg_delete_rows (id INT PRIMARY KEY, value INT)")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("INSERT INTO pg_delete_rows VALUES (1, 10), (2, 20), (3, 30)")
	require.NoError(t, err)

	ctx := context.Background()
	pgConn, err := pgx.Connect(ctx, "postgresql://postgres@127.0.0.1:"+strconv.Itoa(testEnv.DuckPgPort)+"/postgres")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, pgConn.Close(ctx))
	})

	tag, err := pgConn.Exec(ctx, "DELETE FROM task52_update.pg_delete_rows WHERE id=2")
	require.NoError(t, err)
	require.Equal(t, int64(1), tag.RowsAffected())
	tag, err = pgConn.Exec(ctx, "DELETE FROM task52_update.pg_delete_rows WHERE id=2")
	require.NoError(t, err)
	require.Equal(t, int64(0), tag.RowsAffected())
	_, err = pgConn.Exec(ctx, "DELETE FROM task52_update.pg_delete_rows WHERE missing_column=1")
	require.Error(t, err)
	requirePostgresRows(t, ctx, pgConn, "SELECT id, value FROM task52_update.pg_delete_rows ORDER BY id", [][]int{{1, 10}, {3, 30}})
}

func requireRowsAffected(t *testing.T, result sql.Result, expected int64) {
	t.Helper()
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, expected, affected)
}

type mysqlWireQueryer interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

func requireMySQLRows(t *testing.T, db mysqlWireQueryer, query string, expected [][]int) {
	t.Helper()
	rows, err := db.Query(query)
	require.NoError(t, err)
	defer rows.Close()
	requireRows(t, rows, expected)
}

func requirePostgresRows(t *testing.T, ctx context.Context, conn *pgx.Conn, query string, expected [][]int) {
	t.Helper()
	rows, err := conn.Query(ctx, query)
	require.NoError(t, err)
	defer rows.Close()
	requireRows(t, rows, expected)
}

type dmlWireRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func requireRows(t *testing.T, rows dmlWireRows, expected [][]int) {
	t.Helper()
	var actual [][]int
	for rows.Next() {
		var id, value int
		require.NoError(t, rows.Scan(&id, &value))
		actual = append(actual, []int{id, value})
	}
	require.NoError(t, rows.Err())
	require.Equal(t, expected, actual)
}
