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
package pgserver

import (
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/apecloud/myduckserver/testutil"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func TestCreateViewDatabaseSelectionWire(t *testing.T) {
	originalWorkingDir, err := os.Getwd()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Chdir(originalWorkingDir))
	}()

	testDir := testutil.CreateTestDir(t)
	testEnv := testutil.NewTestEnv()
	require.NoError(t, testutil.StartDuckSqlServer(t, testDir, nil, testEnv))
	defer testutil.StopDuckSqlServer(t, testEnv.DuckProcess)
	workingDirAfterStart, err := os.Getwd()
	require.NoError(t, err)
	require.Equal(t, originalWorkingDir, workingDirAfterStart)

	db := testEnv.MyDuckServer
	_, err = db.Exec("CREATE VIEW unqualified_without_database AS SELECT 1")
	require.ErrorContains(t, err, "no database selected")
	assertCurrentDatabase(t, db, "")

	requireExec(t, db, "CREATE DATABASE view_no_default")
	requireExec(t, db, "CREATE TABLE view_no_default.source_table (id INT)")
	requireExec(t, db, "CREATE VIEW view_no_default.qualified_view AS SELECT id FROM view_no_default.source_table")
	assertCurrentDatabase(t, db, "")
	assertViewDefinition(t, db, "view_no_default", "qualified_view", "view_no_default.source_table")

	requireExec(t, db, "CREATE DATABASE view_current")
	requireExec(t, db, "CREATE DATABASE view_other")
	requireExec(t, db, "CREATE TABLE view_current.source_table (id INT)")
	requireExec(t, db, "CREATE TABLE view_other.source_table (id INT)")
	requireExec(t, db, "USE view_current")

	requireExec(t, db, "CREATE VIEW view_other.qualified_view AS SELECT id FROM view_other.source_table")
	assertCurrentDatabase(t, db, "view_current")
	assertViewDefinition(t, db, "view_other", "qualified_view", "view_other.source_table")

	requireExec(t, db, "CREATE VIEW unqualified_view AS SELECT id FROM view_current.source_table")
	assertCurrentDatabase(t, db, "view_current")
	assertViewDefinition(t, db, "view_current", "unqualified_view", "view_current.source_table")
}

func requireExec(t *testing.T, db *sqlx.DB, query string) {
	t.Helper()
	_, err := db.Exec(query)
	require.NoError(t, err)
}

func assertCurrentDatabase(t *testing.T, db *sqlx.DB, expected string) {
	t.Helper()
	var actual sql.NullString
	require.NoError(t, db.QueryRow("SELECT DATABASE()").Scan(&actual))
	if expected == "" {
		require.False(t, actual.Valid)
		return
	}
	require.Equal(t, expected, actual.String)
}

func assertViewDefinition(t *testing.T, db *sqlx.DB, schema, view, source string) {
	t.Helper()
	var definition string
	require.NoError(t, db.QueryRow(
		"SELECT view_definition FROM information_schema.views WHERE table_schema = ? AND table_name = ?",
		schema,
		view,
	).Scan(&definition))
	require.Contains(t, strings.ToLower(definition), strings.ToLower(source))
}
