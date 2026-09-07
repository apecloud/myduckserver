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
package backend

import (
	"context"
	"strings"
	"testing"

	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"
)

func TestMySQLParserRestoresQualifiedCreateViewTarget(t *testing.T) {
	parser := &mysqlParser{Parser: sql.NewMysqlParser()}
	stmt, _, _, err := parser.ParseWithOptions(
		context.Background(),
		"CREATE VIEW wire_schema.recent_events AS SELECT 1",
		';',
		false,
		sqlparser.ParserOptions{},
	)
	require.NoError(t, err)

	ddl, ok := stmt.(*sqlparser.DDL)
	require.True(t, ok)
	require.Equal(t, ddl.ViewSpec.ViewName, ddl.Table)
	require.Equal(t, "wire_schema", ddl.Table.DbQualifier.String())
	require.Equal(t, "recent_events", ddl.Table.Name.String())
}

func TestMySQLCompatibilityVersion(t *testing.T) {
	registerMySQLCompatibilitySystemVariables()
	_, version, ok := sql.SystemVariables.GetGlobal("version")
	require.True(t, ok)
	require.Equal(t, "8.0.23", version)
}

func TestMySQLParserRestoresFilePositionReplicationOptions(t *testing.T) {
	query := "CHANGE REPLICATION SOURCE TO " +
		"SOURCE_HOST='localhost', " +
		"SOURCE_PASSWORD='secret', " +
		"SOURCE_PORT=3306, " +
		"SOURCE_LOG_FILE='mysql-bin.000042', " +
		"SOURCE_LOG_POS=987654"

	_, _, _, err := sql.NewMysqlParser().ParseWithOptions(
		context.Background(), query, ';', false, sqlparser.ParserOptions{},
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "SOURCE_LOG_FILE")

	parser := &mysqlParser{Parser: sql.NewMysqlParser()}
	stmt, parsed, remainder, err := parser.ParseWithOptions(
		context.Background(), query, ';', false, sqlparser.ParserOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, query, parsed)
	require.Empty(t, remainder)

	changeSource, ok := stmt.(*sqlparser.ChangeReplicationSource)
	require.True(t, ok)
	require.Len(t, changeSource.Options, 5)
	require.Equal(t, "SOURCE_HOST", changeSource.Options[0].Name)
	require.Equal(t, "localhost", changeSource.Options[0].Value)
	require.Equal(t, "SOURCE_PASSWORD", changeSource.Options[1].Name)
	require.Equal(t, "secret", changeSource.Options[1].Value)
	require.Equal(t, "SOURCE_PORT", changeSource.Options[2].Name)
	require.Equal(t, 3306, changeSource.Options[2].Value)
	require.Equal(t, "SOURCE_LOG_FILE", changeSource.Options[3].Name)
	require.Equal(t, "mysql-bin.000042", changeSource.Options[3].Value)
	require.Equal(t, "SOURCE_LOG_POS", changeSource.Options[4].Name)
	require.Equal(t, 987654, changeSource.Options[4].Value)
}

func TestMySQLParserRestoresFilePositionOptionsInMultiQuery(t *testing.T) {
	query := "  change replication source to source_log_file='bin.000001', source_log_pos=42; SELECT 1"
	parser := &mysqlParser{Parser: sql.NewMysqlParser()}
	stmt, parsed, remainder, err := parser.ParseWithOptions(
		context.Background(), query, ';', true, sqlparser.ParserOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, "change replication source to source_log_file='bin.000001', source_log_pos=42", parsed)
	require.Equal(t, " SELECT 1", remainder)

	changeSource, ok := stmt.(*sqlparser.ChangeReplicationSource)
	require.True(t, ok)
	require.Equal(t, "source_log_file", changeSource.Options[0].Name)
	require.Equal(t, "bin.000001", changeSource.Options[0].Value)
	require.Equal(t, "source_log_pos", changeSource.Options[1].Name)
	require.Equal(t, 42, changeSource.Options[1].Value)
}

func TestMySQLParserRestoresDatabaseReplicationFilters(t *testing.T) {
	query := "CHANGE REPLICATION FILTER " +
		"REPLICATE_DO_DB=(db01, DB02), " +
		"REPLICATE_IGNORE_TABLE=(db03.t1), " +
		"REPLICATE_IGNORE_DB=(db04)"

	_, _, _, err := sql.NewMysqlParser().ParseWithOptions(
		context.Background(), query, ';', false, sqlparser.ParserOptions{},
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "REPLICATE_DO_DB")

	parser := &mysqlParser{Parser: sql.NewMysqlParser()}
	stmt, parsed, remainder, err := parser.ParseWithOptions(
		context.Background(), query, ';', false, sqlparser.ParserOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, query, parsed)
	require.Empty(t, remainder)

	changeFilter, ok := stmt.(*sqlparser.ChangeReplicationFilter)
	require.True(t, ok)
	require.Len(t, changeFilter.Options, 3)
	require.Equal(t, "REPLICATE_DO_DB", changeFilter.Options[0].Name)
	require.Equal(t, "REPLICATE_IGNORE_TABLE", changeFilter.Options[1].Name)
	require.Equal(t, "REPLICATE_IGNORE_DB", changeFilter.Options[2].Name)

	doDatabases, ok := changeFilter.Options[0].Value.(sqlparser.TableNames)
	require.True(t, ok)
	require.Equal(t, "db01", doDatabases[0].Name.String())
	require.Equal(t, "DB02", doDatabases[1].Name.String())
}

func TestMySQLParserRestoresDatabaseFiltersInMultiQuery(t *testing.T) {
	query := "  change replication filter replicate_ignore_db=(db02); SELECT 1"
	parser := &mysqlParser{Parser: sql.NewMysqlParser()}
	stmt, parsed, remainder, err := parser.ParseWithOptions(
		context.Background(), query, ';', true, sqlparser.ParserOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, "change replication filter replicate_ignore_db=(db02)", parsed)
	require.Equal(t, " SELECT 1", remainder)

	changeFilter, ok := stmt.(*sqlparser.ChangeReplicationFilter)
	require.True(t, ok)
	require.Equal(t, "replicate_ignore_db", changeFilter.Options[0].Name)

	_, index, err := parser.ParseOneWithOptions(context.Background(), query, sqlparser.ParserOptions{})
	require.NoError(t, err)
	require.Equal(t, strings.Index(query, " SELECT 1"), index)
}

func TestMySQLParserTableStorageOptions(t *testing.T) {
	parser := &mysqlParser{Parser: sql.NewMysqlParser()}
	for _, query := range []string{
		"CREATE TABLE object_table (id INT) ENGINE=DUCKLAKE",
		"CREATE TABLE local_table (id INT) ENGINE=InnoDB",
	} {
		stmt, _, _, err := parser.ParseWithOptions(context.Background(), query, ';', false, sqlparser.ParserOptions{})
		require.NoError(t, err, query)
		ddl, ok := stmt.(*sqlparser.DDL)
		require.True(t, ok, query)
		require.NotNil(t, ddl.TableSpec, query)
		require.NotEmpty(t, ddl.TableSpec.TableOpts, query)
		for _, option := range ddl.TableSpec.TableOpts {
			t.Logf("%s => name=%q value=%q", query, option.Name, option.Value)
		}
	}
}

func TestMySQLParserRejectsConflictingTableStorageOptions(t *testing.T) {
	parser := &mysqlParser{Parser: sql.NewMysqlParser()}
	for _, test := range []struct {
		query string
		want  error
	}{
		{query: "CREATE TABLE duplicate_engine (id INT) ENGINE=DUCKLAKE ENGINE=DUCKLAKE", want: catalog.ErrTableStorageDuplicate},
		{query: "CREATE TABLE conflicting_engine (id INT) ENGINE=DUCKLAKE ENGINE=LOCAL", want: catalog.ErrTableStorageConflict},
	} {
		_, _, _, err := parser.ParseWithOptions(context.Background(), test.query, ';', false, sqlparser.ParserOptions{})
		require.Error(t, err, test.query)
		require.ErrorIs(t, err, test.want, test.query)
	}
}
