package pgserver

import (
	"context"
	"strconv"
	"testing"

	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/testutil"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

type schemaSummaryRow struct {
	Schema    string
	Table     string
	Column    string
	DataType  string
	Ordinal   uint64
	TableType string
}

func TestSchemaSummaryWireProtocols(t *testing.T) {
	testDir := testutil.CreateTestDir(t)
	testEnv := testutil.NewTestEnv()
	require.NoError(t, testutil.StartDuckSqlServer(t, testDir, nil, testEnv))
	defer testutil.StopDuckSqlServer(t, testEnv.DuckProcess)

	_, err := testEnv.MyDuckServer.Exec("CREATE DATABASE wire_schema")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("CREATE TABLE wire_schema.events (id BIGINT PRIMARY KEY, label VARCHAR(32), score DECIMAL(10, 2))")
	require.NoError(t, err)
	_, err = testEnv.MyDuckServer.Exec("CREATE VIEW wire_schema.recent_events AS SELECT id FROM wire_schema.events")
	require.NoError(t, err)

	expected := []schemaSummaryRow{
		{Schema: "wire_schema", Table: "events", Column: "id", DataType: "bigint", Ordinal: 1, TableType: "BASE TABLE"},
		{Schema: "wire_schema", Table: "events", Column: "label", DataType: "varchar", Ordinal: 2, TableType: "BASE TABLE"},
		{Schema: "wire_schema", Table: "events", Column: "score", DataType: "decimal", Ordinal: 3, TableType: "BASE TABLE"},
		{Schema: "wire_schema", Table: "recent_events", Column: "id", DataType: "bigint", Ordinal: 1, TableType: "VIEW"},
	}

	t.Run("mysql", func(t *testing.T) {
		rows, err := testEnv.MyDuckServer.Queryx(catalog.SchemaSummaryQuery)
		require.NoError(t, err)
		defer rows.Close()

		actual := make([]schemaSummaryRow, 0, len(expected))
		for rows.Next() {
			var row schemaSummaryRow
			require.NoError(t, rows.Scan(&row.Schema, &row.Table, &row.Column, &row.DataType, &row.Ordinal, &row.TableType))
			actual = append(actual, row)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, expected, actual)
	})

	t.Run("postgres", func(t *testing.T) {
		ctx := context.Background()
		conn, err := pgx.Connect(ctx, "postgresql://postgres@127.0.0.1:"+strconv.Itoa(testEnv.DuckPgPort)+"/postgres")
		require.NoError(t, err)
		defer conn.Close(ctx)

		rows, err := conn.Query(ctx, catalog.SchemaSummaryQuery)
		require.NoError(t, err)
		defer rows.Close()

		actual := make([]schemaSummaryRow, 0, len(expected))
		for rows.Next() {
			var row schemaSummaryRow
			require.NoError(t, rows.Scan(&row.Schema, &row.Table, &row.Column, &row.DataType, &row.Ordinal, &row.TableType))
			actual = append(actual, row)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, expected, actual)
	})
}
