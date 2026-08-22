package backend

import (
	"context"
	stdsql "database/sql"
	"io"
	"testing"

	"github.com/apecloud/myduckserver/pgtypes"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

type trackingRowIter struct {
	rows       []sql.Row
	nextCalls  int
	closeCalls int
}

func (iter *trackingRowIter) Next(*sql.Context) (sql.Row, error) {
	iter.nextCalls++
	if len(iter.rows) == 0 {
		return nil, io.EOF
	}
	row := iter.rows[0]
	iter.rows = iter.rows[1:]
	return row, nil
}

func (iter *trackingRowIter) Close(*sql.Context) error {
	iter.closeCalls++
	return nil
}

func TestQueryRowLimitIter(t *testing.T) {
	tests := []struct {
		name      string
		rows      []sql.Row
		wantError bool
	}{
		{name: "below limit", rows: []sql.Row{{1}}},
		{name: "at limit", rows: []sql.Row{{1}, {2}}},
		{name: "over limit", rows: []sql.Row{{1}, {2}, {3}}, wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			child := &trackingRowIter{rows: tt.rows}
			iter := &queryRowLimitIter{child: child, limit: 2}
			rows, err := sql.RowIterToRows(sql.NewEmptyContext(), iter)
			if tt.wantError {
				require.ErrorContains(t, err, "query returned more than the configured row limit of 2")
				require.Nil(t, rows)
				require.Equal(t, 3, child.nextCalls)
				require.Equal(t, 1, child.closeCalls, "overflow must close the source immediately and only once")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.rows, rows)
			require.Equal(t, 1, child.closeCalls)
		})
	}
}

func TestQueryRowLimitIsOnlyConfiguredForOptedInSessions(t *testing.T) {
	schema := sql.Schema{&sql.Column{Name: "value", Type: types.Int32}}
	newBase := func() *memory.Session {
		return memory.NewSession(sql.NewBaseSession(), nil)
	}

	internalSession := NewSession(newBase(), nil)
	internalCtx := sql.NewContext(context.Background(), sql.WithSession(internalSession))
	internalIter := &trackingRowIter{rows: []sql.Row{{1}, {2}, {3}}}
	unlimited := ApplyQueryRowLimit(internalCtx, schema, internalIter)
	require.Same(t, internalIter, unlimited)
	require.Zero(t, internalSession.QueryRowLimit())
	rows, err := sql.RowIterToRows(internalCtx, unlimited)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{1}, {2}, {3}}, rows)

	ordinarySession := NewSession(newBase(), nil, WithQueryRowLimit(2))
	ordinaryCtx := sql.NewContext(context.Background(), sql.WithSession(ordinarySession))
	ordinaryIter := &trackingRowIter{rows: []sql.Row{{1}, {2}, {3}}}
	limited := ApplyQueryRowLimit(ordinaryCtx, schema, ordinaryIter)
	require.NotSame(t, ordinaryIter, limited)
	_, err = sql.RowIterToRows(ordinaryCtx, limited)
	require.ErrorContains(t, err, "query returned more than the configured row limit of 2")
}

func TestSQLRowIterNormalizesDuckDBJSONValues(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, connector.Close())
	})

	var jsonNull, sqlNull any
	require.NoError(t, db.QueryRow(`SELECT 'null'::JSON, NULL::JSON`).Scan(&jsonNull, &sqlNull))
	require.Nil(t, jsonNull)
	require.Nil(t, sqlNull)

	names := []string{
		"array_value",
		"object_value",
		"string_value",
		"number_value",
		"boolean_value",
		"json_null",
		"sql_null",
		"integer_value",
	}
	schema := make(sql.Schema, len(names))
	for i := range 7 {
		schema[i] = &sql.Column{Name: names[i], Type: types.JSON}
	}
	schema[7] = &sql.Column{Name: names[7], Type: types.Int32}

	query := QueryForJSONScan(`
		SELECT
			?::JSON,
			'{"key":{"items":[1,"two",false,null]}}'::JSON,
			'"scalar"'::JSON,
			'42'::JSON,
			'true'::JSON,
			'null'::JSON,
			NULL::JSON,
			7::INTEGER`, schema)
	rows, err := db.QueryContext(context.Background(), query, `[1,{"nested":[true,null]}]`)
	require.NoError(t, err)
	columns, err := rows.Columns()
	require.NoError(t, err)
	require.Equal(t, names, columns)

	iter, err := NewSQLRowIter(rows, schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, iter.Close(sql.NewEmptyContext()))
	})

	row, err := iter.Next(sql.NewEmptyContext())
	require.NoError(t, err)
	require.Equal(t, sql.Row{
		types.JSONDocument{Val: []any{float64(1), map[string]any{"nested": []any{true, nil}}}},
		types.JSONDocument{Val: map[string]any{"key": map[string]any{"items": []any{float64(1), "two", false, nil}}}},
		types.JSONDocument{Val: "scalar"},
		types.JSONDocument{Val: float64(42)},
		types.JSONDocument{Val: true},
		types.JSONDocument{Val: nil},
		nil,
		int32(7),
	}, row)
}

func TestSQLRowIterPreservesPostgresJSONNull(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, connector.Close())
	})

	for _, oid := range []uint32{pgtype.JSONOID, pgtype.JSONBOID} {
		pgType, err := pgtypes.NewPostgresType(pgtypes.DefaultTypeMap, oid, -1)
		require.NoError(t, err)
		schema := sql.Schema{
			&sql.Column{Name: "json_null", Type: pgType},
			&sql.Column{Name: "sql_null", Type: pgType},
			&sql.Column{Name: "plain_value", Type: types.Int32},
		}

		rows, err := db.Query(QueryForJSONScan(
			`SELECT 'null'::JSON, NULL::JSON, 42::INTEGER`, schema))
		require.NoError(t, err)
		iter, err := NewSQLRowIter(rows, schema)
		require.NoError(t, err)

		row, err := iter.Next(sql.NewEmptyContext())
		require.NoError(t, err)
		require.Equal(t, sql.Row{types.JSONDocument{Val: nil}, nil, int32(42)}, row)
		require.NoError(t, iter.Close(sql.NewEmptyContext()))
	}
}
