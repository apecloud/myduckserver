package delta

import (
	stdsql "database/sql"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

func TestBuildCondenseDeltaSQLKeepsLatestRow(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, connector.Close())
	})

	_, err = db.Exec(`
		CREATE TEMP TABLE delta_rows AS SELECT * FROM (VALUES
			(2::TINYINT, NULL::VARCHAR, ''::BLOB, NULL::VARCHAR, 1::UBIGINT, 0::UBIGINT, 1::INTEGER, 'old'::VARCHAR),
			(2::TINYINT, NULL::VARCHAR, ''::BLOB, NULL::VARCHAR, 2::UBIGINT, 0::UBIGINT, 1::INTEGER, 'new'::VARCHAR)
		) t(action, txn_tag, txn_server, txn_group, txn_seq, txn_stmt, id, name)`)
	require.NoError(t, err)

	appender, err := newDeltaAppender(sql.Schema{
		&sql.Column{Name: "id", Type: types.Int32, PrimaryKey: true},
		&sql.Column{Name: "name", Type: types.Text},
	})
	require.NoError(t, err)
	t.Cleanup(appender.Release)

	query := "SELECT action, id, name FROM (" + buildCondenseDeltaSQL("delta_rows", appender) + ")"
	var action int8
	var id int32
	var name string
	require.NoError(t, db.QueryRow(query).Scan(&action, &id, &name))
	require.Equal(t, int8(2), action)
	require.Equal(t, int32(1), id)
	require.Equal(t, "new", name)
}
