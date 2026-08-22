package pgserver

import (
	"context"
	stdsql "database/sql"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

func TestCopyContextWorksWithDuckDBExecContext(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, connector.Close())
	})

	parent := sql.NewEmptyContext()
	copyCtx, cancel := newCopyContext(parent)
	_, err = db.ExecContext(copyCtx, `CREATE TABLE copy_context_test (id INTEGER)`)
	require.NoError(t, err)

	cancel()
	require.ErrorIs(t, copyCtx.Err(), context.Canceled)
	require.NoError(t, parent.Err())
}
