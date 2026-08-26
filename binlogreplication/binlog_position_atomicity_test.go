package binlogreplication

import (
	"context"
	"testing"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"
)

func TestBinlogPositionAndAppliedDataShareCommitBoundary(t *testing.T) {
	dataDir := t.TempDir()
	provider, err := catalog.NewDBProvider("", dataDir, "myduck")
	require.NoError(t, err)

	_, err = provider.Storage().ExecContext(
		context.Background(),
		"CREATE SCHEMA test; CREATE TABLE test.commit_boundary (id INTEGER)",
	)
	require.NoError(t, err)

	position, err := mysql.ParsePosition(
		mysql56FlavorID,
		"568333e9-2b06-4d33-9a6b-c2308c930c95:1",
	)
	require.NoError(t, err)

	apply := func(applyCtx *sql.Context, id int, commit bool) {
		dataTxn, err := adapter.GetTxn(applyCtx, nil)
		require.NoError(t, err)
		catalogTxn, err := adapter.GetCatalogTxn(applyCtx, nil)
		require.NoError(t, err)
		require.Same(t, dataTxn, catalogTxn)

		_, err = dataTxn.ExecContext(applyCtx, "INSERT INTO test.commit_boundary VALUES (?)", id)
		require.NoError(t, err)
		require.NoError(t, positionStore.Save(applyCtx, nil, position))

		if commit {
			require.NoError(t, dataTxn.Commit())
		} else {
			require.NoError(t, dataTxn.Rollback())
		}
		adapter.CloseTxn(applyCtx)
	}

	// Simulate a failure after Save and before commit. Neither side may persist.
	applyCtx := newBinlogTestContext(provider, 1, "test")
	observerCtx := newBinlogTestContext(provider, 2, "")
	apply(applyCtx, 1, false)
	assertBinlogCommitCounts(t, observerCtx, 0, 0)
	adapter.CloseConn(applyCtx)
	adapter.CloseConn(observerCtx)
	require.NoError(t, provider.Close())

	provider, err = catalog.NewDBProvider("", dataDir, "myduck")
	require.NoError(t, err)
	observerCtx = newBinlogTestContext(provider, 3, "")
	assertBinlogCommitCounts(t, observerCtx, 0, 0)

	applyCtx = newBinlogTestContext(provider, 4, "test")
	apply(applyCtx, 2, true)
	assertBinlogCommitCounts(t, observerCtx, 1, 1)
	adapter.CloseConn(applyCtx)
	adapter.CloseConn(observerCtx)
	require.NoError(t, provider.Close())

	provider, err = catalog.NewDBProvider("", dataDir, "myduck")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	assertBinlogCommitCounts(t, newBinlogTestContext(provider, 5, ""), 1, 1)
}

func newBinlogTestContext(provider *catalog.DatabaseProvider, id uint32, database string) *sql.Context {
	base := sql.NewBaseSessionWithClientServer("", sql.Client{}, id)
	session := backend.NewSession(memory.NewSession(base, provider), provider)
	session.SetCurrentDatabase(database)
	return sql.NewContext(context.Background(), sql.WithSession(session))
}

func assertBinlogCommitCounts(t *testing.T, ctx *sql.Context, dataRows, positions int) {
	t.Helper()

	var gotDataRows, gotPositions int
	require.NoError(t, adapter.QueryRowCatalog(
		ctx,
		"SELECT COUNT(*) FROM test.commit_boundary",
	).Scan(&gotDataRows))
	require.NoError(t, adapter.QueryRowCatalog(
		ctx,
		catalog.InternalTables.BinlogPosition.CountAllStmt(),
	).Scan(&gotPositions))
	require.Equal(t, dataRows, gotDataRows)
	require.Equal(t, positions, gotPositions)
}
