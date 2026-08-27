package catalog

import (
	"context"
	stdsql "database/sql"
	"testing"

	"github.com/apecloud/myduckserver/mycontext"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

func TestStorageIsPassiveAndDoesNotInventOrigin(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	provider := &DatabaseProvider{
		storage:  db,
		duckLake: &duckLakeRuntime{},
	}

	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, connector.Close())
	})

	require.Same(t, db, provider.Storage())
	require.NoError(t, provider.InitializeStorage(context.Background()))
	require.NoError(t, provider.InitializeStorage(mycontext.WithQueryOrigin(
		context.Background(), mycontext.MySQLReplicationQueryOrigin,
	)))
}

func TestInitializeStorageRejectsOnlyMissingStorageForEligibleOrigin(t *testing.T) {
	provider := &DatabaseProvider{duckLake: &duckLakeRuntime{}}

	require.NoError(t, provider.InitializeStorage(context.Background()))
	require.NoError(t, provider.InitializeStorage(mycontext.WithQueryOrigin(
		context.Background(), mycontext.PostgresReplicationQueryOrigin,
	)))
	require.Error(t, provider.InitializeStorage(mycontext.WithMaintenanceQuery(context.Background())))
}

func TestDuckDBStringLiteralRoundTripsQuotesAndBackslashes(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, connector.Close())
	})

	want := "access\\key'with\\slashes"
	var got string
	require.NoError(t, db.QueryRow("SELECT "+duckDBStringLiteral(want)).Scan(&got))
	require.Equal(t, want, got)
}
