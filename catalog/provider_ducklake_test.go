package catalog

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"testing"

	"github.com/apecloud/myduckserver/configuration"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

type recordingDuckLakeExecer struct {
	queries []string
	err     error
}

func (e *recordingDuckLakeExecer) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	e.queries = append(e.queries, query)
	if e.err != nil {
		return nil, e.err
	}
	return driver.RowsAffected(0), nil
}

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

func TestDuckLakeAttachUsesServicePaths(t *testing.T) {
	runtime := &duckLakeRuntime{config: configuration.DuckLakeConfig{
		MetadataPath: "s3://test-bucket/catalog.ducklake",
		DataPath:     "s3://test-bucket/data",
	}}
	execer := &recordingDuckLakeExecer{}

	require.NoError(t, runtime.attachLocked(context.Background(), nil, execer))
	require.Equal(t, []string{
		"ATTACH IF NOT EXISTS 'ducklake:s3://test-bucket/catalog.ducklake' AS \"__myduck_ducklake\" (DATA_PATH 's3://test-bucket/data', DATA_INLINING_ROW_LIMIT 0)",
	}, execer.queries)
}

func TestDuckLakeObjectBoundaryRejectsIncompletePaths(t *testing.T) {
	runtime := &duckLakeRuntime{config: configuration.DuckLakeConfig{
		MetadataPath: "/var/lib/myduck/catalog.ducklake",
	}}
	execer := &recordingDuckLakeExecer{}

	err := runtime.EnsureAttached(mycontext.WithFrontendQuery(context.Background()), nil, execer)
	require.ErrorContains(t, err, "metadata and data paths are required")
	require.Empty(t, execer.queries)
}
