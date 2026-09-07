package catalog

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/configuration"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

// poolBindingTestSession exposes the production pool binding through a GMS
// session so the regression can exercise the exact adapter lookup that used
// to deadlock when a lifecycle lease was held during shutdown.
type poolBindingTestSession struct {
	*memory.Session
	pool *ConnectionPool
	id   uint32
}

var _ adapter.ConnectionHolder = (*poolBindingTestSession)(nil)
var _ adapter.TransactionBindingHolder = (*poolBindingTestSession)(nil)

func (s *poolBindingTestSession) GetConn(ctx context.Context) (*stdsql.Conn, error) {
	return s.pool.GetConn(ctx, s.id)
}

func (s *poolBindingTestSession) GetTxn(ctx context.Context, options *stdsql.TxOptions) (*stdsql.Tx, error) {
	return s.pool.GetTxn(ctx, s.id, "", options)
}

func (s *poolBindingTestSession) GetCatalogConn(ctx context.Context) (*stdsql.Conn, error) {
	return s.pool.GetConn(ctx, s.id)
}

func (s *poolBindingTestSession) GetCatalogTxn(ctx context.Context, options *stdsql.TxOptions) (*stdsql.Tx, error) {
	return s.pool.GetTxn(ctx, s.id, "", options)
}

func (s *poolBindingTestSession) TryGetTxn() *stdsql.Tx {
	return s.pool.TryGetTxn(s.id)
}

func (s *poolBindingTestSession) GetTxnBinding() (*stdsql.Conn, *stdsql.Tx) {
	return s.pool.GetTxnBinding(s.id)
}

func (s *poolBindingTestSession) GetCurrentCatalog() string { return "memory" }

func (s *poolBindingTestSession) GetCurrentSchema() string { return "main" }

func (s *poolBindingTestSession) CloseTxn() { s.pool.CloseTxn(s.id) }

func (s *poolBindingTestSession) CloseConn() { _ = s.pool.CloseConn(s.id) }

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
		MetadataPath: "/var/lib/myduck/catalog.ducklake",
		DataPath:     "s3://test-bucket/data",
	}}
	execer := &recordingDuckLakeExecer{}

	require.NoError(t, runtime.attachLocked(context.Background(), nil, execer))
	require.Equal(t, []string{
		"ATTACH IF NOT EXISTS 'ducklake:/var/lib/myduck/catalog.ducklake' AS \"__myduck_ducklake\" (DATA_PATH 's3://test-bucket/data', DATA_INLINING_ROW_LIMIT 0, CREATE_IF_NOT_EXISTS true)",
	}, execer.queries)
}

func TestDuckLakeAttachOmitsCreateIfCatalogExists(t *testing.T) {
	dir := t.TempDir()
	metadata := filepath.Join(dir, "catalog.ducklake")
	require.NoError(t, os.WriteFile(metadata, []byte("existing"), 0o600))
	runtime := &duckLakeRuntime{config: configuration.DuckLakeConfig{
		MetadataPath: metadata,
		DataPath:     "s3://test-bucket/data",
	}}
	execer := &recordingDuckLakeExecer{}

	require.NoError(t, runtime.attachLocked(context.Background(), nil, execer))
	require.Equal(t, []string{
		"ATTACH IF NOT EXISTS 'ducklake:" + metadata + "' AS \"__myduck_ducklake\" (DATA_PATH 's3://test-bucket/data', DATA_INLINING_ROW_LIMIT 0)",
	}, execer.queries)
	require.NotContains(t, execer.queries[0], "CREATE_IF_NOT_EXISTS")
}

func TestDuckLakeAttachRejectsRemoteCatalogURI(t *testing.T) {
	runtime := &duckLakeRuntime{config: configuration.DuckLakeConfig{
		MetadataPath: "s3://test-bucket/catalog.ducklake",
		DataPath:     "s3://test-bucket/data",
	}}
	execer := &recordingDuckLakeExecer{}

	err := runtime.attachLocked(context.Background(), nil, execer)
	require.Error(t, err)
	require.Contains(t, err.Error(), "reason=invalid_configuration")
	require.Empty(t, execer.queries)
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

func TestDisabledDuckLakeConnectionUsesTableStorageError(t *testing.T) {
	var provider *DatabaseProvider

	err := provider.EnsureDuckLakeConnectionWithExecutor(context.Background(), nil, nil)
	require.ErrorIs(t, err, ErrInvalidTableStorage)
	require.ErrorContains(t, err, "DuckLake service configuration is disabled")
}

func TestDisabledDuckLakeRuntimeUsesTableStorageError(t *testing.T) {
	var runtime *duckLakeRuntime

	err := runtime.EnsureAttached(
		mycontext.WithFrontendQuery(context.Background()),
		nil,
		nil,
	)
	require.ErrorIs(t, err, ErrInvalidTableStorage)
	require.ErrorContains(t, err, "DuckLake service configuration is disabled")
}

func TestRecordObjectStorageSelectionRejectsDisabledDuckLake(t *testing.T) {
	database := NewDatabaseWithProvider("main", "memory", &DatabaseProvider{})

	err := database.RecordTableStorageSelectionWithExecutor(
		nil,
		"disabled_object",
		TableStorageSelection{Kind: TableStorageObject, Explicit: true, Source: "test"},
		nil,
		nil,
	)
	require.ErrorIs(t, err, ErrInvalidTableStorage)
	require.ErrorContains(t, err, "DuckLake service configuration is disabled")
}

func TestEnsureDuckLakeConnectionWithBindingDoesNotReenterLifecycleLock(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	pool := NewConnectionPool(connector, db, "memory")
	t.Cleanup(func() {
		require.NoError(t, pool.Close())
		require.NoError(t, connector.Close())
	})

	const sessionID = uint32(174)
	conn, tx, release, err := pool.GetExecutionSnapshotLease(
		context.Background(), sessionID, "", true,
	)
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.Nil(t, tx)
	t.Cleanup(release)

	// Mark setup complete so the test isolates lifecycle admission rather than
	// extension loading or ATTACH SQL.
	runtime := &duckLakeRuntime{config: configuration.DuckLakeConfig{
		MetadataPath: "/tmp/task77/catalog.ducklake",
		DataPath:     "/tmp/task77/data",
	}}
	require.NoError(t, conn.Raw(func(raw any) error {
		physical, ok := raw.(driver.Conn)
		require.True(t, ok)
		runtime.initialized.Store(physical, struct{}{})
		runtime.attached.Store(physical, struct{}{})
		return nil
	}))
	provider := &DatabaseProvider{duckLake: runtime}
	session := &poolBindingTestSession{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		pool:    pool,
		id:      sessionID,
	}
	ctx := sql.NewContext(mycontext.WithFrontendQuery(context.Background()), sql.WithSession(session))

	closeDone := make(chan error, 1)
	go func() { closeDone <- pool.Close() }()
	waitForPoolShutdown(t, pool)

	// The pool close has announced its writer and is waiting for the retained
	// lease. The binding-aware setup must complete without calling
	// pool.GetTxnBinding (which would queue behind that writer).
	setupDone := make(chan error, 1)
	go func() {
		setupDone <- provider.EnsureDuckLakeConnectionWithBinding(ctx, conn, nil)
	}()
	select {
	case setupErr := <-setupDone:
		require.NoError(t, setupErr)
	case <-time.After(time.Second):
		t.Fatal("binding-aware DuckLake setup re-entered the pending lifecycle writer")
	}

	release()
	select {
	case closeErr := <-closeDone:
		require.NoError(t, closeErr)
	case <-time.After(time.Second):
		t.Fatal("pool close did not finish after execution lease release")
	}
}
