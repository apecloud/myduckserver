package catalog

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/configuration"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

// transactionTestSession supplies only the connection-holder surface needed by
// rowInserter while retaining a real GMS session for sql.Context.
type transactionTestSession struct {
	*memory.Session
	conn *stdsql.Conn
	tx   *stdsql.Tx
}

var _ adapter.ConnectionHolder = (*transactionTestSession)(nil)

func (s *transactionTestSession) GetConn(context.Context) (*stdsql.Conn, error) {
	return s.conn, nil
}

func (s *transactionTestSession) GetTxn(context.Context, *stdsql.TxOptions) (*stdsql.Tx, error) {
	return s.tx, nil
}

func (s *transactionTestSession) GetCatalogConn(context.Context) (*stdsql.Conn, error) {
	return s.conn, nil
}

func (s *transactionTestSession) GetCatalogTxn(context.Context, *stdsql.TxOptions) (*stdsql.Tx, error) {
	return s.tx, nil
}

func (s *transactionTestSession) TryGetTxn() *stdsql.Tx {
	return s.tx
}

func (s *transactionTestSession) GetCurrentCatalog() string {
	return "memory"
}

func (s *transactionTestSession) GetCurrentSchema() string {
	return "main"
}

func (s *transactionTestSession) CloseTxn() {
	s.tx = nil
}

func (s *transactionTestSession) CloseConn() {}

func TestRowInserterUsesActiveTransactionForObjectStyleDML(t *testing.T) {
	ctx := context.Background()
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, connector.Close())
	})

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	_, err = conn.ExecContext(ctx, `CREATE TABLE "object_like" (id INTEGER, payload VARCHAR)`)
	require.NoError(t, err)

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	session := &transactionTestSession{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		conn:    conn,
		tx:      tx,
	}
	session.SetCurrentDatabase("memory")
	sqlCtx := sql.NewContext(ctx, sql.WithSession(session))

	ri := &rowInserter{
		catalog: "memory",
		db:      "main",
		table:   "object_like",
		schema: sql.Schema{
			&sql.Column{Name: "id", Type: types.Int32, Nullable: false},
			&sql.Column{Name: "payload", Type: types.Text, Nullable: true},
		},
	}
	require.NoError(t, ri.Insert(sqlCtx, sql.Row{int32(7), "uncommitted"}))
	require.NoError(t, ri.Close(sqlCtx))

	var inTx int
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT count(*) FROM "object_like"`).Scan(&inTx))
	require.Equal(t, 1, inTx)
	require.NoError(t, tx.Rollback())

	var afterRollback int
	require.NoError(t, conn.QueryRowContext(ctx, `SELECT count(*) FROM "object_like"`).Scan(&afterRollback))
	require.Zero(t, afterRollback)
}

func TestCleanupDuckLakeOrphansUsesCanonicalQueryAndClosesRows(t *testing.T) {
	var actualQuery string
	matcher := sqlmock.QueryMatcherFunc(func(_ string, actual string) error {
		actualQuery = strings.Join(strings.Fields(actual), " ")
		return nil
	})
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	runtime := &duckLakeRuntime{config: configuration.DuckLakeConfig{
		MetadataPath: "/tmp/task77/catalog.ducklake",
		DataPath:     "/tmp/task77/data",
	}}
	provider := &DatabaseProvider{duckLake: runtime, storage: db}
	// Mark the mock physical connection as already initialized and attached;
	// this isolates the cleanup SQL from extension loading while retaining the
	// real *sql.Conn/Raw path used in production.
	require.NoError(t, conn.Raw(func(raw any) error {
		physical, ok := raw.(driver.Conn)
		require.True(t, ok)
		runtime.initialized.Store(physical, struct{}{})
		runtime.attached.Store(physical, struct{}{})
		return nil
	}))

	mock.ExpectQuery("cleanup").WillReturnRows(
		sqlmock.NewRows([]string{"path"}).AddRow("orphan-a.parquet").AddRow("orphan-b.parquet"),
	).RowsWillBeClosed()

	cleanupCtx := mycontext.WithMaintenanceQuery(context.Background())
	require.NoError(t, provider.CleanupDuckLakeOrphansOnConn(cleanupCtx, conn))
	require.Equal(t, strings.Join(strings.Fields(duckLakeOrphanCleanupSQL), " "), actualQuery)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCleanupDuckLakeOrphansNoopsForExtensionOnlyConfiguration(t *testing.T) {
	provider := &DatabaseProvider{duckLake: &duckLakeRuntime{}}
	require.NoError(t, provider.CleanupDuckLakeOrphansOnConn(
		mycontext.WithMaintenanceQuery(context.Background()), nil,
	))
}
