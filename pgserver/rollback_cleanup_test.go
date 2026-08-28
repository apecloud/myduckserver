package pgserver

import (
	"context"
	stdsql "database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

type rollbackCleanupProviderProbe struct {
	enabled       bool
	objectStorage bool
	calls         int
	gotCtx        context.Context
	gotConn       *stdsql.Conn
	cleanupErr    error
}

func (p *rollbackCleanupProviderProbe) DuckLakeEnabled() bool { return p.enabled }

func (p *rollbackCleanupProviderProbe) DuckLakeObjectStorageEnabled() bool {
	return p.objectStorage
}

func (p *rollbackCleanupProviderProbe) CleanupDuckLakeOrphansOnConn(ctx context.Context, conn *stdsql.Conn) error {
	p.calls++
	p.gotCtx = ctx
	p.gotConn = conn
	return p.cleanupErr
}

type rollbackCleanupSessionProbe struct {
	*memory.Session
	conn     *stdsql.Conn
	tx       *stdsql.Tx
	getCalls int
}

func (s *rollbackCleanupSessionProbe) GetConn(context.Context) (*stdsql.Conn, error) {
	s.getCalls++
	return s.conn, nil
}

func (s *rollbackCleanupSessionProbe) GetTxn(context.Context, *stdsql.TxOptions) (*stdsql.Tx, error) {
	return s.tx, nil
}

func (s *rollbackCleanupSessionProbe) GetCatalogConn(context.Context) (*stdsql.Conn, error) {
	return s.conn, nil
}

func (s *rollbackCleanupSessionProbe) GetCatalogTxn(context.Context, *stdsql.TxOptions) (*stdsql.Tx, error) {
	return nil, nil
}

func (s *rollbackCleanupSessionProbe) TryGetTxn() *stdsql.Tx { return s.tx }

func (s *rollbackCleanupSessionProbe) GetCurrentCatalog() string { return "memory" }

func (s *rollbackCleanupSessionProbe) GetCurrentSchema() string { return "main" }

func (s *rollbackCleanupSessionProbe) CloseTxn() {}

func (s *rollbackCleanupSessionProbe) CloseConn() {}

func newRollbackCleanupContext(t *testing.T, conn *stdsql.Conn) *sql.Context {
	t.Helper()
	session := &rollbackCleanupSessionProbe{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		conn:    conn,
	}
	return sql.NewContext(mycontext.WithFrontendQuery(context.Background()), sql.WithSession(session))
}

func TestCleanupPostgresRollbackOrphansUsesSameConnectionAndMaintenanceOrigin(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	provider := &rollbackCleanupProviderProbe{enabled: true, objectStorage: true}
	ctx := newRollbackCleanupContext(t, conn)
	require.NoError(t, cleanupPostgresRollbackOrphans(ctx, &tree.RollbackTransaction{}, provider))
	require.Equal(t, 1, provider.calls)
	require.Same(t, conn, provider.gotConn)
	require.IsType(t, (*sql.Context)(nil), provider.gotCtx)
	require.Equal(t, mycontext.MaintenanceQueryOrigin, mycontext.QueryOrigin(provider.gotCtx))
}

func TestCleanupPostgresRollbackOrphansSkipsIneligibleOriginsAndConfiguration(t *testing.T) {
	provider := &rollbackCleanupProviderProbe{enabled: true, objectStorage: true}
	ctx := newRollbackCleanupContext(t, nil)
	require.NoError(t, cleanupPostgresRollbackOrphans(ctx, &tree.CommitTransaction{}, provider))
	require.Equal(t, 0, provider.calls)

	provider.enabled = false
	require.NoError(t, cleanupPostgresRollbackOrphans(ctx, &tree.RollbackTransaction{}, provider))
	require.Equal(t, 0, provider.calls)

	provider.enabled = true
	provider.objectStorage = false
	require.NoError(t, cleanupPostgresRollbackOrphans(ctx, &tree.RollbackTransaction{}, provider))
	require.Equal(t, 0, provider.calls)

	unknownCtx := sql.NewContext(context.Background(), sql.WithSession(ctx.Session))
	require.NoError(t, cleanupPostgresRollbackOrphans(unknownCtx, &tree.RollbackTransaction{}, provider))
	require.Equal(t, 0, provider.calls)

	replicationCtx := sql.NewContext(
		mycontext.WithQueryOrigin(context.Background(), mycontext.PostgresReplicationQueryOrigin),
		sql.WithSession(ctx.Session),
	)
	require.NoError(t, cleanupPostgresRollbackOrphans(replicationCtx, &tree.RollbackTransaction{}, provider))
	require.Equal(t, 0, provider.calls)
}

func TestCleanupPostgresRollbackOrphansRejectsActiveTransaction(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	tx := new(stdsql.Tx)

	session := &rollbackCleanupSessionProbe{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		conn:    conn,
		tx:      tx,
	}
	ctx := sql.NewContext(mycontext.WithFrontendQuery(context.Background()), sql.WithSession(session))
	provider := &rollbackCleanupProviderProbe{enabled: true, objectStorage: true}
	err = cleanupPostgresRollbackOrphans(ctx, &tree.RollbackTransaction{}, provider)
	require.Error(t, err)
	require.Contains(t, err.Error(), "inactive transaction")
	require.Zero(t, provider.calls)
	require.Zero(t, session.getCalls)
}
