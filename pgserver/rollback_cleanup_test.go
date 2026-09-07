package pgserver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

type rollbackCleanupProviderProbe struct {
	enabled        bool
	objectStorage  bool
	calls          int
	gotCtx         context.Context
	gotConn        *stdsql.Conn
	cleanupErr     error
	reservationErr error
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

func (p *rollbackCleanupProviderProbe) BeginDuckLakeRollbackCleanupWithError(*stdsql.Tx) (func(), error) {
	return func() {}, p.reservationErr
}

type rollbackCleanupSessionProbe struct {
	*memory.Session
	conn           *stdsql.Conn
	tx             *stdsql.Tx
	getCalls       int
	commitCalled   bool
	rollbackCalled bool
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

func (s *rollbackCleanupSessionProbe) GetTxnBinding() (*stdsql.Conn, *stdsql.Tx) {
	return s.conn, s.tx
}

func (s *rollbackCleanupSessionProbe) CommitTxn(*stdsql.Tx) error {
	s.commitCalled = true
	s.tx = nil
	return nil
}

func (s *rollbackCleanupSessionProbe) RollbackTxn(*stdsql.Tx) (*stdsql.Conn, bool, error) {
	s.rollbackCalled = true
	s.tx = nil
	return s.conn, true, nil
}

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

func TestPostgresRollbackReservationErrorStillFinalizesPhysicalTransaction(t *testing.T) {
	reservationErr := errors.New("poisoned cleanup reservation")
	conn := new(stdsql.Conn)
	tx := new(stdsql.Tx)
	session := &rollbackCleanupSessionProbe{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		conn:    conn,
		tx:      tx,
	}
	ctx := sql.NewContext(mycontext.WithFrontendQuery(context.Background()), sql.WithSession(session))
	provider := &rollbackCleanupProviderProbe{
		enabled:        true,
		objectStorage:  true,
		reservationErr: reservationErr,
	}

	err := rollbackPostgresTransactionControl(ctx, &tree.RollbackTransaction{}, provider)
	require.ErrorIs(t, err, reservationErr)
	require.True(t, session.rollbackCalled, "cleanup admission failure must not skip physical rollback")
	require.Nil(t, session.tx)
	require.Zero(t, provider.calls, "poisoned cleanup reservation must not issue cleanup SQL")
}

func TestDeletePreparedObjectsToleratesProtocolOnlyTransactionStatements(t *testing.T) {
	h := &ConnectionHandler{
		preparedStatements: map[string]PreparedStatementData{
			"begin": {Statement: ConvertedStatement{String: "BEGIN"}},
		},
		portals: map[string]PortalData{
			"begin": {Statement: ConvertedStatement{String: "BEGIN"}},
		},
	}

	// Transaction-control statements deliberately have no DuckDB prepared
	// statement. Closing either protocol object must remain nil-safe.
	h.deletePreparedStatement("begin")
	h.deletePortal("begin")
	_, preparedExists := h.preparedStatements["begin"]
	_, portalExists := h.portals["begin"]
	require.False(t, preparedExists)
	require.False(t, portalExists)
}
