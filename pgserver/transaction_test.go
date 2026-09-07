package pgserver

import (
	"context"
	stdsql "database/sql"
	"testing"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// replacingTransactionSession models a finalizer that reports success while a
// different transaction has become current. The protocol finalizer must treat
// that as a replacement/corruption state, not as proof that its old implicit
// scope finalized.
type replacingTransactionSession struct {
	*memory.Session
	conn           *stdsql.Conn
	tx             *stdsql.Tx
	replacement    *stdsql.Tx
	replaceConn    *stdsql.Conn
	commitCalled   bool
	rollbackCalled bool
	getTxnCalls    int
}

var _ adapter.ConnectionHolder = (*replacingTransactionSession)(nil)
var _ adapter.TransactionBindingHolder = (*replacingTransactionSession)(nil)
var _ adapter.TransactionFinalizer = (*replacingTransactionSession)(nil)

func (s *replacingTransactionSession) GetConn(context.Context) (*stdsql.Conn, error) {
	return s.conn, nil
}

func (s *replacingTransactionSession) GetTxn(context.Context, *stdsql.TxOptions) (*stdsql.Tx, error) {
	s.getTxnCalls++
	return s.tx, nil
}

func (s *replacingTransactionSession) GetCatalogConn(context.Context) (*stdsql.Conn, error) {
	return s.conn, nil
}

func (s *replacingTransactionSession) GetCatalogTxn(context.Context, *stdsql.TxOptions) (*stdsql.Tx, error) {
	return s.tx, nil
}

func (s *replacingTransactionSession) TryGetTxn() *stdsql.Tx { return s.tx }

func (s *replacingTransactionSession) GetTxnBinding() (*stdsql.Conn, *stdsql.Tx) {
	return s.conn, s.tx
}

func (s *replacingTransactionSession) GetCurrentCatalog() string { return "memory" }

func (s *replacingTransactionSession) GetCurrentSchema() string { return "main" }

func (s *replacingTransactionSession) CloseTxn() { s.tx = nil }

func (s *replacingTransactionSession) CloseConn() { s.conn = nil }

func (s *replacingTransactionSession) CommitTxn(*stdsql.Tx) error {
	s.commitCalled = true
	s.conn = s.replaceConn
	s.tx = s.replacement
	return nil
}

func (s *replacingTransactionSession) RollbackTxn(*stdsql.Tx) (*stdsql.Conn, bool, error) {
	s.rollbackCalled = true
	s.tx = nil
	return s.conn, true, nil
}

func TestPostgresTransactionControlBindsAndFinalizesDuckDBTransaction(t *testing.T) {
	provider := catalog.NewInMemoryDBProvider()
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	_, err := provider.Storage().ExecContext(context.Background(), "CREATE TABLE pg_tx_control (id INTEGER)")
	require.NoError(t, err)

	session := backend.NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)
	ctx := sql.NewContext(mycontext.WithFrontendQuery(context.Background()), sql.WithSession(session))

	handled, err := postgresTransactionControl(ctx, &tree.BeginTransaction{}, provider)
	require.True(t, handled)
	require.NoError(t, err)
	_, tx := adapter.TryGetTxnBinding(ctx)
	require.NotNil(t, tx)
	_, err = tx.ExecContext(ctx, "INSERT INTO pg_tx_control VALUES (1)")
	require.NoError(t, err)

	handled, err = postgresTransactionControl(ctx, &tree.RollbackTransaction{}, provider)
	require.True(t, handled)
	require.NoError(t, err)
	require.Nil(t, session.TryGetTxn())

	var count int
	require.NoError(t, provider.Storage().QueryRowContext(context.Background(), "SELECT count(*) FROM pg_tx_control").Scan(&count))
	require.Zero(t, count)
}

func TestPostgresTransactionControlCommitLeavesCommittedRows(t *testing.T) {
	provider := catalog.NewInMemoryDBProvider()
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	_, err := provider.Storage().ExecContext(context.Background(), "CREATE TABLE pg_tx_commit (id INTEGER)")
	require.NoError(t, err)

	session := backend.NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)
	ctx := sql.NewContext(mycontext.WithFrontendQuery(context.Background()), sql.WithSession(session))

	err = beginPostgresTransaction(ctx, &tree.BeginTransaction{})
	require.NoError(t, err)
	tx := session.TryGetTxn()
	require.NotNil(t, tx)
	_, err = tx.ExecContext(ctx, "INSERT INTO pg_tx_commit VALUES (1)")
	require.NoError(t, err)
	require.NoError(t, commitPostgresTransaction(ctx))
	require.Nil(t, session.TryGetTxn())

	var count int
	require.NoError(t, provider.Storage().QueryRowContext(context.Background(), "SELECT count(*) FROM pg_tx_commit").Scan(&count))
	require.Equal(t, 1, count)
}

func TestPostgresExplicitBeginRetainsOwnerAcrossStatementContexts(t *testing.T) {
	provider := catalog.NewInMemoryDBProvider()
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	_, err := provider.Storage().ExecContext(context.Background(), "CREATE TABLE pg_tx_owner (id INTEGER)")
	require.NoError(t, err)

	session := backend.NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)
	beginCtx := sql.NewContext(
		withPostgresConnectionHandler(mycontext.WithFrontendQuery(context.Background()), &ConnectionHandler{}),
		sql.WithSession(session),
	)
	require.NoError(t, beginPostgresTransaction(beginCtx, &tree.BeginTransaction{}))
	owner, tx := adapter.TryGetTxnBinding(beginCtx)
	require.NotNil(t, owner)
	require.NotNil(t, tx)

	// Each context represents a separate frontend statement/request. The
	// explicit BEGIN must keep both the transaction and its physical owner
	// stable instead of reacquiring the session connection for the next query.
	statementCtx := sql.NewContext(mycontext.WithFrontendQuery(context.Background()), sql.WithSession(session))
	gotOwner, gotTx := adapter.TryGetTxnBinding(statementCtx)
	require.Same(t, owner, gotOwner)
	require.Same(t, tx, gotTx)
	execer, executionOwner, executionTx, err := adapter.GetExecutionSnapshot(statementCtx)
	require.NoError(t, err)
	require.Same(t, owner, executionOwner)
	require.Same(t, tx, executionTx)
	_, err = execer.ExecContext(statementCtx, "INSERT INTO pg_tx_owner VALUES (1)")
	require.NoError(t, err)

	selectCtx := sql.NewContext(mycontext.WithFrontendQuery(context.Background()), sql.WithSession(session))
	execer, executionOwner, executionTx, err = adapter.GetExecutionSnapshot(selectCtx)
	require.NoError(t, err)
	require.Same(t, owner, executionOwner)
	require.Same(t, tx, executionTx)
	var count int
	require.NoError(t, execer.QueryRowContext(selectCtx, "SELECT count(*) FROM pg_tx_owner").Scan(&count))
	require.Equal(t, 1, count)

	rollbackCtx := sql.NewContext(mycontext.WithFrontendQuery(context.Background()), sql.WithSession(session))
	handled, err := postgresTransactionControl(rollbackCtx, &tree.RollbackTransaction{}, provider)
	require.True(t, handled)
	require.NoError(t, err)
	require.Nil(t, session.TryGetTxn())
	remainingOwner, remainingTx := adapter.TryGetTxnBinding(selectCtx)
	require.Same(t, owner, remainingOwner)
	require.Nil(t, remainingTx)

	require.NoError(t, provider.Storage().QueryRowContext(context.Background(), "SELECT count(*) FROM pg_tx_owner").Scan(&count))
	require.Zero(t, count)
}

func TestFinalizeImplicitTransactionRejectsReplacementBinding(t *testing.T) {
	oldConn := new(stdsql.Conn)
	oldTx := new(stdsql.Tx)
	replacementConn := new(stdsql.Conn)
	replacementTx := new(stdsql.Tx)
	session := &replacingTransactionSession{
		Session:     memory.NewSession(sql.NewBaseSession(), nil),
		conn:        oldConn,
		tx:          oldTx,
		replaceConn: replacementConn,
		replacement: replacementTx,
	}
	ctx := sql.NewContext(
		withPostgresConnectionHandler(mycontext.WithFrontendQuery(context.Background()), &ConnectionHandler{}),
		sql.WithSession(session),
	)
	h := &ConnectionHandler{
		implicitTx:     oldTx,
		implicitTxCtx:  ctx,
		implicitTxConn: oldConn,
		txStatus:       ReadyForQueryTransactionIndicator_Idle,
	}

	err := h.finalizeImplicitPostgresTransaction(true)
	require.ErrorContains(t, err, "binding replaced")
	require.True(t, session.commitCalled)
	require.Same(t, replacementTx, session.tx)
	require.Same(t, replacementConn, session.conn)
	require.Nil(t, h.implicitTx)
	require.Equal(t, ReadyForQueryTransactionIndicator_FailedTransactionBlock, h.readyForQueryStatus())
}

func TestPostgresTransactionControlNestedBeginIsNoOp(t *testing.T) {
	provider := catalog.NewInMemoryDBProvider()
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	session := backend.NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)
	ctx := sql.NewContext(mycontext.WithFrontendQuery(context.Background()), sql.WithSession(session))

	require.NoError(t, beginPostgresTransaction(ctx, &tree.BeginTransaction{}))
	first := session.TryGetTxn()
	require.NotNil(t, first)

	require.NoError(t, beginPostgresTransaction(ctx, &tree.BeginTransaction{}))
	require.Same(t, first, session.TryGetTxn())
	require.NoError(t, rollbackPostgresTransaction(ctx))
}

func TestPostgresFailedCommitRollsBackAndCleansBinding(t *testing.T) {
	provider := catalog.NewInMemoryDBProvider()
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	_, err := provider.Storage().ExecContext(context.Background(), "CREATE TABLE pg_failed_commit (id INTEGER)")
	require.NoError(t, err)

	session := backend.NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)
	ctx := sql.NewContext(mycontext.WithFrontendQuery(context.Background()), sql.WithSession(session))
	require.NoError(t, beginPostgresTransaction(ctx, &tree.BeginTransaction{}))
	tx := session.TryGetTxn()
	require.NotNil(t, tx)
	_, err = tx.ExecContext(ctx, "INSERT INTO pg_failed_commit VALUES (1)")
	require.NoError(t, err)
	// A statement error puts the PostgreSQL protocol in E. COMMIT must then
	// discard the underlying transaction instead of attempting tx.Commit().
	_, err = tx.ExecContext(ctx, "INSERT INTO pg_failed_commit (missing) VALUES (2)")
	require.Error(t, err)

	failedCtx := ctx.WithContext(withPostgresFailedTransactionControl(ctx))
	handled, err := postgresTransactionControl(failedCtx, &tree.CommitTransaction{}, provider)
	require.True(t, handled)
	require.NoError(t, err)
	require.Nil(t, session.TryGetTxn())

	var count int
	require.NoError(t, provider.Storage().QueryRowContext(context.Background(), "SELECT count(*) FROM pg_failed_commit").Scan(&count))
	require.Zero(t, count)
}

func TestPostgresBeginInFailedBlockDoesNotOpenTransaction(t *testing.T) {
	failedTx := new(stdsql.Tx)
	session := &replacingTransactionSession{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		conn:    new(stdsql.Conn),
		tx:      failedTx,
	}
	h := &ConnectionHandler{txStatus: ReadyForQueryTransactionIndicator_FailedTransactionBlock}
	ctx := sql.NewContext(
		withPostgresConnectionHandler(context.Background(), h),
		sql.WithSession(session),
	)

	err := beginPostgresTransaction(ctx, &tree.BeginTransaction{})
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "25P02", pgErr.Code)
	require.Zero(t, session.getTxnCalls, "failed BEGIN must not call GetTxn")
	require.Same(t, failedTx, session.tx, "failed BEGIN must leave the existing binding untouched")
	require.Equal(t, ReadyForQueryTransactionIndicator_FailedTransactionBlock, h.readyForQueryStatus())
}

func TestPostgresBeginWithOwnerlessExplicitBindingFailsClosed(t *testing.T) {
	activeTx := new(stdsql.Tx)
	session := &replacingTransactionSession{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		tx:      activeTx,
	}
	h := &ConnectionHandler{txStatus: ReadyForQueryTransactionIndicator_TransactionBlock}
	ctx := sql.NewContext(
		withPostgresConnectionHandler(context.Background(), h),
		sql.WithSession(session),
	)

	err := beginPostgresTransaction(ctx, &tree.BeginTransaction{})
	require.ErrorContains(t, err, "explicit transaction owner is missing")
	require.Zero(t, session.getTxnCalls)
	require.Nil(t, session.tx, "the ownerless mapping must be removed by identity")
	require.Equal(t, ReadyForQueryTransactionIndicator_FailedTransactionBlock, h.readyForQueryStatus())
}

func TestPostgresDatabaseDDLIsOutsideImplicitTransactions(t *testing.T) {
	for _, query := range []string{
		"CREATE DATABASE ddl_scope_test",
		"DROP DATABASE ddl_scope_test",
	} {
		statements, err := parser.Parse(query)
		require.NoError(t, err)
		require.Len(t, statements, 1)
		converted := ConvertedStatement{AST: statements[0].AST, String: query}
		require.False(t, implicitPostgresStatement(converted), query)

		idle := &ConnectionHandler{txStatus: ReadyForQueryTransactionIndicator_Idle}
		require.NoError(t, idle.ensureImplicitPostgresTransaction(converted), query)

		active := &ConnectionHandler{txStatus: ReadyForQueryTransactionIndicator_TransactionBlock}
		err = active.ensureImplicitPostgresTransaction(converted)
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr, query)
		require.Equal(t, "25001", pgErr.Code, query)
		name, ok := postgresDatabaseDDLName(statements[0].AST)
		require.True(t, ok)
		require.Equal(t, name+" cannot run inside a transaction block", pgErr.Message, query)
	}
}

func TestPostgresDatabaseDDLRejectsFailedBlockWithStickyError(t *testing.T) {
	statements, err := parser.Parse("CREATE DATABASE ddl_failed_test")
	require.NoError(t, err)
	ctx := sql.NewContext(
		withPostgresConnectionHandler(context.Background(), &ConnectionHandler{
			txStatus: ReadyForQueryTransactionIndicator_FailedTransactionBlock,
		}),
	)
	err = rejectPostgresDatabaseDDLInTransaction(ctx, statements[0].AST)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "25P02", pgErr.Code)
}

func TestPostgresImplicitBindingWithoutOwnerRemainsFailed(t *testing.T) {
	activeTx := new(stdsql.Tx)
	session := &replacingTransactionSession{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		tx:      activeTx,
	}
	h := &ConnectionHandler{txStatus: ReadyForQueryTransactionIndicator_Idle, implicitTx: activeTx}
	ctx := sql.NewContext(
		withPostgresConnectionHandler(context.Background(), h),
		sql.WithSession(session),
	)
	h.implicitTxCtx = ctx

	err := h.validateImplicitBinding()
	require.ErrorContains(t, err, "implicit transaction owner is missing")
	require.Nil(t, h.implicitTx)
	require.Nil(t, session.tx)
	require.Equal(t, ReadyForQueryTransactionIndicator_FailedTransactionBlock, h.readyForQueryStatus())
}

func TestFinalizeImplicitRollbackWithoutDuckHandlerDoesNotPanic(t *testing.T) {
	owner := new(stdsql.Conn)
	activeTx := new(stdsql.Tx)
	session := &replacingTransactionSession{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		conn:    owner,
		tx:      activeTx,
	}
	h := &ConnectionHandler{
		implicitTx:     activeTx,
		implicitTxCtx:  nil,
		implicitTxConn: owner,
		duckHandler:    nil,
	}
	ctx := sql.NewContext(
		withPostgresConnectionHandler(context.Background(), h),
		sql.WithSession(session),
	)
	h.implicitTxCtx = ctx

	require.NotPanics(t, func() {
		require.NoError(t, h.finalizeImplicitPostgresTransaction(false))
	})
	require.True(t, session.rollbackCalled)
	require.Nil(t, h.implicitTx)
	require.Nil(t, session.tx)
	require.Equal(t, ReadyForQueryTransactionIndicator_Idle, h.readyForQueryStatus())
}
