package pgserver

import (
	"context"
	stdsql "database/sql"
	"testing"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

type copyBindingTestSession struct {
	*memory.Session
	conn *stdsql.Conn
	tx   *stdsql.Tx
}

var _ adapter.ConnectionHolder = (*copyBindingTestSession)(nil)
var _ adapter.TransactionBindingHolder = (*copyBindingTestSession)(nil)
var _ adapter.ExecutionSnapshotHolder = (*copyBindingTestSession)(nil)

func (s *copyBindingTestSession) GetConn(context.Context) (*stdsql.Conn, error) {
	return s.conn, nil
}

func (s *copyBindingTestSession) GetTxn(context.Context, *stdsql.TxOptions) (*stdsql.Tx, error) {
	return s.tx, nil
}

func (s *copyBindingTestSession) GetCatalogConn(context.Context) (*stdsql.Conn, error) {
	return s.conn, nil
}

func (s *copyBindingTestSession) GetCatalogTxn(context.Context, *stdsql.TxOptions) (*stdsql.Tx, error) {
	return s.tx, nil
}

func (s *copyBindingTestSession) TryGetTxn() *stdsql.Tx { return s.tx }

func (s *copyBindingTestSession) GetTxnBinding() (*stdsql.Conn, *stdsql.Tx) {
	return s.conn, s.tx
}

func (s *copyBindingTestSession) GetExecutionSnapshot(context.Context, bool) (*stdsql.Conn, *stdsql.Tx, error) {
	return s.conn, s.tx, nil
}

func (s *copyBindingTestSession) GetCurrentCatalog() string { return "memory" }

func (s *copyBindingTestSession) GetCurrentSchema() string { return "main" }

func (s *copyBindingTestSession) CloseTxn() {}

func (s *copyBindingTestSession) CloseConn() {}

func TestPostgresCopyBindingRejectsLifecycleReplacement(t *testing.T) {
	ownerConn := new(stdsql.Conn)
	ownerTx := new(stdsql.Tx)
	session := &copyBindingTestSession{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		conn:    ownerConn,
		tx:      ownerTx,
	}
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))

	binding, err := capturePostgresCopyBinding(ctx)
	require.NoError(t, err)
	require.True(t, binding.snapshotCaptured)
	require.Same(t, ownerConn, binding.ownerConn)
	require.Same(t, ownerTx, binding.ownerTx)
	require.Same(t, ownerTx, binding.execer)
	require.NoError(t, binding.validate(ctx))

	session.tx = new(stdsql.Tx)
	require.ErrorContains(t, binding.validate(ctx), "binding changed")

	session.tx = ownerTx
	session.conn = new(stdsql.Conn)
	require.ErrorContains(t, binding.validate(ctx), "binding changed")

	session.conn = nil
	session.tx = nil
	require.ErrorContains(t, binding.validate(ctx), "binding changed")
}

func TestPostgresCopyBindingRejectsExecutorReplacement(t *testing.T) {
	ownerConn := new(stdsql.Conn)
	ownerTx := new(stdsql.Tx)
	session := &copyBindingTestSession{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		conn:    ownerConn,
		tx:      ownerTx,
	}
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))
	binding := postgresCopyBinding{
		execer:           ownerConn,
		ownerConn:        ownerConn,
		ownerTx:          ownerTx,
		snapshotCaptured: true,
	}

	require.ErrorContains(t, binding.validate(ctx), "executor changed")
}
