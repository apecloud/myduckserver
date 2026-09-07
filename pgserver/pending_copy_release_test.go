package pgserver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/apecloud/myduckserver/mycontext"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

// orderedRollbackSession lets this regression observe the exact boundary
// between releasing a completed COPY lease and starting implicit rollback
// without changing the production session implementation.
type orderedRollbackSession struct {
	*replacingTransactionSession
	onRollback func()
}

func (s *orderedRollbackSession) RollbackTxn(tx *stdsql.Tx) (*stdsql.Conn, bool, error) {
	if s.onRollback != nil {
		s.onRollback()
	}
	return s.replacingTransactionSession.RollbackTxn(tx)
}

// A completed extended COPY keeps its lease until Sync so a successful group
// can commit before releasing the provider operation. An error in a later
// pipelined message must release that lease before rollback, or the rollback
// reservation can wait on its own operation forever.
func TestImplicitErrorReleasesPendingCopyLeaseBeforeRollback(t *testing.T) {
	owner := new(stdsql.Conn)
	transaction := new(stdsql.Tx)
	baseSession := &replacingTransactionSession{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		conn:    owner,
		tx:      transaction,
	}
	var events []string
	session := &orderedRollbackSession{
		replacingTransactionSession: baseSession,
		onRollback: func() {
			events = append(events, "rollback-start")
		},
	}
	h := &ConnectionHandler{
		implicitTx:     transaction,
		implicitTxConn: owner,
		txStatus:       ReadyForQueryTransactionIndicator_Idle,
	}
	ctx := sql.NewContext(
		withPostgresConnectionHandler(mycontext.WithFrontendQuery(context.Background()), h),
		sql.WithSession(session),
	)
	h.implicitTxCtx = ctx

	var releaseCalls atomic.Int32
	h.pendingCopyRelease = func() {
		events = append(events, "lease-release")
		releaseCalls.Add(1)
	}

	err := h.finishImplicitPostgresError(errors.New("pipelined statement failed"))
	require.ErrorContains(t, err, "pipelined statement failed")
	require.Equal(t, int32(1), releaseCalls.Load())
	require.True(t, session.rollbackCalled)
	require.Equal(t, []string{"lease-release", "rollback-start"}, events)
	require.Nil(t, session.tx)
	require.Nil(t, h.pendingCopyRelease)
	require.Nil(t, h.implicitTx)
	require.Equal(t, ReadyForQueryTransactionIndicator_Idle, h.readyForQueryStatus())

	// The release slot is cleared at the error boundary, so a repeated cleanup
	// call cannot invoke the callback again.
	_ = h.finishImplicitPostgresError(nil)
	require.Equal(t, int32(1), releaseCalls.Load())
}
