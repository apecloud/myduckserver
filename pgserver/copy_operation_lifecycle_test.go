package pgserver

import (
	"bytes"
	"context"
	stdsql "database/sql"
	"errors"
	"testing"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// copyOperationLifecycleLoader is deliberately synchronous. The production
// loader may finish on a worker, but the handler must observe the same
// ownership boundary after Finish returns.
type copyOperationLifecycleLoader struct {
	finishErr error
	onFinish  func()
	onAbort   func()
	onWait    func()
}

func (l *copyOperationLifecycleLoader) Start() <-chan error {
	ready := make(chan error)
	close(ready)
	return ready
}

func (*copyOperationLifecycleLoader) LoadChunk(*sql.Context, []byte) error { return nil }

func (l *copyOperationLifecycleLoader) Abort(*sql.Context) error {
	if l.onAbort != nil {
		l.onAbort()
	}
	return nil
}

func (l *copyOperationLifecycleLoader) Finish(*sql.Context) (*LoadDataResults, error) {
	if l.onFinish != nil {
		l.onFinish()
	}
	if l.finishErr != nil {
		return nil, l.finishErr
	}
	return &LoadDataResults{RowsLoaded: 1}, nil
}

func (l *copyOperationLifecycleLoader) Wait() {
	if l.onWait != nil {
		l.onWait()
	}
}

// copyPhysicalCleanupRegistrar captures the callback used by a raw physical
// transaction finalizer without requiring a live database/sql driver.
type copyPhysicalCleanupRegistrar struct {
	*memory.Session
	callback func(bool)
}

func (s *copyPhysicalCleanupRegistrar) RegisterPostPhysicalTransactionCleanup(
	_ *sql.Context,
	_ *stdsql.Tx,
	callback func(bool),
) bool {
	s.callback = callback
	return true
}

func TestCopyDoneReleasesSnapshotImmediatelyAfterFinish(t *testing.T) {
	var events []string
	lease := newPostgresCopyLease(
		func() { events = append(events, "snapshot-release") },
		func() { events = append(events, "operation-release") },
	)
	loader := &copyOperationLifecycleLoader{
		onFinish: func() { events = append(events, "finish") },
	}
	h := &ConnectionHandler{
		backend:  pgproto3.NewBackend(bytes.NewReader(nil), &bytes.Buffer{}),
		txStatus: ReadyForQueryTransactionIndicator_TransactionBlock,
		copyFromStdinState: &copyFromStdinState{
			dataLoader: loader,
			binding: postgresCopyBinding{
				lease: lease,
			},
		},
	}

	stop, end, err := h.handleCopyDone(&pgproto3.CopyDone{})
	require.False(t, stop)
	require.True(t, end)
	require.NoError(t, err)
	require.Equal(t,
		[]string{"finish", "snapshot-release", "operation-release"},
		events,
		"COPY must release its snapshot as soon as Finish drains the loader",
	)
	require.Nil(t, h.copyFromStdinState)
}

func TestCopyDoneRegisteredOperationWaitsForPhysicalFinalization(t *testing.T) {
	session := &copyPhysicalCleanupRegistrar{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
	}
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))
	var snapshotCalls, operationCalls int
	lease := newPostgresCopyLease(
		func() { snapshotCalls++ },
		func() { operationCalls++ },
	)
	transaction := new(stdsql.Tx)
	require.True(t, lease.registerPhysicalFinalization(ctx, transaction))
	require.True(t, lease.OperationDeferred())

	h := &ConnectionHandler{
		deferredCommandComplete: true,
		txStatus:                ReadyForQueryTransactionIndicator_TransactionBlock,
		copyFromStdinState: &copyFromStdinState{
			ctx:        ctx,
			dataLoader: &copyOperationLifecycleLoader{},
			binding: postgresCopyBinding{
				lease: lease,
			},
		},
	}

	_, _, err := h.handleCopyDone(&pgproto3.CopyDone{})
	require.NoError(t, err)
	require.Equal(t, 1, snapshotCalls)
	require.Zero(t, operationCalls,
		"COPY completion must not release an operation owned by the physical finalizer")
	require.NotNil(t, session.callback)

	session.callback(false)
	require.Equal(t, 1, operationCalls)
	// A defensive duplicate completion must remain idempotent.
	session.callback(false)
	require.Equal(t, 1, operationCalls)
}

type copyCommitLifecycleSession struct {
	*replacingTransactionSession
	onCommit func()
}

func (s *copyCommitLifecycleSession) CommitTxn(tx *stdsql.Tx) error {
	if s.onCommit != nil {
		s.onCommit()
	}
	return s.replacingTransactionSession.CommitTxn(tx)
}

func TestSimpleCopyDoneReleasesFallbackOperationAfterImplicitCommit(t *testing.T) {
	owner := new(stdsql.Conn)
	transaction := new(stdsql.Tx)
	var events []string
	base := &replacingTransactionSession{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		conn:    owner,
		tx:      transaction,
	}
	session := &copyCommitLifecycleSession{
		replacingTransactionSession: base,
		onCommit:                    func() { events = append(events, "commit") },
	}
	h := &ConnectionHandler{
		deferredCommandComplete: true,
		implicitTx:              transaction,
		implicitTxConn:          owner,
		copyFromStdinState: &copyFromStdinState{
			dataLoader: &copyOperationLifecycleLoader{
				onFinish: func() { events = append(events, "finish") },
			},
			binding: postgresCopyBinding{
				lease: newPostgresCopyLease(
					func() { events = append(events, "snapshot-release") },
					func() { events = append(events, "operation-release") },
				),
			},
		},
	}
	ctx := sql.NewContext(
		withPostgresConnectionHandler(context.Background(), h),
		sql.WithSession(session),
	)
	h.implicitTxCtx = ctx

	_, _, err := h.handleCopyDone(&pgproto3.CopyDone{})
	require.NoError(t, err)
	require.Equal(t,
		[]string{"finish", "snapshot-release", "commit", "operation-release"},
		events,
		"simple-query COPY must finalize its implicit transaction before fallback release",
	)
	require.Nil(t, h.implicitTx)
	require.Equal(t, ReadyForQueryTransactionIndicator_Idle, h.readyForQueryStatus())
}

func TestCopyDoneErrorReleasesFallbackOperation(t *testing.T) {
	finishErr := errors.New("finish failed")
	var events []string
	loader := &copyOperationLifecycleLoader{
		finishErr: finishErr,
		onFinish:  func() { events = append(events, "finish") },
		onAbort:   func() { events = append(events, "abort") },
		onWait:    func() { events = append(events, "wait") },
	}
	h := &ConnectionHandler{
		deferredCommandComplete: true,
		txStatus:                ReadyForQueryTransactionIndicator_TransactionBlock,
		copyFromStdinState: &copyFromStdinState{
			dataLoader: loader,
			binding: postgresCopyBinding{
				lease: newPostgresCopyLease(
					func() { events = append(events, "snapshot-release") },
					func() { events = append(events, "operation-release") },
				),
			},
		},
	}

	_, _, err := h.handleCopyDone(&pgproto3.CopyDone{})
	require.ErrorIs(t, err, finishErr)
	require.Equal(t,
		[]string{"finish", "abort", "wait", "snapshot-release", "operation-release"},
		events,
		"an unsuccessful COPY must release both fallback leases after draining the loader",
	)
	require.Nil(t, h.copyFromStdinState)
	require.Equal(t, ReadyForQueryTransactionIndicator_FailedTransactionBlock, h.readyForQueryStatus())
}
