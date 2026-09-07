package pgserver

import (
	"bytes"
	"context"
	stdsql "database/sql"
	"encoding/binary"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/apecloud/myduckserver/mycontext"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// lifecycleAuditErrorReader lets pgproto3 decode a complete prefix and then
// surface a deterministic receive-side failure on the next message boundary.
// This models both a client EOF and a transport/decode error after an
// interrupted extended exchange.
type lifecycleAuditErrorReader struct {
	remaining []byte
	err       error
}

func malformedParseFrame() []byte {
	// Parse contains two NUL-terminated strings followed by a two-byte
	// parameter-count field. Keep the frame length valid but omit that field so
	// pgproto3 returns its decoder error rather than a transport short-read.
	frame := make([]byte, 7)
	frame[0] = 'P'
	binary.BigEndian.PutUint32(frame[1:5], 6)
	copy(frame[5:], []byte{0, 0})
	return frame
}

func (r *lifecycleAuditErrorReader) Read(p []byte) (int, error) {
	if len(r.remaining) == 0 {
		return 0, r.err
	}
	n := copy(p, r.remaining)
	r.remaining = r.remaining[n:]
	return n, nil
}

// discardImplicitSession is a minimal raw-transaction session double kept in
// this file so the receive-error regression does not depend on another test
// file's uncommitted helper type.
type discardImplicitSession struct {
	*memory.Session
	conn           *stdsql.Conn
	tx             *stdsql.Tx
	rollbackCalled bool
	callback       func(bool)
	events         *[]string
}

func (s *discardImplicitSession) GetConn(context.Context) (*stdsql.Conn, error) {
	return s.conn, nil
}

func (s *discardImplicitSession) GetTxn(context.Context, *stdsql.TxOptions) (*stdsql.Tx, error) {
	return s.tx, nil
}

func (s *discardImplicitSession) GetCatalogConn(context.Context) (*stdsql.Conn, error) {
	return s.conn, nil
}

func (s *discardImplicitSession) GetCatalogTxn(context.Context, *stdsql.TxOptions) (*stdsql.Tx, error) {
	return s.tx, nil
}

func (s *discardImplicitSession) TryGetTxn() *stdsql.Tx {
	return s.tx
}

func (s *discardImplicitSession) GetTxnBinding() (*stdsql.Conn, *stdsql.Tx) {
	return s.conn, s.tx
}

func (s *discardImplicitSession) GetCurrentCatalog() string { return "memory" }

func (s *discardImplicitSession) GetCurrentSchema() string { return "main" }

func (s *discardImplicitSession) CloseTxn() { s.tx = nil }

func (s *discardImplicitSession) CloseConn() { s.conn = nil }

func (s *discardImplicitSession) CommitTxn(*stdsql.Tx) error { return nil }

func (s *discardImplicitSession) RollbackTxn(*stdsql.Tx) (*stdsql.Conn, bool, error) {
	if s.events != nil {
		*s.events = append(*s.events, "rollback")
	}
	s.rollbackCalled = true
	s.tx = nil
	if s.callback != nil {
		callback := s.callback
		s.callback = nil
		callback(false)
	}
	return s.conn, true, nil
}

func (s *discardImplicitSession) RegisterPostPhysicalTransactionCleanup(
	_ *sql.Context,
	_ *stdsql.Tx,
	callback func(bool),
) bool {
	s.callback = callback
	return true
}

func TestDiscardToSyncReceiveErrorCleansCopyLifecycle(t *testing.T) {
	receiveErr := errors.New("copy receive failed")
	loader := &handlerCopyLifecycleLoader{}
	var snapshotCalls, operationCalls, pendingCalls atomic.Int32
	lease := newPostgresCopyLease(
		func() { snapshotCalls.Add(1) },
		func() { operationCalls.Add(1) },
	)
	state := &copyFromStdinState{
		ctx:        sql.NewEmptyContext(),
		dataLoader: loader,
		binding: postgresCopyBinding{
			lease: lease,
		},
	}
	h := &ConnectionHandler{
		backend: pgproto3.NewBackend(
			&lifecycleAuditErrorReader{err: receiveErr},
			io.Discard,
		),
		waitForSync:        true,
		txStatus:           ReadyForQueryTransactionIndicator_TransactionBlock,
		copyFromStdinState: state,
		pendingCopyRelease: func() { pendingCalls.Add(1) },
		pendingCommandCompletes: []*pgproto3.CommandComplete{
			{CommandTag: []byte("COPY 1")},
		},
		pendingProtocolMessages: []pgproto3.BackendMessage{
			&pgproto3.CommandComplete{CommandTag: []byte("COPY 1")},
		},
	}

	err := h.discardToSync()
	require.ErrorIs(t, err, receiveErr)
	require.Nil(t, h.copyFromStdinState)
	require.False(t, h.waitForSync)
	require.Equal(t, ReadyForQueryTransactionIndicator_FailedTransactionBlock, h.readyForQueryStatus())
	require.Equal(t, 1, loader.abortCalls)
	require.Equal(t, 1, loader.waitCalls)
	require.Equal(t, int32(1), snapshotCalls.Load())
	require.Equal(t, int32(1), operationCalls.Load())
	require.Equal(t, int32(1), pendingCalls.Load())
	require.Empty(t, h.pendingCommandCompletes)
	require.Empty(t, h.pendingProtocolMessages)

	// A second teardown attempt sees no state or pending release and must not
	// invoke any lifecycle callback again.
	err = h.discardToSync()
	require.ErrorIs(t, err, receiveErr)
	require.Equal(t, 1, loader.abortCalls)
	require.Equal(t, 1, loader.waitCalls)
	require.Equal(t, int32(1), snapshotCalls.Load())
	require.Equal(t, int32(1), operationCalls.Load())
	require.Equal(t, int32(1), pendingCalls.Load())
}

func TestDiscardToSyncDecodeErrorRollsBackImplicitCopy(t *testing.T) {
	malformed := malformedParseFrame()
	probe := pgproto3.NewBackend(bytes.NewReader(malformed), io.Discard)
	_, expectedDecodeErr := probe.Receive()
	require.Error(t, expectedDecodeErr)
	owner := new(stdsql.Conn)
	tx := new(stdsql.Tx)
	var events []string
	session := &discardImplicitSession{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		conn:    owner,
		tx:      tx,
		events:  &events,
	}
	ctx := sql.NewContext(
		mycontext.WithFrontendQuery(context.Background()),
		sql.WithSession(session),
	)
	loader := &handlerCopyLifecycleLoader{}
	var snapshotCalls, operationCalls, pendingCalls atomic.Int32
	lease := newPostgresCopyLease(
		func() {
			snapshotCalls.Add(1)
			events = append(events, "snapshot-release")
		},
		func() {
			operationCalls.Add(1)
			events = append(events, "operation-release")
		},
	)
	require.True(t, lease.registerPhysicalFinalization(ctx, tx))
	h := &ConnectionHandler{
		backend:            pgproto3.NewBackend(bytes.NewReader(malformed), io.Discard),
		waitForSync:        true,
		implicitTx:         tx,
		implicitTxCtx:      ctx,
		implicitTxConn:     owner,
		copyFromStdinState: &copyFromStdinState{ctx: ctx, dataLoader: loader, binding: postgresCopyBinding{lease: lease}},
		pendingCopyRelease: func() {
			pendingCalls.Add(1)
			events = append(events, "pending-release")
		},
		pendingCommandCompletes: []*pgproto3.CommandComplete{
			{CommandTag: []byte("COPY 1")},
		},
		txStatus: ReadyForQueryTransactionIndicator_Idle,
	}

	err := h.discardToSync()
	require.EqualError(t, err, expectedDecodeErr.Error())
	require.Nil(t, h.copyFromStdinState)
	require.Nil(t, h.implicitTx)
	require.Nil(t, session.tx)
	require.True(t, session.rollbackCalled)
	require.False(t, h.waitForSync)
	require.Equal(t, ReadyForQueryTransactionIndicator_Idle, h.readyForQueryStatus())
	require.Equal(t, 1, loader.abortCalls)
	require.Equal(t, 1, loader.waitCalls)
	require.Equal(t, int32(1), snapshotCalls.Load())
	require.Equal(t, int32(1), operationCalls.Load())
	require.Equal(t, int32(1), pendingCalls.Load())
	require.Empty(t, h.pendingCommandCompletes)
	require.Empty(t, h.pendingProtocolMessages)
	require.Equal(t, []string{"snapshot-release", "pending-release", "rollback", "operation-release"}, events)
}

func TestDiscardToSyncEOFWithoutCopyStillReleasesPendingLease(t *testing.T) {
	leaseErr := io.ErrUnexpectedEOF
	var pendingCalls atomic.Int32
	h := &ConnectionHandler{
		backend:     pgproto3.NewBackend(&lifecycleAuditErrorReader{err: io.EOF}, io.Discard),
		waitForSync: true,
		txStatus:    ReadyForQueryTransactionIndicator_TransactionBlock,
		pendingCopyRelease: func() {
			pendingCalls.Add(1)
		},
	}

	err := h.discardToSync()
	require.ErrorIs(t, err, leaseErr)
	require.False(t, h.waitForSync)
	require.Equal(t, ReadyForQueryTransactionIndicator_FailedTransactionBlock, h.readyForQueryStatus())
	require.Equal(t, int32(1), pendingCalls.Load())
	require.Nil(t, h.pendingCopyRelease)
}
