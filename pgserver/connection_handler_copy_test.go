package pgserver

import (
	"bytes"
	"context"
	stdsql "database/sql"
	"errors"
	"io"
	"testing"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/mycontext"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

type handlerCopyLifecycleLoader struct {
	loadErr   error
	finishErr error
	abortErr  error

	loadCalls   int
	finishCalls int
	abortCalls  int
	waitCalls   int
	abortCtx    *sql.Context
}

func (l *handlerCopyLifecycleLoader) Start() <-chan error {
	ready := make(chan error)
	close(ready)
	return ready
}

func (l *handlerCopyLifecycleLoader) LoadChunk(*sql.Context, []byte) error {
	l.loadCalls++
	return l.loadErr
}

func (l *handlerCopyLifecycleLoader) Abort(ctx *sql.Context) error {
	l.abortCalls++
	l.abortCtx = ctx
	return l.abortErr
}

func (l *handlerCopyLifecycleLoader) Finish(*sql.Context) (*LoadDataResults, error) {
	l.finishCalls++
	if l.finishErr != nil {
		return nil, l.finishErr
	}
	return &LoadDataResults{}, nil
}

func (l *handlerCopyLifecycleLoader) Wait() {
	l.waitCalls++
}

func requireFailedTransactionSQLState(t *testing.T, err error) {
	t.Helper()
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "25P02", pgErr.Code)
}

func TestExtendedLookupUsesFailedTransactionErrorForMissingObject(t *testing.T) {
	tests := []struct {
		name string
		run  func(*ConnectionHandler) error
	}{
		{
			name: "describe statement",
			run: func(h *ConnectionHandler) error {
				return h.handleDescribe(&pgproto3.Describe{ObjectType: 'S', Name: "missing"})
			},
		},
		{
			name: "describe portal",
			run: func(h *ConnectionHandler) error {
				return h.handleDescribe(&pgproto3.Describe{ObjectType: 'P', Name: "missing"})
			},
		},
		{
			name: "bind",
			run: func(h *ConnectionHandler) error {
				return h.handleBind(&pgproto3.Bind{PreparedStatement: "missing"})
			},
		},
		{
			name: "execute",
			run: func(h *ConnectionHandler) error {
				return h.handleExecute(&pgproto3.Execute{Portal: "missing"})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &ConnectionHandler{txStatus: ReadyForQueryTransactionIndicator_FailedTransactionBlock}
			requireFailedTransactionSQLState(t, tt.run(h))
			require.Equal(t, ReadyForQueryTransactionIndicator_FailedTransactionBlock, h.readyForQueryStatus())
		})
	}
}

func TestCopyDataErrorAbortsAndRetainsTerminalState(t *testing.T) {
	loadErr := errors.New("load chunk failed")
	abortErr := errors.New("abort failed")
	ctx := sql.NewEmptyContext()
	loader := &handlerCopyLifecycleLoader{loadErr: loadErr, abortErr: abortErr}
	h := &ConnectionHandler{
		txStatus: ReadyForQueryTransactionIndicator_TransactionBlock,
		copyFromStdinState: &copyFromStdinState{
			ctx:        ctx,
			dataLoader: loader,
		},
	}

	stop, end, err := h.handleCopyData(&pgproto3.CopyData{Data: []byte("row")})
	require.False(t, stop)
	require.True(t, end)
	require.ErrorIs(t, err, loadErr)
	require.ErrorIs(t, err, abortErr)
	require.NotNil(t, h.copyFromStdinState)
	require.ErrorIs(t, h.copyFromStdinState.copyErr, loadErr)
	require.ErrorIs(t, h.copyFromStdinState.copyErr, abortErr)
	require.True(t, h.copyFromStdinState.copyAborted)
	require.Equal(t, ReadyForQueryTransactionIndicator_FailedTransactionBlock, h.readyForQueryStatus())
	require.Equal(t, 1, loader.loadCalls)
	require.Equal(t, 1, loader.abortCalls)
	require.Equal(t, 1, loader.waitCalls)
	require.Same(t, ctx, loader.abortCtx)

	// pgx sends CopyDone after observing the server-side error. It must be
	// consumed as a terminal acknowledgement without a second error/Ready and
	// without invoking the loader lifecycle twice.
	stop, end, err = h.handleCopyDone(&pgproto3.CopyDone{})
	require.False(t, stop)
	require.False(t, end)
	require.NoError(t, err)
	require.Nil(t, h.copyFromStdinState)
	require.Equal(t, 1, loader.abortCalls)
	require.Equal(t, 1, loader.waitCalls)
}

func TestCopyDoneErrorAbortsClearsStateAndFailsTransaction(t *testing.T) {
	finishErr := errors.New("finish failed")
	ctx := sql.NewEmptyContext()
	loader := &handlerCopyLifecycleLoader{finishErr: finishErr}
	h := &ConnectionHandler{
		txStatus: ReadyForQueryTransactionIndicator_TransactionBlock,
		copyFromStdinState: &copyFromStdinState{
			ctx:        ctx,
			dataLoader: loader,
		},
	}

	stop, end, err := h.handleCopyDone(&pgproto3.CopyDone{})
	require.False(t, stop)
	require.True(t, end)
	require.ErrorIs(t, err, finishErr)
	require.Nil(t, h.copyFromStdinState)
	require.Equal(t, ReadyForQueryTransactionIndicator_FailedTransactionBlock, h.readyForQueryStatus())
	require.Equal(t, 1, loader.finishCalls)
	require.Equal(t, 1, loader.abortCalls)
	require.Equal(t, 1, loader.waitCalls)
	require.Same(t, ctx, loader.abortCtx)
}

func TestCopyFailAbortsClearsStateAndFailsTransaction(t *testing.T) {
	ctx := sql.NewEmptyContext()
	loader := &handlerCopyLifecycleLoader{}
	h := &ConnectionHandler{
		txStatus: ReadyForQueryTransactionIndicator_TransactionBlock,
		copyFromStdinState: &copyFromStdinState{
			ctx:        ctx,
			dataLoader: loader,
		},
	}

	stop, end, err := h.handleCopyFail(&pgproto3.CopyFail{Message: "client input failed"})
	require.False(t, stop)
	require.True(t, end)
	require.ErrorIs(t, err, ErrCopyAborted)
	require.ErrorContains(t, err, "client input failed")
	require.Nil(t, h.copyFromStdinState)
	require.Equal(t, ReadyForQueryTransactionIndicator_FailedTransactionBlock, h.readyForQueryStatus())
	require.Equal(t, 1, loader.abortCalls)
	require.Equal(t, 1, loader.waitCalls)
	require.Same(t, ctx, loader.abortCtx)
}

func TestCopyFailAfterServerErrorOnlyClearsTerminalState(t *testing.T) {
	ctx := sql.NewEmptyContext()
	loader := &handlerCopyLifecycleLoader{}
	h := &ConnectionHandler{
		copyFromStdinState: &copyFromStdinState{
			ctx:         ctx,
			dataLoader:  loader,
			copyErr:     errors.New("server-side copy error"),
			copyAborted: true,
		},
	}

	stop, end, err := h.handleCopyFail(&pgproto3.CopyFail{Message: "client acknowledged"})
	require.False(t, stop)
	require.False(t, end)
	require.NoError(t, err)
	require.Nil(t, h.copyFromStdinState)
	require.Zero(t, loader.abortCalls)
	require.Zero(t, loader.waitCalls)
}

func TestCopyDataAfterServerErrorIsIgnoredUntilTerminalMessage(t *testing.T) {
	copyErr := errors.New("server-side copy error")
	loader := &handlerCopyLifecycleLoader{}
	h := &ConnectionHandler{
		copyFromStdinState: &copyFromStdinState{
			dataLoader:  loader,
			copyErr:     copyErr,
			copyAborted: true,
		},
	}

	stop, end, err := h.handleCopyData(&pgproto3.CopyData{Data: []byte("ignored")})
	require.False(t, stop)
	require.False(t, end)
	require.NoError(t, err)
	require.Same(t, loader, h.copyFromStdinState.dataLoader)
	require.Zero(t, loader.loadCalls)
}

func TestCloseBackendConnAbortsCopyLoaderBeforeConnectionRelease(t *testing.T) {
	ctx := sql.NewEmptyContext()
	loader := &handlerCopyLifecycleLoader{}
	h := &ConnectionHandler{
		copyFromStdinState: &copyFromStdinState{
			ctx:        ctx,
			dataLoader: loader,
		},
	}

	// A minimal handler has no backend connection, but closeBackendConn still
	// must drain an active COPY loader before it returns. Production handlers then
	// proceed to rollback/close the physical owner.
	h.closeBackendConn()

	require.Nil(t, h.copyFromStdinState)
	require.Equal(t, 1, loader.abortCalls)
	require.Equal(t, 1, loader.waitCalls)
	require.Same(t, ctx, loader.abortCtx)
}

func TestDiscardAllRejectsActiveTransaction(t *testing.T) {
	h := &ConnectionHandler{
		txStatus: ReadyForQueryTransactionIndicator_TransactionBlock,
	}
	err := h.discardAll(ConvertedStatement{Tag: "DISCARD"})
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "25001", pgErr.Code)
	require.Equal(t, "DISCARD ALL cannot run inside a transaction block", pgErr.Message)
	require.Equal(t, ReadyForQueryTransactionIndicator_TransactionBlock, h.readyForQueryStatus())
}

func TestDiscardAllRejectsImplicitTransactionScope(t *testing.T) {
	h := &ConnectionHandler{
		implicitTx: new(stdsql.Tx),
	}
	err := h.discardAll(ConvertedStatement{Tag: "DISCARD"})
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "25001", pgErr.Code)
}

func TestExtendedCopyDoneSendsCommandCompleteBeforeSync(t *testing.T) {
	ctx := sql.NewEmptyContext()
	loader := &handlerCopyLifecycleLoader{}
	var output bytes.Buffer
	h := &ConnectionHandler{
		backend:     pgproto3.NewBackend(bytes.NewReader(nil), &output),
		waitForSync: true,
		implicitTx:  new(stdsql.Tx),
		copyFromStdinState: &copyFromStdinState{
			ctx:        ctx,
			dataLoader: loader,
		},
	}

	stop, end, err := h.handleCopyDone(&pgproto3.CopyDone{})
	require.False(t, stop)
	require.False(t, end)
	require.NoError(t, err)
	require.Nil(t, h.copyFromStdinState)
	require.Empty(t, h.pendingCommandCompletes)

	// Extended COPY follows the same response ordering as an ordinary Execute:
	// its completion is visible before a later pipelined response or Sync.
	require.NoError(t, h.send(&pgproto3.ParseComplete{}))
	frontend := pgproto3.NewFrontend(bytes.NewReader(output.Bytes()), io.Discard)
	message, err := frontend.Receive()
	require.NoError(t, err)
	commandComplete, ok := message.(*pgproto3.CommandComplete)
	require.True(t, ok)
	require.Equal(t, "COPY 0", string(commandComplete.CommandTag))

	message, err = frontend.Receive()
	require.NoError(t, err)
	_, ok = message.(*pgproto3.ParseComplete)
	require.True(t, ok)
}

func TestSyncDuringCopyIsIgnoredUntilCopyDone(t *testing.T) {
	provider := catalog.NewInMemoryDBProvider()
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	_, err := provider.Storage().ExecContext(context.Background(), "CREATE TABLE pg_sync_copy (id INTEGER)")
	require.NoError(t, err)

	session := backend.NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)
	ctx := sql.NewContext(mycontext.WithFrontendQuery(context.Background()), sql.WithSession(session))
	tx, err := adapter.GetTxn(ctx, &stdsql.TxOptions{})
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "INSERT INTO pg_sync_copy VALUES (1)")
	require.NoError(t, err)
	owner, bound := adapter.TryGetTxnBinding(ctx)
	require.Same(t, tx, bound)
	require.NotNil(t, owner)

	engine := sqle.NewDefault(provider)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	var output bytes.Buffer
	loader := &handlerCopyLifecycleLoader{}
	h := &ConnectionHandler{
		backend:        pgproto3.NewBackend(bytes.NewReader(nil), &output),
		duckHandler:    &DuckHandler{e: engine},
		waitForSync:    true,
		implicitTx:     tx,
		implicitTxCtx:  ctx,
		implicitTxConn: owner,
		copyFromStdinState: &copyFromStdinState{
			ctx:        ctx,
			dataLoader: loader,
		},
	}

	stop, end, err := h.handleMessage(&pgproto3.Sync{})
	require.False(t, stop)
	require.False(t, end)
	require.NoError(t, err)
	require.NotNil(t, h.copyFromStdinState)
	require.True(t, h.waitForSync)
	require.Zero(t, loader.abortCalls)
	require.Zero(t, loader.waitCalls)
	_, bound = adapter.TryGetTxnBinding(ctx)
	require.Same(t, tx, bound)

	// Once COPY terminates, its completion is sent immediately. A subsequent
	// Sync is the transaction boundary that commits the implicit scope.
	stop, end, err = h.handleCopyDone(&pgproto3.CopyDone{})
	require.False(t, stop)
	require.False(t, end)
	require.NoError(t, err)
	require.Nil(t, h.copyFromStdinState)
	require.Equal(t, 1, loader.finishCalls)

	stop, end, err = h.handleMessage(&pgproto3.Sync{})
	require.False(t, stop)
	require.True(t, end)
	require.NoError(t, err)
	require.Nil(t, session.TryGetTxn())

	var count int
	require.NoError(t, provider.Storage().QueryRowContext(context.Background(), "SELECT count(*) FROM pg_sync_copy").Scan(&count))
	require.Equal(t, 1, count)
}

func TestSyncDuringExplicitCopyIsIgnored(t *testing.T) {
	ctx := sql.NewEmptyContext()
	loader := &handlerCopyLifecycleLoader{}
	h := &ConnectionHandler{
		waitForSync: true,
		txStatus:    ReadyForQueryTransactionIndicator_TransactionBlock,
		copyFromStdinState: &copyFromStdinState{
			ctx:        ctx,
			dataLoader: loader,
		},
	}

	stop, end, err := h.handleMessage(&pgproto3.Sync{})
	require.False(t, stop)
	require.False(t, end)
	require.NoError(t, err)
	require.NotNil(t, h.copyFromStdinState)
	require.Equal(t, ReadyForQueryTransactionIndicator_TransactionBlock, h.readyForQueryStatus())
	require.Zero(t, loader.abortCalls)
	require.Zero(t, loader.waitCalls)
}

func TestCopyModeIgnoresFlushAndSync(t *testing.T) {
	ctx := sql.NewEmptyContext()
	loader := &handlerCopyLifecycleLoader{}
	var output bytes.Buffer
	h := &ConnectionHandler{
		backend:     pgproto3.NewBackend(bytes.NewReader(nil), &output),
		waitForSync: true,
		copyFromStdinState: &copyFromStdinState{
			ctx:        ctx,
			dataLoader: loader,
		},
	}

	stop, end, err := h.handleMessage(&pgproto3.Flush{})
	require.False(t, stop)
	require.False(t, end)
	require.NoError(t, err)
	require.NotNil(t, h.copyFromStdinState)
	require.True(t, h.waitForSync)

	stop, end, err = h.handleMessage(&pgproto3.Sync{})
	require.False(t, stop)
	require.False(t, end)
	require.NoError(t, err)
	require.NotNil(t, h.copyFromStdinState)
	require.True(t, h.waitForSync)
	require.Zero(t, loader.abortCalls)
	require.Zero(t, loader.waitCalls)
	require.Empty(t, output.Bytes())
}

func TestFlushIsHandledOutsideCopy(t *testing.T) {
	var output bytes.Buffer
	h := &ConnectionHandler{
		backend: pgproto3.NewBackend(bytes.NewReader(nil), &output),
	}

	stop, end, err := h.handleMessage(&pgproto3.Flush{})
	require.False(t, stop)
	require.False(t, end)
	require.NoError(t, err)
	require.Empty(t, output.Bytes())
}

func TestCopyModeRejectsNonCopyMessageAndAborts(t *testing.T) {
	ctx := sql.NewEmptyContext()
	loader := &handlerCopyLifecycleLoader{}
	h := &ConnectionHandler{
		waitForSync: true,
		copyFromStdinState: &copyFromStdinState{
			ctx:        ctx,
			dataLoader: loader,
		},
	}

	stop, end, err := h.handleMessage(&pgproto3.Parse{Query: "SELECT 1"})
	require.False(t, stop)
	require.False(t, end)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "08P01", pgErr.Code)
	require.Contains(t, pgErr.Message, "COPY FROM STDIN")
	require.NotNil(t, h.copyFromStdinState)
	require.True(t, h.copyFromStdinState.copyAborted)
	require.ErrorIs(t, h.copyFromStdinState.copyErr, err)
	require.Equal(t, 1, loader.abortCalls)
	require.Equal(t, 1, loader.waitCalls)
}

func TestSimpleCopyModeRejectsNonCopyMessageAndClearsState(t *testing.T) {
	ctx := sql.NewEmptyContext()
	loader := &handlerCopyLifecycleLoader{}
	h := &ConnectionHandler{
		copyFromStdinState: &copyFromStdinState{
			ctx:        ctx,
			dataLoader: loader,
		},
	}

	stop, end, err := h.handleMessage(&pgproto3.Query{String: "SELECT 1"})
	require.False(t, stop)
	require.True(t, end)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "08P01", pgErr.Code)
	require.Nil(t, h.copyFromStdinState)
	require.Equal(t, 1, loader.abortCalls)
	require.Equal(t, 1, loader.waitCalls)
}

func TestSimpleCopyErrorSentinelDoesNotSwallowNextQuery(t *testing.T) {
	var output bytes.Buffer
	h := &ConnectionHandler{
		backend: pgproto3.NewBackend(bytes.NewReader(nil), &output),
		copyFromStdinState: &copyFromStdinState{
			copyErr:     errors.New("server-side copy error"),
			copyAborted: true,
		},
	}

	stop, end, err := h.handleMessage(&pgproto3.Query{String: ";"})
	require.False(t, stop)
	require.True(t, end)
	require.NoError(t, err)
	require.Nil(t, h.copyFromStdinState)

	frontend := pgproto3.NewFrontend(bytes.NewReader(output.Bytes()), io.Discard)
	message, err := frontend.Receive()
	require.NoError(t, err)
	commandComplete, ok := message.(*pgproto3.CommandComplete)
	require.True(t, ok)
	require.Empty(t, commandComplete.CommandTag)
}

func TestProtocolErrorDropsDeferredCommandComplete(t *testing.T) {
	var output bytes.Buffer
	h := &ConnectionHandler{
		backend:                 pgproto3.NewBackend(bytes.NewReader(nil), &output),
		pendingCommandCompletes: []*pgproto3.CommandComplete{{CommandTag: []byte("COPY 1")}},
	}

	require.NoError(t, h.handleProtocolError(errors.New("commit failed"), false))
	require.Empty(t, h.pendingCommandCompletes)
	frontend := pgproto3.NewFrontend(bytes.NewReader(output.Bytes()), io.Discard)
	message, err := frontend.Receive()
	require.NoError(t, err)
	_, ok := message.(*pgproto3.ErrorResponse)
	require.True(t, ok)
	message, err = frontend.Receive()
	require.NoError(t, err)
	_, ok = message.(*pgproto3.ReadyForQuery)
	require.True(t, ok)
}
