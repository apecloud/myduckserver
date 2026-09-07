package pgserver

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestRejectArrowCopyInTransaction(t *testing.T) {
	provider := catalog.NewInMemoryDBProvider()
	t.Cleanup(func() { require.NoError(t, provider.Close()) })

	session := backend.NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)
	ctx := sql.NewContext(mycontext.WithFrontendQuery(context.Background()), sql.WithSession(session))
	require.NoError(t, beginPostgresTransaction(ctx, &tree.BeginTransaction{}))

	require.ErrorIs(t, rejectArrowCopyInTransaction(ctx), ErrArrowCopyInTransaction)
	_, err := NewArrowDataLoader(ctx, nil, "", nil, nil, "")
	require.ErrorIs(t, err, ErrArrowCopyInTransaction)
	_, err = NewArrowWriter(ctx, nil, "", nil, nil, "", "")
	require.ErrorIs(t, err, ErrArrowCopyInTransaction)
	require.NoError(t, rollbackPostgresTransaction(ctx))
	require.NoError(t, rejectArrowCopyInTransaction(ctx))
}

type waitingCopyLoader struct {
	abortStarted chan struct{}
	release      chan struct{}
	waitCalled   atomic.Bool

	mu       sync.Mutex
	abortCtx *sql.Context
}

func (l *waitingCopyLoader) Start() <-chan error { return closedErrorChannel() }

func (l *waitingCopyLoader) LoadChunk(*sql.Context, []byte) error { return nil }

func (l *waitingCopyLoader) Abort(ctx *sql.Context) error {
	l.mu.Lock()
	l.abortCtx = ctx
	l.mu.Unlock()
	close(l.abortStarted)
	return nil
}

func (l *waitingCopyLoader) Finish(*sql.Context) (*LoadDataResults, error) {
	return &LoadDataResults{}, nil
}

func (l *waitingCopyLoader) Wait() {
	l.waitCalled.Store(true)
	<-l.release
}

func closedErrorChannel() <-chan error {
	ch := make(chan error)
	close(ch)
	return ch
}

func TestHandleCopyFailAbortsAndWaitsForLoader(t *testing.T) {
	loader := &waitingCopyLoader{
		abortStarted: make(chan struct{}),
		release:      make(chan struct{}),
	}
	ctx := sql.NewEmptyContext()
	h := &ConnectionHandler{
		copyFromStdinState: &copyFromStdinState{
			ctx:        ctx,
			dataLoader: loader,
		},
	}

	type result struct {
		stop, end bool
		err       error
	}
	resultCh := make(chan result, 1)
	go func() {
		stop, end, err := h.handleCopyFail(nil)
		resultCh <- result{stop: stop, end: end, err: err}
	}()

	select {
	case <-loader.abortStarted:
	case <-time.After(time.Second):
		t.Fatal("COPY FAIL did not invoke DataLoader.Abort")
	}

	select {
	case <-resultCh:
		t.Fatal("COPY FAIL returned before the loader finished")
	case <-time.After(20 * time.Millisecond):
	}
	require.True(t, loader.waitCalled.Load())
	close(loader.release)

	select {
	case got := <-resultCh:
		require.False(t, got.stop)
		require.True(t, got.end)
		require.ErrorIs(t, got.err, ErrCopyAborted)
	case <-time.After(time.Second):
		t.Fatal("COPY FAIL did not return after loader completion")
	}
	require.Nil(t, h.copyFromStdinState)
	loader.mu.Lock()
	require.Same(t, ctx, loader.abortCtx)
	loader.mu.Unlock()
}

func TestArrowWriterCloseCancelsAndWaits(t *testing.T) {
	ctx := sql.NewEmptyContext()
	copyCtx, cancel := newCopyContext(ctx)
	done := make(chan struct{})
	writer := &ArrowWriter{
		ctx:      copyCtx,
		cancel:   cancel,
		pipePath: t.TempDir() + "/arrow-copy",
		done:     done,
	}
	writer.started.Store(true)

	go func() {
		<-copyCtx.Done()
		close(done)
	}()

	writer.Close()
	require.ErrorIs(t, copyCtx.Err(), context.Canceled)
	writer.Close() // Close is safe after the producer has already exited.
}
