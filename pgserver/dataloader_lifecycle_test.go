package pgserver

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func newTestPipeDataLoader(t *testing.T, rows int64) *PipeDataLoader {
	t.Helper()
	dir := t.TempDir()
	pipePath := filepath.Join(dir, "copy.pipe")
	require.NoError(t, syscall.Mkfifo(pipePath, 0o600))

	loader := &PipeDataLoader{
		pipePath: pipePath,
		rowCount: make(chan int64, 1),
		logger:   logrus.New().WithField("test", t.Name()),
	}
	loader.read = func() {
		reader, err := os.Open(pipePath)
		if err != nil {
			loader.err.Store(&err)
			return
		}
		defer reader.Close()
		_, _ = io.Copy(io.Discard, reader)
		loader.rowCount <- rows
	}
	return loader
}

func TestPipeDataLoaderFinishClosesAndWaits(t *testing.T) {
	loader := newTestPipeDataLoader(t, 4)
	ready := loader.Start()
	require.NoError(t, <-ready)
	require.NoError(t, loader.LoadChunk(sql.NewEmptyContext(), []byte("1\n")))

	result, err := loader.Finish(sql.NewEmptyContext())
	require.NoError(t, err)
	require.Equal(t, int32(4), result.RowsLoaded)
	require.Eventually(t, func() bool {
		select {
		case <-loader.done:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	_, statErr := os.Stat(loader.pipePath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestPipeDataLoaderFinishCleansUpAfterReaderError(t *testing.T) {
	loader := newTestPipeDataLoader(t, 0)
	ready := loader.Start()
	require.NoError(t, <-ready)

	readerErr := errors.New("reader failed")
	loader.err.Store(&readerErr)
	result, err := loader.Finish(sql.NewEmptyContext())
	require.Nil(t, result)
	require.ErrorIs(t, err, readerErr)
	select {
	case <-loader.done:
	case <-time.After(time.Second):
		t.Fatal("Finish returned before the reader exited")
	}
	_, statErr := os.Stat(loader.pipePath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestPipeDataLoaderAbortIsIdempotentAndPreStartSafe(t *testing.T) {
	loader := newTestPipeDataLoader(t, 0)
	require.NoError(t, loader.Abort(sql.NewEmptyContext()))
	require.NoError(t, loader.Abort(sql.NewEmptyContext()))
	_, err := loader.Finish(sql.NewEmptyContext())
	require.Error(t, err)

	ready := loader.Start()
	require.ErrorIs(t, <-ready, ErrCopyAborted)
	select {
	case <-loader.done:
	default:
		// Abort happened before Start, so no reader goroutine is expected.
	}
	_, statErr := os.Stat(loader.pipePath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestPipeDataLoaderStartIsIdempotentAndLoadBeforeReadyIsSafe(t *testing.T) {
	loader := newTestPipeDataLoader(t, 1)
	first := loader.Start()
	second := loader.Start()
	require.True(t, first == second)
	require.NoError(t, <-first)
	require.NoError(t, loader.LoadChunk(sql.NewEmptyContext(), []byte("x\n")))
	_, err := loader.Finish(sql.NewEmptyContext())
	require.NoError(t, err)

	var zero PipeDataLoader
	require.NotPanics(t, func() {
		require.Error(t, zero.LoadChunk(sql.NewEmptyContext(), []byte("x")))
	})
	_, finishErr := zero.Finish(sql.NewEmptyContext())
	require.Error(t, finishErr)
}

func TestPipeDataLoaderStartWithoutReaderReturnsError(t *testing.T) {
	loader := &PipeDataLoader{}
	ready := loader.Start()
	require.EqualError(t, <-ready, "COPY data loader has no reader")
	require.EqualError(t, loader.currentError(), "COPY data loader has no reader")
}

func TestPipeDataLoaderStartupErrorClosesUnblockPipe(t *testing.T) {
	loader := newTestPipeDataLoader(t, 0)
	readerErr := errors.New("reader startup failed")
	loader.read = func() {
		loader.err.Store(&readerErr)
		unblock, err := os.OpenFile(loader.pipePath, os.O_RDONLY|syscall.O_NONBLOCK, os.ModeNamedPipe)
		if err != nil {
			loader.err.Store(&err)
			return
		}
		loader.errPipe.Store(unblock)
	}
	t.Cleanup(func() {
		if unblock := loader.errPipe.Swap(nil); unblock != nil {
			_ = unblock.Close()
		}
	})

	require.ErrorIs(t, <-loader.Start(), readerErr)
	loader.Wait()
	require.Nil(t, loader.errPipe.Load())
}

func TestPipeDataLoaderWaitClosesLateUnblockPipe(t *testing.T) {
	loader := newTestPipeDataLoader(t, 0)
	readerErr := errors.New("reader startup failed")
	loader.err.Store(&readerErr)

	opened := make(chan *os.File, 1)
	publish := make(chan struct{})
	loader.read = func() {
		unblock, err := os.OpenFile(loader.pipePath, os.O_RDONLY|syscall.O_NONBLOCK, os.ModeNamedPipe)
		if err != nil {
			loader.err.Store(&err)
			return
		}
		opened <- unblock
		<-publish
		loader.errPipe.Store(unblock)
	}

	ready := loader.Start()
	var unblock *os.File
	select {
	case unblock = <-opened:
	case <-time.After(time.Second):
		t.Fatal("reader did not open the unblock pipe")
	}
	require.ErrorIs(t, <-ready, readerErr)
	close(publish)
	loader.Wait()
	require.Nil(t, loader.errPipe.Load())
	require.ErrorIs(t, unblock.Close(), os.ErrClosed)
	loader.Wait() // Finalization is idempotent.
}

func TestArrowDataLoaderMissingPipeDoesNotBlock(t *testing.T) {
	loader := &ArrowDataLoader{
		PipeDataLoader: PipeDataLoader{
			ctx:      sql.NewEmptyContext(),
			pipePath: filepath.Join(t.TempDir(), "missing.pipe"),
			rowCount: make(chan int64, 1),
			logger:   logrus.New().WithField("test", t.Name()),
		},
	}
	done := make(chan struct{})
	go func() {
		loader.executeInsert("INSERT INTO missing FROM __arrow", loader.pipePath)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ArrowDataLoader blocked while reporting a missing FIFO")
	}
	require.Error(t, loader.currentError())
	select {
	case _, ok := <-loader.rowCount:
		require.False(t, ok)
	default:
		t.Fatal("ArrowDataLoader did not close its row-count channel")
	}
}
