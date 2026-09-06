package main

import (
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestMainRegistersShutdownSignalsOnlyForServerMode(t *testing.T) {
	contents, err := os.ReadFile("main.go")
	require.NoError(t, err)
	source := string(contents)
	serverModeGuard := strings.Index(source, "\tif !initMode {")
	notify := strings.Index(source, "\tsignal.Notify(shutdownSignals")
	initBranch := strings.Index(source, "\tif initMode {")
	serverProvider := strings.Index(source, "\tprovider, err := catalog.NewDBProvider")
	require.GreaterOrEqual(t, serverModeGuard, 0)
	require.Greater(t, notify, serverModeGuard)
	require.Greater(t, initBranch, notify)
	require.Greater(t, serverProvider, notify)
}

func TestRunServerLifecycleSignalStopsAndJoinsAllLoops(t *testing.T) {
	shutdownSignals := make(chan os.Signal, 2)
	releaseLoops := make(chan struct{})
	var started sync.WaitGroup
	started.Add(3)
	var stopped atomic.Int32
	var stopCalls atomic.Int32
	var finalizeCalls atomic.Int32

	loops := make([]serveLoop, 0, 3)
	for _, name := range []string{"mysql", "postgres", "flight"} {
		name := name
		loops = append(loops, serveLoop{
			name: name,
			serve: func() error {
				started.Done()
				<-releaseLoops
				stopped.Add(1)
				return nil
			},
		})
	}

	go func() {
		started.Wait()
		shutdownSignals <- syscall.SIGTERM
		shutdownSignals <- syscall.SIGTERM
	}()

	err := runServerLifecycle(
		shutdownSignals,
		loops,
		func() error {
			stopCalls.Add(1)
			close(releaseLoops)
			return nil
		},
		func() error {
			finalizeCalls.Add(1)
			require.Equal(t, int32(3), stopped.Load(), "finalize ran before every serve loop returned")
			return nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, int32(3), stopped.Load())
	require.Equal(t, int32(1), stopCalls.Load())
	require.Equal(t, int32(1), finalizeCalls.Load())
}

func TestRunServerLifecyclePreloadedSignalStopsAndJoinsAllLoops(t *testing.T) {
	shutdownSignals := make(chan os.Signal, 1)
	shutdownSignals <- syscall.SIGTERM
	releaseLoops := make(chan struct{})
	var stopped atomic.Int32

	loops := make([]serveLoop, 0, 3)
	for _, name := range []string{"mysql", "postgres", "flight"} {
		name := name
		loops = append(loops, serveLoop{
			name: name,
			serve: func() error {
				<-releaseLoops
				stopped.Add(1)
				return nil
			},
		})
	}

	err := runServerLifecycle(
		shutdownSignals,
		loops,
		func() error {
			close(releaseLoops)
			return nil
		},
		func() error {
			require.Equal(t, int32(3), stopped.Load(), "finalize ran before preloaded-signal joins completed")
			return nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, int32(3), stopped.Load())
}

func TestRunServerLifecycleUnexpectedServeReturn(t *testing.T) {
	var stopCalls atomic.Int32
	var finalizeCalls atomic.Int32

	err := runServerLifecycle(
		make(chan os.Signal),
		[]serveLoop{{name: "mysql", serve: func() error { return nil }}},
		func() error {
			stopCalls.Add(1)
			return nil
		},
		func() error {
			finalizeCalls.Add(1)
			return nil
		},
	)

	require.ErrorIs(t, err, errServeLoopStopped)
	require.Equal(t, int32(1), stopCalls.Load())
	require.Equal(t, int32(1), finalizeCalls.Load())
}

func TestRunServerLifecycleUnexpectedReturnStopsAndJoinsPeer(t *testing.T) {
	peerStarted := make(chan struct{})
	releasePeer := make(chan struct{})
	var peerStopped atomic.Bool

	err := runServerLifecycle(
		make(chan os.Signal),
		[]serveLoop{
			{
				name: "postgres",
				serve: func() error {
					close(peerStarted)
					<-releasePeer
					peerStopped.Store(true)
					return nil
				},
			},
			{
				name: "mysql",
				serve: func() error {
					<-peerStarted
					return nil
				},
			},
		},
		func() error {
			close(releasePeer)
			return nil
		},
		func() error {
			require.True(t, peerStopped.Load(), "finalize ran before the peer serve loop returned")
			return nil
		},
	)

	require.ErrorIs(t, err, errServeLoopStopped)
	require.True(t, peerStopped.Load())
}

func TestRunServerLifecyclePreservesObservableErrors(t *testing.T) {
	serveErr := errors.New("serve failed")
	stopErr := errors.New("stop failed")
	finalizeErr := errors.New("finalize failed")

	err := runServerLifecycle(
		make(chan os.Signal),
		[]serveLoop{{name: "mysql", serve: func() error { return serveErr }}},
		func() error { return stopErr },
		func() error { return finalizeErr },
	)

	require.ErrorIs(t, err, serveErr)
	require.ErrorIs(t, err, stopErr)
	require.ErrorIs(t, err, finalizeErr)
}

func TestRunServerLifecycleSignalPreservesStoppedLoopError(t *testing.T) {
	shutdownSignals := make(chan os.Signal, 1)
	releaseLoop := make(chan struct{})
	started := make(chan struct{})
	serveErr := errors.New("serve stopped with error")
	var stopped atomic.Bool

	go func() {
		<-started
		shutdownSignals <- syscall.SIGTERM
	}()

	err := runServerLifecycle(
		shutdownSignals,
		[]serveLoop{{
			name: "mysql",
			serve: func() error {
				close(started)
				<-releaseLoop
				stopped.Store(true)
				return serveErr
			},
		}},
		func() error {
			close(releaseLoop)
			return nil
		},
		func() error {
			require.True(t, stopped.Load(), "finalize ran before the serve loop returned")
			return nil
		},
	)

	require.ErrorIs(t, err, serveErr)
}

func TestNormalizeFlightServeError(t *testing.T) {
	require.NoError(t, normalizeFlightServeError(grpc.ErrServerStopped))
	serveErr := errors.New("flight listener failed")
	require.ErrorIs(t, normalizeFlightServeError(serveErr), serveErr)
}

func TestFlightServerShutdownBeforeServeReturns(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	server := flight.NewServerWithMiddleware(nil)
	server.InitListener(listener)
	server.Shutdown()
	require.NoError(t, normalizeFlightServeError(server.Serve()))
}
