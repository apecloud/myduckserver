package main

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

type mysqlAcceptObservedListener struct {
	net.Listener
	acceptOnce    sync.Once
	acceptEntered chan struct{}
}

func (l *mysqlAcceptObservedListener) Accept() (net.Conn, error) {
	l.acceptOnce.Do(func() { close(l.acceptEntered) })
	return l.Listener.Accept()
}

func TestMySQLServerStartReturnsAfterClose(t *testing.T) {
	testMySQLServerClose(t, false)
}

func TestMySQLServerCloseBeforeStartReturns(t *testing.T) {
	testMySQLServerClose(t, true)
}

func testMySQLServerClose(t *testing.T, closeBeforeStart bool) {
	t.Helper()
	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = tcpListener.Close() })
	listener := &mysqlAcceptObservedListener{
		Listener:      tcpListener,
		acceptEntered: make(chan struct{}),
	}

	provider := catalog.NewInMemoryDBProvider()
	engine, _ := backend.NewEngine(provider)
	t.Cleanup(func() {
		if err := engine.Close(); err != nil {
			t.Errorf("close SQL engine: %v", err)
		}
		if err := provider.Close(); err != nil {
			t.Errorf("close database provider: %v", err)
		}
	})
	mysqlServer, err := server.NewServer(
		server.Config{
			Protocol:                 "tcp",
			Address:                  tcpListener.Addr().String(),
			Listener:                 listener,
			DisableConnectionWatcher: true,
		},
		engine,
		sql.NewContext,
		backend.NewSessionBuilder(provider),
		nil,
	)
	require.NoError(t, err)

	closeServer := sync.OnceValue(mysqlServer.Close)
	t.Cleanup(func() {
		if err := closeServer(); err != nil {
			t.Errorf("close MySQL server: %v", err)
		}
	})

	serveDone := make(chan error, 1)
	if closeBeforeStart {
		require.NoError(t, closeServer())
	}
	go func() {
		serveDone <- mysqlServer.Start()
	}()

	select {
	case <-listener.acceptEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("MySQL server did not enter its accept loop")
	}

	if !closeBeforeStart {
		require.NoError(t, closeServer())
	}
	select {
	case err := <-serveDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("MySQL server Start did not return after Close")
	}
}
