package pgserver

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

type postgresAcceptObservedListener struct {
	net.Listener
	acceptOnce    sync.Once
	acceptEntered chan struct{}
}

func (l *postgresAcceptObservedListener) Accept() (net.Conn, error) {
	l.acceptOnce.Do(func() { close(l.acceptEntered) })
	return l.Listener.Accept()
}

func TestPostgresServerStartReturnsAfterClose(t *testing.T) {
	testPostgresServerClose(t, false)
}

func TestPostgresServerCloseBeforeStartReturns(t *testing.T) {
	testPostgresServerClose(t, true)
}

func testPostgresServerClose(t *testing.T, closeBeforeStart bool) {
	t.Helper()
	provider := catalog.NewInMemoryDBProvider()
	t.Cleanup(func() {
		require.NoError(t, provider.Close())
	})

	newCtx := func() *sql.Context {
		session := backend.NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)
		return sql.NewContext(context.Background(), sql.WithSession(session))
	}
	postgresServer, err := NewServer(provider, "127.0.0.1", 0, "", newCtx)
	require.NoError(t, err)
	t.Cleanup(postgresServer.Close)

	listener := &postgresAcceptObservedListener{
		Listener:      postgresServer.Listener.listener,
		acceptEntered: make(chan struct{}),
	}
	postgresServer.Listener.listener = listener

	serveDone := make(chan struct{})
	if closeBeforeStart {
		postgresServer.Close()
	}
	go func() {
		postgresServer.Start()
		close(serveDone)
	}()

	select {
	case <-listener.acceptEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("PostgreSQL server did not enter its accept loop")
	}

	if !closeBeforeStart {
		postgresServer.Close()
	}
	select {
	case <-serveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("PostgreSQL server Start did not return after Close")
	}
}
