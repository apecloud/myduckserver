package backend

import (
	"context"
	stdsql "database/sql"
	"net"
	"sync/atomic"
	"testing"

	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"
)

// replacementOnSessionEnd models SessionManager replacing a connection's GMS
// session while MyHandler.ConnectionClosed is between its initial binding
// snapshot and the final pool close. It uses only public pool operations: the
// old raw transaction is finalized, its generic entry is retired, and a new
// connection is acquired under the same numeric session ID.
type replacementOnSessionEnd struct {
	*Session
	pool        *catalog.ConnectionPool
	id          uint32
	replacement *stdsql.Conn
	err         error
}

func (s *replacementOnSessionEnd) SessionEnd() {
	s.Session.SessionEnd()
	if s.err != nil {
		return
	}
	if s.err = s.pool.CloseConn(s.id); s.err != nil {
		return
	}
	s.replacement, s.err = s.pool.GetConn(context.Background(), s.id)
}

// MyHandler must close the exact transaction/owner pair it observed before
// GMS removes the session. If GMS installs a replacement generic connection
// during that callback, the stale pair cleanup must leave the replacement
// usable instead of closing it by numeric connection ID.
func TestMyHandlerConnectionClosedPreservesReplacementConnection(t *testing.T) {
	provider := catalog.NewInMemoryDBProvider()
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	engine, _ := NewEngine(provider)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	const connectionID uint32 = 313
	baseBuilder := NewSessionBuilder(provider)
	var replacementSession *replacementOnSessionEnd
	builder := func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
		built, buildErr := baseBuilder(ctx, conn, addr)
		if buildErr != nil {
			return nil, buildErr
		}
		session, ok := built.(*Session)
		if !ok {
			t.Fatalf("session builder returned %T, want *Session", built)
		}
		replacementSession = &replacementOnSessionEnd{
			Session: session,
			pool:    provider.Pool(),
			id:      connectionID,
		}
		return replacementSession, nil
	}

	var handler *MyHandler
	srv, err := server.NewServerWithHandler(
		server.Config{
			Listener:                 listener,
			Protocol:                 "tcp",
			Address:                  listener.Addr().String(),
			DisableConnectionWatcher: true,
		},
		engine,
		sql.NewContext,
		builder,
		nil,
		func(h mysql.Handler) (mysql.Handler, error) {
			wrapped, wrapErr := WrapHandler(provider, engine, nil, false)(h)
			if wrapErr == nil {
				handler = wrapped.(*MyHandler)
			}
			return wrapped, wrapErr
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = srv.Close()
		_ = listener.Close()
	})
	require.NotNil(t, handler)

	clientConn, peerConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = peerConn.Close()
	})
	mysqlConn := &mysql.Conn{Conn: clientConn, ConnectionID: connectionID}
	sm := srv.SessionManager()
	sm.AddConn(mysqlConn)
	require.NoError(t, sm.NewSession(context.Background(), mysqlConn))
	require.NotNil(t, replacementSession)

	tx, err := replacementSession.GetCatalogTxn(context.Background(), nil)
	require.NoError(t, err)
	owner, bound := replacementSession.GetTxnBindingForRelease()
	require.NotNil(t, owner)
	require.Same(t, tx, bound)
	var callbacks atomic.Int32
	require.True(t, replacementSession.RegisterPostPhysicalTransactionCleanup(nil, tx, func(success bool) {
		require.False(t, success)
		callbacks.Add(1)
	}))

	handler.ConnectionClosed(mysqlConn)

	require.NoError(t, replacementSession.err)
	require.NotNil(t, replacementSession.replacement)
	require.NotSame(t, owner, replacementSession.replacement)
	require.Equal(t, int32(1), callbacks.Load())
	currentConn, currentTx := provider.Pool().GetTxnBindingForRelease(connectionID)
	require.Same(t, replacementSession.replacement, currentConn)
	require.Nil(t, currentTx)
	var got int
	require.NoError(t, replacementSession.replacement.QueryRowContext(context.Background(), "SELECT 1").Scan(&got))
	require.Equal(t, 1, got)
}
