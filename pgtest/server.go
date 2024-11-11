package pgtest

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/pgserver"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
)

func CreateTestServer(t *testing.T, port int) (ctx context.Context, pgServer *pgserver.Server, conn *pgx.Conn, close func() error, err error) {
	provider := catalog.NewInMemoryDBProvider()
	pool := backend.NewConnectionPool(provider.CatalogName(), provider.Connector(), provider.Storage())

	engine := sqle.NewDefault(provider)

	builder := backend.NewDuckBuilder(engine.Analyzer.ExecBuilder, pool, provider)
	engine.Analyzer.ExecBuilder = builder

	config := server.Config{
		Address: fmt.Sprintf("127.0.0.1:%d", port-1), // Unused
	}

	sb := backend.NewSessionBuilder(provider, pool)
	tracer := sql.NoopTracer

	sm := server.NewSessionManager(
		sb, tracer,
		engine.Analyzer.Catalog.Database,
		engine.MemoryManager,
		engine.ProcessList,
		config.Address,
	)

	var connID atomic.Uint32

	pgServer, err = pgserver.NewServer(
		"127.0.0.1", port,
		func() *sql.Context {
			session := backend.NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider, pool)
			return sql.NewContext(context.Background(), sql.WithSession(session))
		},
		pgserver.WithEngine(engine),
		pgserver.WithSessionManager(sm),
		pgserver.WithConnID(&connID),
	)
	if err != nil {
		panic(err)
	}
	go pgServer.Start()

	ctx = context.Background()

	close = func() error {
		pgServer.Listener.Close()
		return errors.Join(
			pool.Close(),
			provider.Close(),
		)
	}

	dsn := fmt.Sprintf("postgres://mysql:@127.0.0.1:%d/postgres", port)
	conn, err = pgx.Connect(ctx, dsn)
	if err != nil {
		close()
		return nil, nil, nil, nil, err
	}
	return
}
