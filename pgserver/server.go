package pgserver

import (
	"fmt"
	"sync"

	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
)

type Server struct {
	Listener       *Listener
	Provider       *catalog.DatabaseProvider
	NewInternalCtx func() *sql.Context
	ReadOnly       bool
}

var authInitOnce sync.Once

func initializeAuth() {
	authInitOnce.Do(func() {
		factory := sql.GetAuthorizationHandlerFactory()
		auth.Init(nil, nil)
		// MyDuck's GMS engine uses MySQL authorization. Its PostgreSQL frontend
		// only needs Doltgres' in-memory role database for SCRAM authentication.
		sql.SetAuthorizationHandlerFactory(factory)
	})
}

func NewServer(provider *catalog.DatabaseProvider, host string, port int, password string, newCtx func() *sql.Context, options ...ListenerOpt) (*Server, error) {
	initializeAuth()
	InitSuperuser(password)
	addr := fmt.Sprintf("%s:%d", host, port)
	l, err := server.NewListener("tcp", addr, "")
	if err != nil {
		panic(err)
	}
	listener, err := NewListenerWithOpts(
		mysql.ListenerConfig{
			Protocol: "tcp",
			Address:  addr,
			Listener: l,
		},
		options...,
	)
	if err != nil {
		return nil, err
	}
	return &Server{Listener: listener, Provider: provider, NewInternalCtx: newCtx, ReadOnly: listener.readOnly}, nil
}

func (s *Server) Start() {
	s.Listener.Accept(s)
}

func (s *Server) Close() {
	s.Listener.Close()
}
