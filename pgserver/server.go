package pgserver

import (
	"fmt"

	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
)

type Server struct {
	Listener       *Listener
	Provider       *catalog.DatabaseProvider
	ConnPool       *backend.ConnectionPool
	NewInternalCtx func() *sql.Context
}

func NewServer(provider *catalog.DatabaseProvider, connPool *backend.ConnectionPool, host string, port int, password string, newCtx func() *sql.Context, options ...ListenerOpt) (*Server, error) {
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
	return &Server{Listener: listener, Provider: provider, ConnPool: connPool, NewInternalCtx: newCtx}, nil
}

func (s *Server) Start() {
	s.Listener.Accept(s)
}

func (s *Server) Close() {
	s.Listener.Close()
}
