package pgserver

import (
	"fmt"

	"github.com/apecloud/myduckserver/pgserver/logrepl"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
)

type Server struct {
	Listener *Listener

	NewInternalCtx func() *sql.Context
}

func NewServer(host string, port int, newCtx func() *sql.Context, options ...ListenerOpt) (*Server, error) {
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
	return &Server{Listener: listener, NewInternalCtx: newCtx}, nil
}

func (s *Server) Start() {
	s.Listener.Accept()
}

func (s *Server) StartReplication(primaryDsn string, slotName string) error {
	replicator, err := logrepl.NewLogicalReplicator(primaryDsn)
	if err != nil {
		return err
	}
	return replicator.StartReplication(s.NewInternalCtx(), slotName)
}

func (s *Server) Close() {
	s.Listener.Close()
}
