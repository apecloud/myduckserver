package pgserver

import (
	"fmt"
	"sync"

	"github.com/apecloud/myduckserver/pgserver/logrepl"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
)

type Server struct {
	Listener *Listener

	NewInternalCtx func() *sql.Context
}

var (
	once     sync.Once
	instance *Server
)

func NewServer(host string, port int, newCtx func() *sql.Context, options ...ListenerOpt) (*Server, error) {
	// Ensure the instance is created only once
	once.Do(func() {
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
			panic(err)
		}

		instance = &Server{Listener: listener, NewInternalCtx: newCtx}
	})

	return instance, nil
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

// GetServerInstance retrieves the single instance of Server.
func GetServerInstance() *Server {
	return instance
}
