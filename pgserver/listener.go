// Copyright 2023 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgserver

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"sync/atomic"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/netutil"
)

var (
	processID   = uint32(os.Getpid())
	certificate tls.Certificate //TODO: move this into the mysql.ListenerConfig
)

// Listener listens for connections to process PostgreSQL requests into Dolt requests.
type Listener struct {
	listener net.Listener
	cfg      mysql.ListenerConfig

	engine *gms.Engine
	sm     *server.SessionManager
	connID *atomic.Uint32
}

var _ server.ProtocolListener = (*Listener)(nil)

type ListenerOpt func(*Listener)

func WithCertificate(cert tls.Certificate) ListenerOpt {
	return func(l *Listener) {
		certificate = cert
	}
}

func WithEngine(engine *gms.Engine) ListenerOpt {
	return func(l *Listener) {
		l.engine = engine
	}
}

func WithSessionManager(sm *server.SessionManager) ListenerOpt {
	return func(l *Listener) {
		l.sm = sm
	}
}

func WithConnID(connID *atomic.Uint32) ListenerOpt {
	return func(l *Listener) {
		l.connID = connID
	}
}

// NewListener creates a new Listener.
func NewListener(listenerCfg mysql.ListenerConfig) (*Listener, error) {
	return NewListenerWithOpts(listenerCfg)
}

func NewListenerWithOpts(listenerCfg mysql.ListenerConfig, opts ...ListenerOpt) (*Listener, error) {
	l := &Listener{
		listener: listenerCfg.Listener,
		cfg:      listenerCfg,
	}

	for _, opt := range opts {
		opt(l)
	}

	return l, nil
}

// Accept handles incoming connections.
func (l *Listener) Accept() {
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			if err.Error() == "use of closed network connection" {
				break
			}
			fmt.Printf("Unable to accept connection:\n%v\n", err)
			continue
		}

		// Configure read timeouts on this connection
		// TODO: use timeouts from the live server values
		if l.cfg.ConnReadTimeout != 0 || l.cfg.ConnWriteTimeout != 0 {
			conn = netutil.NewConnWithTimeouts(conn, l.cfg.ConnReadTimeout, l.cfg.ConnWriteTimeout)
		}

		connectionHandler := NewConnectionHandler(conn, l.cfg.Handler, l.engine, l.sm, l.connID.Add(1))
		go connectionHandler.HandleConnection()
	}
}

// Close stops the handling of incoming connections.
func (l *Listener) Close() {
	_ = l.listener.Close()
}

// Addr returns the address that the listener is listening on.
func (l *Listener) Addr() net.Addr {
	return l.listener.Addr()
}

// Engine returns the engine that the listener is using.
func (l *Listener) Engine() *gms.Engine {
	return l.engine
}

// SessionManager returns the session manager that the listener is using.
func (l *Listener) SessionManager() *server.SessionManager {
	return l.sm
}
