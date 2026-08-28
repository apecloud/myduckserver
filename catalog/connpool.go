// Copyright 2024-2025 ApeCloud, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package catalog

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/sirupsen/logrus"
)

type ConnectionPool struct {
	*stdsql.DB
	connector             *duckdb.Connector
	defaultCatalog        string
	conns                 sync.Map // concurrent-safe map[uint32]*stdsql.Conn
	txns                  sync.Map // concurrent-safe map[uint32]*stdsql.Tx
	closedConns           sync.Map // connection IDs that completed their lifecycle
	initializerMu         sync.RWMutex
	connectionInitializer func(context.Context, *stdsql.Conn) error
	registerMySQLUDFsOnce sync.Once
	registerMySQLUDFsErr  error
}

func NewConnectionPool(connector *duckdb.Connector, db *stdsql.DB, defaultCatalog string) *ConnectionPool {
	return &ConnectionPool{
		DB:             db,
		connector:      connector,
		defaultCatalog: defaultCatalog,
	}
}

func (p *ConnectionPool) Connector() *duckdb.Connector {
	return p.connector
}

// SetConnectionInitializer installs a hook that runs whenever a logical
// session acquires a connection outside an active session transaction. The
// hook receives the acquisition context, including its query-origin
// classification. Running it for both new and reused logical connections
// prevents a connection that was previously used by one origin from carrying
// session settings into another origin; GetTxn runs it before BeginTx and then
// keeps transaction-scoped state stable until that transaction closes.
func (p *ConnectionPool) SetConnectionInitializer(initializer func(context.Context, *stdsql.Conn) error) {
	p.initializerMu.Lock()
	p.connectionInitializer = initializer
	p.initializerMu.Unlock()
}

func (p *ConnectionPool) initializeConnection(ctx context.Context, conn *stdsql.Conn) error {
	p.initializerMu.RLock()
	initializer := p.connectionInitializer
	p.initializerMu.RUnlock()
	if initializer == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return initializer(ctx, conn)
}

// CurrentSchema retrieves the current schema of the connection.
// Returns an empty string if the connection is not established
// or the schema cannot be retrieved.
func (p *ConnectionPool) CurrentSchema(id uint32) string {
	entry, ok := p.conns.Load(id)
	if !ok {
		return ""
	}
	conn := entry.(*stdsql.Conn)
	var schema string
	if err := conn.QueryRowContext(context.Background(), "SELECT CURRENT_SCHEMA").Scan(&schema); err != nil {
		logrus.WithError(err).Error("Failed to get current schema")
		return ""
	}
	return schema
}

// CurrentCatalog retrieves the current catalog of the connection. Before the
// first connection, it returns the owning provider's catalog so GMS can resolve
// fully qualified names. Closed or broken connections still return empty.
func (p *ConnectionPool) CurrentCatalog(id uint32) string {
	entry, ok := p.conns.Load(id)
	if !ok {
		if _, closed := p.closedConns.Load(id); closed {
			return ""
		}
		return p.defaultCatalog
	}
	conn := entry.(*stdsql.Conn)
	var catalog string
	if err := conn.QueryRowContext(context.Background(), "SELECT CURRENT_CATALOG").Scan(&catalog); err != nil {
		logrus.WithError(err).Error("Failed to get current catalog")
		return ""
	}
	return catalog
}

func (p *ConnectionPool) GetConn(ctx context.Context, id uint32) (*stdsql.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var conn *stdsql.Conn
	entry, ok := p.conns.Load(id)
	if !ok {
		c, err := p.DB.Conn(ctx)
		if err != nil {
			return nil, err
		}
		if _, transactionActive := p.txns.Load(id); !transactionActive {
			if err := p.initializeConnection(ctx, c); err != nil {
				_ = c.Close()
				return nil, err
			}
		}
		if err := p.registerMySQLUDFs(c); err != nil {
			_ = c.Close()
			return nil, err
		}
		p.closedConns.Delete(id)
		p.conns.Store(id, c)
		conn = c
	} else {
		conn = entry.(*stdsql.Conn)
		// A session transaction owns this connection's transaction-scoped
		// state. Re-running the initializer here would execute LOAD/CREATE
		// SECRET inside that transaction and could alter or roll back with user
		// work. GetTxn initializes before BeginTx; keep the state stable until
		// the transaction is closed.
		if _, transactionActive := p.txns.Load(id); !transactionActive {
			if err := p.initializeConnection(ctx, conn); err != nil {
				// Do not leave a failed or partially initialized connection available
				// to a later request. CompareAndDelete avoids removing a replacement
				// installed by a concurrent recovery path.
				p.conns.CompareAndDelete(id, conn)
				p.closedConns.Store(id, struct{}{})
				_ = conn.Close()
				return nil, err
			}
		}
	}
	return conn, nil
}

// DuckDB stores registered scalar UDFs in the database catalog shared by all connections.
func (p *ConnectionPool) registerMySQLUDFs(conn *stdsql.Conn) error {
	p.registerMySQLUDFsOnce.Do(func() {
		if err := registerMySQLRand(conn); err != nil {
			p.registerMySQLUDFsErr = fmt.Errorf("register mysql_rand: %w", err)
			return
		}
		if err := registerMySQLRandomBytes(conn); err != nil {
			p.registerMySQLUDFsErr = fmt.Errorf("register mysql_random_bytes: %w", err)
			return
		}
		if err := registerMySQLStringToVector(conn); err != nil {
			p.registerMySQLUDFsErr = fmt.Errorf("register string_to_vector: %w", err)
		}
	})
	return p.registerMySQLUDFsErr
}

func (p *ConnectionPool) GetConnForSchema(ctx context.Context, id uint32, schemaName string) (*stdsql.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := p.GetConn(ctx, id)
	if err != nil {
		return nil, err
	}
	// A session transaction owns this connection and its transaction-scoped
	// schema state. Do not issue CURRENT_SCHEMA/USE through *sql.Conn while a
	// *sql.Tx may be executing on the same physical connection; GetTxn performs
	// schema selection before BeginTx.
	if _, transactionActive := p.txns.Load(id); transactionActive {
		return conn, nil
	}

	if schemaName != "" {
		// Schema selection is session state, but it should retain the origin
		// value while avoiding cancellation of a request that is already being
		// serviced.
		schemaCtx := context.WithoutCancel(ctx)
		var currentSchema string
		if err := conn.QueryRowContext(schemaCtx, "SELECT CURRENT_SCHEMA").Scan(&currentSchema); err != nil {
			logrus.WithError(err).Error("Failed to get current schema")
			return nil, err
		} else if currentSchema != schemaName {
			if _, err := conn.ExecContext(schemaCtx, "USE "+FullSchemaName(p.CurrentCatalog(id), schemaName)); err != nil {
				if IsDuckDBSetSchemaNotFoundError(err) {
					return nil, sql.ErrDatabaseNotFound.New(schemaName)
				}
				logrus.WithField("schema", schemaName).WithError(err).Error("Failed to switch schema")
				return nil, err
			}
		}
	}

	return conn, nil
}

func (p *ConnectionPool) CloseConn(id uint32) error {
	defer p.conns.Delete(id)
	defer p.closedConns.Store(id, struct{}{})
	var lastErr error
	if entry, ok := p.txns.LoadAndDelete(id); ok {
		if err := entry.(*stdsql.Tx).Rollback(); err != nil &&
			!errors.Is(err, stdsql.ErrTxDone) &&
			!strings.Contains(err.Error(), "no transaction is active") {
			logrus.WithError(err).Warn("Failed to rollback transaction")
			lastErr = err
		}
	}
	entry, ok := p.conns.Load(id)
	if ok {
		conn := entry.(*stdsql.Conn)
		if err := conn.Raw(func(driverConn any) error {
			// When driver.ErrBadConn is returned here,
			// the connection will not be put back into
			// the pool and will be closed instead.
			return driver.ErrBadConn
		}); err != nil && !errors.Is(err, driver.ErrBadConn) {
			logrus.WithError(err).Warn("Failed to close connection during Raw function call")
			return errors.Join(lastErr, err)
		}
		if err := conn.Close(); err != nil && !errors.Is(err, stdsql.ErrConnDone) {
			logrus.WithError(err).Warn("Failed to close connection")
			return errors.Join(lastErr, err)
		}
	}
	return lastErr
}

func (p *ConnectionPool) GetTxn(ctx context.Context, id uint32, schemaName string, options *stdsql.TxOptions) (*stdsql.Tx, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var tx *stdsql.Tx
	entry, ok := p.txns.Load(id)
	if !ok {
		conn, err := p.GetConnForSchema(ctx, id, schemaName)
		if err != nil {
			return nil, err
		}
		// A session transaction can span multiple protocol requests (for example,
		// after SET autocommit=0), so a request-scoped cancellation must not end it.
		t, err := conn.BeginTx(context.WithoutCancel(ctx), options)
		if err != nil {
			return nil, err
		}
		p.txns.Store(id, t)
		tx = t
	} else {
		tx = entry.(*stdsql.Tx)
	}
	return tx, nil
}

func (p *ConnectionPool) TryGetTxn(id uint32) *stdsql.Tx {
	entry, ok := p.txns.Load(id)
	if !ok {
		return nil
	}
	return entry.(*stdsql.Tx)
}

func (p *ConnectionPool) CloseTxn(id uint32) {
	p.txns.Delete(id)
}

func (p *ConnectionPool) Close() error {
	var txns []*stdsql.Tx
	p.txns.Range(func(_, value any) bool {
		txns = append(txns, value.(*stdsql.Tx))
		return true
	})
	var lastErr error
	for _, tx := range txns {
		if err := tx.Rollback(); err != nil && !strings.Contains(err.Error(), "no transaction is active") {
			logrus.WithError(err).Warn("Failed to rollback transaction")
			lastErr = err
		}
	}

	var conns []*stdsql.Conn
	p.conns.Range(func(_, value any) bool {
		conns = append(conns, value.(*stdsql.Conn))
		return true
	})
	for _, conn := range conns {
		if err := conn.Close(); err != nil && !errors.Is(err, stdsql.ErrConnDone) {
			logrus.WithError(err).Warn("Failed to close connection")
			lastErr = err
		}
	}
	p.conns.Clear()
	p.txns.Clear()
	p.closedConns.Clear()
	return errors.Join(lastErr, p.DB.Close())
}

func (p *ConnectionPool) Reset(connector *duckdb.Connector, db *stdsql.DB) error {
	err := p.Close()
	if err != nil {
		return fmt.Errorf("failed to close connection pool: %w", err)
	}

	p.conns.Clear()
	p.txns.Clear()
	p.closedConns.Clear()
	p.DB = db
	p.connector = connector
	p.registerMySQLUDFsOnce = sync.Once{}
	p.registerMySQLUDFsErr = nil

	return nil
}
