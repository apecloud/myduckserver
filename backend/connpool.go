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
package backend

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/marcboeker/go-duckdb"
	"github.com/sirupsen/logrus"
)

type ConnectionPool struct {
	*stdsql.DB
	connector *duckdb.Connector
	catalog   string
	conns     sync.Map // concurrent-safe map[uint32]*stdsql.Conn
	txns      sync.Map // concurrent-safe map[uint32]*stdsql.Tx
}

func NewConnectionPool(catalog string, connector *duckdb.Connector, db *stdsql.DB) *ConnectionPool {
	return &ConnectionPool{
		DB:        db,
		connector: connector,
		catalog:   catalog,
	}
}

func (p *ConnectionPool) Connector() *duckdb.Connector {
	return p.connector
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
	if err := conn.QueryRowContext(context.Background(), "SELECT CURRENT_SCHEMA()").Scan(&schema); err != nil {
		logrus.WithError(err).Error("Failed to get current schema")
		return ""
	}
	return schema
}

func (p *ConnectionPool) GetConn(ctx context.Context, id uint32) (*stdsql.Conn, error) {
	var conn *stdsql.Conn
	entry, ok := p.conns.Load(id)
	if !ok {
		c, err := p.DB.Conn(ctx)
		if err != nil {
			return nil, err
		}
		p.conns.Store(id, c)
		conn = c
	} else {
		conn = entry.(*stdsql.Conn)
	}
	return conn, nil
}

func (p *ConnectionPool) GetConnForSchema(ctx context.Context, id uint32, schemaName string) (*stdsql.Conn, error) {
	conn, err := p.GetConn(ctx, id)
	if err != nil {
		return nil, err
	}

	if schemaName != "" {
		var currentSchema string
		if err := conn.QueryRowContext(context.Background(), "SELECT CURRENT_SCHEMA()").Scan(&currentSchema); err != nil {
			logrus.WithError(err).Error("Failed to get current schema")
			return nil, err
		} else if currentSchema != schemaName {
			if _, err := conn.ExecContext(context.Background(), "USE "+catalog.FullSchemaName(p.catalog, schemaName)); err != nil {
				if catalog.IsDuckDBSetSchemaNotFoundError(err) {
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
	entry, ok := p.conns.Load(id)
	if ok {
		conn := entry.(*stdsql.Conn)
		if err := conn.Close(); err != nil {
			logrus.WithError(err).Warn("Failed to close connection")
			return err
		}
	}
	return nil
}

func (p *ConnectionPool) GetTxn(ctx context.Context, id uint32, schemaName string, options *stdsql.TxOptions) (*stdsql.Tx, error) {
	var tx *stdsql.Tx
	entry, ok := p.txns.Load(id)
	if !ok {
		conn, err := p.GetConnForSchema(ctx, id, schemaName)
		if err != nil {
			return nil, err
		}
		t, err := conn.BeginTx(ctx, options)
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
	return errors.Join(lastErr, p.DB.Close())
}

func (p *ConnectionPool) Reset(catalog string, connector *duckdb.Connector, db *stdsql.DB) error {
	err := p.Close()
	if err != nil {
		return fmt.Errorf("failed to close connection pool: %w", err)
	}

	p.conns.Clear()
	p.txns.Clear()
	p.catalog = catalog
	p.DB = db
	p.connector = connector

	return nil
}
