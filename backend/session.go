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
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/sirupsen/logrus"

	adapter "github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/vitess/go/mysql"
)

type Session struct {
	*memory.Session
	db               *catalog.DatabaseProvider
	queryRowLimit    uint64
	mysqlImplicitDDL atomic.Bool
}

type SessionOption func(*Session)

// WithQueryRowLimit limits the number of rows returned by queries in a session.
// A limit of zero leaves query results unlimited.
func WithQueryRowLimit(limit uint64) SessionOption {
	return func(sess *Session) {
		sess.queryRowLimit = limit
	}
}

func NewSession(base *memory.Session, provider *catalog.DatabaseProvider, opts ...SessionOption) *Session {
	sess := &Session{Session: base, db: provider}
	for _, opt := range opts {
		opt(sess)
	}
	return sess
}

// Provider returns the database provider for the session.
func (sess *Session) Provider() *catalog.DatabaseProvider {
	return sess.db
}

// QueryRowLimit returns the maximum number of rows a query may return. Zero
// means unlimited.
func (sess *Session) QueryRowLimit() uint64 {
	return sess.queryRowLimit
}

func (sess *Session) SetSessionVariable(ctx *sql.Context, name string, value interface{}) error {
	if err := sess.Session.SetSessionVariable(ctx, name, value); err != nil {
		return err
	}
	if !strings.EqualFold(name, sql.AutoCommitSessionVar) || ctx.GetIgnoreAutoCommit() {
		return nil
	}
	autocommit, err := plan.IsSessionAutocommit(ctx)
	if err != nil || autocommit {
		return err
	}

	// SET autocommit=0 begins while the old value is still 1. GMS therefore
	// creates an autocommit transaction without a DuckDB transaction for the
	// SET statement. Clear only that empty wrapper so the next statement starts
	// the real session transaction required by the new value.
	transaction, ok := ctx.GetTransaction().(*Transaction)
	if !ok || transaction.tx != nil {
		return nil
	}
	if err := sess.Session.Rollback(ctx, &transaction.Transaction); err != nil {
		return err
	}
	ctx.SetTransaction(nil)
	return nil
}

func (sess *Session) CurrentSchemaOfUnderlyingConn() string {
	return sess.db.Pool().CurrentSchema(sess.ID())
}

// NewSessionBuilder returns a session builder for the given database provider.
func NewSessionBuilder(provider *catalog.DatabaseProvider, opts ...SessionOption) func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
	return func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
		host := ""
		user := ""
		mysqlConnectionUser, ok := conn.UserData.(sql.MysqlConnectionUser)
		if ok {
			host = mysqlConnectionUser.Host
			user = mysqlConnectionUser.User
		}

		client := sql.Client{Address: host, User: user, Capabilities: conn.Capabilities}
		baseSession := sql.NewBaseSessionWithClientServer(addr, client, conn.ConnectionID)
		memSession := memory.NewSession(baseSession, provider)

		schema := provider.Pool().CurrentSchema(conn.ConnectionID)
		if schema != "" {
			logrus.Traceln("SessionBuilder: new session: current schema:", schema)
			memSession.SetCurrentDatabase(schema)
		}

		return NewSession(memSession, provider, opts...), nil
	}
}

var _ sql.TransactionSession = (*Session)(nil)
var _ sql.PersistableSession = (*Session)(nil)
var _ adapter.ConnectionHolder = (*Session)(nil)

type Transaction struct {
	memory.Transaction
	tx *stdsql.Tx
}

var _ sql.Transaction = (*Transaction)(nil)

// StartTransaction implements sql.TransactionSession.
func (sess *Session) StartTransaction(ctx *sql.Context, tCharacteristic sql.TransactionCharacteristic) (sql.Transaction, error) {
	sess.GetLogger().Trace("StartTransaction")
	base, err := sess.Session.StartTransaction(ctx, tCharacteristic)
	if err != nil {
		return nil, err
	}

	startUnderlyingTx := !sess.mysqlImplicitDDL.Load()
	if startUnderlyingTx && !ctx.GetIgnoreAutoCommit() {
		autocommit, err := plan.IsSessionAutocommit(ctx)
		if err != nil {
			return nil, err
		}
		if autocommit {
			// Don't start a DuckDB transcation if it is in autocommit mode
			startUnderlyingTx = false
		}
	}

	var tx *stdsql.Tx
	if startUnderlyingTx {
		sess.GetLogger().Trace("StartDuckTransaction")
		tx, err = sess.GetTxn(ctx, &stdsql.TxOptions{ReadOnly: tCharacteristic == sql.ReadOnly})
		if err != nil {
			return nil, err
		}
	}
	return &Transaction{*base.(*memory.Transaction), tx}, nil
}

// CommitTransaction implements sql.TransactionSession.
func (sess *Session) CommitTransaction(ctx *sql.Context, tx sql.Transaction) error {
	sess.GetLogger().Trace("CommitTransaction")
	transaction := tx.(*Transaction)
	if transaction.tx != nil {
		sess.GetLogger().Trace("CommitDuckTransaction")
		defer sess.CloseTxn()
		if err := transaction.tx.Commit(); err != nil {
			return err
		}
	}
	return sess.Session.CommitTransaction(ctx, &transaction.Transaction)
}

// Rollback implements sql.TransactionSession.
func (sess *Session) Rollback(ctx *sql.Context, tx sql.Transaction) error {
	sess.GetLogger().Trace("Rollback")
	transaction := tx.(*Transaction)
	var rollbackErr error
	underlyingInactive := transaction.tx == nil
	if transaction.tx != nil {
		sess.GetLogger().Trace("RollbackDuckTransaction")
		rollbackErr = transaction.tx.Rollback()
		underlyingInactive = rollbackErr == nil || errors.Is(rollbackErr, stdsql.ErrTxDone) ||
			strings.Contains(strings.ToLower(rollbackErr.Error()), "no transaction is active")
		if underlyingInactive {
			// Remove the mapping before any post-rollback work. This makes the
			// session connection reusable and prevents cleanup from trying to acquire
			// a second pooled connection while the old transaction is still mapped.
			sess.CloseTxn()
		}
	}

	sessionErr := sess.Session.Rollback(ctx, &transaction.Transaction)
	var cleanupErr error
	if underlyingInactive && sess.db != nil && sess.db.DuckLakeEnabled() {
		cleanupBase := context.Background()
		if ctx != nil {
			cleanupBase = context.WithoutCancel(ctx)
		}
		// Rollback cleanup is service-owned maintenance. Preserve replication
		// origins so the provider's fail-closed eligibility check remains intact.
		cleanupCtx := mycontext.WithMaintenanceQuery(cleanupBase)
		conn, connErr := sess.GetConn(cleanupCtx)
		if connErr != nil {
			cleanupErr = connErr
		} else {
			cleanupErr = sess.db.CleanupDuckLakeOrphansOnConn(cleanupCtx, conn)
		}
	}
	return errors.Join(rollbackErr, sessionErr, cleanupErr)
}

// PersistGlobal implements sql.PersistableSession.
func (sess *Session) PersistGlobal(ctx *sql.Context, sysVarName string, value interface{}) error {
	if _, _, ok := sql.SystemVariables.GetGlobal(sysVarName); !ok {
		return sql.ErrUnknownSystemVariable.New(sysVarName)
	}
	sess.GetLogger().Tracef("Persisting global variable %s = %v", sysVarName, value)
	_, err := sess.ExecContext(
		ctx,
		catalog.InternalTables.PersistentVariable.UpsertStmt(),
		sysVarName, value, fmt.Sprintf("%T", value),
	)
	return err
}

// RemovePersistedGlobal implements sql.PersistableSession.
func (sess *Session) RemovePersistedGlobal(sysVarName string) error {
	_, err := sess.ExecContext(
		context.Background(),
		catalog.InternalTables.PersistentVariable.DeleteStmt(),
		sysVarName,
	)
	return err
}

// RemoveAllPersistedGlobals implements sql.PersistableSession.
func (sess *Session) RemoveAllPersistedGlobals() error {
	_, err := sess.ExecContext(context.Background(), "DELETE FROM "+catalog.InternalTables.PersistentVariable.Name)
	return err
}

// GetPersistedValue implements sql.PersistableSession.
func (sess *Session) GetPersistedValue(k string) (interface{}, error) {
	var value, vtype string
	err := sess.QueryRow(
		context.Background(),
		catalog.InternalTables.PersistentVariable.SelectStmt(),
		k,
	).Scan(&value, &vtype)
	sess.GetLogger().Tracef("Getting persisted global variable %s = %s [%s]", k, value, vtype)
	switch {
	case err == stdsql.ErrNoRows:
		return nil, nil
	case err != nil:
		return nil, err
	default:
		switch vtype {
		case "string":
			return value, nil
		case "int":
			return strconv.Atoi(value)
		case "bool":
			return value == "true", nil
		default:
			return nil, fmt.Errorf("unknown variable type %s", vtype)
		}
	}
}

// GetConn implements adapter.ConnectionHolder.
func (sess *Session) GetConn(ctx context.Context) (*stdsql.Conn, error) {
	return sess.db.Pool().GetConnForSchema(ctx, sess.ID(), sess.GetCurrentDatabase())
}

// GetCatalogConn implements adapter.ConnectionHolder.
func (sess *Session) GetCatalogConn(ctx context.Context) (*stdsql.Conn, error) {
	return sess.db.Pool().GetConn(ctx, sess.ID())
}

// GetTxn implements adapter.ConnectionHolder.
func (sess *Session) GetTxn(ctx context.Context, options *stdsql.TxOptions) (*stdsql.Tx, error) {
	return sess.db.Pool().GetTxn(ctx, sess.ID(), sess.GetCurrentDatabase(), options)
}

// GetCatalogTxn implements adapter.ConnectionHolder.
func (sess *Session) GetCatalogTxn(ctx context.Context, options *stdsql.TxOptions) (*stdsql.Tx, error) {
	return sess.db.Pool().GetTxn(ctx, sess.ID(), "", options)
}

// TryGetTxn implements adapter.ConnectionHolder.
func (sess *Session) TryGetTxn() *stdsql.Tx {
	return sess.db.Pool().TryGetTxn(sess.ID())
}

// GetCurrentCatalog implements adapter.ConnectionHolder.
func (sess *Session) GetCurrentCatalog() string {
	return sess.db.Pool().CurrentCatalog(sess.ID())
}

// GetCurrentSchema implements adapter.ConnectionHolder.
func (sess *Session) GetCurrentSchema() string {
	return sess.db.Pool().CurrentSchema(sess.ID())
}

// CloseTxn implements adapter.ConnectionHolder.
func (sess *Session) CloseTxn() {
	sess.db.Pool().CloseTxn(sess.ID())
}

// CloseConn implements adapter.ConnectionHolder.
func (sess *Session) CloseConn() {
	sess.db.Pool().CloseConn(sess.ID())
}

func (sess *Session) ExecContext(ctx context.Context, query string, args ...any) (stdsql.Result, error) {
	conn, err := sess.GetCatalogConn(ctx)
	if err != nil {
		return nil, err
	}
	return conn.ExecContext(ctx, query, args...)
}

func (sess *Session) QueryRow(ctx context.Context, query string, args ...any) *stdsql.Row {
	conn, err := sess.GetCatalogConn(ctx)
	if err != nil {
		return nil
	}
	return conn.QueryRowContext(ctx, query, args...)
}
