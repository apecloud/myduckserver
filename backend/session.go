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
	"fmt"
	"strconv"

	adapter "github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/vitess/go/mysql"
)

type Session struct {
	*memory.Session
	db      *catalog.DatabaseProvider
	builder *DuckBuilder
}

// NewSessionBuilder returns a session builder for the given database provider.
func NewSessionBuilder(provider *catalog.DatabaseProvider, builder *DuckBuilder) func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
	_, err := provider.Storage().Exec("CREATE TABLE IF NOT EXISTS main.persistent_variables (name TEXT PRIMARY KEY, value TEXT, type TEXT)")
	if err != nil {
		panic(err)
	}

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
		return Session{memSession, provider, builder}, nil
	}
}

var _ sql.TransactionSession = (*Session)(nil)
var _ sql.PersistableSession = (*Session)(nil)
var _ adapter.ConnHolder = (*Session)(nil)

type Transaction struct {
	memory.Transaction
	tx *stdsql.Tx
}

var _ sql.Transaction = (*Transaction)(nil)

// StartTransaction implements sql.TransactionSession.
func (sess Session) StartTransaction(ctx *sql.Context, tCharacteristic sql.TransactionCharacteristic) (sql.Transaction, error) {
	sess.GetLogger().Infoln("StartTransaction")
	base, err := sess.Session.StartTransaction(ctx, tCharacteristic)
	if err != nil {
		return nil, err
	}

	startUnderlyingTx := true
	if !ctx.GetIgnoreAutoCommit() {
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
		sess.GetLogger().Infoln("StartDuckTransaction")
		conn, err := sess.GetConn(ctx)
		if err != nil {
			return nil, err
		}
		tx, err = conn.BeginTx(ctx, &stdsql.TxOptions{ReadOnly: tCharacteristic == sql.ReadOnly})
		if err != nil {
			return nil, err
		}
	}
	return &Transaction{*base.(*memory.Transaction), tx}, nil
}

// CommitTransaction implements sql.TransactionSession.
func (sess Session) CommitTransaction(ctx *sql.Context, tx sql.Transaction) error {
	sess.GetLogger().Infoln("CommitTransaction")
	transaction := tx.(*Transaction)
	if transaction.tx != nil {
		sess.GetLogger().Infoln("CommitDuckTransaction")
		if err := transaction.tx.Commit(); err != nil {
			return err
		}
	}
	return sess.Session.CommitTransaction(ctx, &transaction.Transaction)
}

// Rollback implements sql.TransactionSession.
func (sess Session) Rollback(ctx *sql.Context, tx sql.Transaction) error {
	sess.GetLogger().Infoln("Rollback")
	transaction := tx.(*Transaction)
	if transaction.tx != nil {
		sess.GetLogger().Infoln("RollbackDuckTransaction")
		if err := transaction.tx.Rollback(); err != nil {
			return err
		}
	}
	return sess.Session.Rollback(ctx, &transaction.Transaction)
}

// PersistGlobal implements sql.PersistableSession.
func (sess Session) PersistGlobal(sysVarName string, value interface{}) error {
	if _, _, ok := sql.SystemVariables.GetGlobal(sysVarName); !ok {
		return sql.ErrUnknownSystemVariable.New(sysVarName)
	}
	_, err := sess.db.Storage().Exec(
		"INSERT OR REPLACE INTO main.persistent_variables (name, value, vtype) VALUES (?, ?, ?)",
		sysVarName, value, fmt.Sprintf("%T", value),
	)
	return err
}

// RemovePersistedGlobal implements sql.PersistableSession.
func (sess Session) RemovePersistedGlobal(sysVarName string) error {
	_, err := sess.db.Storage().Exec(
		"DELETE FROM main.persistent_variables WHERE name = ?",
		sysVarName,
	)
	return err
}

// RemoveAllPersistedGlobals implements sql.PersistableSession.
func (sess Session) RemoveAllPersistedGlobals() error {
	_, err := sess.db.Storage().Exec("DELETE FROM main.persistent_variables")
	return err
}

// GetPersistedValue implements sql.PersistableSession.
func (sess Session) GetPersistedValue(k string) (interface{}, error) {
	var value, vtype string
	err := sess.db.Storage().QueryRow(
		"SELECT value, vtype FROM main.persistent_variables WHERE name = ?", k,
	).Scan(&value, &vtype)
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

// GetConn implements adapter.ConnHolder.
func (see Session) GetConn(ctx *sql.Context) (*stdsql.Conn, error) {
	return see.builder.GetConn(ctx, ctx.ID(), ctx.GetCurrentDatabase())
}
