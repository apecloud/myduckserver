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
package replica

import (
	"context"
	stdsql "database/sql"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
	"vitess.io/vitess/go/mysql"

	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/binlog"
	"github.com/apecloud/myduckserver/binlogreplication"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/delta"
	"github.com/apecloud/myduckserver/mycontext"
)

// registerReplicaController registers the replica controller into the engine
// to handle the replication commands, such as START REPLICA, STOP REPLICA, etc.
func RegisterReplicaController(provider *catalog.DatabaseProvider, engine *sqle.Engine, builder *backend.DuckBuilder) {
	replica := binlogreplication.MyBinlogReplicaController
	replica.SetEngine(engine)

	stdctx := context.Background()
	stdctx = mycontext.WithQueryOrigin(stdctx, mycontext.MySQLReplicationQueryOrigin)

	session := backend.NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)
	ctx := sql.NewContext(stdctx, sql.WithSession(session))
	ctx.SetCurrentDatabase("mysql")
	replica.SetExecutionContext(ctx)

	twp := &tableWriterProvider{provider: provider}
	twp.controller = delta.NewController()

	replica.SetTableWriterProvider(twp)
	builder.FlushDeltaBuffer = nil // TODO: implement this

	engine.Analyzer.Catalog.BinlogReplicaController = binlogreplication.MyBinlogReplicaController

	// If we're unable to restart replication, log an error, but don't prevent the server from starting up
	if err := binlogreplication.MyBinlogReplicaController.AutoStart(ctx); err != nil {
		logrus.Errorf("unable to restart replication: %s", err.Error())
	}
}

type tableWriterProvider struct {
	provider   *catalog.DatabaseProvider
	controller *delta.DeltaController
}

var _ binlogreplication.TableWriterProvider = &tableWriterProvider{}

func (twp *tableWriterProvider) GetTableWriter(
	ctx *sql.Context,
	txn *stdsql.Tx,
	databaseName, tableName string,
	schema sql.PrimaryKeySchema,
	columnCount, rowCount int,
	identifyColumns, dataColumns mysql.Bitmap,
	eventType binlog.RowEventType,
	foreignKeyChecksDisabled bool,
) (binlogreplication.TableWriter, error) {
	return twp.newTableUpdater(ctx, txn, databaseName, tableName, schema, columnCount, rowCount, identifyColumns, dataColumns, eventType)
}

func (twp *tableWriterProvider) GetDeltaAppender(
	ctx *sql.Context,
	databaseName, tableName string,
	schema sql.Schema,
) (binlogreplication.DeltaAppender, error) {
	return twp.controller.GetDeltaAppender(databaseName, tableName, schema)
}

func (twp *tableWriterProvider) FlushDeltaBuffer(ctx *sql.Context, conn *stdsql.Conn, tx *stdsql.Tx, reason delta.FlushReason) error {
	_, err := twp.controller.Flush(ctx, conn, tx, reason)
	return err
}

func (twp *tableWriterProvider) DiscardDeltaBuffer(ctx *sql.Context) {
	twp.controller.Close()
}
