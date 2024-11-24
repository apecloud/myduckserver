package binlogreplication

import (
	stdsql "database/sql"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apecloud/myduckserver/binlog"
	"github.com/apecloud/myduckserver/delta"
	"github.com/dolthub/go-mysql-server/sql"
	"vitess.io/vitess/go/mysql"
)

type TableWriter interface {
	Insert(ctx *sql.Context, keyRows []sql.Row) error
	Delete(ctx *sql.Context, keyRows []sql.Row) error
	Update(ctx *sql.Context, keyRows []sql.Row, valueRows []sql.Row) error
}

type DeltaAppender interface {
	Field(i int) array.Builder
	Fields() []array.Builder
	Action() *array.Int8Builder
	TxnTag() *array.BinaryDictionaryBuilder
	TxnServer() *array.BinaryDictionaryBuilder
	TxnGroup() *array.BinaryDictionaryBuilder
	TxnSeqNumber() *array.Uint64Builder
	TxnStmtOrdinal() *array.Uint64Builder
	IncInsertEventCount()
	IncDeleteEventCount()
	GetInsertEventCount() int
	GetDeleteEventCount() int
	ResetEventCounts()
}

type TableWriterProvider interface {
	// GetTableWriter returns a TableWriter for writing to the specified |table| in the specified |database|.
	GetTableWriter(
		ctx *sql.Context,
		txn *stdsql.Tx,
		databaseName, tableName string,
		schema sql.PrimaryKeySchema,
		columnCount, rowCount int,
		identifyColumns, dataColumns mysql.Bitmap,
		eventType binlog.RowEventType,
		foreignKeyChecksDisabled bool,
	) (TableWriter, error)

	// GetDeltaAppender returns a DeltaAppender for appending updates to the specified |table| in the specified |database|.
	GetDeltaAppender(
		ctx *sql.Context,
		databaseName, tableName string,
		schema sql.Schema,
	) (DeltaAppender, error)

	// FlushDelta writes the accumulated changes to the database.
	FlushDeltaBuffer(ctx *sql.Context, tx *stdsql.Tx, reason delta.FlushReason) error

	// DiscardDeltaBuffer discards the accumulated changes.
	DiscardDeltaBuffer(ctx *sql.Context)
}
