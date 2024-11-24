package delta

import (
	"bytes"
	stdsql "database/sql"
	"fmt"
	"math/bits"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apecloud/myduckserver/binlog"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/sirupsen/logrus"
)

type FlushStats struct {
	DeltaSize  int64
	Insertions int64
	Deletions  int64
}

type DeltaController struct {
	mutex  sync.Mutex
	tables map[tableIdentifier]*DeltaAppender
}

func NewController() *DeltaController {
	return &DeltaController{
		tables: make(map[tableIdentifier]*DeltaAppender),
	}
}

func (c *DeltaController) GetDeltaAppender(
	databaseName, tableName string,
	schema sql.Schema,
) (*DeltaAppender, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	id := tableIdentifier{databaseName, tableName}
	appender, ok := c.tables[id]
	if ok {
		return appender, nil
	}
	appender, err := newDeltaAppender(schema)
	if err != nil {
		return nil, err
	}
	c.tables[id] = appender
	return appender, nil
}

func (c *DeltaController) Close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for k, da := range c.tables {
		da.appender.Release()
		delete(c.tables, k)
	}
}

// Flush writes the accumulated changes to the database.
func (c *DeltaController) Flush(ctx *sql.Context, tx *stdsql.Tx, reason FlushReason) (FlushStats, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Due to DuckDB's lack of support for atomic MERGE INTO, we have to do the following two steps separately:
	//   1. Delete rows that are being updated.
	//   2. Insert new rows.
	// To guarantee the atomicity of the two steps, we have to wrap them in a transaction.
	// Again, due to DuckDB's limitations of indexes, specifically over-eagerly unique constraint checking,
	// if we do **DELETE then INSERT** in the same transaction, we would get
	//   a unique constraint violation error for INSERT,
	//   or data corruption for INSERT OR REPLACE|IGNORE INTO.
	//
	// This is a noteworthy pitfall and seems unlikely to be fixed in DuckDB in the near future,
	// but we have to live with it.
	//
	// On the other hand, fortunately enough, **INSERT OR REPLACE then DELETE** in the same transaction works fine.
	//
	// The ultimate solution is to wait for DuckDB to improve its index handling.
	// In the meantime, we could contribute a patch to DuckDB to support atomic MERGE INTO,
	// which is another way to avoid the issue elegantly.
	//
	// See:
	//  https://duckdb.org/docs/sql/indexes.html#limitations-of-art-indexes
	//  https://github.com/duckdb/duckdb/issues/14133
	var (
		// Share the buffer among all tables.
		buf   bytes.Buffer
		stats FlushStats
	)

	for table, appender := range c.tables {
		deltaRowCount := appender.RowCount()
		if deltaRowCount > 0 {
			if err := c.updateTable(ctx, tx, table, appender, &buf, &stats); err != nil {
				return stats, err
			}
		}
		switch reason {
		case DDLStmtFlushReason:
			// DDL statement may change the schema
			delete(c.tables, table)
		default:
			// Pre-allocate memory for the next delta
			if deltaRowCount > 0 {
				// Next power of 2
				appender.Grow(1 << bits.Len64(uint64(deltaRowCount)-1))
			}
		}
	}

	if stats.DeltaSize > 0 {
		if log := ctx.GetLogger(); log.Logger.IsLevelEnabled(logrus.DebugLevel) {
			log.WithFields(logrus.Fields{
				"DeltaSize":  stats.DeltaSize,
				"Insertions": stats.Insertions,
				"Deletions":  stats.Deletions,
				"Reason":     reason.String(),
			}).Debug("Flushed delta buffer")
		}
	}

	return stats, nil
}

func (c *DeltaController) updateTable(
	ctx *sql.Context,
	tx *stdsql.Tx,
	table tableIdentifier,
	appender *DeltaAppender,
	buf *bytes.Buffer,
	stats *FlushStats,
) error {
	if tx == nil {
		return fmt.Errorf("no active transaction")
	}

	buf.Reset()

	schema := appender.BaseSchema() // schema of the base table
	record := appender.Build()
	defer func() {
		record.Release()
		appender.ResetEventCounts()
	}()

	// fmt.Println("record:", record)

	// TODO(fan): Switch to zero-copy Arrow ingestion once this PR is merged:
	//   https://github.com/marcboeker/go-duckdb/pull/283
	w := ipc.NewWriter(buf, ipc.WithSchema(record.Schema()))
	if err := w.Write(record); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	bytes := buf.Bytes()
	size := len(bytes)
	ptr := unsafe.Pointer(&bytes[0])
	ipcSQL := fmt.Sprintf(
		" FROM scan_arrow_ipc([{ptr: %d::ubigint, size: %d::ubigint}])",
		uintptr(ptr), size,
	)

	qualifiedTableName := catalog.ConnectIdentifiersANSI(table.dbName, table.tableName)

	pkColumns := make([]int, 0, 1) // Most tables have a single-column primary key
	for i, col := range schema {
		if col.PrimaryKey {
			pkColumns = append(pkColumns, i)
		}
	}
	pkList := catalog.QuoteIdentifierANSI(schema[pkColumns[0]].Name)
	for _, i := range pkColumns[1:] {
		pkList += ", " + catalog.QuoteIdentifierANSI(schema[i].Name)
	}

	// Use the following SQL to get the latest view of the rows being updated.
	//
	// SELECT r[0] as action, ...
	// FROM (
	//   SELECT
	//     pk1, pk2, ...,
	//     LAST(ROW(*COLUMNS(*)) ORDER BY txn_group, txn_seq, txn_stmt, action) AS r
	//   FROM delta
	//   GROUP BY pk1, pk2, ...
	// )
	//
	// Note that an update generates two rows: one for DELETE and one for INSERT.
	// So the numeric value of DELETE action MUST be smaller than that of INSERT.
	augmentedSchema := appender.Schema()
	var builder strings.Builder
	builder.Grow(512)
	if appender.GetDeleteEventCount() > 0 {
		builder.WriteString("SELECT ")
		builder.WriteString("r[1] AS ")
		builder.WriteString(catalog.QuoteIdentifierANSI(augmentedSchema[0].Name))
		for i, col := range augmentedSchema[1:] {
			builder.WriteString(", r[")
			builder.WriteString(strconv.Itoa(i + 2))
			builder.WriteString("]")
			if types.IsTimestampType(col.Type) {
				builder.WriteString("::TIMESTAMP")
			}
			builder.WriteString(" AS ")
			builder.WriteString(catalog.QuoteIdentifierANSI(col.Name))
		}
		builder.WriteString(" FROM (SELECT ")
		builder.WriteString(pkList)
		builder.WriteString(", LAST(ROW(*COLUMNS(*)) ORDER BY txn_group, txn_seq, txn_stmt, action) AS r")
		builder.WriteString(ipcSQL)
		builder.WriteString(" GROUP BY ")
		builder.WriteString(pkList)
		builder.WriteString(")")
	} else {
		builder.WriteString("SELECT ")
		builder.WriteString(catalog.QuoteIdentifierANSI(augmentedSchema[0].Name))
		for _, col := range augmentedSchema[1:] {
			builder.WriteString(", ")
			builder.WriteString(catalog.QuoteIdentifierANSI(col.Name))
			if types.IsTimestampType(col.Type) {
				builder.WriteString("::TIMESTAMP")
			}
		}
		builder.WriteString(ipcSQL)
	}
	condenseDeltaSQL := builder.String()

	var (
		result   stdsql.Result
		affected int64
		err      error
	)

	// Create a temporary table to store the latest delta view.
	result, err = tx.ExecContext(ctx, "CREATE OR REPLACE TEMP TABLE delta AS "+condenseDeltaSQL)
	if err == nil {
		affected, err = result.RowsAffected()
	}
	if err != nil {
		return err
	}
	stats.DeltaSize += affected
	defer tx.ExecContext(ctx, "DROP TABLE IF EXISTS temp.main.delta")

	if log := ctx.GetLogger(); log.Logger.IsLevelEnabled(logrus.DebugLevel) {
		log.WithFields(logrus.Fields{
			"table": qualifiedTableName,
			"rows":  affected,
		}).Debug("Delta created")
	}

	// Insert or replace new rows (action = INSERT) into the base table.
	insertSQL := "INSERT "
	if appender.GetDeleteEventCount() > 0 {
		insertSQL += "OR REPLACE "
	}
	insertSQL += "INTO " +
		qualifiedTableName +
		" SELECT * EXCLUDE (" + AugmentedColumnList + ") FROM temp.main.delta WHERE action = " +
		strconv.Itoa(int(binlog.InsertRowEvent))
	result, err = tx.ExecContext(ctx, insertSQL)
	if err == nil {
		affected, err = result.RowsAffected()
	}
	if err != nil {
		return err
	}
	stats.Insertions += affected

	if log := ctx.GetLogger(); log.Logger.IsLevelEnabled(logrus.DebugLevel) {
		log.WithFields(logrus.Fields{
			"table": qualifiedTableName,
			"rows":  affected,
		}).Debug("Inserted")
	}

	// Delete rows that have been deleted.
	// The plan for `IN` is optimized to a SEMI JOIN,
	// which is more efficient than ordinary INNER JOIN.
	// DuckDB does not support multiple columns in `IN` clauses,
	// so we need to handle this case separately using the `row()` function.
	inTuple := pkList
	if len(pkColumns) > 1 {
		inTuple = "row(" + pkList + ")"
	}
	deleteSQL := "DELETE FROM " + qualifiedTableName +
		" WHERE " + inTuple + " IN (SELECT " + inTuple +
		"FROM temp.main.delta WHERE action = " + strconv.Itoa(int(binlog.DeleteRowEvent)) + ")"
	result, err = tx.ExecContext(ctx, deleteSQL)
	if err == nil {
		affected, err = result.RowsAffected()
	}
	if err != nil {
		return err
	}
	stats.Deletions += affected

	// For debugging:
	//
	// rows, err := tx.QueryContext(ctx, "SELECT * FROM "+qualifiedTableName)
	// if err != nil {
	// 	return err
	// }
	// defer rows.Close()
	// row := make([]any, len(schema))
	// pointers := make([]any, len(row))
	// for i := range row {
	// 	pointers[i] = &row[i]
	// }
	// for rows.Next() {
	// 	if err := rows.Scan(pointers...); err != nil {
	// 		return err
	// 	}
	// 	fmt.Printf("row:%+v\n", row)
	// }

	if log := ctx.GetLogger(); log.Logger.IsLevelEnabled(logrus.DebugLevel) {
		log.WithFields(logrus.Fields{
			"table": qualifiedTableName,
			"rows":  affected,
		}).Debug("Deleted")
	}

	return nil
}
