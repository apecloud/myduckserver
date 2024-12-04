package delta

import (
	stdsql "database/sql"
	"fmt"
	"hash/maphash"
	"math/bits"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apecloud/myduckserver/binlog"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/pgtypes"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/marcboeker/go-duckdb"
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
	seed   maphash.Seed
}

func NewController() *DeltaController {
	return &DeltaController{
		tables: make(map[tableIdentifier]*DeltaAppender),
		seed:   maphash.MakeSeed(),
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

	for _, da := range c.tables {
		da.appender.Release()
	}
	clear(c.tables)
}

// Flush writes the accumulated changes to the database.
func (c *DeltaController) Flush(ctx *sql.Context, conn *stdsql.Conn, tx *stdsql.Tx, reason FlushReason) (FlushStats, error) {
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

	var stats FlushStats

	for table, appender := range c.tables {
		deltaRowCount := appender.RowCount()
		if deltaRowCount > 0 {
			if err := c.updateTable(ctx, conn, tx, table, appender, &stats); err != nil {
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
	conn *stdsql.Conn,
	tx *stdsql.Tx,
	table tableIdentifier,
	appender *DeltaAppender,
	stats *FlushStats,
) error {
	if tx == nil {
		return fmt.Errorf("no active transaction")
	}
	defer appender.ResetCounters()

	// We consider the following cases:
	//  1. INSERT only - no DELETE or UPDATE. In this case, we can do a simple INSERT INTO in an optimized way,
	//     without the deduplication step (as the source has confirmed that there are no duplicates) and the DELETE step.
	//     The data can go directly from the delta view to the base table.
	//  2. DELETE only - no INSERT or UPDATE. In this case, we can do a simple DELETE FROM in an optimized way,
	//     without the the INSERT step and the deduplication step (as the source has confirmed that there are no duplicates).
	//     The delta view can be directly used to delete rows from the base table, without the need for a temporary table.
	//  3. INSERT + non-primary-key UPDATE - no DELETE. In this case, we can skip the DELETE step.
	//     Therefore, the temporary table is not needed as the delta view will be read only once.
	//  4. The general case - INSERT, DELETE, and UPDATE. In this case, we need to create a temporary table
	//     to store the deduplicated delta and then do the INSERT and DELETE steps.

	// Identify the types of changes in the delta
	hasInserts := appender.counters.event.insert > 0
	hasDeletes := appender.counters.event.delete > 0
	hasUpdates := appender.counters.event.update > 0

	if log := ctx.GetLogger(); log.Logger.IsLevelEnabled(logrus.DebugLevel) {
		log.Debugf("Delta: %s.%s: stats: %+v", table.dbName, table.tableName, appender.counters)
	}

	switch {
	case hasInserts && !hasDeletes && !hasUpdates:
		// Case 1: INSERT only
		return c.handleInsertOnly(ctx, conn, tx, table, appender, stats)
	case hasDeletes && !hasInserts && !hasUpdates:
		// Case 2: DELETE only
		return c.handleDeleteOnly(ctx, conn, tx, table, appender, stats)
	case appender.counters.action.delete == 0:
		// Case 3: INSERT + non-primary-key UPDATE
		return c.handleZeroDelete(ctx, conn, tx, table, appender, stats)
	default:
		// Case 4: General case
		return c.handleGeneralCase(ctx, conn, tx, table, appender, stats)
	}
}

// Helper function to build the Arrow record and register the view
func (c *DeltaController) prepareArrowView(
	ctx *sql.Context,
	conn *stdsql.Conn,
	table tableIdentifier,
	appender *DeltaAppender,
	fieldOffset int,
	fieldIndices []int,
) (viewName string, close func(), err error) {
	record := appender.Build()

	// fmt.Println("record:", record)

	var ar *duckdb.Arrow
	err = conn.Raw(func(driverConn any) error {
		var err error
		ar, err = duckdb.NewArrowFromConn(driverConn.(*duckdb.Conn))
		return err
	})
	if err != nil {
		record.Release()
		return "", nil, err
	}

	// Project the fields before registering the Arrow record into DuckDB.
	// Currently, this is necessary because RegisterView uses `arrow_scan` instead of `arrow_scan_dumb` under the hood.
	// The former allows projection & filter pushdown, but the implementation does not work as expected in some cases.
	schema := record.Schema()
	if fieldOffset > 0 {
		fields := schema.Fields()[fieldOffset:]
		schema = arrow.NewSchema(fields, nil)
		columns := record.Columns()[fieldOffset:]
		projected := array.NewRecord(schema, columns, record.NumRows())
		record.Release()
		record = projected
	} else if len(fieldIndices) > 0 {
		fields := make([]arrow.Field, len(fieldIndices))
		columns := make([]arrow.Array, len(fieldIndices))
		for i, idx := range fieldIndices {
			fields[i] = schema.Field(idx)
			columns[i] = record.Column(idx)
		}
		schema = arrow.NewSchema(fields, nil)
		projected := array.NewRecord(schema, columns, record.NumRows())
		record.Release()
		record = projected
	}

	reader, err := array.NewRecordReader(schema, []arrow.Record{record})
	if err != nil {
		record.Release()
		return "", nil, err
	}

	// Register the Arrow view
	hash := maphash.String(c.seed, table.dbName+"\x00"+table.tableName)
	viewName = "__sys_view_arrow_delta_" + strconv.FormatUint(hash, 16) + "__"

	release, err := ar.RegisterView(reader, viewName)
	if err != nil {
		reader.Release()
		record.Release()
		return "", nil, err
	}

	close = func() {
		conn.ExecContext(ctx, "DROP VIEW IF EXISTS "+viewName)
		release()
		reader.Release()
		record.Release()
	}
	return viewName, close, nil
}

func (c *DeltaController) handleInsertOnly(
	ctx *sql.Context,
	conn *stdsql.Conn,
	tx *stdsql.Tx,
	table tableIdentifier,
	appender *DeltaAppender,
	stats *FlushStats,
) error {
	// Ignore the augmented fields
	viewName, release, err := c.prepareArrowView(ctx, conn, table, appender, appender.NumAugmentedFields(), nil)
	if err != nil {
		return err
	}
	defer release()

	// Perform direct INSERT without deduplication
	var b strings.Builder
	b.Grow(128)

	b.WriteString("INSERT INTO ")
	b.WriteString(catalog.ConnectIdentifiersANSI(table.dbName, table.tableName))
	b.WriteString(" SELECT ")
	buildColumnList(&b, appender.BaseSchema())
	b.WriteString(" FROM ")
	b.WriteString(viewName)

	sql := b.String()
	ctx.GetLogger().Debug("Insert SQL: ", b.String())

	result, err := tx.ExecContext(ctx, sql)
	if err != nil {
		return err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	stats.Insertions += affected
	stats.DeltaSize += affected

	if log := ctx.GetLogger(); log.Logger.IsLevelEnabled(logrus.DebugLevel) {
		log.WithFields(logrus.Fields{
			"db":    table.dbName,
			"table": table.tableName,
			"rows":  affected,
		}).Debug("Inserted")
	}

	return nil
}

func (c *DeltaController) handleDeleteOnly(
	ctx *sql.Context,
	conn *stdsql.Conn,
	tx *stdsql.Tx,
	table tableIdentifier,
	appender *DeltaAppender,
	stats *FlushStats,
) error {
	// Ignore all but the primary key fields
	viewName, release, err := c.prepareArrowView(ctx, conn, table, appender, 0, getPrimaryKeyIndices(appender))
	if err != nil {
		return err
	}
	defer release()

	qualifiedTableName := catalog.ConnectIdentifiersANSI(table.dbName, table.tableName)
	pk := getPrimaryKeyStruct(appender.BaseSchema())

	// Perform direct DELETE without deduplication
	deleteSQL := "DELETE FROM " + qualifiedTableName +
		" WHERE " + pk + " IN (SELECT " + pk + " FROM " + viewName + ")"
	result, err := tx.ExecContext(ctx, deleteSQL)
	if err != nil {
		return err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	stats.Deletions += affected
	stats.DeltaSize += affected

	if log := ctx.GetLogger(); log.Logger.IsLevelEnabled(logrus.DebugLevel) {
		log.WithFields(logrus.Fields{
			"db":    table.dbName,
			"table": table.tableName,
			"rows":  affected,
		}).Debug("Deleted")
	}

	return nil
}

func (c *DeltaController) handleZeroDelete(
	ctx *sql.Context,
	conn *stdsql.Conn,
	tx *stdsql.Tx,
	table tableIdentifier,
	appender *DeltaAppender,
	stats *FlushStats,
) error {
	viewName, release, err := c.prepareArrowView(ctx, conn, table, appender, 0, nil)
	if err != nil {
		return err
	}
	defer release()

	condenseDeltaSQL := buildCondenseDeltaSQL(viewName, appender)

	insertSQL := "INSERT OR REPLACE INTO " +
		catalog.ConnectIdentifiersANSI(table.dbName, table.tableName) +
		" SELECT * EXCLUDE (" + AugmentedColumnList + ") FROM (" + condenseDeltaSQL + ")"
	result, err := tx.ExecContext(ctx, insertSQL)
	if err != nil {
		return err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	stats.Insertions += affected
	stats.DeltaSize += affected

	if log := ctx.GetLogger(); log.Logger.IsLevelEnabled(logrus.DebugLevel) {
		log.WithFields(logrus.Fields{
			"db":    table.dbName,
			"table": table.tableName,
			"rows":  affected,
		}).Debug("Upserted")
	}

	return nil
}

func (c *DeltaController) handleGeneralCase(
	ctx *sql.Context,
	conn *stdsql.Conn,
	tx *stdsql.Tx,
	table tableIdentifier,
	appender *DeltaAppender,
	stats *FlushStats,
) error {
	viewName, release, err := c.prepareArrowView(ctx, conn, table, appender, 0, nil)
	if err != nil {
		return err
	}

	// Create a temporary table to store the latest delta view
	condenseDeltaSQL := buildCondenseDeltaSQL(viewName, appender)
	result, err := tx.ExecContext(ctx, "CREATE OR REPLACE TEMP TABLE delta AS "+condenseDeltaSQL)
	release() // release the Arrow view immediately
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	stats.DeltaSize += affected
	defer tx.ExecContext(ctx, "DROP TABLE IF EXISTS temp.main.delta")

	if log := ctx.GetLogger(); log.Logger.IsLevelEnabled(logrus.DebugLevel) {
		log.WithFields(logrus.Fields{
			"db":    table.dbName,
			"table": table.tableName,
			"rows":  affected,
		}).Debug("Delta created")
	}

	qualifiedTableName := catalog.ConnectIdentifiersANSI(table.dbName, table.tableName)

	// Insert or replace new rows (action = INSERT) into the base table.
	insertSQL := "INSERT OR REPLACE INTO " +
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
			"db":    table.dbName,
			"table": table.tableName,
			"rows":  affected,
		}).Debug("Upserted")
	}

	// Delete rows that have been deleted.
	// The plan for `IN` is optimized to a SEMI JOIN,
	// which is more efficient than ordinary INNER JOIN.
	// DuckDB does not support multiple columns in `IN` clauses,
	// so we need to handle this case separately using the `row()` function.
	inTuple := getPrimaryKeyStruct(appender.BaseSchema())
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
			"db":    table.dbName,
			"table": table.tableName,
			"rows":  affected,
		}).Debug("Deleted")
	}

	return nil
}

// Helper function to build column list with timestamp handling
func buildColumnList(b *strings.Builder, schema sql.Schema) {
	for i, col := range schema {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(catalog.QuoteIdentifierANSI(col.Name))
		if isTimestampType(col.Type) {
			b.WriteString("::TIMESTAMP")
		}
	}
}

// Helper function to get the primary key indices.
func getPrimaryKeyIndices(appender *DeltaAppender) []int {
	schema := appender.BaseSchema()
	indices := make([]int, 0, 1)
	for i, col := range schema {
		if col.PrimaryKey {
			indices = append(indices, i+appender.NumAugmentedFields())
		}
	}
	return indices
}

// Helper function to get the primary key. For composite primary keys, `row()` is used.
func getPrimaryKeyStruct(schema sql.Schema) string {
	pks := make([]string, 0, 1)
	for _, col := range schema {
		if col.PrimaryKey {
			pks = append(pks, catalog.QuoteIdentifierANSI(col.Name))
		}
	}
	if len(pks) == 0 {
		return ""
	} else if len(pks) == 1 {
		return pks[0]
	}
	return "row(" + strings.Join(pks, ", ") + ")"
}

// Helper function to get the primary key list.
func getPrimaryKeyList(schema sql.Schema) string {
	pks := make([]string, 0, 1)
	for _, col := range schema {
		if col.PrimaryKey {
			pks = append(pks, catalog.QuoteIdentifierANSI(col.Name))
		}
	}
	return strings.Join(pks, ", ")
}

func buildCondenseDeltaSQL(viewName string, appender *DeltaAppender) string {
	var (
		augmentedSchema = appender.Schema()
		pkList          = getPrimaryKeyList(appender.BaseSchema())
		builder         strings.Builder
	)
	builder.Grow(512)
	// Use the following SQL to get the latest view of the rows being updated.
	//
	// SELECT r[1] as action, ...
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
	builder.Grow(512)
	builder.WriteString("SELECT ")
	builder.WriteString("r[1] AS ")
	builder.WriteString(catalog.QuoteIdentifierANSI(augmentedSchema[0].Name))
	for i, col := range augmentedSchema[1:] {
		builder.WriteString(", r[")
		builder.WriteString(strconv.Itoa(i + 2))
		builder.WriteString("]")
		if isTimestampType(col.Type) {
			builder.WriteString("::TIMESTAMP")
		}
		builder.WriteString(" AS ")
		builder.WriteString(catalog.QuoteIdentifierANSI(col.Name))
	}
	builder.WriteString(" FROM (SELECT ")
	builder.WriteString(pkList)
	builder.WriteString(", LAST(ROW(*COLUMNS(*)) ORDER BY txn_group, txn_seq, txn_stmt, action) AS r")
	builder.WriteString(" FROM ")
	builder.WriteString(viewName)
	builder.WriteString(" GROUP BY ")
	builder.WriteString(pkList)
	builder.WriteString(")")
	return builder.String()
}

func isTimestampType(t sql.Type) bool {
	if types.IsTimestampType(t) {
		return true
	}
	if pgt, ok := t.(pgtypes.PostgresType); ok {
		return pgt.PG.OID == pgtype.TimestampOID
	}
	return false
}
