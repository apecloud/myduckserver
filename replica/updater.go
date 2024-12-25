package replica

import (
	stdsql "database/sql"
	"errors"
	"github.com/apecloud/myduckserver/catalog"
	"strings"

	"github.com/apecloud/myduckserver/binlog"
	"github.com/apecloud/myduckserver/binlogreplication"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
	"vitess.io/vitess/go/mysql"
)

var ErrPartialPrimaryKeyUpdate = errors.New("primary key columns are (partially) updated but are not fully specified in the binlog")

func isPkUpdate(schema sql.Schema, identifyColumns, dataColumns mysql.Bitmap) bool {
	for i, c := range schema {
		if c.PrimaryKey && identifyColumns.Bit(i) && dataColumns.Bit(i) {
			return true
		}
	}
	return false
}

func getPrimaryKeyIndices(schema sql.Schema, columns mysql.Bitmap) []int {
	var count int
	var indices []int
	for i, c := range schema {
		set := columns.Count() > i && columns.Bit(i)
		if c.PrimaryKey && !set {
			return nil
		} else if c.PrimaryKey && set {
			indices = append(indices, count)
		}
		if set {
			count++
		}
	}
	return indices
}

func (twp *tableWriterProvider) newTableUpdater(
	ctx *sql.Context,
	txn *stdsql.Tx,
	databaseName, tableName string,
	pkSchema sql.PrimaryKeySchema,
	columnCount, rowCount int,
	identifyColumns, dataColumns mysql.Bitmap,
	eventType binlog.RowEventType,
) (*tableUpdater, error) {
	schema := pkSchema.Schema
	pkColumns := pkSchema.PkOrdinals
	pkIndicesInIdentify := getPrimaryKeyIndices(schema, identifyColumns)
	pkIndicesInData := getPrimaryKeyIndices(schema, dataColumns)
	if len(pkIndicesInIdentify) == 0 && len(pkColumns) > 0 {
		pkColumns = nil // disable primary key utilization
	}
	pkSubSchema := make(sql.Schema, len(pkColumns))
	for i, idx := range pkColumns {
		pkSubSchema[i] = schema[idx]
	}

	var (
		sql                 string
		paramCount          int
		pkUpdate            bool
		replace             = false
		cleanup             string
		fullTableName       = quoteIdentifier(databaseName) + "." + quoteIdentifier(tableName)
		keyCount, dataCount = identifyColumns.BitCount(), dataColumns.BitCount()
	)
	switch eventType {
	case binlog.DeleteRowEvent:
		sql, paramCount = buildDeleteTemplate(fullTableName, columnCount, schema, pkColumns, identifyColumns)
	case binlog.UpdateRowEvent:
		pkUpdate = isPkUpdate(schema, identifyColumns, dataColumns)
		if pkUpdate {
			// If the primary key is being updated, we need to use DELETE + INSERT.
			//
			// For example, if the primary has executed `UPDATE t SET pk = pk + 1;`,
			// then both `UPDATE` and `INSERT OR REPLACE` will fail on the replica because the primary key is being updated:
			// - `UPDATE` will fail because of violation of the primary key constraint.
			// - `REPLACE` will fail because it will insert a new row but leave the old row unchanged.
			//
			// However, `DELETE` then `INSERT` in the same transaction will still fail
			// due to the over-eager unique constraint checking in DuckDB, just like the `UPDATE` case.
			//
			// The only way to work around this without breaking atomicity is to do `INSERT OR REPLACE` first,
			// then `DELETE` the old row if the primary key has actually been modified.
			// This requires the occurrence of the primary key columns in both the `identifyColumns` and `dataColumns`.
			if len(pkIndicesInIdentify) == 0 || len(pkIndicesInData) == 0 {
				return nil, ErrPartialPrimaryKeyUpdate
			}
			sql, paramCount = buildInsertTemplate(fullTableName, columnCount, true)
			cleanup, _ = buildDeleteTemplate(fullTableName, columnCount, schema, pkColumns, identifyColumns)
			replace = true
		} else if keyCount < columnCount || dataCount < columnCount {
			sql, paramCount = buildUpdateTemplate(fullTableName, columnCount, schema, pkColumns, identifyColumns, dataColumns)
		} else {
			sql, paramCount = buildInsertTemplate(fullTableName, columnCount, true)
			replace = true
		}
	case binlog.InsertRowEvent:
		sql, paramCount = buildInsertTemplate(fullTableName, columnCount, false)
	}

	logrus.WithFields(logrus.Fields{
		"sql":       sql,
		"replace":   replace,
		"cleanup":   cleanup,
		"keyCount":  keyCount,
		"dataCount": dataCount,
		"pkUpdate":  pkUpdate,
	}).Infoln("Creating table updater...")

	stmt, err := txn.PrepareContext(ctx.Context, sql)
	if err != nil {
		return nil, err
	}

	return &tableUpdater{
		provider:   twp.provider,
		stmt:       stmt,
		replace:    replace,
		cleanup:    cleanup,
		paramCount: paramCount,

		pkIndicesInIdentify: pkIndicesInIdentify,
		pkIndicesInData:     pkIndicesInData,
	}, nil
}

func buildInsertTemplate(tableName string, columnCount int, replace bool) (string, int) {
	var builder strings.Builder
	builder.Grow(32)
	builder.WriteString("INSERT")
	if replace {
		builder.WriteString(" OR REPLACE")
	}
	builder.WriteString(" INTO ")
	builder.WriteString(tableName)
	builder.WriteString(" VALUES (")
	for i := range columnCount {
		builder.WriteString("?")
		if i < columnCount-1 {
			builder.WriteString(", ")
		}
	}
	builder.WriteString(")")
	return builder.String(), columnCount
}

func buildDeleteTemplate(tableName string, columnCount int, schema sql.Schema, pkColumns []int, identifyColumns mysql.Bitmap) (string, int) {
	var builder strings.Builder
	builder.Grow(32)
	builder.WriteString("DELETE FROM ")
	builder.WriteString(tableName)
	builder.WriteString(" WHERE ")

	if len(pkColumns) > 0 {
		for i, c := range pkColumns {
			if i > 0 {
				builder.WriteString(" AND ")
			}
			builder.WriteString(quoteIdentifier(schema[c].Name))
			builder.WriteString(" = ?")
		}
		return builder.String(), len(pkColumns)
	}

	count := 0
	for i := range columnCount {
		if identifyColumns.Bit(i) {
			if count > 0 {
				builder.WriteString(" AND ")
			}
			builder.WriteString(quoteIdentifier(schema[i].Name))
			builder.WriteString(" = ?")
			count++
		}
	}
	return builder.String(), count
}

func buildUpdateTemplate(tableName string, columnCount int, schema sql.Schema, pkColumns []int, identifyColumns, dataColumns mysql.Bitmap) (string, int) {
	var builder strings.Builder
	builder.Grow(32)
	builder.WriteString("UPDATE ")
	builder.WriteString(tableName)
	builder.WriteString(" SET ")
	count := 0
	dataCount := dataColumns.BitCount()
	for i := range columnCount {
		if dataColumns.Bit(i) {
			if count > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString(quoteIdentifier(schema[i].Name))
			builder.WriteString(" = ?")
			count++
		}
	}
	builder.WriteString(" WHERE ")

	if len(pkColumns) > 0 {
		for i, c := range pkColumns {
			if i > 0 {
				builder.WriteString(" AND ")
			}
			builder.WriteString(quoteIdentifier(schema[c].Name))
			builder.WriteString(" = ?")
		}
		return builder.String(), dataCount + len(pkColumns)
	}

	count = 0
	for i := range columnCount {
		if identifyColumns.Bit(i) {
			if count > 0 {
				builder.WriteString(" AND ")
			}
			builder.WriteString(quoteIdentifier(schema[i].Name))
			builder.WriteString(" = ?")
			count++
		}
	}
	return builder.String(), dataCount + identifyColumns.BitCount()
}

type tableUpdater struct {
	provider   *catalog.DatabaseProvider
	tx         *stdsql.Tx
	stmt       *stdsql.Stmt
	replace    bool
	cleanup    string
	paramCount int

	pkSubSchema         sql.Schema
	pkIndicesInIdentify []int
	pkIndicesInData     []int
}

var _ binlogreplication.TableWriter = &tableUpdater{}

func (tu *tableUpdater) Insert(ctx *sql.Context, rows []sql.Row) error {
	defer tu.stmt.Close()
	for _, row := range rows {
		if _, err := tu.stmt.ExecContext(ctx.Context, row...); err != nil {
			return err
		}
	}
	return nil
}

func (tu *tableUpdater) Delete(ctx *sql.Context, keyRows []sql.Row) error {
	defer tu.stmt.Close()
	buf := make(sql.Row, len(tu.pkIndicesInIdentify))
	for _, row := range keyRows {
		var keys sql.Row
		if len(tu.pkIndicesInIdentify) > 0 {
			for i, idx := range tu.pkIndicesInIdentify {
				buf[i] = row[idx]
			}
			keys = buf
		} else {
			keys = row
		}
		if _, err := tu.stmt.ExecContext(ctx.Context, keys...); err != nil {
			return err
		}
	}
	return nil
}

func (tu *tableUpdater) Update(ctx *sql.Context, keyRows []sql.Row, valueRows []sql.Row) error {
	if tu.replace && tu.cleanup == "" {
		return tu.Insert(ctx, valueRows)
	}

	if tu.cleanup != "" {
		return tu.doInsertThenDelete(ctx, keyRows, valueRows)
	}

	// UPDATE t SET col1 = ?, col2 = ? WHERE key1 = ? AND key2 = ?
	buf := make([]interface{}, tu.paramCount)
	for i, values := range valueRows {
		keys := keyRows[i]
		copy(buf, values)
		if len(tu.pkIndicesInIdentify) > 0 {
			for j, idx := range tu.pkIndicesInIdentify {
				buf[len(values)+j] = keys[idx]
			}
		} else {
			copy(buf[len(values):], keys)
		}
		args := buf[:len(values)+len(keys)]
		if _, err := tu.stmt.ExecContext(ctx.Context, args...); err != nil {
			return err
		}
	}
	return nil
}

// https://duckdb.org/docs/sql/indexes#over-eager-unique-constraint-checking
// https://github.com/duckdb/duckdb/issues/14133
func (tu *tableUpdater) doInsertThenDelete(ctx *sql.Context, beforeRows []sql.Row, afterRows []sql.Row) error {
	var err error

	// INSERT OR REPLACE
	if err = tu.Insert(ctx, afterRows); err != nil {
		return err
	}

	// DELETE if the primary key has actually been modified
	stmt, err := tu.tx.PrepareContext(ctx.Context, tu.cleanup)
	if err != nil {
		return err
	}
	defer stmt.Close()

	beforeKey := make(sql.Row, len(tu.pkSubSchema))
	afterKey := make(sql.Row, len(tu.pkSubSchema))
	for i, before := range beforeRows {
		after := afterRows[i]
		for j, idx := range tu.pkIndicesInIdentify {
			beforeKey[j] = before[idx]
		}
		for j, idx := range tu.pkIndicesInData {
			afterKey[j] = after[idx]
		}
		if yes, err := beforeKey.Equals(afterKey, tu.pkSubSchema); err != nil {
			return err
		} else if yes {
			// the row has already been deleted by the INSERT OR REPLACE statement
			continue
		}
		if _, err := stmt.ExecContext(ctx.Context, beforeKey...); err != nil {
			return err
		}
	}

	return nil
}

func quoteIdentifier(identifier string) string {
	return `"` + identifier + `"`
}
