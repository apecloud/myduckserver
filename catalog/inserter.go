package catalog

import (
	stdsql "database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/dolthub/go-mysql-server/sql"
)

type rowInserter struct {
	catalog  string
	db       string
	table    string
	schema   sql.Schema
	hasPK    bool
	replace  bool
	provider *DatabaseProvider

	once     sync.Once
	conn     *stdsql.Conn
	tx       *stdsql.Tx
	execer   adapter.SQLExecutor
	tmpTable string
	stmt     *stdsql.Stmt
	err      error
	flushSQL string
	enums    []int
}

var _ sql.RowInserter = &rowInserter{}
var _ sql.RowReplacer = &rowInserter{}

// Delete implements sql.RowReplacer.
// Since REPLACE is handled in the same way as INSERT,
// we don't need to implement it and it should never be called.
func (ri *rowInserter) Delete(ctx *sql.Context, row sql.Row) error {
	return errors.New("unexpected call to Delete")
}

func (ri *rowInserter) init(ctx *sql.Context) {
	ri.tmpTable = fmt.Sprintf("%s_%s_%d", ri.db, ri.table, ctx.ID())
	ri.conn, ri.err = ctx.Session.(adapter.ConnectionHolder).GetConn(ctx)
	if ri.err != nil {
		return
	}
	ri.execer = adapter.SQLExecutorForConn(ctx, ri.conn)
	ri.tx = adapter.TryGetTxn(ctx)
	target := ConnectIdentifiersANSI(ri.db, ri.table)
	if ri.provider != nil && ri.catalog != "" {
		if physical, found, err := ri.provider.ObjectTableName(ctx, ri.catalog, ri.db, ri.table); err != nil {
			ri.err = err
			return
		} else if found {
			if err := ri.provider.EnsureDuckLakeConnection(ctx, ri.conn); err != nil {
				ri.err = err
				return
			}
			target = physical
		}
	}
	ctx.GetLogger().WithField("db", ri.db).WithField("table", ri.table).Infoln("Creating temp table", ri.tmpTable)
	createTable := fmt.Sprintf(
		"CREATE TEMP TABLE IF NOT EXISTS %s AS FROM %s LIMIT 0",
		QuoteIdentifierANSI(ri.tmpTable),
		target,
	)
	if _, ri.err = ri.execer.ExecContext(ctx, createTable); ri.err != nil {
		return
	}

	// TODO(fan): Appender is faster, but it requires strict type alignment.
	var insert strings.Builder
	insert.Grow(64)
	insert.WriteString("INSERT INTO ") // the temp table is keyless, so REPLACE is not needed
	insert.WriteString(QuoteIdentifierANSI(ri.tmpTable))
	insert.WriteString(" VALUES (")
	insert.WriteByte('?')
	for range ri.schema[1:] {
		insert.WriteString(", ?")
	}
	insert.WriteByte(')')
	ri.stmt, ri.err = ri.execer.PrepareContext(ctx, insert.String())
	if ri.err != nil {
		return
	}

	insert.Reset()
	insert.WriteString("INSERT ")
	if ri.replace && ri.hasPK {
		insert.WriteString(" OR REPLACE")
	}
	insert.WriteString(" INTO ")
	insert.WriteString(target)
	insert.WriteString(" FROM ")
	insert.WriteString(QuoteIdentifierANSI(ri.tmpTable))
	ri.flushSQL = insert.String()

	for i, col := range ri.schema {
		if _, ok := col.Type.(sql.EnumType); ok {
			ri.enums = append(ri.enums, i)
		}
	}
}

func (ri *rowInserter) StatementBegin(ctx *sql.Context) {
	ri.once.Do(func() {
		ri.init(ctx)
	})
}

func (ri *rowInserter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return ri.clear(ctx)
}

func (ri *rowInserter) StatementComplete(ctx *sql.Context) error {
	return ri.err
}

func (ri *rowInserter) Close(ctx *sql.Context) error {
	ri.StatementBegin(ctx)
	defer ri.clear(ctx)
	if ri.err == nil {
		_, ri.err = ri.execer.ExecContext(ctx, ri.flushSQL)
	}
	return ri.err
}

func (ri *rowInserter) Insert(ctx *sql.Context, row sql.Row) error {
	// Insert ... RETURNING is driven directly by the GMS insert iterator, which
	// may call Insert without wrapping it in statement-boundary callbacks.
	ri.StatementBegin(ctx)
	if ri.err != nil {
		return ri.err
	}

	// For enum columns, we have to convert the enum ordinal to the enum string.
	for _, i := range ri.enums {
		if idx, ok := row[i].(uint16); ok {
			if s, ok := ri.schema[i].Type.(sql.EnumType).At(int(idx)); ok {
				row[i] = s
			} else {
				return fmt.Errorf("invalid enum value %d for column %s", idx, ri.schema[i].Name)
			}
		}
	}

	if _, err := ri.stmt.ExecContext(ctx, row...); err != nil {
		ri.err = err
		return err
	}
	return nil
}

func (ri *rowInserter) clear(ctx *sql.Context) error {
	if ri.stmt != nil {
		closeErr := ri.stmt.Close()
		// A statement prepared on the session transaction reports ErrTxDone when
		// GMS invokes cleanup after ROLLBACK. The transaction is already inactive
		// in that case, so it is not a product failure and must not mask the
		// rollback result.
		if !errors.Is(closeErr, stdsql.ErrTxDone) {
			ri.err = errors.Join(ri.err, closeErr)
		}
		ri.stmt = nil
	}
	if ri.conn != nil {
		dropper := ri.execer
		// Once the owning transaction has been rolled back, its executor is no
		// longer usable. Reuse the same physical connection for cleanup rather
		// than acquiring another pooled connection (which could block forever
		// while the session still owns this one).
		if ri.tx != nil {
			current := adapter.TryGetTxn(ctx)
			if current == nil || current != ri.tx {
				dropper = ri.conn
			}
		}
		if dropper != nil {
			_, err := dropper.ExecContext(ctx, "DROP TABLE IF EXISTS temp.main."+QuoteIdentifierANSI(ri.tmpTable))
			if errors.Is(err, stdsql.ErrTxDone) && dropper != ri.conn {
				// This is the narrow race where rollback happened after the
				// transaction check above. Retry on the now-inactive connection.
				_, err = ri.conn.ExecContext(ctx, "DROP TABLE IF EXISTS temp.main."+QuoteIdentifierANSI(ri.tmpTable))
			}
			ri.err = errors.Join(ri.err, err)
		}
	}
	return ri.err
}
