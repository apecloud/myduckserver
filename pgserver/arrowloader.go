package pgserver

import (
	"context"
	"os"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/backend"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/marcboeker/go-duckdb"
)

type ArrowDataLoader struct {
	PipeDataLoader
	arrowName string
	options   string
}

var _ DataLoader = (*ArrowDataLoader)(nil)

func NewArrowDataLoader(ctx *sql.Context, handler *DuckHandler, schema string, table sql.InsertableTable, columns tree.NameList, options string) (DataLoader, error) {
	// Create the FIFO pipe
	duckBuilder := handler.e.Analyzer.ExecBuilder.(*backend.DuckBuilder)
	pipePath, err := duckBuilder.CreatePipe(ctx, "pg-from-arrow")
	if err != nil {
		return nil, err
	}
	arrowName := "__sys_copy_from_arrow_" + strconv.Itoa(int(ctx.ID())) + "__"

	// Create cancelable context
	childCtx, cancel := context.WithCancel(ctx)
	ctx.Context = childCtx

	loader := &ArrowDataLoader{
		PipeDataLoader: PipeDataLoader{
			ctx:      ctx,
			cancel:   cancel,
			schema:   schema,
			table:    table,
			columns:  columns,
			pipePath: pipePath,
			rowCount: make(chan int64, 1),
			logger:   ctx.GetLogger(),
		},
		arrowName: arrowName,
		options:   options,
	}
	loader.read = func() {
		loader.executeInsert(loader.buildSQL(), pipePath)
	}

	return loader, nil
}

// buildSQL builds the DuckDB INSERT statement.
func (loader *ArrowDataLoader) buildSQL() string {
	var b strings.Builder
	b.Grow(256)

	b.WriteString("INSERT INTO ")
	if loader.schema != "" {
		b.WriteString(loader.schema)
		b.WriteString(".")
	}
	b.WriteString(loader.table.Name())

	if len(loader.columns) > 0 {
		b.WriteString(" (")
		b.WriteString(loader.columns.String())
		b.WriteString(")")
	}

	b.WriteString(" FROM ")
	b.WriteString(loader.arrowName)

	return b.String()
}

func (loader *ArrowDataLoader) executeInsert(sql string, pipePath string) {
	defer close(loader.rowCount)

	// Open the pipe for reading.
	loader.logger.Debugf("Opening pipe for reading: %s", pipePath)
	pipe, err := os.OpenFile(pipePath, os.O_RDONLY, os.ModeNamedPipe)
	if err != nil {
		loader.err.Store(&err)
		// Open the pipe once to unblock the writer
		pipe, _ = os.OpenFile(pipePath, os.O_RDONLY, os.ModeNamedPipe)
		loader.errPipe.Store(pipe)
		return
	}

	// Create an Arrow IPC reader from the pipe.
	loader.logger.Debugf("Creating Arrow IPC reader from pipe: %s", pipePath)
	arrowReader, err := ipc.NewReader(pipe)
	if err != nil {
		loader.err.Store(&err)
		return
	}
	defer arrowReader.Release()

	conn, err := adapter.GetConn(loader.ctx)
	if err != nil {
		loader.err.Store(&err)
		return
	}

	// Register the Arrow IPC reader to DuckDB.
	loader.logger.Debugf("Registering Arrow IPC reader into DuckDB: %s", loader.arrowName)
	var release func()
	if err := conn.Raw(func(driverConn any) error {
		conn := driverConn.(*duckdb.Conn)
		arrow, err := duckdb.NewArrowFromConn(conn)
		if err != nil {
			return err
		}

		release, err = arrow.RegisterView(arrowReader, loader.arrowName)
		return err
	}); err != nil {
		loader.err.Store(&err)
		return
	}
	defer release()

	// Execute the INSERT statement.
	// This will block until the reader has finished reading the data.
	loader.logger.Debugln("Executing SQL:", sql)
	result, err := conn.ExecContext(loader.ctx, sql)
	if err != nil {
		loader.err.Store(&err)
		return
	}

	rows, err := result.RowsAffected()
	if err != nil {
		loader.err.Store(&err)
		return
	}

	loader.logger.Debugf("Inserted %d rows", rows)
	loader.rowCount <- rows
}
