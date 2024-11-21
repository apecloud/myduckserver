package pgserver

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/backend"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
)

// DataLoader allows callers to insert rows from multiple chunks into a table. Rows encoded in each chunk will not
// necessarily end cleanly on a chunk boundary, so DataLoader implementations must handle recognizing partial, or
// incomplete records, and saving that partial record until the next call to LoadChunk, so that it may be prefixed
// with the incomplete record.
type DataLoader interface {
	// LoadChunk reads the records from |data| and inserts them into the previously configured table. Data records
	// are not guaranteed to stard and end cleanly on chunk boundaries, so implementations must recognize incomplete
	// records and save them to prepend on the next processed chunk.
	LoadChunk(ctx *sql.Context, data *bufio.Reader) error

	// Abort aborts the current load operation and releases all used resources.
	Abort(ctx *sql.Context) error

	// Finish finalizes the current load operation and commits the inserted rows so that the data becomes visibile
	// to clients. Implementations should check that the last call to LoadChunk did not end with an incomplete
	// record and return an error to the caller if so. The returned LoadDataResults describe the load operation,
	// including how many rows were inserted.
	Finish(ctx *sql.Context) (*LoadDataResults, error)
}

// LoadDataResults contains the results of a load data operation, including the number of rows loaded.
type LoadDataResults struct {
	// RowsLoaded contains the total number of rows inserted during a load data operation.
	RowsLoaded int32
}

var ErrCopyAborted = fmt.Errorf("COPY operation aborted")

type CsvDataLoader struct {
	ctx      *sql.Context
	cancel   context.CancelFunc
	schema   string
	table    sql.InsertableTable
	columns  tree.NameList
	options  *tree.CopyOptions
	pipePath string
	pipe     *os.File
	errPipe  *os.File // for error handling
	rowCount chan int64
	err      atomic.Pointer[error]
}

var _ DataLoader = (*CsvDataLoader)(nil)

func NewCsvDataLoader(ctx *sql.Context, handler *DuckHandler, schema string, table sql.InsertableTable, columns tree.NameList, options *tree.CopyOptions) (DataLoader, error) {
	// Create the FIFO pipe
	duckBuilder := handler.e.Analyzer.ExecBuilder.(*backend.DuckBuilder)
	pipePath, err := duckBuilder.CreatePipe(ctx, "pg-copy-from")
	if err != nil {
		return nil, err
	}

	// Create cancelable context
	childCtx, cancel := context.WithCancel(ctx)
	ctx.Context = childCtx

	loader := &CsvDataLoader{
		ctx:      ctx,
		cancel:   cancel,
		schema:   schema,
		table:    table,
		columns:  columns,
		options:  options,
		pipePath: pipePath,
		rowCount: make(chan int64, 1),
	}

	// Execute the DuckDB COPY statement in a goroutine.
	sql := loader.buildSQL()
	loader.ctx.GetLogger().Trace(sql)
	go loader.executeCopy(sql, pipePath)

	// TODO(fan): If the reader fails to open the pipe, the writer will block forever.

	// Open the pipe for writing.
	// This operation will block until the reader opens the pipe for reading.
	pipe, err := os.OpenFile(pipePath, os.O_WRONLY, os.ModeNamedPipe)
	if err != nil {
		return nil, err
	}

	// If the COPY operation failed to start, close the pipe and return the error.
	if loader.errPipe != nil {
		return nil, errors.Join(*loader.err.Load(), pipe.Close(), loader.errPipe.Close())
	}

	loader.pipe = pipe

	return loader, nil
}

// buildSQL builds the DuckDB COPY FROM statement.
func (loader *CsvDataLoader) buildSQL() string {
	var b strings.Builder
	b.Grow(256)

	b.WriteString("COPY ")
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

	b.WriteString(" FROM '")
	b.WriteString(loader.pipePath)
	b.WriteString("' (FORMAT CSV, AUTO_DETECT false")

	options := loader.options

	b.WriteString(", HEADER ")
	if options.HasHeader && options.Header {
		b.WriteString("true")
	} else {
		b.WriteString("false")
	}

	if options.Delimiter != nil {
		b.WriteString(", SEP ")
		b.WriteString(options.Delimiter.String())
	} else if options.CopyFormat == tree.CopyFormatText {
		b.WriteString(`, SEP '\t'`)
	}

	if options.Quote != nil {
		b.WriteString(", QUOTE ")
		b.WriteString(singleQuotedDuckChar(options.Quote.RawString()))
	} else if options.CopyFormat == tree.CopyFormatText {
		b.WriteString(`, QUOTE ''`)
	}

	if options.Escape != nil {
		b.WriteString(", ESCAPE ")
		b.WriteString(singleQuotedDuckChar(options.Escape.RawString()))
	} else if options.CopyFormat == tree.CopyFormatText {
		b.WriteString(`, ESCAPE ''`)
	}

	if options.Null != nil {
		b.WriteString(", NULLSTR ")
		b.WriteString(options.Null.String())
	} else if options.CopyFormat == tree.CopyFormatText {
		b.WriteString(`, NULLSTR '\N'`)
	}

	b.WriteString(")")

	return b.String()
}

func (loader *CsvDataLoader) executeCopy(sql string, pipePath string) {
	defer close(loader.rowCount)
	result, err := adapter.Exec(loader.ctx, sql)
	if err != nil {
		loader.ctx.GetLogger().Error(err)
		loader.err.Store(&err)
		// Open the pipe once to unblock the writer
		loader.errPipe, _ = os.OpenFile(pipePath, os.O_RDONLY, os.ModeNamedPipe)
		return
	}

	rows, err := result.RowsAffected()
	if err != nil {
		loader.ctx.GetLogger().Error(err)
		loader.err.Store(&err)
		return
	}
	loader.rowCount <- rows
}

func (loader *CsvDataLoader) LoadChunk(ctx *sql.Context, data *bufio.Reader) error {
	if errp := loader.err.Load(); errp != nil {
		return fmt.Errorf("COPY operation has been aborted: %w", *errp)
	}
	// Write the data to the FIFO pipe.
	_, err := io.Copy(loader.pipe, data)
	if err != nil {
		ctx.GetLogger().Error("Copying data to pipe failed:", err)
		loader.Abort(ctx)
		return err
	}
	return nil
}

func (loader *CsvDataLoader) Abort(ctx *sql.Context) error {
	defer os.Remove(loader.pipePath)
	loader.err.Store(&ErrCopyAborted)
	loader.cancel()
	<-loader.rowCount // Ensure the reader has exited
	return loader.pipe.Close()
}

func (loader *CsvDataLoader) Finish(ctx *sql.Context) (*LoadDataResults, error) {
	defer os.Remove(loader.pipePath)

	if errp := loader.err.Load(); errp != nil {
		return nil, *errp
	}

	// Close the pipe to signal the reader to exit
	if err := loader.pipe.Close(); err != nil {
		return nil, err
	}

	rows := <-loader.rowCount

	// Now the reader has exited, check the error again
	if errp := loader.err.Load(); errp != nil {
		return nil, *errp
	}

	return &LoadDataResults{
		RowsLoaded: int32(rows),
	}, nil
}

func singleQuotedDuckChar(s string) string {
	if len(s) == 0 {
		return `''`
	}
	r := []rune(s)[0]
	if r == '\\' {
		return `'\'` // Slash does not need to be escaped in DuckDB
	}
	return strconv.QuoteRune(r) // e.g., tab -> '\t'
}
