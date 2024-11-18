package pgserver

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"

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
	schema   *string
	table    sql.InsertableTable
	columns  tree.NameList
	options  *tree.CopyOptions
	pipePath string
	pipe     *os.File
	rowCount chan int64
	err      atomic.Pointer[error]
}

var _ DataLoader = (*CsvDataLoader)(nil)

func NewCsvDataLoader(sqlCtx *sql.Context, handler *DuckHandler, schema *string, table sql.InsertableTable, columns tree.NameList, options *tree.CopyOptions) (DataLoader, error) {
	duckBuilder := handler.e.Analyzer.ExecBuilder.(*backend.DuckBuilder)
	dataDir := duckBuilder.Provider().DataDir()

	// Create the FIFO pipe
	pipeDir := filepath.Join(dataDir, "pipes", "load-data")
	if err := os.MkdirAll(pipeDir, 0755); err != nil {
		return nil, err
	}
	pipeName := strconv.Itoa(int(sqlCtx.ID())) + ".pipe"
	pipePath := filepath.Join(pipeDir, pipeName)
	sqlCtx.GetLogger().Traceln("Creating FIFO pipe for COPY operation:", pipePath)
	if err := syscall.Mkfifo(pipePath, 0600); err != nil {
		return nil, err
	}

	// Create cancelable context
	childCtx, cancel := context.WithCancel(sqlCtx)
	sqlCtx.Context = childCtx

	loader := &CsvDataLoader{
		ctx:      sqlCtx,
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
	go loader.executeCopy(sql)

	// Open the pipe for writing.
	// This operation will block until the reader opens the pipe for reading.
	pipe, err := os.OpenFile(pipePath, os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	loader.pipe = pipe

	return loader, nil
}

// buildSQL builds the DuckDB COPY FROM statement.
func (loader *CsvDataLoader) buildSQL() string {
	var b strings.Builder
	b.Grow(256)

	b.WriteString("COPY ")
	if loader.schema != nil {
		b.WriteString(*loader.schema)
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
	b.WriteString("' (AUTO_DETECT false")

	options := loader.options

	if options.HasHeader && options.Header {
		b.WriteString(", HEADER")
	}

	if options.Delimiter != nil {
		b.WriteString(", SEP ")
		b.WriteString(options.Delimiter.String())
	}

	if options.Quote != nil {
		b.WriteString(", QUOTE ")
		b.WriteString(singleQuotedDuckChar(options.Quote.RawString()))
	}

	if options.Escape != nil {
		b.WriteString(", ESCAPE ")
		b.WriteString(singleQuotedDuckChar(options.Escape.RawString()))
	}

	if options.Null != nil {
		b.WriteString(", NULLSTR ")
		b.WriteString(loader.options.Null.String())
	}

	b.WriteString(")")

	return b.String()
}

func (loader *CsvDataLoader) executeCopy(sql string) {
	defer close(loader.rowCount)
	result, err := adapter.Exec(loader.ctx, sql)
	if err != nil {
		loader.ctx.GetLogger().Error(err)
		loader.err.Store(&err)
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
