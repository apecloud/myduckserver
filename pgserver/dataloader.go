package pgserver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/backend"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
)

// DataLoader allows callers to insert rows from multiple chunks into a table. Rows encoded in each chunk will not
// necessarily end cleanly on a chunk boundary, so DataLoader implementations must handle recognizing partial, or
// incomplete records, and saving that partial record until the next call to LoadChunk, so that it may be prefixed
// with the incomplete record.
type DataLoader interface {
	// Start prepares the DataLoader for loading data. This may involve creating goroutines, opening files, etc.
	// Start must be called before any calls to LoadChunk.
	Start() <-chan error

	// LoadChunk reads the records from |data| and inserts them into the previously configured table. Data records
	// are not guaranteed to stard and end cleanly on chunk boundaries, so implementations must recognize incomplete
	// records and save them to prepend on the next processed chunk.
	LoadChunk(ctx *sql.Context, data []byte) error

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

type PipeDataLoader struct {
	ctx      *sql.Context
	cancel   context.CancelFunc
	schema   string
	table    sql.InsertableTable
	columns  tree.NameList
	pipePath string
	read     func()
	pipe     atomic.Pointer[os.File] // for writing
	errPipe  atomic.Pointer[os.File] // for error handling
	rowCount chan int64
	err      atomic.Pointer[error]
	logger   *logrus.Entry
}

func (loader *PipeDataLoader) Start() <-chan error {
	// Open the reader.
	go loader.read()

	ready := make(chan error, 1)
	go func() {
		defer close(ready)

		// TODO(fan): If the reader fails to open the pipe, the writer will block forever.
		// Open the pipe for writing.
		// This operation will block until the reader opens the pipe for reading.
		loader.logger.Debugf("Opening pipe for writing: %s", loader.pipePath)
		pipe, err := os.OpenFile(loader.pipePath, os.O_WRONLY, os.ModeNamedPipe)
		if err != nil {
			ready <- err
			return
		}

		// If the COPY operation failed to start, close the pipe and return the error.
		if loader.errPipe.Load() != nil {
			ready <- errors.Join(*loader.err.Load(), pipe.Close(), loader.errPipe.Load().Close())
			return
		}

		loader.pipe.Store(pipe)
	}()
	return ready
}

func (loader *PipeDataLoader) LoadChunk(ctx *sql.Context, data []byte) error {
	if errp := loader.err.Load(); errp != nil {
		return fmt.Errorf("COPY operation has been aborted: %w", *errp)
	}
	loader.logger.Tracef("Copying %d bytes to pipe %s", len(data), loader.pipePath)
	// Write the data to the FIFO pipe.
	_, err := loader.pipe.Load().Write(data)
	if err != nil {
		loader.logger.Error("Copying data to pipe failed:", err)
		loader.Abort(ctx)
		return err
	}
	loader.logger.Tracef("Copied %d bytes to pipe %s", len(data), loader.pipePath)
	return nil
}

func (loader *PipeDataLoader) Abort(ctx *sql.Context) error {
	defer os.Remove(loader.pipePath)
	loader.err.Store(&ErrCopyAborted)
	loader.cancel()
	<-loader.rowCount // Ensure the reader has exited
	return loader.pipe.Load().Close()
}

func (loader *PipeDataLoader) Finish(ctx *sql.Context) (*LoadDataResults, error) {
	defer os.Remove(loader.pipePath)

	if errp := loader.err.Load(); errp != nil {
		loader.logger.Errorln("COPY operation failed:", *errp)
		return nil, *errp
	}

	// Close the pipe to signal the reader to exit
	if err := loader.pipe.Load().Close(); err != nil {
		return nil, err
	}

	rows := <-loader.rowCount

	// Now the reader has exited, check the error again
	if errp := loader.err.Load(); errp != nil {
		loader.logger.Errorln("COPY operation failed:", *errp)
		return nil, *errp
	}

	return &LoadDataResults{
		RowsLoaded: int32(rows),
	}, nil
}

type CsvDataLoader struct {
	PipeDataLoader
	options *tree.CopyOptions
}

var _ DataLoader = (*CsvDataLoader)(nil)

func NewCsvDataLoader(
	ctx *sql.Context, handler *DuckHandler,
	schema string, table sql.InsertableTable, columns tree.NameList, options *tree.CopyOptions,
	rawOptions string, // For non-PG-parsable COPY FROM, unused for now
) (DataLoader, error) {
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
		options: options,
	}
	loader.read = func() {
		loader.executeCopy(loader.buildSQL(), pipePath)
	}

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
	loader.logger.Debugf("Executing COPY statement: %s", sql)
	result, err := adapter.Exec(loader.ctx, sql)
	if err != nil {
		loader.ctx.GetLogger().Error(err)
		loader.err.Store(&err)
		// Open the pipe once to unblock the writer
		pipe, _ := os.OpenFile(pipePath, os.O_RDONLY, os.ModeNamedPipe)
		loader.errPipe.Store(pipe)
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
