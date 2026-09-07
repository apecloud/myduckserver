package pgserver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

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
	ctx              *sql.Context
	cancel           context.CancelFunc
	execer           adapter.SQLExecutor
	ownerConn        *stdsql.Conn
	ownerTx          *stdsql.Tx
	snapshotCaptured bool
	lease            *postgresCopyLease
	ownsLease        bool
	schema           string
	table            sql.InsertableTable
	target           string
	columns          tree.NameList
	pipePath         string
	read             func()
	pipe             atomic.Pointer[os.File] // for writing
	blocked          atomic.Bool             // for writing
	errPipe          atomic.Pointer[os.File] // for error handling
	rowCount         chan int64
	done             chan struct{}
	writerDone       chan struct{}
	ready            chan error
	startOnce        sync.Once
	started          atomic.Bool
	aborted          atomic.Bool
	abortOnce        sync.Once
	abortErr         error
	err              atomic.Pointer[error]
	logger           *logrus.Entry
}

func (loader *PipeDataLoader) Start() <-chan error {
	loader.startOnce.Do(func() {
		loader.ready = make(chan error, 1)
		if loader.done == nil {
			loader.done = make(chan struct{})
		}
		loader.writerDone = make(chan struct{})
		if loader.aborted.Load() {
			close(loader.done)
			close(loader.writerDone)
			loader.ready <- ErrCopyAborted
			close(loader.ready)
			loader.releaseOwnedLease()
			return
		}
		if err := loader.validateBinding(loader.ctx); err != nil {
			loader.err.Store(&err)
			close(loader.done)
			close(loader.writerDone)
			loader.ready <- err
			close(loader.ready)
			loader.releaseOwnedLease()
			return
		}
		if loader.read == nil {
			err := errors.New("COPY data loader has no reader")
			loader.err.Store(&err)
			close(loader.done)
			close(loader.writerDone)
			loader.ready <- err
			close(loader.ready)
			loader.releaseOwnedLease()
			return
		}

		loader.started.Store(true)
		loader.blocked.Store(true)

		// Open the reader. A panic in a loader implementation must still wake
		// the protocol side and release any writer blocked on the FIFO.
		go func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					err := fmt.Errorf("COPY data loader panicked: %v", recovered)
					loader.err.Store(&err)
					if loader.cancel != nil {
						loader.cancel()
					}
					_ = loader.closePipe()
				}
				if loader.blocked.Load() {
					// A reader that exits before opening the FIFO must release a
					// writer blocked in OpenFile. This also covers non-panic setup
					// failures in custom DataLoader implementations.
					loader.unblockWriter()
				}
				close(loader.done)
				if loader.currentError() != nil {
					go func() {
						loader.Wait()
						loader.releaseOwnedLease()
					}()
				}
			}()
			loader.read()
		}()

		go func() {
			defer close(loader.writerDone)
			defer close(loader.ready)

			// Open the pipe for writing. This operation blocks until the reader
			// opens the FIFO, so Abort has an explicit unblock path below.
			if loader.logger != nil {
				loader.logger.Debugf("Opening pipe for writing: %s", loader.pipePath)
			}
			pipe, err := os.OpenFile(loader.pipePath, os.O_WRONLY, os.ModeNamedPipe)
			loader.blocked.Store(false)
			if err != nil {
				loader.err.Store(&err)
				if loader.cancel != nil {
					loader.cancel()
				}
				loader.ready <- err
				go func() {
					loader.Wait()
					loader.releaseOwnedLease()
				}()
				return
			}
			// If the COPY operation failed to start, close both ends and surface
			// the original reader error. Keep this path nil-safe: a reader may
			// have published an unblock descriptor before its primary error.
			if errPipe := loader.errPipe.Swap(nil); errPipe != nil {
				copyErr := loader.currentError()
				if copyErr == nil {
					copyErr = errors.New("COPY data loader failed before startup")
				}
				loader.ready <- errors.Join(copyErr, pipe.Close(), errPipe.Close())
				go func() {
					loader.Wait()
					loader.releaseOwnedLease()
				}()
				return
			}
			if copyErr := loader.currentError(); copyErr != nil {
				_ = pipe.Close()
				loader.ready <- copyErr
				go func() {
					loader.Wait()
					loader.releaseOwnedLease()
				}()
				return
			}
			// Abort may race the writer opening the FIFO. Do not publish a pipe
			// to a loader that has already been canceled.
			if loader.aborted.Load() {
				_ = pipe.Close()
				loader.ready <- ErrCopyAborted
				go func() {
					loader.Wait()
					loader.releaseOwnedLease()
				}()
				return
			}

			loader.pipe.Store(pipe)
			if loader.aborted.Load() {
				if loader.pipe.CompareAndSwap(pipe, nil) {
					_ = pipe.Close()
				}
				loader.ready <- ErrCopyAborted
				go func() {
					loader.Wait()
					loader.releaseOwnedLease()
				}()
			}
		}()
	})
	return loader.ready
}

func (loader *PipeDataLoader) LoadChunk(ctx *sql.Context, data []byte) error {
	if ctx == nil {
		ctx = loader.ctx
	}
	if err := loader.validateBinding(ctx); err != nil {
		return err
	}
	if copyErr := loader.currentError(); copyErr != nil {
		return fmt.Errorf("COPY operation has been aborted: %w", copyErr)
	}
	pipe := loader.pipe.Load()
	if pipe == nil {
		return errors.New("COPY data loader is not ready")
	}
	if loader.logger != nil {
		loader.logger.Tracef("Copying %d bytes to pipe %s", len(data), loader.pipePath)
	}
	// Write the data to the FIFO pipe.
	_, err := pipe.Write(data)
	if err != nil {
		if loader.logger != nil {
			loader.logger.Error("Copying data to pipe failed:", err)
		}
		loader.Abort(ctx)
		return err
	}
	if loader.logger != nil {
		loader.logger.Tracef("Copied %d bytes to pipe %s", len(data), loader.pipePath)
	}
	return nil
}

func (loader *PipeDataLoader) Abort(ctx *sql.Context) error {
	loader.abortOnce.Do(func() {
		loader.aborted.Store(true)
		loader.err.Store(&ErrCopyAborted)
		if loader.cancel != nil {
			loader.cancel()
		}

		// Close the writer before waiting. Arrow IPC readers can be blocked in an
		// operating-system read from the FIFO, which cancellation alone cannot
		// interrupt; EOF is what lets the reader goroutine finish.
		loader.abortErr = loader.closePipe()
		if loader.blocked.Load() {
			// If the writer side is still blocked in OpenFile, briefly opening a
			// non-blocking reader gives it an EOF/error path so it can observe the
			// aborted flag and exit.
			loader.unblockWriter()
		}
		loader.Wait()
		_ = os.Remove(loader.pipePath)
		loader.releaseOwnedLease()
	})
	return loader.abortErr
}

func (loader *PipeDataLoader) currentError() error {
	if errp := loader.err.Load(); errp != nil && *errp != nil {
		return *errp
	}
	return nil
}

func (loader *PipeDataLoader) releaseOwnedLease() {
	if loader != nil && loader.ownsLease && loader.lease != nil {
		loader.lease.Release()
	}
}

func (loader *PipeDataLoader) validateBinding(ctx *sql.Context) error {
	if loader == nil || !loader.snapshotCaptured {
		return nil
	}
	return (postgresCopyBinding{
		execer:           loader.execer,
		ownerConn:        loader.ownerConn,
		ownerTx:          loader.ownerTx,
		snapshotCaptured: true,
		lease:            loader.lease,
	}).validate(ctx)
}

func (loader *PipeDataLoader) closePipe() error {
	pipe := loader.pipe.Swap(nil)
	if pipe == nil {
		return nil
	}
	return pipe.Close()
}

func (loader *PipeDataLoader) unblockWriter() {
	if loader.pipePath == "" {
		return
	}
	if unblock, err := os.OpenFile(loader.pipePath, os.O_RDONLY|syscall.O_NONBLOCK, os.ModeNamedPipe); err == nil {
		_ = unblock.Close()
	}
}

// Wait blocks until the reader goroutine has exited. Abort already waits for
// the row-count signal, but keeping a separate completion signal makes the
// lifecycle guarantee explicit for protocol handlers and custom loaders.
func (loader *PipeDataLoader) Wait() {
	if loader.started.Load() && loader.done != nil {
		<-loader.done
	}
	if loader.started.Load() && loader.writerDone != nil {
		<-loader.writerDone
	}
	// The reader opens and then publishes errPipe, so the writer can wake and
	// race past its initial Swap before that Store becomes visible. Once both
	// goroutines are done, no later publisher exists and this final Swap closes
	// any descriptor left by that race. Repeated Wait calls remain safe.
	if errPipe := loader.errPipe.Swap(nil); errPipe != nil {
		_ = errPipe.Close()
	}
}

func (loader *PipeDataLoader) Finish(ctx *sql.Context) (*LoadDataResults, error) {
	defer loader.releaseOwnedLease()
	defer func() {
		_ = os.Remove(loader.pipePath)
	}()
	if ctx == nil {
		ctx = loader.ctx
	}
	if err := loader.validateBinding(ctx); err != nil {
		if loader.started.Load() {
			abortErr := loader.Abort(ctx)
			return nil, errors.Join(err, abortErr)
		}
		return nil, err
	}

	if !loader.started.Load() {
		if copyErr := loader.currentError(); copyErr != nil {
			return nil, copyErr
		}
		return nil, errors.New("COPY data loader has not been started")
	}

	// Close the pipe to signal the reader to exit
	closeErr := loader.closePipe()
	loader.Wait()
	if loader.cancel != nil {
		loader.cancel()
	}

	copyErr := loader.currentError()
	if copyErr != nil {
		if loader.logger != nil {
			loader.logger.Errorln("COPY operation failed:", copyErr)
		}
		return nil, errors.Join(copyErr, closeErr)
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if loader.rowCount == nil {
		return nil, errors.New("COPY data loader has no row-count channel")
	}
	rows, ok := <-loader.rowCount
	if !ok {
		if copyErr := loader.currentError(); copyErr != nil {
			return nil, copyErr
		}
		return nil, errors.New("COPY data loader stopped without a row count")
	}

	// Now the reader has exited, check the error again
	if copyErr := loader.currentError(); copyErr != nil {
		if loader.logger != nil {
			loader.logger.Errorln("COPY operation failed:", copyErr)
		}
		return nil, copyErr
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
	binding, err := capturePostgresCopyBindingWithHandler(ctx, handler, nil)
	if err != nil {
		return nil, err
	}
	loader, err := newCsvDataLoaderWithBinding(ctx, handler, schema, table, columns, options, rawOptions, binding)
	if err != nil {
		binding.releaseLease()
		return nil, err
	}
	if pipeLoader, ok := loader.(*CsvDataLoader); ok {
		pipeLoader.ownsLease = true
	}
	return loader, nil
}

func newCsvDataLoaderWithBinding(
	ctx *sql.Context, handler *DuckHandler,
	schema string, table sql.InsertableTable, columns tree.NameList, options *tree.CopyOptions,
	rawOptions string, binding postgresCopyBinding,
) (DataLoader, error) {
	ownedBinding := false
	if !binding.snapshotCaptured {
		var err error
		binding, err = capturePostgresCopyBindingWithHandler(ctx, handler, nil)
		if err != nil {
			return nil, err
		}
		ownedBinding = true
	}
	cleanup := true
	defer func() {
		if cleanup && ownedBinding {
			binding.releaseLease()
		}
	}()
	target, err := resolvePostgresCopyTarget(ctx, handler, schema, table, binding.execer, binding.ownerConn)
	if err != nil {
		return nil, err
	}

	// Create the FIFO pipe
	duckBuilder := handler.e.Analyzer.ExecBuilder.PriorityBuilder.(*backend.DuckBuilder)
	pipePath, err := duckBuilder.CreatePipe(ctx, "pg-copy-from")
	if err != nil {
		return nil, err
	}

	// Create a child without mutating the parent context into its own ancestor.
	ctx, cancel := newCopyContext(ctx)

	loader := &CsvDataLoader{
		PipeDataLoader: PipeDataLoader{
			ctx:              ctx,
			cancel:           cancel,
			execer:           binding.execer,
			ownerConn:        binding.ownerConn,
			ownerTx:          binding.ownerTx,
			snapshotCaptured: binding.snapshotCaptured,
			lease:            binding.lease,
			schema:           schema,
			table:            table,
			target:           target,
			columns:          columns,
			pipePath:         pipePath,
			rowCount:         make(chan int64, 1),
			done:             make(chan struct{}),
			logger:           ctx.GetLogger(),
		},
		options: options,
	}
	loader.read = func() {
		loader.executeCopy(loader.buildSQL(), pipePath)
	}

	cleanup = false
	return loader, nil
}

func newCopyContext(ctx *sql.Context) (*sql.Context, context.CancelFunc) {
	return ctx.NewSubContext()
}

// buildSQL builds the DuckDB COPY FROM statement.
func (loader *CsvDataLoader) buildSQL() string {
	var b strings.Builder
	b.Grow(256)

	b.WriteString("COPY ")
	if loader.target != "" {
		b.WriteString(loader.target)
	} else if loader.schema != "" {
		b.WriteString(loader.schema)
		b.WriteString(".")
		b.WriteString(loader.table.Name())
	} else {
		b.WriteString(loader.table.Name())
	}

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
	defer func() {
		if loader.rowCount != nil {
			close(loader.rowCount)
		}
	}()
	if err := loader.validateBinding(loader.ctx); err != nil {
		loader.err.Store(&err)
		if loader.blocked.Load() {
			loader.unblockWriter()
		}
		return
	}
	if loader.logger != nil {
		loader.logger.Debugf("Executing COPY statement: %s", sql)
	}
	if err := loader.validateBinding(loader.ctx); err != nil {
		loader.err.Store(&err)
		if loader.blocked.Load() {
			loader.unblockWriter()
		}
		return
	}
	var result stdsql.Result
	var err error
	if loader.execer != nil {
		result, err = loader.execer.ExecContext(loader.ctx, sql)
	} else {
		result, err = adapter.Exec(loader.ctx, sql)
	}
	if err != nil {
		if loader.ctx != nil && loader.ctx.GetLogger() != nil {
			loader.ctx.GetLogger().Error(err)
		}
		loader.err.Store(&err)
		if loader.blocked.Load() {
			// Open the pipe once to unblock a writer that may still be waiting in
			// OpenFile. O_NONBLOCK avoids turning this error path into a second
			// permanently blocked goroutine when the FIFO has disappeared.
			if unblock, openErr := os.OpenFile(pipePath, os.O_RDONLY|syscall.O_NONBLOCK, os.ModeNamedPipe); openErr == nil {
				loader.errPipe.Store(unblock)
			}
		}
		return
	}

	rows, err := result.RowsAffected()
	if err != nil {
		if loader.ctx != nil && loader.ctx.GetLogger() != nil {
			loader.ctx.GetLogger().Error(err)
		}
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
