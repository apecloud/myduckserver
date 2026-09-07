package pgserver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/backend"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/duckdb/duckdb-go/v2"
)

type ArrowWriter struct {
	ctx        *sql.Context
	cancel     context.CancelFunc
	execer     adapter.SQLExecutor
	ownerConn  *stdsql.Conn
	ownerTx    *stdsql.Tx
	duckSQL    string
	pipePath   string
	rawOptions string

	started  atomic.Bool
	blocked  atomic.Bool
	done     chan struct{}
	pipeMu   sync.Mutex
	pipe     *os.File
	lease    *postgresCopyLease
	close    sync.Once
	doneOnce sync.Once
}

func NewArrowWriter(
	ctx *sql.Context,
	handler *DuckHandler,
	schema string, table sql.Table, columns tree.NameList,
	query string,
	rawOptions string,
) (*ArrowWriter, error) {
	if err := rejectArrowCopyGMSInTransaction(ctx); err != nil {
		return nil, err
	}
	execer, ownerConn, ownerTx, lease, err := postgresCopySnapshotLease(ctx, handler, nil)
	if err != nil {
		return nil, err
	}
	if err := rejectArrowCopyBinding(ctx, postgresCopyBinding{
		execer:           execer,
		ownerConn:        ownerConn,
		ownerTx:          ownerTx,
		snapshotCaptured: ownerConn != nil,
		lease:            lease,
	}); err != nil {
		lease.Release()
		return nil, err
	}
	if ownerTx != nil {
		lease.Release()
		return nil, ErrArrowCopyInTransaction
	}
	cleanup := true
	defer func() {
		if cleanup {
			lease.Release()
		}
	}()
	copyQuery := query
	if table == nil && handler != nil {
		if rewritten, rewriteErr := handler.rewritePostgresObjectRelationsWithSnapshot(ctx, query, execer, ownerConn); rewriteErr != nil {
			return nil, rewriteErr
		} else {
			copyQuery = rewritten
		}
	}
	target, err := resolvePostgresCopyTarget(ctx, handler, schema, table, execer, ownerConn)
	if err != nil {
		return nil, err
	}

	// Create the FIFO pipe
	db := handler.e.Analyzer.ExecBuilder.PriorityBuilder.(*backend.DuckBuilder)
	pipePath, err := db.CreatePipe(ctx, "pg-to-arrow")
	if err != nil {
		return nil, err
	}

	var builder strings.Builder
	builder.Grow(128)

	if table != nil {
		// https://duckdb.org/docs/sql/query_syntax/from.html#from-first-syntax
		// FROM table_name [ SELECT column_list ]
		builder.WriteString("FROM ")
		builder.WriteString(target)
		if columns != nil {
			builder.WriteString(" SELECT ")
			builder.WriteString(columns.String())
		}
	} else {
		builder.WriteString(copyQuery)
	}

	// Keep the producer on a child context so Close can cancel a query that is
	// still writing to the FIFO. The parent protocol context remains the source
	// of cancellation and frontend origin metadata.
	copyCtx, cancel := newCopyContext(ctx)

	writer := &ArrowWriter{
		ctx:        copyCtx,
		cancel:     cancel,
		execer:     execer,
		ownerConn:  ownerConn,
		ownerTx:    ownerTx,
		duckSQL:    builder.String(),
		pipePath:   pipePath,
		rawOptions: rawOptions, // TODO(fan): parse rawOptions
		done:       make(chan struct{}),
		lease:      lease,
	}
	cleanup = false
	return writer, nil
}

func (dw *ArrowWriter) Start(globalErr *atomic.Pointer[error]) (string, chan CopyToResult, error) {
	// Execute the statement in a separate goroutine.
	dw.ensureDone()
	ch := make(chan CopyToResult, 1)
	dw.started.Store(true)
	dw.blocked.Store(true)
	go func() {
		defer close(dw.done)
		defer close(ch)

		dw.ctx.GetLogger().Tracef("Executing statement via Arrow interface: %s", dw.duckSQL)
		if err := rejectArrowCopyBinding(dw.ctx, postgresCopyBinding{
			execer:           dw.execer,
			ownerConn:        dw.ownerConn,
			ownerTx:          dw.ownerTx,
			snapshotCaptured: dw.ownerConn != nil,
			lease:            dw.lease,
		}); err != nil {
			globalErr.Store(&err)
			ch <- CopyToResult{Err: err}
			return
		}
		conn := dw.ownerConn
		if conn == nil {
			err := errors.New("COPY connection owner is unavailable")
			globalErr.Store(&err)
			ch <- CopyToResult{Err: err}
			return
		}

		// If there is a global error, return immediately.
		if e := globalErr.Load(); e != nil {
			ch <- CopyToResult{Err: *e}
			return
		}

		// Open the pipe for writing.
		// This operation will block until the reader opens the pipe for reading.
		pipe, err := os.OpenFile(dw.pipePath, os.O_WRONLY, os.ModeNamedPipe)
		dw.blocked.Store(false)
		if err != nil {
			globalErr.Store(&err)
			ch <- CopyToResult{Err: err}
			return
		}
		dw.setPipe(pipe)
		defer dw.clearPipe(pipe)
		defer pipe.Close()

		rowCount := int64(0)

		if err := conn.Raw(func(driverConn any) error {
			conn := driverConn.(*duckdb.Conn)
			arrow, err := duckdb.NewArrowFromConn(conn)
			if err != nil {
				return err
			}

			// TODO(fan): Currently, this API materializes the entire result set in memory.
			//   We should consider modifying the API to allow streaming the result set.
			recordReader, err := arrow.QueryContext(dw.ctx, dw.duckSQL)
			if err != nil {
				return err
			}
			defer recordReader.Release()

			writer := ipc.NewWriter(pipe, ipc.WithSchema(recordReader.Schema()))
			defer writer.Close()

			for recordReader.Next() {
				record := recordReader.Record()
				rowCount += record.NumRows()
				if err := writer.Write(record); err != nil {
					return err
				}
			}
			return recordReader.Err()
		}); err != nil {
			globalErr.Store(&err)
			ch <- CopyToResult{Err: err}
			return
		}

		ch <- CopyToResult{RowCount: rowCount}
	}()

	return dw.pipePath, ch, nil
}

func (dw *ArrowWriter) Close() {
	dw.ensureDone()
	dw.close.Do(func() {
		if dw.cancel != nil {
			dw.cancel()
		}

		// Closing the producer side unblocks an in-flight IPC write. The protocol
		// reader normally closes it first, but Close must also be safe on error
		// paths where the reader has already returned.
		dw.closePipe()
		if dw.blocked.Load() {
			// A producer blocked in OpenFile has no *os.File to close yet. A
			// short-lived non-blocking reader lets that open return so cancellation
			// can be observed by the producer goroutine.
			if unblock, err := os.OpenFile(dw.pipePath, os.O_RDONLY|syscall.O_NONBLOCK, os.ModeNamedPipe); err == nil {
				_ = unblock.Close()
			}
		}
	})

	if dw.started.Load() && dw.done != nil {
		<-dw.done
	}
	_ = os.Remove(dw.pipePath)
	if dw.lease != nil {
		// Keep provider operation admission through a physical transaction's
		// COMMIT/ROLLBACK callback; the pool snapshot itself is no longer needed
		// after the producer and pipe reader have terminated.
		dw.lease.ReleaseSnapshot()
		if !dw.lease.OperationDeferred() {
			dw.lease.ReleaseOperation()
		}
	}
}

func (dw *ArrowWriter) ensureDone() {
	if dw == nil {
		return
	}
	dw.doneOnce.Do(func() {
		if dw.done == nil {
			dw.done = make(chan struct{})
		}
	})
}

func (dw *ArrowWriter) setPipe(pipe *os.File) {
	dw.pipeMu.Lock()
	dw.pipe = pipe
	dw.pipeMu.Unlock()
}

func (dw *ArrowWriter) clearPipe(pipe *os.File) {
	dw.pipeMu.Lock()
	if dw.pipe == pipe {
		dw.pipe = nil
	}
	dw.pipeMu.Unlock()
}

func (dw *ArrowWriter) closePipe() {
	dw.pipeMu.Lock()
	pipe := dw.pipe
	dw.pipe = nil
	dw.pipeMu.Unlock()
	if pipe != nil {
		_ = pipe.Close()
	}
}
