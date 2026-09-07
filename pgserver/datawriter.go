package pgserver

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/backend"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
)

type DataWriter interface {
	Start(globalErr *atomic.Pointer[error]) (string, chan CopyToResult, error)
	Close()
}

type CopyToResult struct {
	RowCount int64
	Err      error
}

type DuckDataWriter struct {
	ctx       *sql.Context
	execer    adapter.SQLExecutor
	ownerConn *stdsql.Conn
	duckSQL   string
	options   *tree.CopyOptions
	pipePath  string
	cancel    context.CancelFunc
	done      chan struct{}
	started   atomic.Bool
	blocked   atomic.Bool
	close     sync.Once
	doneOnce  sync.Once
	lease     *postgresCopyLease
}

func NewDuckDataWriter(
	ctx *sql.Context,
	handler *DuckHandler,
	schema string, table sql.Table, columns tree.NameList,
	query string,
	options *tree.CopyOptions, rawOptions string,
) (*DuckDataWriter, error) {
	execer, ownerConn, _, lease, err := postgresCopySnapshotLease(ctx, handler, nil)
	if err != nil {
		return nil, err
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
	pipePath, err := db.CreatePipe(ctx, "pg-copy-to")
	if err != nil {
		return nil, err
	}

	// https://www.postgresql.org/docs/current/sql-copy.html
	// https://duckdb.org/docs/sql/statements/copy.html#csv-options
	var builder strings.Builder
	builder.Grow(128)

	builder.WriteString("COPY ")
	if table != nil {
		builder.WriteString(target)
		if columns != nil {
			builder.WriteString("(")
			builder.WriteString(columns.String())
			builder.WriteString(")")
		}
	} else {
		// the parentheses have already been added
		builder.WriteString(copyQuery)
	}

	builder.WriteString(" TO '")
	builder.WriteString(pipePath)

	switch options.CopyFormat {
	case CopyFormatParquet:
		builder.WriteString("' (FORMAT PARQUET")
		if rawOptions != "" {
			builder.WriteString(", ")
			builder.WriteString(rawOptions)
		}
		builder.WriteString(")")

	case CopyFormatJSON:
		builder.WriteString("' (FORMAT JSON")
		if rawOptions != "" {
			builder.WriteString(", ")
			builder.WriteString(rawOptions)
		}
		builder.WriteString(")")

	case tree.CopyFormatText, tree.CopyFormatCSV:
		builder.WriteString("' (FORMAT CSV")

		if rawOptions != "" {
			// TODO(fan): For TEXT format, we should add some default options if not specified.
			builder.WriteString(", ")
			builder.WriteString(rawOptions)
			builder.WriteString(")")
			break
		}

		builder.WriteString(", HEADER ")
		if options.HasHeader && options.Header {
			builder.WriteString("true")
		} else {
			builder.WriteString("false")
		}

		if options.Delimiter != nil {
			builder.WriteString(", DELIMITER ")
			builder.WriteString(options.Delimiter.String())
		} else if options.CopyFormat == tree.CopyFormatText {
			builder.WriteString(`, DELIMITER '\t'`)
		}

		if options.Quote != nil {
			builder.WriteString(", QUOTE ")
			builder.WriteString(singleQuotedDuckChar(options.Quote.RawString()))
		} else if options.CopyFormat == tree.CopyFormatText {
			builder.WriteString(`, QUOTE ''`)
		}

		if options.Escape != nil {
			builder.WriteString(", ESCAPE ")
			builder.WriteString(singleQuotedDuckChar(options.Escape.RawString()))
		} else if options.CopyFormat == tree.CopyFormatText {
			builder.WriteString(`, ESCAPE ''`)
		}

		if options.Null != nil {
			builder.WriteString(`, NULLSTR '`)
			builder.WriteString(options.Null.String())
			builder.WriteString(`'`)
		} else if options.CopyFormat == tree.CopyFormatText {
			builder.WriteString(`, NULLSTR '\N'`)
		}
		builder.WriteString(")")

	case tree.CopyFormatBinary:
		return nil, fmt.Errorf("BINARY format is not supported for COPY TO")
	}

	copyCtx, cancel := newCopyContext(ctx)
	writer := &DuckDataWriter{
		ctx:       ctx,
		execer:    execer,
		ownerConn: ownerConn,
		duckSQL:   builder.String(),
		options:   options,
		pipePath:  pipePath,
		cancel:    cancel,
		done:      make(chan struct{}),
		lease:     lease,
	}
	writer.ctx = copyCtx
	cleanup = false
	return writer, nil
}

func (dw *DuckDataWriter) Start(globalErr *atomic.Pointer[error]) (string, chan CopyToResult, error) {
	// Execute the COPY TO statement in a separate goroutine.
	dw.ensureDone()
	ch := make(chan CopyToResult, 1)
	dw.started.Store(true)
	dw.blocked.Store(true)
	go func() {
		defer close(dw.done)
		defer close(ch)

		dw.ctx.GetLogger().Tracef("Executing COPY TO statement: %s", dw.duckSQL)

		// This operation will block until the reader opens the pipe for reading.
		var result stdsql.Result
		var err error
		if dw.execer != nil {
			result, err = dw.execer.ExecContext(dw.ctx, dw.duckSQL)
		} else {
			result, err = adapter.ExecCatalog(dw.ctx, dw.duckSQL)
		}
		dw.blocked.Store(false)
		if err != nil {
			globalErr.Store(&err)
			ch <- CopyToResult{Err: err}
			return
		}
		affected, _ := result.RowsAffected()
		ch <- CopyToResult{RowCount: affected}
	}()

	return dw.pipePath, ch, nil
}

func (dw *DuckDataWriter) Close() {
	dw.ensureDone()
	dw.close.Do(func() {
		if dw.cancel != nil {
			dw.cancel()
		}
		if dw.blocked.Load() && dw.pipePath != "" {
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
		// The physical snapshot is no longer used once the producer and pipe
		// reader have both terminated. A registered operation lease remains
		// admitted until the surrounding PostgreSQL transaction finalizer.
		dw.lease.ReleaseSnapshot()
		if !dw.lease.OperationDeferred() {
			dw.lease.ReleaseOperation()
		}
	}
}

func (dw *DuckDataWriter) ensureDone() {
	if dw == nil {
		return
	}
	dw.doneOnce.Do(func() {
		if dw.done == nil {
			dw.done = make(chan struct{})
		}
	})
}
