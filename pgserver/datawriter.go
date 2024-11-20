package pgserver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/backend"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
)

type DataWriter struct {
	ctx      *sql.Context
	cancel   context.CancelFunc
	duckSQL  string
	options  *tree.CopyOptions
	pipePath string
}

func NewDataWriter(
	ctx *sql.Context,
	handler *DuckHandler,
	table sql.Table, tableName tree.TableName, columns tree.NameList,
	query string,
	options *tree.CopyOptions,
) (*DataWriter, error) {
	// https://www.postgresql.org/docs/current/sql-copy.html
	// https://duckdb.org/docs/sql/statements/copy.html#csv-options
	var format string
	switch options.CopyFormat {
	case tree.CopyFormatText:
		format = `FORMAT CSV, DELIMITER '\t', QUOTE '', ESCAPE '', NULLSTR '\N'`
	case tree.CopyFormatCSV:
		format = `FORMAT CSV`
	case tree.CopyFormatBinary:
		return nil, fmt.Errorf("BINARY format is not supported for COPY TO")
	}

	var source string
	if table != nil {
		source = tableName.FQString()
		if columns != nil {
			source += "(" + columns.String() + ")"
		}
	} else {
		source = "(" + query + ")"
	}

	duckBuilder := handler.e.Analyzer.ExecBuilder.(*backend.DuckBuilder)
	dataDir := duckBuilder.Provider().DataDir()

	// Create the FIFO pipe
	pipeDir := filepath.Join(dataDir, "pipes", "pg-copy-to")
	if err := os.MkdirAll(pipeDir, 0755); err != nil {
		return nil, err
	}
	pipeName := strconv.Itoa(int(ctx.ID())) + ".pipe"
	pipePath := filepath.Join(pipeDir, pipeName)
	ctx.GetLogger().Traceln("Creating FIFO pipe for COPY TO operation:", pipePath)
	if err := syscall.Mkfifo(pipePath, 0600); err != nil {
		return nil, err
	}

	// Create cancelable context
	childCtx, cancel := context.WithCancel(ctx)
	ctx.Context = childCtx

	// Initialize DataWriter
	writer := &DataWriter{
		ctx:      ctx,
		cancel:   cancel,
		duckSQL:  fmt.Sprintf("COPY %s TO '%s' (%s)", source, pipePath, format),
		options:  options,
		pipePath: pipePath,
		rowCount: make(chan int64, 1),
	}

	return writer, nil
}

type copyToResult struct {
	RowCount int64
	Err      error
}

func (dw *DataWriter) Start() (*os.File, chan int64, error) {
	// Open the pipe for reading.
	pipe, err := os.OpenFile(dw.pipePath, os.O_RDONLY, os.ModeNamedPipe)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open pipe for reading: %w", err)
	}

	go func() {
		defer dw.cancel()
		defer pipe.Close()
		defer os.Remove(dw.pipePath)
		defer close(dw.rowCount)
		// This operation will block until the reader opens the pipe for reading.
		result, err := adapter.ExecCatalog(dw.ctx, dw.duckSQL)
		if err != nil {
			dw.err.Store(&err)
			return
		}
		affected, _ := result.RowsAffected()
		dw.rowCount <- affected
	}()

	return pipe, dw.rowCount, nil
}

func (dw *DataWriter) Cancel() {
	dw.cancel()
}
