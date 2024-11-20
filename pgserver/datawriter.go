package pgserver

import (
	"fmt"
	"os"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
)

type DataWriter struct {
	ctx      *sql.Context
	duckSQL  string
	options  *tree.CopyOptions
	pipePath string
}

func NewDataWriter(
	ctx *sql.Context,
	handler *DuckHandler,
	schema string, table sql.Table, columns tree.NameList,
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
		if schema != "" {
			source += catalog.QuoteIdentifierANSI(schema) + "."
		}
		source += catalog.QuoteIdentifierANSI(table.Name())
		if columns != nil {
			source += "(" + columns.String() + ")"
		}
	} else {
		source = "(" + query + ")"
	}

	// Create the FIFO pipe
	db := handler.e.Analyzer.ExecBuilder.(*backend.DuckBuilder)
	pipePath, err := db.CreatePipe(ctx, "pg-copy-to")
	if err != nil {
		return nil, err
	}

	// Initialize DataWriter
	writer := &DataWriter{
		ctx:      ctx,
		duckSQL:  fmt.Sprintf("COPY %s TO '%s' (%s)", source, pipePath, format),
		options:  options,
		pipePath: pipePath,
	}

	return writer, nil
}

type copyToResult struct {
	RowCount int64
	Err      error
}

func (dw *DataWriter) Start() (string, chan copyToResult, error) {
	// Execute the COPY TO statement in a separate goroutine.
	ch := make(chan copyToResult, 1)
	go func() {
		defer os.Remove(dw.pipePath)
		defer close(ch)

		dw.ctx.GetLogger().Tracef("Executing COPY TO statement: %s", dw.duckSQL)

		// This operation will block until the reader opens the pipe for reading.
		result, err := adapter.ExecCatalog(dw.ctx, dw.duckSQL)
		if err != nil {
			ch <- copyToResult{Err: err}
			return
		}
		affected, _ := result.RowsAffected()
		ch <- copyToResult{RowCount: affected}
	}()

	return dw.pipePath, ch, nil
}

func (dw *DataWriter) Close() {
	os.Remove(dw.pipePath)
}
