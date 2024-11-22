package pgserver

import (
	"os"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/marcboeker/go-duckdb"
)

type ArrowWriter struct {
	ctx        *sql.Context
	duckSQL    string
	pipePath   string
	rawOptions string
}

func NewArrowWriter(
	ctx *sql.Context,
	handler *DuckHandler,
	schema string, table sql.Table, columns tree.NameList,
	query string,
	rawOptions string,
) (*ArrowWriter, error) {
	// Create the FIFO pipe
	db := handler.e.Analyzer.ExecBuilder.(*backend.DuckBuilder)
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
		if schema != "" {
			builder.WriteString(catalog.QuoteIdentifierANSI(schema))
			builder.WriteString(".")
		}
		builder.WriteString(catalog.QuoteIdentifierANSI(table.Name()))
		if columns != nil {
			builder.WriteString(" SELECT ")
			builder.WriteString(columns.String())
		}
	} else {
		builder.WriteString(query)
	}

	return &ArrowWriter{
		ctx:        ctx,
		duckSQL:    builder.String(),
		pipePath:   pipePath,
		rawOptions: rawOptions, // TODO(fan): parse rawOptions
	}, nil
}

func (dw *ArrowWriter) Start() (string, chan CopyToResult, error) {
	// Execute the statement in a separate goroutine.
	ch := make(chan CopyToResult, 1)
	go func() {
		defer os.Remove(dw.pipePath)
		defer close(ch)

		dw.ctx.GetLogger().Tracef("Executing statement via Arrow interface: %s", dw.duckSQL)
		conn, err := adapter.GetConn(dw.ctx)
		if err != nil {
			ch <- CopyToResult{Err: err}
			return
		}

		// Open the pipe for writing.
		// This operation will block until the reader opens the pipe for reading.
		pipe, err := os.OpenFile(dw.pipePath, os.O_WRONLY, os.ModeNamedPipe)
		if err != nil {
			ch <- CopyToResult{Err: err}
			return
		}
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
			ch <- CopyToResult{Err: err}
			return
		}

		ch <- CopyToResult{RowCount: rowCount}
	}()

	return dw.pipePath, ch, nil
}

func (dw *ArrowWriter) Close() {
	os.Remove(dw.pipePath)
}
