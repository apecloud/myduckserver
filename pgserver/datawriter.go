package pgserver

import (
	"fmt"
	"os"
	"strings"

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
	// Create the FIFO pipe
	db := handler.e.Analyzer.ExecBuilder.(*backend.DuckBuilder)
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
		if schema != "" {
			builder.WriteString(catalog.QuoteIdentifierANSI(schema))
			builder.WriteString(".")
		}
		builder.WriteString(catalog.QuoteIdentifierANSI(table.Name()))
		if columns != nil {
			builder.WriteString("(")
			builder.WriteString(columns.String())
			builder.WriteString(")")
		}
	} else {
		builder.WriteString("(")
		builder.WriteString(query)
		builder.WriteString(")")
	}

	builder.WriteString(" TO '")
	builder.WriteString(pipePath)

	switch options.CopyFormat {
	case tree.CopyFormatText, tree.CopyFormatCSV:
		builder.WriteString("' (FORMAT CSV")

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

	case tree.CopyFormatBinary:
		return nil, fmt.Errorf("BINARY format is not supported for COPY TO")
	}

	builder.WriteString(")")

	return &DataWriter{
		ctx:      ctx,
		duckSQL:  builder.String(),
		options:  options,
		pipePath: pipePath,
	}, nil
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
