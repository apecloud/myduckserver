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

type DataWriter interface {
	Start() (string, chan CopyToResult, error)
	Close()
}

type CopyToResult struct {
	RowCount int64
	Err      error
}

type DuckDataWriter struct {
	ctx      *sql.Context
	duckSQL  string
	options  *tree.CopyOptions
	pipePath string
}

func NewDuckDataWriter(
	ctx *sql.Context,
	handler *DuckHandler,
	schema string, table sql.Table, columns tree.NameList,
	query string,
	options *tree.CopyOptions, rawOptions string,
) (*DuckDataWriter, error) {
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
		// the parentheses have already been added
		builder.WriteString(query)
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

	return &DuckDataWriter{
		ctx:      ctx,
		duckSQL:  builder.String(),
		options:  options,
		pipePath: pipePath,
	}, nil
}

func (dw *DuckDataWriter) Start() (string, chan CopyToResult, error) {
	// Execute the COPY TO statement in a separate goroutine.
	ch := make(chan CopyToResult, 1)
	go func() {
		defer os.Remove(dw.pipePath)
		defer close(ch)

		dw.ctx.GetLogger().Tracef("Executing COPY TO statement: %s", dw.duckSQL)

		// This operation will block until the reader opens the pipe for reading.
		result, err := adapter.ExecCatalog(dw.ctx, dw.duckSQL)
		if err != nil {
			ch <- CopyToResult{Err: err}
			return
		}
		affected, _ := result.RowsAffected()
		ch <- CopyToResult{RowCount: affected}
	}()

	return dw.pipePath, ch, nil
}

func (dw *DuckDataWriter) Close() {
	os.Remove(dw.pipePath)
}
