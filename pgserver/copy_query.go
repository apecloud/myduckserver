package pgserver

import (
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/jackc/pgx/v5/pgconn"
)

// rejectCopyFromStdinTrailingStatements prevents a simple-query batch from
// entering COPY FROM STDIN mode while later SQL is still queued in the same
// Query message. The wire protocol delivers CopyData/CopyDone as separate
// messages, so executing the later statements here would run them before the
// COPY has completed. Custom COPY formats are not understood by the SQL
// parser; the lexical pass below covers those forms as well.
func rejectCopyFromStdinTrailingStatements(statements []ConvertedStatement, query string) error {
	copySeen := false
	for _, statement := range statements {
		// Parser adapters may preserve an empty statement for a trailing
		// separator. Empty queries are protocol no-ops and do not count as SQL
		// queued after COPY FROM STDIN.
		if isEmptyConvertedStatement(statement) {
			continue
		}
		if copySeen {
			return copyFromStdinTrailingStatementError()
		}
		copySeen = isCopyFromStdinStatement(statement)
	}

	if rawQueryHasTrailingCopyFromStdin(query) {
		return copyFromStdinTrailingStatementError()
	}
	return nil
}

func isCopyFromStdinStatement(statement ConvertedStatement) bool {
	if copyFrom, ok := statement.AST.(*tree.CopyFrom); ok {
		return copyFrom.Stdin
	}
	// COPY FORMAT PARQUET/JSON/ARROW is handled by the custom parser path and
	// therefore arrives here with a synthetic AST and a COPY tag.
	if !strings.EqualFold(statement.Tag, "COPY") {
		return false
	}
	_, _, _, ok := ParseCopyFrom(statement.String)
	return ok
}

// rawQueryHasTrailingCopyFromStdin handles custom COPY syntax for which
// convertQuery intentionally falls back to a synthetic statement after the
// PostgreSQL parser reports an unsupported format. SplitFirstStatement uses
// the parser's lexer, so semicolons inside quoted values/comments are not
// mistaken for statement boundaries.
func rawQueryHasTrailingCopyFromStdin(query string) bool {
	copySeen := false
	for len(query) > 0 {
		position, ok := parser.SplitFirstStatement(query)
		segment := query
		if ok {
			segment = query[:position]
			query = query[position:]
		} else {
			query = ""
		}

		// SplitFirstStatement includes the separator in each segment. A
		// trailing `;;` therefore produces a segment containing only `;`,
		// which is another separator rather than a queued statement. Strip
		// separators only at the segment boundary; quoted semicolons remain
		// untouched.
		segment = strings.TrimSpace(RemoveComments(segment))
		if strings.Trim(segment, "; \t\r\n") == "" {
			continue
		}
		if copySeen {
			return true
		}
		if _, _, _, ok := ParseCopyFrom(segment); ok {
			copySeen = true
		}
	}
	return false
}

func copyFromStdinTrailingStatementError() error {
	return &pgconn.PgError{
		Severity: "ERROR",
		Code:     "0A000",
		Message:  "COPY FROM STDIN must be the last statement in a simple query",
	}
}
