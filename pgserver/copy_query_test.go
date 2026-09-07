package pgserver

import (
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestRejectCopyFromStdinTrailingStatements(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{name: "copy before select", query: "COPY rows FROM STDIN; SELECT 1"},
		{name: "copy between statements", query: "SELECT 1; COPY rows FROM STDIN; SELECT 2"},
		{name: "custom format", query: "COPY rows FROM STDIN (FORMAT PARQUET); SELECT 1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &ConnectionHandler{}
			statements, err := h.convertQuery(tt.query)
			require.NoError(t, err)

			err = rejectCopyFromStdinTrailingStatements(statements, tt.query)
			var pgErr *pgconn.PgError
			require.ErrorAs(t, err, &pgErr)
			require.Equal(t, "0A000", pgErr.Code)

			// The guard runs before statement execution and therefore cannot leave
			// the handler in COPY mode.
			end, err := h.handleQuery(&pgproto3.Query{String: tt.query})
			require.True(t, end)
			require.ErrorAs(t, err, &pgErr)
			require.Nil(t, h.copyFromStdinState)
		})
	}
}

func TestRawQueryCopyFromStdinTrailingStatementLexing(t *testing.T) {
	for _, query := range []string{
		"COPY rows FROM STDIN (FORMAT PARQUET, DELIMITER ';'); SELECT 1",
		"COPY rows FROM STDIN (FORMAT PARQUET, DELIMITER ';'); SELECT ';'",
	} {
		require.True(t, rawQueryHasTrailingCopyFromStdin(query), query)
	}

	for _, query := range []string{
		"COPY rows FROM STDIN;;",
		"SELECT 'COPY rows FROM STDIN; SELECT 1'",
		"COPY rows FROM 'input.csv'; SELECT 1",
	} {
		require.False(t, rawQueryHasTrailingCopyFromStdin(query), query)
	}
}

func TestRejectCopyFromStdinTrailingStatementsIgnoresEmptyConvertedStatements(t *testing.T) {
	copyStmt := ConvertedStatement{
		AST:    &tree.CopyFrom{Stdin: true},
		Tag:    "COPY",
		String: "COPY rows FROM STDIN",
	}
	for _, empty := range []ConvertedStatement{
		{},
		{String: ""},
		{String: ";"},
	} {
		require.NoError(t, rejectCopyFromStdinTrailingStatements([]ConvertedStatement{copyStmt, empty}, copyStmt.String))
	}

	require.Error(t, rejectCopyFromStdinTrailingStatements([]ConvertedStatement{
		copyStmt,
		{AST: &tree.Select{}, Tag: "SELECT", String: "SELECT 1"},
	}, "COPY rows FROM STDIN; SELECT 1"))
}
