package backend

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestQueryForTranslationHonorsAnsiQuotes(t *testing.T) {
	ctx := sql.NewEmptyContext()
	require.NoError(t, ctx.SetSessionVariable(ctx, "sql_mode", "ANSI_QUOTES"))

	ctx = ctx.WithQuery(`select "data", '"' from auctions order by "ai" desc`)
	require.Equal(t, "select `data`, '\\\"' from auctions order by ai desc", queryForTranslation(ctx))
}

func TestQueryForTranslationLeavesDoubleQuotedStringsWhenAnsiQuotesDisabled(t *testing.T) {
	ctx := sql.NewEmptyContext()
	require.NoError(t, ctx.SetSessionVariable(ctx, "sql_mode", "NO_ENGINE_SUBSTITUTION"))

	query := `select "data" from auctions order by "ai" desc`
	ctx = ctx.WithQuery(query)
	require.Equal(t, query, queryForTranslation(ctx))
}
