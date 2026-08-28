package backend

import (
	"context"
	"errors"
	"testing"

	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
)

func TestMySQLOkResultSchemaWrapperIsIdempotent(t *testing.T) {
	ctx := sql.NewEmptyContext()
	once := withMySQLOkResultSchema(plan.NewUnresolvedTable("t", "db"))
	twice := withMySQLOkResultSchema(once)

	require.Same(t, once, twice)
	require.Equal(t, types.OkResultSchema, twice.Schema(ctx))
}

func TestReplicationControllerCommandsBypassQueryFlush(t *testing.T) {
	ctx := sql.NewEmptyContext()
	deltaRows := 3
	position := "before"
	builder := &DuckBuilder{
		FlushDeltaBuffer: func(*sql.Context) error {
			deltaRows = 0
			position = "after"
			return nil
		},
	}

	for _, node := range []sql.Node{
		plan.NewChangeReplicationSource(nil),
		plan.NewChangeReplicationFilter(nil),
		plan.NewStartReplica(),
		plan.NewStopReplica(),
		plan.NewResetReplica(false),
		plan.NewShowReplicaStatus(),
	} {
		require.False(t, shouldFlushDeltaBuffer(node), "%T", node)
		require.NoError(t, builder.flushDeltaBuffer(ctx, node))
		require.Equal(t, 3, deltaRows, "%T must not commit delta", node)
		require.Equal(t, "before", position, "%T must not advance position", node)
	}

	ordinary := plan.NewEmptyTableWithSchema(nil)
	require.True(t, shouldFlushDeltaBuffer(ordinary))
	require.NoError(t, builder.flushDeltaBuffer(ctx, ordinary))
	require.Zero(t, deltaRows)
	require.Equal(t, "after", position)
}

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

func TestStartTransactionBindsUnderlyingDuckDBTransactionBeforeGMSStart(t *testing.T) {
	provider := catalog.NewInMemoryDBProvider()
	t.Cleanup(func() { require.NoError(t, provider.Close()) })

	session := NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))

	base := rowexec.NewBuilder(nil, sql.EngineOverrides{})
	builder := &DuckBuilder{base: base}
	iter, err := builder.Build(ctx, plan.NewStartTransaction(sql.ReadWrite), sql.Row{})
	require.NoError(t, err)
	require.NoError(t, iter.Close(ctx))
	require.True(t, ctx.GetIgnoreAutoCommit())
	require.NotNil(t, ctx.GetTransaction())
	require.NotNil(t, session.TryGetTxn())

	require.NoError(t, session.Rollback(ctx, ctx.GetTransaction()))
	ctx.SetTransaction(nil)
	ctx.SetIgnoreAutoCommit(false)
}

type startTransactionErrorBuilder struct{}

func (startTransactionErrorBuilder) Build(*sql.Context, sql.Node, sql.Row) (sql.RowIter, error) {
	return nil, errors.New("start transaction sentinel")
}

func TestStartTransactionRestoresIgnoreAutoCommitOnBuildError(t *testing.T) {
	ctx := sql.NewEmptyContext()
	builder := &DuckBuilder{base: startTransactionErrorBuilder{}}

	_, err := builder.Build(ctx, plan.NewStartTransaction(sql.ReadWrite), sql.Row{})
	require.EqualError(t, err, "start transaction sentinel")
	require.False(t, ctx.GetIgnoreAutoCommit())
}
