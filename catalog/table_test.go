package catalog

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

type tableEditorTestTransaction struct{}

func (*tableEditorTestTransaction) String() string   { return "test transaction" }
func (*tableEditorTestTransaction) IsReadOnly() bool { return false }

type failingTableEditorIter struct {
	err error
}

func (i *failingTableEditorIter) Next(*sql.Context) (sql.Row, error) {
	return nil, i.err
}

func (*failingTableEditorIter) Close(*sql.Context) error { return nil }

func TestEmptyTableEditorStatementLifecycle(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tx := &tableEditorTestTransaction{}
	ctx.SetTransaction(tx)
	editor := &EmptyTableEditor{}

	require.NotPanics(t, func() {
		editor.StatementBegin(ctx)
		editor.StatementBegin(ctx)
	})
	require.NoError(t, editor.StatementComplete(ctx))
	require.NoError(t, editor.StatementComplete(ctx))

	statementErr := errors.New("statement failed")
	require.NoError(t, editor.DiscardChanges(ctx, statementErr))
	require.NoError(t, editor.DiscardChanges(ctx, statementErr))
	require.Same(t, tx, ctx.GetTransaction())

	iter := plan.NewTableEditorIter(&failingTableEditorIter{err: statementErr}, editor)
	_, err := iter.Next(ctx)
	require.ErrorIs(t, err, statementErr)
	require.ErrorIs(t, iter.Close(ctx), statementErr)
	require.Same(t, tx, ctx.GetTransaction())

	require.PanicsWithValue(t, "unimplemented", func() {
		_ = editor.Update(ctx, sql.Row{1}, sql.Row{2})
	})
}
