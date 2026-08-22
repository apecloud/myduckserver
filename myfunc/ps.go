package myfunc

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type PSCurrentThreadID struct {
	function.NoArgFunc
}

func (c PSCurrentThreadID) IsNonDeterministic() bool {
	return true
}

var _ sql.FunctionExpression = PSCurrentThreadID{}
var _ sql.CollationCoercible = PSCurrentThreadID{}

func NewPSCurrentThreadID() sql.Expression {
	return PSCurrentThreadID{
		NoArgFunc: function.NoArgFunc{Name: "ps_current_thread_id", SQLType: types.Uint64},
	}
}

// FunctionName implements sql.FunctionExpression
func (c PSCurrentThreadID) FunctionName() string {
	return "ps_current_thread_id"
}

// Description implements sql.FunctionExpression
func (c PSCurrentThreadID) Description() string {
	return "Returns a BIGINT UNSIGNED value representing the Performance Schema thread ID assigned to the current connection."
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (PSCurrentThreadID) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_utf8mb3_general_ci, 3
}

// Eval implements sql.Expression
func (c PSCurrentThreadID) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Golang discourages the use of thread ID or goroutine ID as they are not stable, so we use connection ID instead.
	return uint64(ctx.ID()), nil
}

// WithChildren implements sql.Expression
func (c PSCurrentThreadID) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return function.NoArgFuncWithChildren(c, children)
}
