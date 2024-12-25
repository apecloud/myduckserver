package backend

import (
	"github.com/dolthub/vitess/go/sqltypes"
)

// ResultModifier transforms a Result.
type ResultModifier func(*sqltypes.Result) *sqltypes.Result
