package main

import (
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestFormatPreservesNumericPrecision(t *testing.T) {
	h := NewDefaultDuckHarness()
	h.Setup(setup.MydbData, setup.MytableData)
	e, err := h.NewEngine(t)
	require.NoError(t, err)
	defer e.Close()

	for _, test := range []queries.QueryTest{
		{
			Query:    "SELECT FORMAT(9007199254740993, 0) FROM mytable LIMIT 1",
			Expected: []sql.Row{{"9,007,199,254,740,993"}},
		},
		{
			Query:    "SELECT FORMAT(CAST('1234567890123456789012345678.12' AS DECIMAL(30, 2)), 2) FROM mytable LIMIT 1",
			Expected: []sql.Row{{"1,234,567,890,123,456,789,012,345,678.12"}},
		},
	} {
		enginetest.TestQueryWithEngine(t, h, e, test)
	}
}
