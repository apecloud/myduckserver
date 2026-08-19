package main

import (
	"testing"

	"github.com/apecloud/myduckserver/backend"
	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
)

func TestIssue329CreateViewWithParens(t *testing.T) {
	raw := `CREATE OR REPLACE SQL SECURITY INVOKER VIEW v_paren AS (
    SELECT i FROM t329 WHERE i > 0
)`
	rewritten := backend.RewriteIncomingQuery(raw)

	script := queries.ScriptTest{
		Name: "CREATE VIEW AS (SELECT ...) from issue 329",
		SetUpScript: []string{
			"CREATE TABLE t329 (i BIGINT PRIMARY KEY)",
			"INSERT INTO t329 VALUES (1), (2)",
			rewritten,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT i FROM v_paren ORDER BY i",
				Expected: []sql.Row{
					{int64(1)},
					{int64(2)},
				},
			},
		},
	}
	enginetest.TestScript(t, NewDefaultDuckHarness(), script)
}

func TestIssue329SkipFunctionDDL(t *testing.T) {
	rewritten := backend.RewriteIncomingQuery("/*!50003 DROP FUNCTION IF EXISTS `toDate` */")
	script := queries.ScriptTest{
		Name: "DROP FUNCTION from replica dump is a no-op",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    rewritten,
				Expected: []sql.Row{{int64(1)}},
			},
		},
	}
	enginetest.TestScript(t, NewDefaultDuckHarness(), script)
}
