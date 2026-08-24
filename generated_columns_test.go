package main

import (
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
)

type generatedColumnScriptSupport struct {
	name       string
	assertions []int
	skipScript bool
}

// MyDuck currently stores generated columns as ordinary DuckDB columns. Keep
// assertions that execute with MySQL-compatible results, and skip assertions
// that depend on generation, generated-column validation, or generated indexes.
var generatedColumnScriptSupportMatrix = []generatedColumnScriptSupport{
	{name: "stored generated column", assertions: []int{3, 4, 6, 11}},
	{name: "generated column with DEFAULT in UPDATE clause (issue #9438)", assertions: []int{3}},
	{name: "generated column with DEFAULT in VALUES clause (issue #9428)", assertions: []int{0, 2}},
	{name: "Add stored column first with literal"},
	{name: "Add stored column first with expression"},
	{name: "index on stored generated column", assertions: []int{0, 2, 6}},
	{name: "creating index on stored generated column", assertions: []int{0}},
	{name: "creating index on stored generated column with type conversion", assertions: []int{0}},
	{name: "creating index on stored generated column within multi-alter statement", assertions: []int{0}},
	{name: "creating unique index on stored generated column", assertions: []int{0}},
	{name: "creating index on virtual generated column", assertions: []int{0}},
	{name: "virtual column preceding primary key used in index", assertions: []int{1, 3, 4}},
	{name: "creating index on stored generated column with type conversion", assertions: []int{0}},
	{name: "creating index on virtual generated column with type conversion", assertions: []int{0}},
	{name: "index on stored generated column and one non-generated column", assertions: []int{0, 2, 4}},
	{name: "add new generated column", assertions: []int{0}},
	{name: "stored generated column with spaces", assertions: []int{0, 2, 5, 7}},
	{name: "virtual generated column with spaces", assertions: []int{0, 2, 5, 7}},
	{name: "Add virtual column first with literal"},
	{name: "Add virtual column first with expression"},
	{name: "virtual column inserts, updates, deletes", assertions: []int{0, 2, 4}},
	{name: "virtual column selects", assertions: []int{0, 2}},
	{name: "virtual column in triggers", assertions: []int{0, 1, 3}},
	{name: "virtual column json extract", assertions: []int{0}},
	{name: "virtual column with function", assertions: []int{0}},
	{name: "physical columns added after virtual one"},
	{name: "virtual column ordering", assertions: []int{0, 1, 3}},
	{name: "adding a virtual column", assertions: []int{0, 1}},
	{name: "creating index on virtual generated column", assertions: []int{0}},
	{name: "virtual column index", assertions: []int{1}},
	{name: "virtual column index survives DROP COLUMN on an unrelated column", assertions: []int{3}},
	{name: "virtual column index survives DROP COLUMN of a column between the generated column's dependencies"},
	{name: "virtual column index survives MODIFY COLUMN reordering a base column", assertions: []int{1}},
	{name: "virtual column index survives ADD PRIMARY KEY on an unrelated column"},
	{name: "virtual column index survives DROP PRIMARY KEY"},
	{name: "virtual column index survives ADD COLUMN on an unrelated column", assertions: []int{1}},
	{name: "unique virtual column index survives DROP COLUMN on an unrelated column", assertions: []int{6}},
	{name: "virtual column index survives DROP COLUMN on an unrelated column, keyless table", assertions: []int{3}},
	{name: "virtual column index survives MODIFY COLUMN reordering the generated column itself", assertions: []int{1}},
	{name: "virtual column index on a keyless table"},
	{name: "creating index on virtual generated column with type conversion", assertions: []int{0}},
	{name: "creating index on virtual generated column within multi-alter statement", assertions: []int{0}},
	{name: "creating unique index on virtual generated column", assertions: []int{0}},
	{name: "illegal table definitions", assertions: []int{0, 1}},
	{name: "generated columns in primary key"},
	{name: "can select all columns from table with generated column", assertions: []int{5, 6}},
	{name: "INSERT ON DUPLICATE KEY UPDATE with an index over a virtual generated column", skipScript: true},
	{name: "REPLACE INTO with an index over a virtual generated column", skipScript: true},
}

func generatedColumnTestsForMyDuck(t testing.TB) []queries.ScriptTest {
	t.Helper()

	if len(queries.GeneratedColumnTests) != len(generatedColumnScriptSupportMatrix) {
		t.Fatalf("generated column test matrix changed: got %d scripts, want %d", len(queries.GeneratedColumnTests), len(generatedColumnScriptSupportMatrix))
	}

	tests := append([]queries.ScriptTest(nil), queries.GeneratedColumnTests...)
	for scriptIndex := range tests {
		support := generatedColumnScriptSupportMatrix[scriptIndex]
		if tests[scriptIndex].Name != support.name {
			t.Fatalf("generated column test %d changed: got %q, want %q", scriptIndex, tests[scriptIndex].Name, support.name)
		}
		if support.skipScript {
			if len(support.assertions) != 0 {
				t.Fatalf("generated column script cannot have assertions and be skipped: %q", support.name)
			}
			tests[scriptIndex].Skip = true
			continue
		}

		tests[scriptIndex].Assertions = append([]queries.ScriptTestAssertion(nil), tests[scriptIndex].Assertions...)
		supported := make(map[int]struct{}, len(support.assertions))
		for _, assertionIndex := range support.assertions {
			if assertionIndex < 0 || assertionIndex >= len(tests[scriptIndex].Assertions) {
				t.Fatalf("generated column assertion index out of range: script=%q assertion=%d", support.name, assertionIndex)
			}
			supported[assertionIndex] = struct{}{}
		}

		for assertionIndex := range tests[scriptIndex].Assertions {
			if tests[scriptIndex].Assertions[assertionIndex].Skip {
				continue
			}
			if _, ok := supported[assertionIndex]; !ok {
				tests[scriptIndex].Assertions[assertionIndex].Skip = true
			}
		}
	}

	return tests
}

func testGeneratedColumns(t *testing.T, harness enginetest.Harness) {
	harness.Setup(setup.MydbData)
	for _, script := range generatedColumnTestsForMyDuck(t) {
		enginetest.TestScript(t, harness, script)
	}

	// Keep the upstream capability skips separate from MyDuck's compatibility matrix.
	for _, script := range queries.BrokenGeneratedColumnTests {
		t.Run(script.Name, func(t *testing.T) {
			t.Skip(script.Name)
		})
	}
}

func TestGeneratedColumnCompatibilityMatrix(t *testing.T) {
	tests := generatedColumnTestsForMyDuck(t)
	var pass, myDuckSkip, upstreamSkip, myDuckScriptSkip, upstreamScriptSkip int
	for scriptIndex, script := range tests {
		if script.Skip {
			if queries.GeneratedColumnTests[scriptIndex].Skip {
				upstreamScriptSkip++
			} else {
				myDuckScriptSkip++
			}
			continue
		}
		for assertionIndex, assertion := range script.Assertions {
			if !assertion.Skip {
				pass++
			} else if queries.GeneratedColumnTests[scriptIndex].Assertions[assertionIndex].Skip {
				upstreamSkip++
			} else {
				myDuckSkip++
			}
		}
	}

	if pass != 62 || myDuckSkip != 201 || upstreamSkip != 8 || myDuckScriptSkip != 2 || upstreamScriptSkip != 0 {
		t.Fatalf("unexpected generated column matrix: pass=%d myduck_skip=%d upstream_skip=%d myduck_script_skip=%d upstream_script_skip=%d", pass, myDuckSkip, upstreamSkip, myDuckScriptSkip, upstreamScriptSkip)
	}
}
