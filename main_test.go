// Copyright 2024 ApeCloud, Inc.

// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/harness"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
	_ "github.com/dolthub/go-mysql-server/sql/variables"
	"github.com/dolthub/vitess/go/sqltypes"
)

var NewDuckHarness = harness.NewDuckHarness
var NewDefaultDuckHarness = harness.NewDefaultDuckHarness
var NewSkippingDuckHarness = harness.NewSkippingDuckHarness

const testNumPartitions = harness.TestNumPartitions

type indexBehaviorTestParams struct {
	name              string
	driverInitializer harness.IndexDriverInitializer
	nativeIndexes     bool
}

func TestDebugHarness(t *testing.T) {
	t.Skip("only used for debugging")

	harness := NewDuckHarness("debug", 1, 1, true, nil)

	setupData := []setup.SetupScript{{
		`create database if not exists mydb`,
		`use mydb`,
		`CREATE table xy (x int primary key, y int, unique index y_idx(y));`,
	}}

	harness.Setup(setupData)
	engine, err := harness.NewEngine(t)
	require.NoError(t, err)

	engine.EngineAnalyzer().Debug = true
	engine.EngineAnalyzer().Verbose = true

	ctx := enginetest.NewContext(harness)
	_, iter, _, err := engine.Query(ctx, "select * from xy where x in (select 1 having false);")
	require.NoError(t, err)
	defer iter.Close(ctx)

	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		fmt.Println(row)
	}
}

func TestIsPureDataQuery(t *testing.T) {
	harness := harness.NewDefaultDuckHarness()
	harness.Setup(
		setup.MydbData,
		[]setup.SetupScript{
			{
				"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))",
				"CREATE table xy (x int primary key, y int, unique index y_idx(y));",
				"CREATE view myview as select * from users",
			},
		})
	engine, err := harness.NewEngine(t)
	require.NoError(t, err)
	tests := []struct {
		name     string
		query    string
		expected bool
	}{
		{
			name:     "Simple SELECT query",
			query:    "SELECT * FROM users",
			expected: true,
		},
		{
			name:     "Query from mysql system table",
			query:    "SELECT * FROM mysql.user",
			expected: false,
		},
		{
			name:     "Query with system function",
			query:    "SELECT DATABASE()",
			expected: false,
		},
		{
			name:     "Query with subquery from system table",
			query:    "SELECT u.name, (SELECT COUNT(*) FROM mysql.user) FROM users u",
			expected: false,
		},
		{
			name:     "Query from information_schema",
			query:    "SELECT * FROM information_schema.tables",
			expected: false,
		},
		{
			name:     "Query with subquery",
			query:    "select * from xy where x in (select 1 having false);",
			expected: true,
		},
		{
			name:     "Date parse query",
			query:    "SELECT STR_TO_DATE('Jan 3, 2000', '%b %e, %Y')",
			expected: false,
		},
		{
			name:     "View query",
			query:    "SELECT * FROM myview WHERE id = 1",
			expected: true,
		},
	}
	for _, tt := range tests {
		ctx := enginetest.NewContext(harness)
		analyzed, err := engine.AnalyzeQuery(ctx, tt.query)
		require.NoError(t, err)
		result := backend.IsPureDataQuery(analyzed)
		require.Equal(t, tt.expected, result, "isPureDataQuery() for query '%s'", tt.query)
	}
}

var numPartitionsVals = []int{
	1,
	testNumPartitions,
}
var indexBehaviors = []*indexBehaviorTestParams{
	{"none", nil, false},
	{"mergableIndexes", mergableIndexDriver, false},
	{"nativeIndexes", nil, true},
	{"nativeAndMergable", mergableIndexDriver, true},
}
var parallelVals = []int{
	1,
	2,
}

// TestQueries tests the given queries on an engine under a variety of circumstances:
// 1) Partitioned tables / non partitioned tables
// 2) Mergeable / unmergeable / native / no indexes
// 3) Parallelism on / off
func TestQueries(t *testing.T) {
	t.Skip("wait for fix")
	for _, numPartitions := range numPartitionsVals {
		for _, indexBehavior := range indexBehaviors {
			for _, parallelism := range parallelVals {
				if parallelism == 1 && numPartitions == testNumPartitions && indexBehavior.name == "nativeIndexes" {
					// This case is covered by TestQueriesSimple
					continue
				}
				testName := fmt.Sprintf("partitions=%d,indexes=%v,parallelism=%v", numPartitions, indexBehavior.name, parallelism)
				harness := NewDuckHarness(testName, parallelism, numPartitions, indexBehavior.nativeIndexes, indexBehavior.driverInitializer)

				harness.SetupScriptsToSkip(
					setup.Fk_tblData, // Skip foreign key setup (not supported)
				)

				t.Run(testName, func(t *testing.T) {
					enginetest.TestQueries(t, harness)
				})
			}
		}
	}
}

// TestQueriesPreparedSimple runs the canonical test queries against a single threaded index enabled harness.
func TestQueriesPreparedSimple(t *testing.T) {
	t.Skip("wait for fix")
	harness := NewDefaultDuckHarness()
	if harness.IsUsingServer() {
		t.Skip("issue: https://github.com/dolthub/dolt/issues/6904 and https://github.com/dolthub/dolt/issues/6901")
	}
	enginetest.TestQueriesPrepared(t, harness)
}

// TestQueriesSimple runs the canonical test queries against a single threaded index enabled harness.
func TestQueriesSimple(t *testing.T) {
	harness := NewDefaultDuckHarness()

	notApplicableQueries := []string{
		"SELECT_*_FROM_mytable_t0_INNER_JOIN_mytable_t1_ON_(t1.i_IN_(((true)%(''))));",
		"SELECT_count(*)_from_mytable_WHERE_(i_IN_(-''));",
		"select_sum('abc')_from_mytable",

		// We have added performance_schema while Dolt does not have it.
		"SHOW_DATABASES",
		"SHOW_SCHEMAS",
		"SELECT_SCHEMA_NAME,_DEFAULT_CHARACTER_SET_NAME,_DEFAULT_COLLATION_NAME_FROM_information_schema.SCHEMATA",
	}

	// auto-generated by dev/extract_queries_to_skip.py
	waitForFixQueries := []string{
		"select_i+0.0/(lag(i)_over_(order_by_s))_from_mytable_order_by_1;",
		"select_f64/f32,_f32/(lag(i)_over_(order_by_f64))_from_floattable_order_by_1,2;",

		"select_pk,_________row_number()_over_(order_by_pk_desc),_________sum(v1)_over_(partition_by_v2_order_by_pk),_________percent_rank()_over(partition_by_v2_order_by_pk)_____from_one_pk_three_idx_order_by_pk",
		"select_pk,____________________percent_rank()_over(partition_by_v2_order_by_pk),____________________dense_rank()_over(partition_by_v2_order_by_pk),____________________rank()_over(partition_by_v2_order_by_pk)_____from_one_pk_three_idx_order_by_pk",
		"select_pk,_________first_value(pk)_over_(order_by_pk_desc),_________lag(pk,_1)_over_(order_by_pk_desc),_________count(pk)_over(partition_by_v1_order_by_pk),_________max(pk)_over(partition_by_v1_order_by_pk_desc),_________avg(v2)_over_(partition_by_v1_order_by_pk)_____from_one_pk_three_idx_order_by_pk",





		"select_i,_row_number()_over_(order_by_i_desc)_+_3,____row_number()_over_(order_by_length(s),i)_+_0.0_/_row_number()_over_(order_by_length(s)_desc,i_desc)_+_0.0____from_mytable_order_by_1;",
		"SELECT_pk,_row_number()_over_(partition_by_v2_order_by_pk_),_max(v3)_over_(partition_by_v2_order_by_pk)_FROM_one_pk_three_idx_ORDER_BY_pk",


		"_select_*_from_mytable,__lateral_(__with_recursive_cte(a)_as_(___select_y_from_xy___union___select_x_from_cte___join___(____select_*_____from_xy____where_x_=_1____)_sqa1___on_x_=_a___limit_3___)__select_*_from_cte_)_sqa2_where_i_=_a_order_by_i;",


	}

	// Order undefined
	undefinedOrderQueries := []string{
		// UNION ALL LIMIT OFFSET has no ORDER BY; scan order is not defined
		"_Select_x_from_(select_*_from_xy)_sq1_union_all_select_u_from_(select_*_from_uv)_sq2_limit_1_offset_1;",
		// partial order by, the order of remaining columns is undefined
		"select_distinct_pk1_from_two_pk_order_by_pk2",
		"select_distinct_pk2_from_two_pk_order_by_pk1",
		"select_distinct_pk1,_pk2_from_two_pk_order_by_pk1",
		"select_distinct_pk1,_pk2_from_two_pk_order_by_pk2",
	}

	// failed during CI
	waitForFixQueries = append(waitForFixQueries,
		// DuckDB TAN/COT results differ in the last bits across architectures.
		"SELECT_TAN(i)_from_mytable_order_by_i_limit_1",
		"SELECT_COT(i)_from_mytable_order_by_i_limit_1")

	panicQueries := []string{}

	harness.QueriesToSkip(notApplicableQueries...)
	harness.QueriesToSkip(undefinedOrderQueries...)
	harness.QueriesToSkip(waitForFixQueries...)
	harness.QueriesToSkip(panicQueries...)
	enginetest.TestQueries(t, harness)
}

// TestJoinQueries runs the canonical test queries against a single threaded index enabled harness.
func TestJoinQueries(t *testing.T) {
	harness := NewDefaultDuckHarness()
	harness.QueriesToSkip(
		// DuckDB does not allow LIMIT or OFFSET in a recursive CTE.
		"with recursive a(x,y) as (select i,i from mytable where i < 4 union select a.x, mytable.i from a join mytable on a.x+1 = mytable.i limit 2) select * from a;",
		// DuckDB requires explicit aliases when tables in different schemas share a base name.
		"select * from othertable join foo.othertable on othertable.s2 = 'third'",
		"select * from othertable join foo.othertable on mydb.othertable.s2 = 'third'",
		"select * from othertable join foo.othertable on foo.othertable.text = 'a'",
		"select * from foo.othertable join othertable on othertable.s2 = 'third'",
		"select * from foo.othertable join othertable on mydb.othertable.s2 = 'third'",
		"select * from foo.othertable join othertable on foo.othertable.text = 'a'",
		"select * from mydb.othertable join foo.othertable on othertable.s2 = 'third'",
		"select * from mydb.othertable join foo.othertable on mydb.othertable.s2 = 'third'",
		"select * from mydb.othertable join foo.othertable on foo.othertable.text = 'a'",
		"select * from foo.othertable join mydb.othertable on othertable.s2 = 'third'",
		"select * from foo.othertable join mydb.othertable on mydb.othertable.s2 = 'third'",
		"select * from foo.othertable join mydb.othertable on foo.othertable.text = 'a'",
		// DuckHarness tables do not support the script's foreign key setup.
		"Complex join query with foreign key constraints",
		// DuckDB orders SELECT * columns differently for these USING joins.
		"select * from t1 join t2 using (j);",
		"select * from t1 right join t2 using (i);",
		"select * from t1 right join t2 using (j);",
	)
	enginetest.TestJoinQueries(t, harness)
}

func TestLateralJoin(t *testing.T) {
	harness := NewDefaultDuckHarness()
	harness.QueriesToSkip(
		// DuckDB requires a single comparison between the left and right sides for
		// non-inner lateral joins, so it rejects this disjunctive join condition.
		"select * from t left join lateral (select * from t1 where t.i != t1.j) as tt on t.i + 1 = tt.j or t.i + 2 = tt.j order by t.i, tt.j",
	)
	enginetest.TestLateralJoinQueries(t, harness)
}

// TestJoinPlanning runs join-specific tests for merge
func TestJoinPlanning(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestJoinPlanning(t, NewDefaultDuckHarness())
}

// TestJoinOps runs join-specific tests for merge
func TestJoinOps(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestJoinOps(t, NewDefaultDuckHarness(), enginetest.DefaultJoinOpTests)
}

func TestJoinStats(t *testing.T) {
	t.Skip("wait for fix")
	harness := NewDefaultDuckHarness()
	if harness.IsUsingServer() {
		t.Skip("join stats don't work with bindvars")
	}
	enginetest.TestJoinStats(t, harness)
}

// TestJSONTableQueries runs the canonical test queries against a single threaded index enabled harness.
func TestJSONTableQueries(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestJSONTableQueries(t, NewDefaultDuckHarness())
}

// TestJSONTableScripts runs the canonical test queries against a single threaded index enabled harness.
func TestJSONTableScripts(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestJSONTableScripts(t, NewDefaultDuckHarness())
}

// TestBrokenJSONTableScripts runs the canonical test queries against a single threaded index enabled harness.
func TestBrokenJSONTableScripts(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestBrokenJSONTableScripts(t, enginetest.NewSkippingMemoryHarness())
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleQuery(t *testing.T) {
	t.Skip()
	test := queries.QueryTest{
		Query:    `select a.i,a.f, b.i2 from niltable a left join niltable b on a.i = b.i2 order by a.i`,
		Expected: []sql.Row{{1, nil, nil}, {2, nil, 2}, {3, nil, nil}, {4, 4.0, 4}, {5, 5.0, nil}, {6, 6.0, 6}},
	}

	// fmt.Sprintf("%v", test)
	harness := NewDuckHarness("", 1, testNumPartitions, true, nil)
	// harness.UseServer()
	harness.Setup(setup.MydbData, setup.NiltableData)
	engine, err := harness.NewEngine(t)
	require.NoError(t, err)

	engine.EngineAnalyzer().Debug = true
	engine.EngineAnalyzer().Verbose = true

	enginetest.TestQueryWithEngine(t, harness, engine, test)
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleQueryPrepared(t *testing.T) {
	t.Skip()
	var test = queries.ScriptTest{
		Name:        "renaming views with RENAME TABLE ... TO .. statement",
		SetUpScript: []string{},
		Assertions: []queries.ScriptTestAssertion{
			{
				// Original Issue: https://github.com/dolthub/dolt/issues/5714
				Query: `select 1.0/0.0 from dual`,

				Expected: []sql.Row{
					{4},
				},
			},
		},
	}

	// fmt.Sprintf("%v", test)
	harness := NewDuckHarness("", 1, testNumPartitions, false, nil)
	harness.Setup(setup.KeylessSetup...)
	engine, err := harness.NewEngine(t)
	if err != nil {
		panic(err)
	}

	engine.EngineAnalyzer().Debug = true
	engine.EngineAnalyzer().Verbose = true

	enginetest.TestScriptWithEnginePrepared(t, engine, harness, test)
}

// Convenience test for debugging a single query. Unskip and set to the desired query.
func TestSingleScript(t *testing.T) {
	t.Skip()
	var scripts = []queries.ScriptTest{
		{
			Name:        "test script",
			SetUpScript: []string{},
			Assertions:  []queries.ScriptTestAssertion{},
		},
	}

	for _, test := range scripts {
		harness := NewDuckHarness("", 1, testNumPartitions, true, nil)
		engine, err := harness.NewEngine(t)
		if err != nil {
			panic(err)
		}

		enginetest.TestScriptWithEngine(t, engine, harness, test)
	}
}

func TestUnbuildableIndex(t *testing.T) {
	var scripts = []queries.ScriptTest{
		{
			Name: "Failing index builder still returning correct results",
			SetUpScript: []string{
				"CREATE TABLE mytable2 (i BIGINT PRIMARY KEY, s VARCHAR(20))",
				"CREATE UNIQUE INDEX mytable2_s ON mytable2 (s)",
				fmt.Sprintf("CREATE INDEX mytable2_i_s ON mytable2 (i, s) COMMENT '%s'", memory.CommentPreventingIndexBuilding),
				"INSERT INTO mytable2 VALUES (1, 'first row'), (2, 'second row'), (3, 'third row')",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query: "SELECT i FROM mytable2 WHERE i IN (SELECT i FROM mytable2) ORDER BY i",
					Expected: []sql.Row{
						{1},
						{2},
						{3},
					},
				},
			},
		},
	}

	for _, test := range scripts {
		harness := NewDefaultDuckHarness()
		enginetest.TestScript(t, harness, test)
	}
}

func TestBrokenQueries(t *testing.T) {
	enginetest.TestBrokenQueries(t, NewSkippingDuckHarness())
}

func TestQueryPlanTODOs(t *testing.T) {
	harness := NewSkippingDuckHarness()
	harness.Setup(setup.MydbData, setup.Pk_tablesData, setup.NiltableData)
	e, err := harness.NewEngine(t)
	if err != nil {
		log.Fatal(err)
	}
	for _, tt := range queries.QueryPlanTODOs {
		t.Run(tt.Query, func(t *testing.T) {
			enginetest.TestQueryPlan(t, harness, e, tt)
		})
	}
}

// TODO: implement support for versioned queries
// func TestVersionedQueries(t *testing.T) {
// 	for _, numPartitions := range numPartitionsVals {
// 		for _, indexInit := range indexBehaviors {
// 			for _, parallelism := range parallelVals {
// 				testName := fmt.Sprintf("partitions=%d,indexes=%v,parallelism=%v", numPartitions, indexInit.name, parallelism)
// 				harness := NewMetaHarness(testName, parallelism, numPartitions, indexInit.nativeIndexes, indexInit.driverInitializer)

// 				t.Run(testName, func(t *testing.T) {
// 					enginetest.TestVersionedQueries(t, harness)
// 				})
// 			}
// 		}
// 	}
// }

func TestAnsiQuotesSqlMode(t *testing.T) {
	harness := NewDefaultDuckHarness()
	ansiQuotesTests := append([]queries.ScriptTest(nil), queries.AnsiQuotesTests...)
	ansiQuotesTests[0].Assertions = append([]queries.ScriptTestAssertion(nil), ansiQuotesTests[0].Assertions...)
	ansiQuotesTests[0].Assertions[0].Query = `select  "data" from auctions order by "ai" desc;`
	harness.QueriesToSkip(
		// MyDuck's query path does not honor session ANSI_QUOTES mode, so double-quoted
		// expressions are treated as string literals instead of identifiers. Extra whitespace
		// distinguishes this mode-on assertion from the identical mode-off query below.
		`select  "data" from auctions order by "ai" desc;`,
		`select "data", '"' from auctions order by "ai";`,
		`select "data", '\"' from auctions order by "ai";`,
		`insert into t ("pk", "data") values (1, 'one');`,
		`select "pk", "data" from "t" order by "pk" asc;`,
		`insert into public_keys("item", "type", "hash", "count", "public") values (42, 'type', 1010, 1, 'public');`,
		`select "public", "count" from view1;`,
		// The skipped quoted-column INSERT leaves the view empty.
		"select public, `count` from view1;",
		// DuckDB does not provide MySQL triggers, stored procedures, or events.
		"ANSI_QUOTES: triggers",
		"ANSI_QUOTES: stored procedures",
		"ANSI_QUOTES: events",
		// DuckDB rejects a DEFAULT expression that refers to another column.
		"ANSI_QUOTES: column defaults",
	)
	for _, script := range ansiQuotesTests {
		enginetest.TestScript(t, harness, script)
	}
}

func TestAnsiQuotesSqlModePrepared(t *testing.T) {
	harness := NewDefaultDuckHarness()
	if harness.IsUsingServer() {
		t.Skip("prepared test depend on context for current sql_mode information, but it does not get updated when using ServerEngine")
	}

	ansiQuotesTests := append([]queries.ScriptTest(nil), queries.AnsiQuotesTests...)
	for i := range ansiQuotesTests {
		script := &ansiQuotesTests[i]
		switch script.Name {
		case "ANSI_QUOTES: triggers", "ANSI_QUOTES: stored procedures", "ANSI_QUOTES: events":
			// DuckDB does not provide these MySQL server-side objects.
			script.SkipPrepared = true
		case "ANSI_QUOTES: column defaults":
			// DuckDB rejects DEFAULT expressions that refer to another column.
			script.SkipPrepared = true
		}
	}

	for _, script := range ansiQuotesTests {
		enginetest.TestScriptPrepared(t, harness, script)
	}
}

func TestAnsiQuotesSqlModeExecution(t *testing.T) {
	test := queries.ScriptTest{
		Name: "ANSI_QUOTES: ordinary execution uses the session mode",
		SetUpScript: []string{
			"create table auctions (ai int primary key, data varchar(100));",
			"insert into auctions values (1, 'forty-two');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    `select "data" from auctions order by "ai" desc;`,
				Expected: []sql.Row{{"data"}},
			},
			{
				Query:    "SET @@sql_mode='ANSI_QUOTES';",
				Expected: []sql.Row{{}},
			},
			{
				Query:    `select "data" from auctions order by "ai" desc;`,
				Expected: []sql.Row{{"forty-two"}},
			},
			{
				Query:    "SET @@sql_mode='NO_ENGINE_SUBSTITUTION';",
				Expected: []sql.Row{{}},
			},
			{
				Query:    `select "data" from auctions order by "ai" desc;`,
				Expected: []sql.Row{{"data"}},
			},
		},
	}

	enginetest.TestScript(t, NewDefaultDuckHarness(), test)
}

// Tests of choosing the correct execution plan independent of result correctness. Mostly useful for confirming that
// the right indexes are being used for joining tables.
func TestQueryPlans(t *testing.T) {
	t.Skip("myduckserver has different query plans")
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
		{"nativeAndMergable", mergableIndexDriver, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := NewDuckHarness(indexInit.name, 1, 2, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestQueryPlans(t, harness, queries.PlanTests)
		})
	}
}

func TestSingleQueryPlan(t *testing.T) {
	t.Skip()
	tt := []queries.QueryPlanTest{
		{
			Query: `SELECT mytable.i, selfjoin.i FROM mytable INNER JOIN mytable selfjoin ON mytable.i = selfjoin.i WHERE selfjoin.i IN (SELECT 1 FROM DUAL)`,
			ExpectedPlan: "Project\n" +
				" ├─ columns: [mytable.i:0!null, selfjoin.i:2!null]\n" +
				" └─ SemiJoin\n" +
				"     ├─ MergeJoin\n" +
				"     │   ├─ cmp: Eq\n" +
				"     │   │   ├─ mytable.i:0!null\n" +
				"     │   │   └─ selfjoin.i:2!null\n" +
				"     │   ├─ IndexedTableAccess(mytable)\n" +
				"     │   │   ├─ index: [mytable.i,mytable.s]\n" +
				"     │   │   ├─ static: [{[NULL, ∞), [NULL, ∞)}]\n" +
				"     │   │   ├─ colSet: (1,2)\n" +
				"     │   │   ├─ tableId: 1\n" +
				"     │   │   └─ Table\n" +
				"     │   │       ├─ name: mytable\n" +
				"     │   │       └─ columns: [i s]\n" +
				"     │   └─ Filter\n" +
				"     │       ├─ Eq\n" +
				"     │       │   ├─ selfjoin.i:0!null\n" +
				"     │       │   └─ 1 (tinyint)\n" +
				"     │       └─ TableAlias(selfjoin)\n" +
				"     │           └─ IndexedTableAccess(mytable)\n" +
				"     │               ├─ index: [mytable.i]\n" +
				"     │               ├─ static: [{[1, 1]}]\n" +
				"     │               ├─ colSet: (3,4)\n" +
				"     │               ├─ tableId: 2\n" +
				"     │               └─ Table\n" +
				"     │                   ├─ name: mytable\n" +
				"     │                   └─ columns: [i s]\n" +
				"     └─ Project\n" +
				"         ├─ columns: [1 (tinyint)]\n" +
				"         └─ ProcessTable\n" +
				"             └─ Table\n" +
				"                 ├─ name: \n" +
				"                 └─ columns: []\n" +
				"",
			ExpectedEstimates: "Project\n" +
				" ├─ columns: [mytable.i, selfjoin.i]\n" +
				" └─ SemiJoin (estimated cost=4.515 rows=1)\n" +
				"     ├─ MergeJoin (estimated cost=6.090 rows=3)\n" +
				"     │   ├─ cmp: (mytable.i = selfjoin.i)\n" +
				"     │   ├─ IndexedTableAccess(mytable)\n" +
				"     │   │   ├─ index: [mytable.i,mytable.s]\n" +
				"     │   │   └─ filters: [{[NULL, ∞), [NULL, ∞)}]\n" +
				"     │   └─ Filter\n" +
				"     │       ├─ (selfjoin.i = 1)\n" +
				"     │       └─ TableAlias(selfjoin)\n" +
				"     │           └─ IndexedTableAccess(mytable)\n" +
				"     │               ├─ index: [mytable.i]\n" +
				"     │               └─ filters: [{[1, 1]}]\n" +
				"     └─ Project\n" +
				"         ├─ columns: [1]\n" +
				"         └─ Table\n" +
				"             └─ name: \n" +
				"",
			ExpectedAnalysis: "Project\n" +
				" ├─ columns: [mytable.i, selfjoin.i]\n" +
				" └─ SemiJoin (estimated cost=4.515 rows=1) (actual rows=1 loops=1)\n" +
				"     ├─ MergeJoin (estimated cost=6.090 rows=3) (actual rows=1 loops=1)\n" +
				"     │   ├─ cmp: (mytable.i = selfjoin.i)\n" +
				"     │   ├─ IndexedTableAccess(mytable)\n" +
				"     │   │   ├─ index: [mytable.i,mytable.s]\n" +
				"     │   │   └─ filters: [{[NULL, ∞), [NULL, ∞)}]\n" +
				"     │   └─ Filter\n" +
				"     │       ├─ (selfjoin.i = 1)\n" +
				"     │       └─ TableAlias(selfjoin)\n" +
				"     │           └─ IndexedTableAccess(mytable)\n" +
				"     │               ├─ index: [mytable.i]\n" +
				"     │               └─ filters: [{[1, 1]}]\n" +
				"     └─ Project\n" +
				"         ├─ columns: [1]\n" +
				"         └─ Table\n" +
				"             └─ name: \n" +
				"",
		},
	}

	harness := NewDuckHarness("nativeIndexes", 1, 2, true, nil)
	harness.Setup(setup.PlanSetup...)

	for _, test := range tt {
		t.Run(test.Query, func(t *testing.T) {
			engine, err := harness.NewEngine(t)
			engine.EngineAnalyzer().Verbose = true
			engine.EngineAnalyzer().Debug = true

			require.NoError(t, err)
			enginetest.TestQueryPlan(t, harness, engine, test)
		})
	}
}

func TestIntegrationQueryPlans(t *testing.T) {
	t.Skip("myduckserver has different query plans")
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := NewDuckHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestIntegrationPlans(t, harness)
		})
	}
}

func TestImdbQueryPlans(t *testing.T) {
	t.Skip("myduckserver has different query plans")
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := NewDuckHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestImdbPlans(t, harness)
		})
	}
}

func TestTpccQueryPlans(t *testing.T) {
	t.Skip("myduckserver has different query plans")
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := NewDuckHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestTpccPlans(t, harness)
		})
	}
}

func TestTpchQueryPlans(t *testing.T) {
	t.Skip("myduckserver has different query plans")
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := NewDuckHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestTpchPlans(t, harness)
		})
	}
}

func TestTpcdsQueryPlans(t *testing.T) {
	t.Skip("myduckserver has different query plans")
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := NewDuckHarness(indexInit.name, 1, 1, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestTpcdsPlans(t, harness)
		})
	}
}

func TestIndexQueryPlans(t *testing.T) {
	t.Skip("myduckserver has different query plans")
	indexBehaviors := []*indexBehaviorTestParams{
		{"nativeIndexes", nil, true},
		{"nativeAndMergable", mergableIndexDriver, true},
	}

	for _, indexInit := range indexBehaviors {
		t.Run(indexInit.name, func(t *testing.T) {
			harness := NewDuckHarness(indexInit.name, 1, 2, indexInit.nativeIndexes, indexInit.driverInitializer)
			enginetest.TestIndexQueryPlans(t, harness)
		})
	}
}

func TestQueryErrors(t *testing.T) {
	harness := NewDefaultDuckHarness()
	harness.QueriesToSkip(
		// DuckDB rejects this malformed regexp, but returns its native error text.
		`SELECT * FROM mytable WHERE s REGEXP("*main.go")`,
		// Unbound placeholders reach DuckDB and produce an argument-count error instead
		// of go-mysql-server's ErrUnboundPreparedStatementVariable.
		"SELECT pk FROM one_pk WHERE pk > ?",
		"SELECT pk FROM one_pk WHERE pk > :pk",
		// DuckDB rejects multi-character ESCAPE strings as syntax errors rather than
		// go-mysql-server's ErrInvalidArgument.
		`SELECT name FROM specialtable t WHERE t.name LIKE '$%' ESCAPE 'abc'`,
		`SELECT name FROM specialtable t WHERE t.name LIKE '$%' ESCAPE '$$'`,
	)
	enginetest.TestQueryErrors(t, harness)
}

func TestInfoSchema(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestInfoSchema(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestMySqlDb(t *testing.T) {
	enginetest.TestMySqlDb(t, NewDefaultDuckHarness())
}

// func TestReadOnlyDatabases(t *testing.T) {
// 	enginetest.TestReadOnlyDatabases(t, NewReadOnlyMetaHarness())
// }

// func TestReadOnlyVersionedQueries(t *testing.T) {
// 	enginetest.TestReadOnlyVersionedQueries(t, NewReadOnlyMetaHarness())
// }

func TestColumnAliases(t *testing.T) {
	harness := NewDefaultDuckHarness()
	harness.QueriesToSkip(
		// GROUP BY alias resolution needs the input schema to prefer a real column with the same name.
		"SELECT s as COL1, SUM(i) COL2 FROM mytable group by col1 order by col2",
		// DuckDB treats HAVING as an aggregate filter and rejects MySQL's row-level alias form.
		"select t1.u as a from uv as t1 having a > 0 order by a;",
		"select t1.u as a from uv as t1 having a = t1.u order by a;",
		"select t1.i as a from mytable as t1 having a = t1.i;",
		// The compatibility rewrite cannot aggregate a derived-table star as one expression.
		"select 1 as a, one + 1 as mod1, dt.* from mytable as t1, (select 1, 2 from mytable) as dt (one, two) where dt.one > 0 group by one;",
	)
	enginetest.TestColumnAliases(t, harness)
}

func TestDerivedTableOuterScopeVisibility(t *testing.T) {
	harness := NewDefaultDuckHarness()
	harness.QueriesToSkip(
		// DuckDB treats HAVING as an aggregate filter and rejects these MySQL row-level filters.
		"SELECT * FROM t1 HAVING t1.d > (SELECT dt.a FROM (SELECT t2.a AS a FROM t2 WHERE t2.b = t1.b) dt);",
		"SELECT val, row_number() over (partition by val) as 'row_number', (SELECT two from (SELECT val*2, val*3) as dt(one, two)) as a1 from numbers having a1 > 10;",
		"SELECT DISTINCT numbers.val, (WITH cte1 AS (SELECT val * 2 as val2 from numbers) SELECT count(*) from cte1 where numbers.val = cte1.val2) as count from numbers having count > 0;",
		// DuckHarness tables reject the setup's FOREIGN KEY creation as unsupported.
		"https://github.com/dolthub/go-mysql-server/issues/1282",
	)
	enginetest.TestDerivedTableOuterScopeVisibility(t, harness)
}

func TestOrderByGroupBy(t *testing.T) {
	harness := NewDefaultDuckHarness()
	harness.QueriesToSkip(
		// DuckDB cannot cast BIGINT to BLOB for MySQL's BINARY expression.
		"SELECT DISTINCT BINARY t1.id as id FROM members AS t1 JOIN members AS t2 ON t1.id = t2.id WHERE t1.id > 0 ORDER BY BINARY t1.id",
		"SELECT DISTINCT BINARY t1.id as id FROM members AS t1 JOIN members AS t2 ON t1.id = t2.id WHERE t1.id > 0 ORDER BY t1.id",
		// The projected alias is not visible inside DuckDB's grouped correlated subquery.
		"SELECT id as alias1, (SELECT alias1+1 group by alias1 having alias1 > 0) FROM members where id < 6;",
		// DuckDB requires ORDER BY to use the same grouped BLOB expression.
		"select binary s from t group by binary s order by s",
		// MySQL ANY_VALUE suppresses grouping checks; DuckDB implements it as an aggregate.
		"select any_value(id), any_value(team) from members order by id",
	)
	for _, script := range queries.OrderByGroupByScriptTests {
		enginetest.TestScript(t, harness, script)
	}
	t.Run("non-deterministic group by", func(t *testing.T) {
		t.Skip("DuckDB ANY_VALUE and non-strict GROUP BY semantics differ from MySQL")
	})
}

func TestAmbiguousColumnResolution(t *testing.T) {
	enginetest.TestAmbiguousColumnResolution(t, NewDefaultDuckHarness())
}

func TestInsertInto(t *testing.T) {
	harness := NewDefaultDuckHarness()
	// First enablement of #64: run the suite, skip cases that still need
	// MySQL-identical errors, ON DUPLICATE KEY, AUTO_INCREMENT, or FK.
	harness.QueriesToSkip(
		// insert_queries
		"INSERT_INTO_auto_increment_tbl_VALUES_('4',_44)",
		"INSERT_INTO_auto_increment_tbl_values_(0,_44)",
		"INSERT_INTO_auto_increment_tbl_values_(5,_44)",
		"INSERT_INTO_auto_increment_tbl_values_(NULL,_44),_(NULL,_55),_(9,_99),_(NULL,_110),_(NULL,_121)",
		"INSERT_INTO_keyless_()_VALUES_();",
		"INSERT_INTO_keyless_VALUES_();",
		"INSERT_INTO_mytable_(i,s)_values_(1,'hi')_ON_DUPLICATE_KEY_UPDATE_s=VALUES(s)",
		"INSERT_INTO_mytable_(i,s)_values_(1,'mar'),_(2,'par')_ON_DUPLICATE_KEY_UPDATE_s=CONCAT(VALUES(s),_'tial')",
		"INSERT_INTO_mytable_(i,s)_values_(1,'maybe')_ON_DUPLICATE_KEY_UPDATE_i=VALUES(i)+8000,_s=VALUES(s)",
		"INSERT_INTO_mytable_(i,s)_values_(1,_'hello')_ON_DUPLICATE_KEY_UPDATE_i=10",
		"INSERT_INTO_mytable_(i,s)_values_(1,_'hello')_ON_DUPLICATE_KEY_UPDATE_s='hello'",
		"INSERT_INTO_mytable_(i,s)_values_(1,_'hello2'),_(2,_'hello3'),_(4,_'no_conflict')_ON_DUPLICATE_KEY_UPDATE_s='hello4'",
		"INSERT_INTO_mytable_(i,s)_values_(1,_'hello2')_ON_DUPLICATE_KEY_UPDATE_s='hello3'",
		"INSERT_INTO_mytable_(i,s)_values_(1,_'hello2')_ON_DUPLICATE_KEY_UPDATE_s='hello3'#01",
		"INSERT_INTO_mytable_(i,s)_values_(10,_'hello')_ON_DUPLICATE_KEY_UPDATE_s='hello'",
		"INSERT_INTO_mytable_(s,i)_values_('dup',1)_ON_DUPLICATE_KEY_UPDATE_s=CONCAT(VALUES(s),_'licate')",

		"INSERT_INTO_othertable_VALUES_(\"fourth\",_1)_ON_DUPLICATE_KEY_UPDATE_s2=\"fourth\"",
		"INSERT_INTO_typestable_SET____id_=_999,_i8_=_-128,_i16_=_-32768,_i32_=_-2147483648,_i64_=_-9223372036854775808,____u8_=_0,_u16_=_0,_u32_=_0,_u64_=_0,____f32_=_1.401298464324817070923729583289916131280e-45,_f64_=_4.940656458412465441765687928682213723651e-324,____ti_=_'0000-00-00_00:00:00',_da_=_'0000-00-00',____te_=_'',_bo_=_false,_js_=_'\"\"',_bl_=_'',_e1_=_'v1',_s1_=_'v2'____;",
		"INSERT_INTO_typestable_SET____id_=_999,_i8_=_-128,_i16_=_-32768,_i32_=_-2147483648,_i64_=_-9223372036854775808,____u8_=_0,_u16_=_0,_u32_=_0,_u64_=_0,____f32_=_1.401298464324817070923729583289916131280e-45,_f64_=_4.940656458412465441765687928682213723651e-324,____ti_=_'2037-04-05_12:51:36_-0000_UTC',_da_=_'0000-00-00',____te_=_'',_bo_=_false,_js_=_'\"\"',_bl_=_'',_e1_=_'v1',_s1_=_'v2'____;",
		"INSERT_INTO_typestable_VALUES_(____999,_-128,_-32768,_-2147483648,_-9223372036854775808,____0,_0,_0,_0,____1.401298464324817070923729583289916131280e-45,_4.940656458412465441765687928682213723651e-324,____'0000-00-00_00:00:00',_'0000-00-00',____'',_false,_'\"\"',_'',_'',_''____);",
		"with_recursive_t_(i,f)_as_(select_4,4_from_dual_union_all_select_i_+_1,_i_+_1_from_t_where_i_<_5)_insert_into_mytable_select_i,f_from_t",
		// insert_scripts (skipped by script name)
		"Explicit_default_with_column_reference",
		"INSERT_Accumulator_tests",
		"INSERT_IGNORE_works_with_FK_Violations",
		"INSERT_INTO_..._SELECT_with_TEXT_types",
		"Insert_into_unique_key_that_overlaps_with_primary_key",
		"Insert_on_duplicate_key",
		"Insert_on_duplicate_key_references_table_in_aliased_subquery",
		"Insert_on_duplicate_key_references_table_in_cte",
		"Insert_on_duplicate_key_references_table_in_subquery",
		"Insert_on_duplicate_key_references_table_in_subquery_with_join",
		"Insert_throws_primary_key_violations",
		"Insert_throws_unique_key_violations",
		"Insert_throws_unique_key_violations_for_keyless_tables",
		"Try_INSERT_IGNORE_with_primary_key,_non_null,_and_single_row_violations",
		"auto_increment_on_bigint",
		"auto_increment_on_bigint_unsigned",
		"auto_increment_on_double",
		"auto_increment_on_float",
		"auto_increment_on_int",
		"auto_increment_on_int_unsigned",
		"auto_increment_on_mediumint",
		"auto_increment_on_mediumint_unsigned",
		"auto_increment_on_smallint",
		"auto_increment_on_smallint_unsigned",
		"auto_increment_on_tinyint",
		"auto_increment_on_tinyint_unsigned",
		"auto_increment_table_handles_deletes",
		"explicit_DEFAULT",
		"insert_duplicate_key_doesn't_prevent_other_updates",
		"insert_duplicate_key_doesn't_prevent_other_updates,_autocommit_off",
		"insert_into_sparse_auto_increment_table",
		"insert_negative_values_into_auto_increment_values",
		"issue_6675:_on_duplicate_rearranged_getfield_indexes_from_select_source",
		"issue_7322:_values_expression_is_subquery",
	)
	enginetest.TestInsertInto(t, harness)
}

func TestInsertIgnoreInto(t *testing.T) {
	harness := NewDefaultDuckHarness()
	harness.QueriesToSkip(
		// DuckDB does not coerce invalid or NULL values and report warnings like MySQL.
		"Test that INSERT IGNORE with Non nullable columns works",
		"Test that INSERT IGNORE properly addresses data conversion",
		"Insert Ignore works correctly with ON DUPLICATE UPDATE",
		// The rows are correct, but DuckDB reports zero affected rows when conflicts are ignored.
		"Test that INSERT IGNORE INTO works with unique keys",
	)
	enginetest.TestInsertIgnoreInto(t, harness)
}

func TestInsertDuplicateKeyKeyless(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestInsertDuplicateKeyKeyless(t, NewDefaultDuckHarness())
}

func TestIgnoreIntoWithDuplicateUniqueKeyKeyless(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestIgnoreIntoWithDuplicateUniqueKeyKeyless(t, NewDefaultDuckHarness())
}

func TestInsertIntoErrors(t *testing.T) {
	harness := NewDefaultDuckHarness()
	harness.QueriesToSkip(
		// DuckDB treats VARCHAR and VARBINARY lengths as advisory.
		"insert into bad values ('1234567890')",
		"insert into bad values (repeat('0', 65536))",
	)
	enginetest.TestInsertIntoErrors(t, harness)
}

func TestBrokenInsertScripts(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestBrokenInsertScripts(t, NewDefaultDuckHarness())
}

func TestGeneratedColumns(t *testing.T) {
	testGeneratedColumns(t, NewDefaultDuckHarness())
}

func TestGeneratedColumnPlans(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestGeneratedColumnPlans(t, NewDefaultDuckHarness())
}

func TestSysbenchPlans(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestSysbenchPlans(t, NewDefaultDuckHarness())
}

func TestStatistics(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestStatistics(t, NewDefaultDuckHarness())
}

func TestStatisticIndexFilters(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestStatisticIndexFilters(t, NewDefaultDuckHarness())
}

func TestSpatialInsertInto(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestSpatialInsertInto(t, NewDefaultDuckHarness())
}

func TestLoadData(t *testing.T) {
	harness := NewDefaultDuckHarness()
	harness.QueriesToSkip(
		"create table loadtable(pk int primary key, check (pk > 1))",
		"CREATE TABLE test1 (pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2 * 10), v2 BIGINT DEFAULT 5);",
		"CREATE TABLE test1 (pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2 * 10), v2 BIGINT DEFAULT 5);",
		"CREATE TABLE inmate_population_snapshots (id char(21) NOT NULL, snapshot_date date NOT NULL, total int,"+
			"total_off_site int, male int, female int, other_gender int, white int, black int, hispanic int,"+
			"asian int, american_indian int, mexican_american int, multi_racial int, other_race int,"+
			"on_probation int, on_parole int, felony int, misdemeanor int, other_offense int,"+
			"convicted_or_sentenced int, detained_or_awaiting_trial int, first_time_incarcerated int, employed int,"+
			"unemployed int, citizen int, noncitizen int, juvenile int, juvenile_male int, juvenile_female int,"+
			"death_row_condemned int, solitary_confinement int, technical_parole_violators int,"+
			"source_url varchar(2043) NOT NULL, source_url_2 varchar(2043), civil_offense int, federal_offense int,"+
			"PRIMARY KEY (id,snapshot_date), KEY id (id));", // load empty string into int column should result in 0
	)
	enginetest.TestLoadData(t, harness)
}

func TestLoadDataErrors(t *testing.T) {
	harness := NewDefaultDuckHarness()
	harness.QueriesToSkip(
		"create table loadtable(pk int primary key, c1 varchar(10))",
	)
	enginetest.TestLoadDataErrors(t, harness)
}

func TestLoadDataFailing(t *testing.T) {
	harness := NewDefaultDuckHarness()
	enginetest.TestLoadDataFailing(t, harness)
}

func TestSelectIntoFile(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestSelectIntoFile(t, NewDefaultDuckHarness())
}

func TestReplaceInto(t *testing.T) {
	harness := NewDefaultDuckHarness()
	harness.QueriesToSkip(
		// DuckDB reports one affected row for a replacement; MySQL reports delete + insert.
		"REPLACE INTO mytable VALUES (1, 'first row');",
		"REPLACE INTO mytable SET i = 1, s = 'first row';",
		"REPLACE INTO mytable VALUES (1, 'new row same i');",
		"REPLACE INTO mytable SET i = 1, s = 'new row same i';",
		// DuckDB does not accept MySQL zero dates.
		"REPLACE_INTO_typestable_VALUES_(____999,_-128,_-32768,_-2147483648,_-9223372036854775808,____0,_0,_0,_0,____1.401298464324817070923729583289916131280e-45,_4.940656458412465441765687928682213723651e-324,____'0000-00-00_00:00:00',_'0000-00-00',____'',_false,_'\"\"',_'',_'',_''____);",
		"REPLACE_INTO_typestable_SET____id_=_999,_i8_=_-128,_i16_=_-32768,_i32_=_-2147483648,_i64_=_-9223372036854775808,____u8_=_0,_u16_=_0,_u32_=_0,_u64_=_0,____f32_=_1.401298464324817070923729583289916131280e-45,_f64_=_4.940656458412465441765687928682213723651e-324,____ti_=_'0000-00-00_00:00:00',_da_=_'0000-00-00',____te_=_'',_bo_=_false,_js_=_'\"\"',_bl_=_'',_e1_=_'',_s1_=_''____;",
	)
	enginetest.TestReplaceInto(t, harness)
}

func TestReplaceIntoErrors(t *testing.T) {
	enginetest.TestReplaceIntoErrors(t, NewDefaultDuckHarness())
}

func TestUpdate(t *testing.T) {
	harness := NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	harness.QueriesToSkip(
		"UPDATE_IGNORE_one_pk_INNER_JOIN_two_pk_on_one_pk.pk_=_two_pk.pk1_SET_two_pk.c1_=_two_pk.c1_+_1",
		"UPDATE_IGNORE_one_pk_JOIN_one_pk_one_pk2_on_one_pk.pk_=_one_pk2.pk_SET_one_pk.pk_=_10",
		"UPDATE_floattable_SET_f32_=_f32_+_f32,_f64_=_f32_*_f64_WHERE_i_=_2;",
		"UPDATE_mytable_SET_s_=_'first_row'_WHERE_i_=_1;",
		"UPDATE_niltable_SET_b_=_NULL_WHERE_f_IS_NULL;",
		"UPDATE_othertable_INNER_JOIN_tabletest_on_othertable.i2=3_and_tabletest.i=3_SET_othertable.s2_=_'fourth'",
		"UPDATE_one_pk_INNER_JOIN_two_pk_on_one_pk.pk_=_two_pk.pk1_SET_one_pk.c1_=_one_pk.c1_+_1,_one_pk.c2_=_one_pk.c2_+_1_ORDER_BY_one_pk.pk",
		"UPDATE_one_pk_INNER_JOIN_two_pk_on_one_pk.pk_=_two_pk.pk1_SET_one_pk.c1_=_one_pk.c1_+_1,_two_pk.c1_=_two_pk.c2_+_1",
		"UPDATE_othertable_LEFT_JOIN_tabletest_on_othertable.i2=3_and_tabletest.i=3_LEFT_JOIN_one_pk_on_othertable.i2_=_1_and_one_pk.pk_=_1_SET_one_pk.c1_=_one_pk.c1_+_1",
		"UPDATE_othertable_LEFT_JOIN_tabletest_on_othertable.i2=3_and_tabletest.i=3_LEFT_JOIN_one_pk_on_othertable.i2_=_one_pk.pk_SET_one_pk.c1_=_one_pk.c1_+_1",
		"UPDATE_othertable_LEFT_JOIN_tabletest_on_othertable.i2=3_and_tabletest.i=3_LEFT_JOIN_one_pk_on_othertable.i2_=_one_pk.pk_SET_one_pk.c1_=_one_pk.c1_+_1_where_one_pk.pk_>_4",
		"UPDATE_othertable_LEFT_JOIN_tabletest_on_othertable.i2=3_and_tabletest.i=3_SET_othertable.s2_=_'fourth'",
		"UPDATE_othertable_LEFT_JOIN_tabletest_on_othertable.i2=3_and_tabletest.i=3_SET_tabletest.s_=_'fourth_row',_tabletest.i_=_tabletest.i_+_1",
		"UPDATE_othertable_LEFT_JOIN_tabletest_on_othertable.i2=tabletest.i_RIGHT_JOIN_one_pk_on_othertable.i2_=_1_and_one_pk.pk_=_1_SET_tabletest.s_=_'updated';",
		"UPDATE_othertable_LEFT_JOIN_tabletest_t3_on_othertable.i2=3_and_t3.i=3_SET_t3.s_=_'fourth_row',_t3.i_=_t3.i_+_1",
		"UPDATE_othertable_RIGHT_JOIN_tabletest_on_othertable.i2=3_and_tabletest.i=3_SET_othertable.i2_=_othertable.i2_+_1",
		"UPDATE_othertable_RIGHT_JOIN_tabletest_on_othertable.i2=3_and_tabletest.i=3_SET_othertable.s2_=_'fourth'",
		"UPDATE_typestable_SET_da_=_'0000-00-00',_ti_=_'0000-00-00_00:00:00';",
	)
	enginetest.TestUpdate(t, harness)
}

func TestUpdateIgnore(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestUpdateIgnore(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestUpdateErrors(t *testing.T) {
	harness := NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	harness.QueriesToSkip(
		// DuckDB treats VARCHAR lengths as advisory.
		"update bad set s = '1234567890'",
	)
	// TODO different errors
	enginetest.TestUpdateErrors(t, harness)
}

func TestOnUpdateExprScripts(t *testing.T) {
	harness := NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	harness.QueriesToSkip(
		// ON UPDATE values are not stored or applied yet. Triggers, foreign keys, and procedures are unsupported.
		"basic case",
		"precision 3",
		"precision 6",
		"default time is current time",
		"alter table",
		"multiple columns case",
		"before update trigger",
		"after update trigger",
		"insert triggers",
		"foreign key tests",
		"stored procedure tests",
		"now() synonyms",
	)
	enginetest.TestOnUpdateExprScripts(t, harness)
}

func TestSpatialUpdate(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestSpatialUpdate(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestDeleteFromErrors(t *testing.T) {
	harness := NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	harness.QueriesToSkip(
		// The query fails correctly, but the bind-parameter error text differs.
		"DELETE FROM mytable WHERE i = ?;",
	)
	enginetest.TestDeleteErrors(t, harness)
}

func TestSpatialDeleteFrom(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestSpatialDelete(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestTruncate(t *testing.T) {
	// Foreign keys and triggers are unsupported. Run the TRUNCATE cases that work.
	harness := NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	harness.Setup(setup.MydbData, setup.MytableData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := enginetest.NewContext(harness)

	t.Run("Standard TRUNCATE", func(t *testing.T) {
		enginetest.RunQueryWithContext(t, e, harness, ctx, "CREATE TABLE t1 (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1))")
		enginetest.RunQueryWithContext(t, e, harness, ctx, "INSERT INTO t1 VALUES (1,1), (2,2), (3,3)")
		enginetest.TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t1 ORDER BY 1", []sql.Row{{int64(1), int64(1)}, {int64(2), int64(2)}, {int64(3), int64(3)}}, nil, nil, nil)
		enginetest.TestQueryWithContext(t, ctx, e, harness, "TRUNCATE t1", []sql.Row{{types.NewOkResult(3)}}, nil, nil, nil)
		enginetest.TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t1 ORDER BY 1", []sql.Row{}, nil, nil, nil)

		enginetest.RunQueryWithContext(t, e, harness, ctx, "INSERT INTO t1 VALUES (4,4), (5,5)")
		enginetest.TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t1 WHERE v1 > 0 ORDER BY 1", []sql.Row{{int64(4), int64(4)}, {int64(5), int64(5)}}, nil, nil, nil)
		enginetest.TestQueryWithContext(t, ctx, e, harness, "TRUNCATE TABLE t1", []sql.Row{{types.NewOkResult(2)}}, nil, nil, nil)
		enginetest.TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t1 ORDER BY 1", []sql.Row{}, nil, nil, nil)
	})

	t.Run("auto_increment column", func(t *testing.T) {
		enginetest.RunQueryWithContext(t, e, harness, ctx, "CREATE TABLE t4 (pk BIGINT AUTO_INCREMENT PRIMARY KEY, v1 BIGINT)")
		enginetest.RunQueryWithContext(t, e, harness, ctx, "INSERT INTO t4(v1) VALUES (5), (6)")
		enginetest.TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t4 ORDER BY 1", []sql.Row{{int64(1), int64(5)}, {int64(2), int64(6)}}, nil, nil, nil)
		enginetest.TestQueryWithContext(t, ctx, e, harness, "TRUNCATE t4", []sql.Row{{types.NewOkResult(2)}}, nil, nil, nil)
		enginetest.TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t4 ORDER BY 1", []sql.Row{}, nil, nil, nil)
		enginetest.RunQueryWithContext(t, e, harness, ctx, "INSERT INTO t4(v1) VALUES (7)")
		enginetest.TestQueryWithContext(t, ctx, e, harness, "SELECT * FROM t4 ORDER BY 1", []sql.Row{{int64(1), int64(7)}}, nil, nil, nil)
	})
}

func TestDeleteFrom(t *testing.T) {
	harness := NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	harness.QueriesToSkip(
		// DuckDB deletes from one table per statement.
		"DELETE mytable, tabletest FROM mytable join tabletest where mytable.i=tabletest.i;",
		"DELETE MYTABLE, TABLETEST FROM mytable join tabletest where mytable.i=tabletest.i;",
		"DELETE FROM mytable, tabletest USING mytable inner join tabletest on mytable.i=tabletest.i;",
		"DELETE mytable, tabletest FROM mytable join tabletest where mytable.i=tabletest.i and mytable.i = 2;",
		"DELETE tabletest, mytable FROM mytable join tabletest where mytable.i=tabletest.i and mytable.i = 2;",
		"with t (n) as (select (1) from dual) delete mytable, tabletest from mytable join tabletest where mytable.i=tabletest.i and mytable.i in (select n from t)",
		// DuckDB has no JSON_TABLE.
		"DELETE mytable FROM mytable join tabletest on mytable.i=tabletest.i join JSON_TABLE('[{\"x\": 1},{\"x\": 2}]', '$[*]' COLUMNS (x INT PATH '$.x')) as jt on jt.x=mytable.i;",
		"DELETE mytable, tabletest FROM mytable join tabletest on mytable.i=tabletest.i join JSON_TABLE('[{\"x\": 1},{\"x\": 2}]', '$[*]' COLUMNS (x INT PATH '$.x')) as jt on jt.x=mytable.i;",
	)
	enginetest.TestDelete(t, harness)
}

func TestConvert(t *testing.T) {
	harness := NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	for _, tt := range queries.ConvertTests {
		// DuckDB does not apply MySQL's numeric coercion when a numeric column is
		// compared with a nonnumeric string or a DATE value.
		if tt.Operand == "'string'" || strings.HasPrefix(tt.Operand, "STR_TO_DATE(") {
			query := fmt.Sprintf("select count(*) from typestable where %s %s %s", tt.Field, tt.Op, tt.Operand)
			harness.QueriesToSkip(query)
		}
	}
	enginetest.TestConvert(t, harness)
}

func TestScripts(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestScripts(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialScripts(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestSpatialScripts(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialIndexScripts(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestSpatialIndexScripts(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialIndexPlans(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestSpatialIndexPlans(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestNumericErrorScripts(t *testing.T) {
	// The only script requires DECIMAL(65,30), but DuckDB supports at most 38 digits.
	t.Skip("DuckDB DECIMAL precision is limited to 38; the only script requires DECIMAL(65,30)")
	enginetest.TestNumericErrorScripts(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestUserPrivileges(t *testing.T) {
	t.Skip("wait for fix")
	harness := NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	if harness.IsUsingServer() {
		t.Skip("TestUserPrivileges test depend on Context to switch the user to run test queries")
	}
	enginetest.TestUserPrivileges(t, harness)
}

func TestUserAuthentication(t *testing.T) {
	t.Skip("wait for fix")
	harness := NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	if harness.IsUsingServer() {
		t.Skip("TestUserPrivileges test depend on Context to switch the user to run test queries")
	}
	enginetest.TestUserAuthentication(t, harness)
}

func TestPrivilegePersistence(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestPrivilegePersistence(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestComplexIndexQueries(t *testing.T) {
	t.Skip("wait for fix")
	harness := NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	enginetest.TestComplexIndexQueries(t, harness)
}

func TestTriggers(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestTriggers(t, NewDefaultDuckHarness())
}

func TestShowTriggers(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestShowTriggers(t, NewDefaultDuckHarness())
}

func TestBrokenTriggers(t *testing.T) {
	t.Skip("wait for support")
	h := enginetest.NewSkippingMemoryHarness()
	for _, script := range queries.BrokenTriggerQueries {
		enginetest.TestScript(t, h, script)
	}
}

func TestStoredProcedures(t *testing.T) {
	t.Skip("wait for support")
	for i, test := range queries.ProcedureLogicTests {
		//TODO: the RowIter returned from a SELECT should not take future changes into account
		if test.Name == "FETCH captures state at OPEN" {
			queries.ProcedureLogicTests[0], queries.ProcedureLogicTests[i] = queries.ProcedureLogicTests[i], queries.ProcedureLogicTests[0]
			queries.ProcedureLogicTests = queries.ProcedureLogicTests[1:]
		}
	}
	enginetest.TestStoredProcedures(t, NewDefaultDuckHarness())
}

func TestEvents(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestEvents(t, NewDefaultDuckHarness())
}

func TestTriggersErrors(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestTriggerErrors(t, NewDefaultDuckHarness())
}

func TestCreateTable(t *testing.T) {

	// Generated by dev/extract_queries_to_skip.py
	waitForFixQueries := []string{
		// DuckDB enforces uniqueness on the full values, not the indexed prefixes.
		"create_table_t1_(i_int_primary_key,_b1_blob,_b2_blob,_unique_index(b1(123),_b2(456)))",
		"create_table_t1_(i_int_primary_key,_b1_blob,_b2_blob,_unique_index(b1(123),_b2(456)))",
		// SUM(VARCHAR) is not supported by DuckDB
		"CREATE_TABLE_t1_as_select_s,_sum(i)_from_mytable_group_by_s",
		"CREATE_TABLE_t1_as_select_s,_sum(i)_from_mytable_group_by_s_having_sum(i)_>_2",
		"display_width_for_numeric_types",
		"SHOW_FULL_FIELDS_FROM_numericDisplayWidthTest;",
		"Identifier_lengths",
		"table_charset_options",
		"event_contains_CREATE_TABLE_AS",
		"CREATE_EVENT_foo_ON_SCHEDULE_EVERY_1_YEAR_DO_CREATE_TABLE_bar_AS_SELECT_1;",
		"trigger_contains_CREATE_TABLE_AS",
		"CREATE_TRIGGER_foo_AFTER_UPDATE_ON_t_FOR_EACH_ROW_BEGIN_CREATE_TABLE_bar_AS_SELECT_1;_END;",
	}

	// Patch auto-generated queries that are known to fail
	waitForFixQueries = append(waitForFixQueries, []string{
		"create table a (i int primary key, j int default 100);", // skip the case "create table with select preserves default" since there is no support for CREATE TABLE SELECT
	}...)

	// The following queries are known to panic the engine
	panicQueries := []string{
		"create_table_t7_select_(select_j_from_a)_sq_from_dual;",
		"create_table_t9_select_*_from_json_table('[{\"c1\":_1}]',_'$[*]'_columns_(c1_int_path_'$.c1'_default_'100'_on_empty))_as_jt;",
	}

	harness := NewDefaultDuckHarness()

	harness.QueriesToSkip(waitForFixQueries...)
	harness.QueriesToSkip(panicQueries...)
	RunCreateTableTest(t, harness)
}

// Adapted from enginetests to skip known issues and pending fixes
func RunCreateTableTest(t *testing.T, harness enginetest.Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.FooData)
	for _, tt := range queries.CreateTableQueries {
		t.Run(tt.WriteQuery, func(t *testing.T) {
			enginetest.RunWriteQueryTest(t, harness, tt)
		})
	}

	for _, script := range queries.CreateTableScriptTests {
		enginetest.TestScript(t, harness, script)
	}

	for _, script := range queries.CreateTableInSubroutineTests {
		enginetest.TestScript(t, harness, script)
	}

	for _, script := range queries.CreateTableAutoIncrementTests {
		enginetest.TestScript(t, harness, script)
	}

	harness.Setup(setup.MydbData, setup.MytableData)
	e := mustNewEngine(t, harness)
	defer e.Close()

	t.Run("no database selected", func(t *testing.T) {
		ctx := enginetest.NewContext(harness)
		ctx.SetCurrentDatabase("")

		enginetest.TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE mydb.t11 (a INTEGER NOT NULL PRIMARY KEY, "+
			"b VARCHAR(10) NOT NULL)", []sql.Row{{types.NewOkResult(0)}}, nil, nil, nil)

		db, err := e.EngineAnalyzer().Catalog.Database(ctx, "mydb")
		require.NoError(t, err)

		testTable, ok, err := db.GetTableInsensitive(ctx, "t11")
		require.NoError(t, err)
		require.True(t, ok)

		s := sql.Schema{
			{Name: "a", Type: types.Int32, Nullable: false, PrimaryKey: true, DatabaseSource: "mydb", Source: "t11"},
			{Name: "b", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 10), Nullable: false, DatabaseSource: "mydb", Source: "t11"},
		}

		require.Equal(t, s, testTable.Schema())
	})

	t.Run("CREATE TABLE with multiple unnamed indexes", func(t *testing.T) {
		ctx := enginetest.NewContext(harness)
		ctx.SetCurrentDatabase("")

		enginetest.TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE mydb.t12 (a INTEGER NOT NULL PRIMARY KEY, "+
			"b VARCHAR(10) UNIQUE, c varchar(10) UNIQUE)", []sql.Row{{types.NewOkResult(0)}}, nil, nil, nil)

		db, err := e.EngineAnalyzer().Catalog.Database(ctx, "mydb")
		require.NoError(t, err)

		t12Table, ok, err := db.GetTableInsensitive(ctx, "t12")
		require.NoError(t, err)
		require.True(t, ok)

		t9TableIndexable, ok := t12Table.(sql.IndexAddressableTable)
		require.True(t, ok)
		t9Indexes, err := t9TableIndexable.GetIndexes(ctx)
		require.NoError(t, err)
		uniqueCount := 0
		for _, index := range t9Indexes {
			if index.IsUnique() {
				uniqueCount += 1
			}
		}

		// We want two unique indexes to be created with unique names being generated. It is up to the integrator
		// to decide how empty string indexes are created. Adding in the primary key gives us a result of 3.
		require.Equal(t, 3, uniqueCount)

		// Validate No Unique Index has an empty Name
		for _, index := range t9Indexes {
			require.True(t, index.ID() != "")
		}
	})
}

func mustNewEngine(t *testing.T, h enginetest.Harness) enginetest.QueryEngine {
	e, err := h.NewEngine(t)
	if err != nil {
		require.NoError(t, err)
	}
	return e
}

func TestRowLimit(t *testing.T) {
	t.Skip("DuckDB does not enforce MySQL maximum row length; both scripts are incompatible and provide no coverage")
	enginetest.TestRowLimit(t, NewDefaultDuckHarness())
}

func TestDropTable(t *testing.T) {
	enginetest.TestDropTable(t, NewDefaultDuckHarness())
}

func TestRenameTable(t *testing.T) {
	queries.RenameTableScripts = []queries.ScriptTest{
		{
			Name: "simple rename table",
			SetUpScript: []string{
				"CREATE TABLE mytable0 (pk int primary key, mk int)",
				"INSERT INTO mytable0 VALUES (1, 1)",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:    "RENAME TABLE mytable0 TO newTableName",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Query:       "SELECT COUNT(*) FROM mytable0",
					ExpectedErr: sql.ErrTableNotFound,
				},
				{
					Query:    "SELECT COUNT(*) FROM newTableName",
					Expected: []sql.Row{{1}},
				},
			},
		},
		{
			Name: "rename multiple tables in one stmt",
			SetUpScript: []string{
				"CREATE TABLE othertable0 (pk int primary key, mk int)",
				"INSERT INTO othertable0 VALUES (1, 1)",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:    "RENAME TABLE othertable0 to othertable2, newTableName to mytable0",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Query:       "SELECT COUNT(*) FROM othertable0",
					ExpectedErr: sql.ErrTableNotFound,
				},
				{
					Query:       "SELECT COUNT(*) FROM newTableName",
					ExpectedErr: sql.ErrTableNotFound,
				},
				{
					Query:    "SELECT COUNT(*) FROM mytable0",
					Expected: []sql.Row{{1}},
				},
				{
					Query:    "SELECT COUNT(*) FROM othertable2",
					Expected: []sql.Row{{1}},
				},
			},
		},
		{
			Name: "error cases",
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:       "ALTER TABLE not_exist RENAME foo",
					ExpectedErr: sql.ErrTableNotFound,
				},
				{
					Query:       "ALTER TABLE emptytable RENAME niltable",
					ExpectedErr: sql.ErrTableAlreadyExists,
				},
			},
		},
	}
	enginetest.TestRenameTable(t, NewDefaultDuckHarness())
}

func TestRenameColumn(t *testing.T) {
	queries.RenameColumnScripts = []queries.ScriptTest{
		{
			Name: "error cases",
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:       "ALTER TABLE mytable RENAME COLUMN i2 TO iX",
					ExpectedErr: sql.ErrTableColumnNotFound,
				},
				{
					Query:       "ALTER TABLE mytable RENAME COLUMN i TO iX, RENAME COLUMN iX TO i2",
					ExpectedErr: sql.ErrTableColumnNotFound,
				},
				{
					Query:       "ALTER TABLE mytable RENAME COLUMN i TO iX, RENAME COLUMN i TO i2",
					ExpectedErr: sql.ErrTableColumnNotFound,
				},
				{
					Query:       "ALTER TABLE mytable RENAME COLUMN i TO S",
					ExpectedErr: sql.ErrColumnExists,
				},
				{
					Query:       "ALTER TABLE mytable RENAME COLUMN i TO n, RENAME COLUMN s TO N",
					ExpectedErr: sql.ErrColumnExists,
				},
			},
		},
		{
			Name: "simple rename column",
			SetUpScript: []string{
				"CREATE TABLE mytable1 (i bigint not null, s varchar(20) not null comment 'column s')",
				"INSERT INTO mytable1 VALUES (1, 'first row')",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:    "ALTER TABLE mytable1 RENAME COLUMN i TO i2, RENAME COLUMN s TO s2",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Query: "SHOW FULL COLUMNS FROM mytable1",
					Expected: []sql.Row{
						{"i2", "bigint", nil, "NO", "", nil, "", "", ""},
						{"s2", "varchar(20)", "utf8mb4_0900_bin", "NO", "", nil, "", "", "column s"},
					},
				},
				{
					Query: "select * from mytable1 order by i2 limit 1",
					Expected: []sql.Row{
						{1, "first row"},
					},
				},
			},
		},
	}
	enginetest.TestRenameColumn(t, NewDefaultDuckHarness())
}

func TestAddColumn(t *testing.T) {
	queries.AddColumnScripts = []queries.ScriptTest{
		{
			Name: "column at end with default",
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:    "ALTER TABLE mytable ADD COLUMN i2 INT COMMENT 'hello' default 42",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Query: "SHOW FULL COLUMNS FROM mytable",
					// | Field | Type | Collation | Null | Key | Default | Extra | Privileges | Comment |
					// TODO: missing privileges
					Expected: []sql.Row{
						{"i", "bigint", nil, "NO", "PRI", nil, "", "", ""},
						{"s", "varchar(20)", "utf8mb4_0900_bin", "NO", "UNI", nil, "", "", "column s"},
						{"i2", "int", nil, "YES", "", "42", "", "", "hello"},
					},
				},
				{
					Query: "SELECT * FROM mytable ORDER BY i;",
					Expected: []sql.Row{
						sql.NewRow(int64(1), "first row", int32(42)),
						sql.NewRow(int64(2), "second row", int32(42)),
						sql.NewRow(int64(3), "third row", int32(42)),
					},
				},
			},
		},
		{
			Name: "add column, no default",
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:    "ALTER TABLE mytable ADD COLUMN s2 TEXT COMMENT 'hello';",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Query: "SHOW FULL COLUMNS FROM mytable",
					Expected: []sql.Row{
						{"i", "bigint", nil, "NO", "PRI", nil, "", "", ""},
						{"s2", "text", "utf8mb4_0900_bin", "YES", "", nil, "", "", "hello"},
						{"s", "varchar(20)", "utf8mb4_0900_bin", "NO", "UNI", nil, "", "", "column s"},
						{"i2", "int", nil, "YES", "", "42", "", "", "hello"},
					},
				},
				{
					Query: "SELECT * FROM mytable ORDER BY i;",
					Expected: []sql.Row{
						sql.NewRow(int64(1), "first row", int32(42), nil),
						sql.NewRow(int64(2), "second row", int32(42), nil),
						sql.NewRow(int64(3), "third row", int32(42), nil),
					},
				},
				{
					Query:    "insert into mytable values (4, 'fourth row', 11, 's2');",
					Expected: []sql.Row{{types.NewOkResult(1)}},
				},
				{
					Query:    "update mytable set s2 = 'updated s2' where i2 = 42;",
					Expected: []sql.Row{{types.OkResult{RowsAffected: 3}}},
				},
				{
					Query: "SELECT * FROM mytable ORDER BY i;",
					Expected: []sql.Row{
						sql.NewRow(int64(1), "first row", int32(42), "updated s2"),
						sql.NewRow(int64(2), "second row", int32(42), "updated s2"),
						sql.NewRow(int64(3), "third row", int32(42), "updated s2"),
						sql.NewRow(int64(4), "fourth row", int32(11), "s2"),
					},
				},
			},
		},
		{
			Name: "multiple in one statement",
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:    "ALTER TABLE mytable ADD COLUMN s5 VARCHAR(26), ADD COLUMN s6 VARCHAR(27)",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Query: "SHOW FULL COLUMNS FROM mytable",
					Expected: []sql.Row{
						{"i", "bigint", nil, "NO", "PRI", nil, "", "", ""},
						{"s2", "text", "utf8mb4_0900_bin", "YES", "", nil, "", "", "hello"},
						{"s", "varchar(20)", "utf8mb4_0900_bin", "NO", "UNI", nil, "", "", "column s"},
						{"i2", "int", nil, "YES", "", "42", "", "", "hello"},
						{"s5", "varchar(26)", "utf8mb4_0900_bin", "YES", "", nil, "", "", ""},
						{"s6", "varchar(27)", "utf8mb4_0900_bin", "YES", "", nil, "", "", ""},
					},
				},
				{
					Query: "SELECT * FROM mytable ORDER BY i;",
					Expected: []sql.Row{
						sql.NewRow(int64(1), "first row", int32(42), "updated s2", nil, nil),
						sql.NewRow(int64(2), "second row", int32(42), "updated s2", nil, nil),
						sql.NewRow(int64(3), "third row", int32(42), "updated s2", nil, nil),
						sql.NewRow(int64(4), "fourth row", int32(11), "s2", nil, nil),
					},
				},
			},
		},
		{
			Name: "error cases",
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:       "ALTER TABLE not_exist ADD COLUMN i2 INT COMMENT 'hello'",
					ExpectedErr: sql.ErrTableNotFound,
				},
				{
					Query:       "ALTER TABLE mytable ADD COLUMN b BIGINT COMMENT 'ok' AFTER not_exist",
					ExpectedErr: sql.ErrTableColumnNotFound,
				},
				{
					Query:       "ALTER TABLE mytable ADD COLUMN i BIGINT COMMENT 'ok'",
					ExpectedErr: sql.ErrColumnExists,
				},
				{
					Query:       "ALTER TABLE mytable ADD COLUMN b INT NOT NULL DEFAULT 'yes'",
					ExpectedErr: sql.ErrIncompatibleDefaultType,
				},
				{
					Query:       "ALTER TABLE mytable ADD COLUMN c int, add c int",
					ExpectedErr: sql.ErrColumnExists,
				},
			},
		},
	}
	RunAddColumnTest(t, NewDefaultDuckHarness())
}

func RunAddColumnTest(t *testing.T, harness enginetest.Harness) {
	harness.Setup(setup.MydbData, setup.MytableData)
	e := mustNewEngine(t, harness)
	defer e.Close()

	for _, tt := range queries.AddColumnScripts {
		enginetest.TestScriptWithEngine(t, e, harness, tt)
	}

	t.Run("no database selected", func(t *testing.T) {
		ctx := enginetest.NewContext(harness)
		ctx.SetCurrentDatabase("")
		if se, ok := e.(*enginetest.ServerQueryEngine); ok {
			se.NewConnection(ctx)
		}
		enginetest.TestQueryWithContext(t, ctx, e, harness, "select database()", []sql.Row{{nil}}, nil, nil, nil)
		enginetest.TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE mydb.mytable ADD COLUMN s10 VARCHAR(26)", []sql.Row{{types.NewOkResult(0)}}, nil, nil, nil)
		enginetest.TestQueryWithContext(t, ctx, e, harness, "SHOW FULL COLUMNS FROM mydb.mytable", []sql.Row{
			{"i", "bigint", nil, "NO", "PRI", nil, "", "", ""},
			{"s2", "text", "utf8mb4_0900_bin", "YES", "", nil, "", "", "hello"},
			{"s", "varchar(20)", "utf8mb4_0900_bin", "NO", "UNI", nil, "", "", "column s"},
			{"i2", "int", nil, "YES", "", "42", "", "", "hello"},
			{"s5", "varchar(26)", "utf8mb4_0900_bin", "YES", "", nil, "", "", ""},
			{"s6", "varchar(27)", "utf8mb4_0900_bin", "YES", "", nil, "", "", ""},
			{"s10", "varchar(26)", "utf8mb4_0900_bin", "YES", "", nil, "", "", ""},
		}, nil, nil, nil)
	})
}

func TestModifyColumn(t *testing.T) {
	queries.ModifyColumnScripts = []queries.ScriptTest{
		{
			Name: "column at end with default",
			SetUpScript: []string{
				"CREATE TABLE mytable_m1 (i bigint not null, s varchar(20) not null comment 'column s')",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:    "ALTER TABLE mytable_m1 MODIFY COLUMN i bigint NOT NULL COMMENT 'modified'",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Query: "SHOW FULL COLUMNS FROM mytable_m1 /* 1 */",
					Expected: []sql.Row{
						{"i", "bigint", nil, "NO", "", nil, "", "", "modified"},
						{"s", "varchar(20)", "utf8mb4_0900_bin", "NO", "", nil, "", "", "column s"},
					},
				},
				{
					Query:    "ALTER TABLE mytable_m1 MODIFY COLUMN i TINYINT NOT NULL COMMENT 'yes'",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Query: "SHOW FULL COLUMNS FROM mytable_m1 /* 2 */",
					Expected: []sql.Row{
						{"i", "tinyint", nil, "NO", "", nil, "", "", "yes"},
						{"s", "varchar(20)", "utf8mb4_0900_bin", "NO", "", nil, "", "", "column s"},
					},
				},
				{
					Query:    "ALTER TABLE mytable_m1 MODIFY COLUMN i BIGINT NOT NULL COMMENT 'ok'",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Query: "SHOW FULL COLUMNS FROM mytable_m1 /* 3 */",
					Expected: []sql.Row{
						{"i", "bigint", nil, "NO", "", nil, "", "", "ok"},
						{"s", "varchar(20)", "utf8mb4_0900_bin", "NO", "", nil, "", "", "column s"},
					},
				},
				{
					Query:    "ALTER TABLE mytable_m1 MODIFY COLUMN s VARCHAR(20) NULL COMMENT 'changed'",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Query: "SHOW FULL COLUMNS FROM mytable_m1 /* 4 */",
					Expected: []sql.Row{
						{"i", "bigint", nil, "NO", "", nil, "", "", "ok"},
						{"s", "varchar(20)", "utf8mb4_0900_bin", "YES", "", nil, "", "", "changed"},
					},
				},
			},
		},
		{
			Name: "auto increment attribute",
			SetUpScript: []string{
				"CREATE TABLE mytable_m2 (i bigint not null primary key, s varchar(20) comment 'changed')",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:    "ALTER TABLE mytable_m2 MODIFY i BIGINT auto_increment",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Query: "SHOW FULL COLUMNS FROM mytable_m2 /* 1 */",
					Expected: []sql.Row{
						{"i", "bigint", nil, "NO", "PRI", nil, "auto_increment", "", ""},
						{"s", "varchar(20)", "utf8mb4_0900_bin", "YES", "", nil, "", "", "changed"},
					},
				},
				{
					Query: "insert into mytable_m2 (s) values ('new row')",
				},
				{
					Query:       "ALTER TABLE mytable_m2 add column i2 bigint auto_increment",
					ExpectedErr: sql.ErrInvalidAutoIncCols,
				},
				{
					Query: "alter table mytable_m2 add column i2 bigint",
				},
				{
					Query:       "ALTER TABLE mytable_m2 modify column i2 bigint auto_increment",
					ExpectedErr: sql.ErrInvalidAutoIncCols,
				},
				{
					Query: "SHOW FULL COLUMNS FROM mytable_m2 /* 2 */",
					Expected: []sql.Row{
						{"i", "bigint", nil, "NO", "PRI", nil, "auto_increment", "", ""},
						{"s", "varchar(20)", "utf8mb4_0900_bin", "YES", "", nil, "", "", "changed"},
						{"i2", "bigint", nil, "YES", "", nil, "", "", ""},
					},
				},
				{
					Skip:     true,
					Query:    "ALTER TABLE mytable_m2 MODIFY COLUMN i BIGINT NOT NULL COMMENT 'ok' FIRST",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Skip:  true,
					Query: "SHOW FULL COLUMNS FROM mytable_m2 /* 3 */",
					Expected: []sql.Row{
						{"i", "bigint", nil, "NO", "PRI", nil, "", "", "ok"},
						{"s", "varchar(20)", "utf8mb4_0900_bin", "YES", "", nil, "", "", "changed"},
						{"i2", "bigint", nil, "YES", "", nil, "", "", ""},
					},
				},
				{
					Skip:     true,
					Query:    "ALTER TABLE mytable_m2 MODIFY COLUMN s VARCHAR(20) NULL COMMENT 'changed'",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Skip:  true,
					Query: "SHOW FULL COLUMNS FROM mytable_m2 /* 4 */",
					Expected: []sql.Row{
						{"i", "bigint", nil, "NO", "PRI", nil, "", "", "ok"},
						{"s", "varchar(20)", "utf8mb4_0900_bin", "YES", "", nil, "", "", "changed"},
						{"i2", "bigint", nil, "YES", "", nil, "", "", ""},
					},
				},
			},
		},
		{
			Name:        "error cases",
			SetUpScript: []string{},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:       "ALTER TABLE mytable MODIFY not_exist BIGINT NOT NULL COMMENT 'ok' FIRST",
					ExpectedErr: sql.ErrTableColumnNotFound,
				},
				{
					Query:       "ALTER TABLE mytable MODIFY i BIGINT NOT NULL COMMENT 'ok' AFTER not_exist",
					ExpectedErr: sql.ErrTableColumnNotFound,
				},
				{
					Query:       "ALTER TABLE not_exist MODIFY COLUMN i INT NOT NULL COMMENT 'hello'",
					ExpectedErr: sql.ErrTableNotFound,
				},
				{
					Query:       "ALTER TABLE mytable ADD COLUMN b INT NOT NULL DEFAULT 'yes'",
					ExpectedErr: sql.ErrIncompatibleDefaultType,
				},
				{
					Query:       "ALTER TABLE mytable ADD COLUMN c int, add c int",
					ExpectedErr: sql.ErrColumnExists,
				},
			},
		},
	}

	RunModifyColumnTest(t, NewDefaultDuckHarness())
}

func RunModifyColumnTest(t *testing.T, harness enginetest.Harness) {
	harness.Setup(setup.MydbData, setup.MytableData, setup.Mytable_del_idxData)
	e := mustNewEngine(t, harness)
	defer e.Close()

	for _, tt := range queries.ModifyColumnScripts {
		enginetest.TestScriptWithEngine(t, e, harness, tt)
	}

	t.Run("no database selected", func(t *testing.T) {
		ctx := enginetest.NewContext(harness)
		ctx.SetCurrentDatabase("")
		if se, ok := e.(*enginetest.ServerQueryEngine); ok {
			se.NewConnection(ctx)
		}
		enginetest.TestQueryWithContext(t, ctx, e, harness, "select database()", []sql.Row{{nil}}, nil, nil, nil)
		enginetest.TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE mydb.mytable_m1 MODIFY COLUMN s VARCHAR(21) NULL COMMENT 'changed again'", []sql.Row{{types.NewOkResult(0)}}, nil, nil, nil)
		enginetest.TestQueryWithContext(t, ctx, e, harness, "SHOW FULL COLUMNS FROM mydb.mytable_m1", []sql.Row{
			{"i", "bigint", nil, "NO", "", nil, "", "", "ok"},
			{"s", "varchar(21)", "utf8mb4_0900_bin", "YES", "", nil, "", "", "changed again"},
		}, nil, nil, nil)
	})
}

func TestDropColumn(t *testing.T) {
	queries.DropColumnScripts = []queries.ScriptTest{
		{
			Name: "drop column",
			SetUpScript: []string{
				"CREATE TABLE mytable_drop_column (i bigint primary key, s varchar(20))",
				"INSERT INTO mytable_drop_column VALUES (1, 'first row'), (2, 'second row'), (3, 'third row')",
				"ALTER TABLE mytable_drop_column DROP COLUMN s",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:    "SHOW FULL COLUMNS FROM mytable_drop_column",
					Expected: []sql.Row{{"i", "bigint", nil, "NO", "PRI", nil, "", "", ""}},
				},
				{
					Query:    "select * from mytable_drop_column order by i",
					Expected: []sql.Row{{1}, {2}, {3}},
				},
			},
		},
		{
			Name: "drop first column",
			SetUpScript: []string{
				"CREATE TABLE t1 (a int, b varchar(10), c bigint, k bigint not null)",
				"insert into t1 values (1, 'abc', 2, 3), (4, 'def', 5, 6)",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:    "ALTER TABLE t1 DROP COLUMN a",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Query: "SHOW FULL COLUMNS FROM t1",
					Expected: []sql.Row{
						{"b", "varchar(10)", "utf8mb4_0900_bin", "YES", "", nil, "", "", ""},
						{"c", "bigint", nil, "YES", "", nil, "", "", ""},
						{"k", "bigint", nil, "NO", "", nil, "", "", ""},
					},
				},
				{
					Query: "SELECT * FROM t1 ORDER BY b",
					Expected: []sql.Row{
						{"abc", 2, 3},
						{"def", 5, 6},
					},
				},
			},
		},
		{
			Name: "drop middle column",
			SetUpScript: []string{
				"CREATE TABLE t2 (a int, b varchar(10), c bigint, k bigint not null)",
				"insert into t2 values (1, 'abc', 2, 3), (4, 'def', 5, 6)",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:    "ALTER TABLE t2 DROP COLUMN b",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Query: "SHOW FULL COLUMNS FROM t2",
					Expected: []sql.Row{
						{"a", "int", nil, "YES", "", nil, "", "", ""},
						{"c", "bigint", nil, "YES", "", nil, "", "", ""},
						{"k", "bigint", nil, "NO", "", nil, "", "", ""},
					},
				},
				{
					Query: "SELECT * FROM t2 ORDER BY c",
					Expected: []sql.Row{
						{1, 2, 3},
						{4, 5, 6},
					},
				},
			},
		},
		{
			// TODO: primary key column drops not well supported yet
			Name: "drop primary key column",
			SetUpScript: []string{
				"CREATE TABLE t3 (a int primary key, b varchar(10), c bigint)",
				"insert into t3 values (1, 'abc', 2), (3, 'def', 4)",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Skip:     true,
					Query:    "ALTER TABLE t3 DROP COLUMN a",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Skip:  true,
					Query: "SHOW FULL COLUMNS FROM t3",
					Expected: []sql.Row{
						{"b", "varchar(10)", "utf8mb4_0900_bin", "YES", "", nil, "", "", ""},
						{"c", "bigint", nil, "YES", "", nil, "", "", ""},
					},
				},
				{
					Skip:  true,
					Query: "SELECT * FROM t3 ORDER BY b",
					Expected: []sql.Row{
						{"abc", 2},
						{"def", 4},
					},
				},
			},
		},
		{
			Name: "error cases",
			SetUpScript: []string{
				"create table t4 (a int primary key, b int, c int default (10))",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:       "ALTER TABLE not_exist DROP COLUMN s",
					ExpectedErr: sql.ErrTableNotFound,
				},
				{
					Query:       "ALTER TABLE t4 DROP COLUMN s",
					ExpectedErr: sql.ErrTableColumnNotFound,
				},
				{
					Skip:        true,
					Query:       "ALTER TABLE t4 DROP COLUMN b",
					ExpectedErr: sql.ErrDropColumnReferencedInDefault,
				},
			},
		},
	}
	enginetest.TestDropColumn(t, NewDefaultDuckHarness())
}

func TestDropColumnKeylessTables(t *testing.T) {
	enginetest.TestDropColumnKeylessTables(t, NewDefaultDuckHarness())
}

func TestCreateDatabase(t *testing.T) {
	enginetest.TestCreateDatabase(t, NewDefaultDuckHarness())
}

func TestPkOrdinalsDDL(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestPkOrdinalsDDL(t, NewDefaultDuckHarness())
}

func TestPkOrdinalsDML(t *testing.T) {
	enginetest.TestPkOrdinalsDML(t, NewDefaultDuckHarness())
}

func TestDropDatabase(t *testing.T) {
	enginetest.TestDropDatabase(t, NewDefaultDuckHarness())
}

func TestCreateForeignKeys(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestCreateForeignKeys(t, NewDefaultDuckHarness())
}

func TestDropForeignKeys(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestDropForeignKeys(t, NewDefaultDuckHarness())
}

func TestForeignKeys(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestForeignKeys(t, NewDefaultDuckHarness())
}

func TestFulltextIndexes(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestFulltextIndexes(t, NewDefaultDuckHarness())
}

func TestCreateCheckConstraints(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestCreateCheckConstraints(t, NewDefaultDuckHarness())
}

func TestChecksOnInsert(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestChecksOnInsert(t, NewDefaultDuckHarness())
}

func TestChecksOnUpdate(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestChecksOnUpdate(t, NewDefaultDuckHarness())
}

func TestDisallowedCheckConstraints(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestDisallowedCheckConstraints(t, NewDefaultDuckHarness())
}

func TestDropCheckConstraints(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestDropCheckConstraints(t, NewDefaultDuckHarness())
}

func TestReadOnly(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestReadOnly(t, NewDefaultDuckHarness(), true /* testStoredProcedures */)
}

func TestViews(t *testing.T) {
	// patch view query tests since the slight difference in output format
	replaceQueryTestByQuery(queries.ViewTests, "select * from information_schema.views where table_schema = 'mydb' order by table_name", queries.QueryTest{
		Query: "select * from information_schema.views where table_schema = 'mydb' order by table_name",
		Expected: []sql.Row{
			sql.NewRow("def", "mydb", "myview", "SELECT * FROM mytable", "NONE", "YES", "root@localhost", "DEFINER", "utf8mb4", "utf8mb4_0900_bin"),
			sql.NewRow("def", "mydb", "myview2", "SELECT * FROM myview WHERE (i = 1)", "NONE", "YES", "root@localhost", "DEFINER", "utf8mb4", "utf8mb4_0900_bin"),
		},
	})

	waitForFixQueries := []string{
		"insert into tab1 values (6, 0, 52.14, 'jxmel', 22, 2.27, 'pzxbn')",
		"create view v as select 2+2",
		"CREATE TABLE xy (x int primary key, y int);",
		"CREATE VIEW caseSensitive AS SELECT id as AbCdEfG FROM strs;",
		`CREATE TABLE strs ( id int NOT NULL AUTO_INCREMENT,
                                 str  varchar(15) NOT NULL,
                                 PRIMARY KEY (id));`,
		"create table t (i int primary key, j int default 100);",
		"CREATE EVENT foo ON SCHEDULE EVERY 1 YEAR DO CREATE VIEW bar AS SELECT 1;",
		"CREATE TRIGGER foo AFTER UPDATE ON t FOR EACH ROW BEGIN CREATE TABLE bar AS SELECT 1; END;",
	}
	harness := NewDefaultDuckHarness()
	harness.QueriesToSkip(waitForFixQueries...)
	enginetest.TestViews(t, harness)
}

// func TestVersionedViews(t *testing.T) {
// 	enginetest.TestVersionedViews(t, NewDefaultMetaHarness())
// }

func TestNaturalJoin(t *testing.T) {
	enginetest.TestNaturalJoin(t, NewDefaultDuckHarness())
}

func TestWindowFunctions(t *testing.T) {
	testWindowFunctionsCompat(t)
}

func TestWindowRangeFrames(t *testing.T) {
	testWindowRangeFramesCompat(t)
}

func TestNamedWindows(t *testing.T) {
	testNamedWindowsCompat(t)
}

func TestNaturalJoinEqual(t *testing.T) {
	enginetest.TestNaturalJoinEqual(t, NewDefaultDuckHarness())
}

func TestNaturalJoinDisjoint(t *testing.T) {
	t.Skip("DuckDB rejects NATURAL JOIN without shared columns; MySQL treats it as CROSS JOIN")
	enginetest.TestNaturalJoinDisjoint(t, NewDefaultDuckHarness())
}

func TestInnerNestedInNaturalJoins(t *testing.T) {
	enginetest.TestInnerNestedInNaturalJoins(t, NewDefaultDuckHarness())
}

func TestColumnDefaults(t *testing.T) {

	// Generated by dev/extract_queries_to_skip.py
	waitForFixScriptName := []string{
		"update_join_ambiguous_default",
		"update_join_ambiguous_generated_column",
		"update_join_ambiguous_generated_column/update_t1_n_inner_join_t2_m_on_n.y_=_m.y_set_n.x_=n.y_where_n.x_=_3;",
		"update_join_ambiguous_generated_column/select_*_from_t1",
		"Default_expression_with_function_and_referenced_column",
		"Default_expression_converting_to_proper_column_type",
		"Back_reference_to_default_literal",
		"Forward_reference_to_default_literal",
		"Forward_reference_to_default_expression",
		"Back_reference_to_value",
		"REPLACE_INTO_with_default_expression",
		"Add_column_implicit_last_default_expression",
		"Add_column_explicit_last_default_expression",
		"Add_column_first_default_literal",
		"Add_column_first_default_literal/SELECT_*_FROM_t16",
		"Add_column_first_default_expression",
		"Add_column_forward_reference_to_default_expression",
		"Add_column_back_reference_to_default_literal",
		"Add_column_first_with_existing_defaults_still_functioning",
		"Drop_column_referencing_other_column",
		"Modify_column_move_first_forward_reference_default_literal",
		"Modify_column_move_first_add_reference",
		"Modify_column_move_last_being_referenced",
		"Modify_column_move_last_add_reference",
		"Modify_column_no_move_add_reference",
		"Column_referenced_with_name_change",
		"Add_non-nullable_column_without_default_#1",
		"Add_non-nullable_column_without_default_#2",
		"Column_defaults_with_functions",
		"BLOB_types_can_define_defaults_with_literals",
		"BLOB_types_can_define_defaults_with_literals/CREATE_TABLE_t997(pk_BIGINT_PRIMARY_KEY,_v1_BLOB_DEFAULT_0x61)",
		"BLOB_types_can_define_defaults_with_literals/INSERT_INTO_t997_VALUES(42,_DEFAULT)",
		"BLOB_types_can_define_defaults_with_literals/SELECT_*_from_t997",
		"Stored_procedures_are_not_valid_in_column_default_value_expressions",
		"Expression_contains_invalid_literal,_fails_on_insertion",
		"Expression_contains_invalid_literal,_fails_on_insertion/INSERT_INTO_t1000_(pk)_VALUES_(1)",
		"Expression_contains_null_on_NOT_NULL,_fails_on_insertion",
		"Expression_contains_null_on_NOT_NULL,_fails_on_insertion/INSERT_INTO_t1001_(pk)_VALUES_(1)",
		"Add_column_first_back_reference_to_expression",
		"Add_column_after_back_reference_to_expression",
		"Add_column_self_reference",
		"Drop_column_referenced_by_other_column",
		"Modify_column_moving_back_creates_back_reference_to_expression",
		"Modify_column_moving_forward_creates_back_reference_to_expression",
		"DATETIME/TIMESTAMP_NOW/CURRENT_TIMESTAMP_current_timestamp",
		"DATETIME/TIMESTAMP_NOW/CURRENT_TIMESTAMP_literals",
		"Non-DATETIME/TIMESTAMP_NOW/CURRENT_TIMESTAMP_expression",
		"Table_referenced_with_column",
		"column_default_normalization:_int_column_rounds",
		"column_default_normalization:_float_column_rounds",
		"column_default_normalization:_double_quotes",
		"column_default_normalization:_expression_string_literal",
		"column_default_normalization:_expression_int_literal",
	}
	//Currently, “myduckserver” does not have a method to insert default values, duckdb does not support “insert into t values ()”;
	//sqlglot.transpile will translate "insert into t(i) values (default)" into "insert into t(i) values ("default")"
	//Therefore, the following tests are skipped
	replaceQueryInScriptTest(queries.ColumnDefaultTests, "column default normalization: int column rounds",
		"insert into t values ();",
		"insert into t(i) values (default);",
	)
	replaceQueryInScriptTest(queries.ColumnDefaultTests, "column default normalization: float column rounds",
		"insert into t values ();",
		"insert into t(f) values (default);",
	)
	replaceQueryInScriptTest(queries.ColumnDefaultTests, "column default normalization: double quotes",
		"insert into t values ();",
		"insert into t(f) values (default);",
	)
	replaceQueryInScriptTest(queries.ColumnDefaultTests, "column default normalization: expression string literal",
		"insert into t values ();",
		"insert into t(f) values (default);",
	)
	replaceQueryInScriptTest(queries.ColumnDefaultTests, "column default normalization: expression int literal",
		"insert into t values ();",
		"insert into t(i) values (default);",
	)

	harness := NewDefaultDuckHarness()
	waitForFixQueries := harness.GetScriptQueries(queries.ColumnDefaultTests, waitForFixScriptName)

	harness.QueriesToSkip(waitForFixQueries...)
	// enginetest.TestColumnDefaults(t, harness)
	RunTestColumnDefaults(t, harness)
}
func RunTestColumnDefaults(t *testing.T, harness enginetest.Harness) {
	harness.Setup(setup.MydbData)

	for _, tt := range queries.ColumnDefaultTests {
		enginetest.TestScript(t, harness, tt)
	}

	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := enginetest.NewContext(harness)

	// Some tests can't currently be run with as a script because they do additional checks
	t.Run("DATETIME/TIMESTAMP NOW/CURRENT_TIMESTAMP current_timestamp", func(t *testing.T) {
		if enginetest.IsServerEngine(e) {
			t.Skip("TODO: fix result formatting for server engine tests")
		}
		// ctx = NewContext(harness)
		// e.Query(ctx, "set @@session.time_zone='SYSTEM';")
		enginetest.TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t10(pk BIGINT PRIMARY KEY, v1 DATETIME(6) DEFAULT NOW(6), v2 DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),"+
			"v3 TIMESTAMP(6) DEFAULT NOW(6), v4 TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6))", []sql.Row{{types.NewOkResult(0)}}, nil, nil, nil)

		// truncating time to microseconds for compatibility with integrators who may store more precision (go gives nanos)
		now := time.Now().Truncate(time.Microsecond).UTC()
		sql.RunWithNowFunc(func() time.Time {
			return now
		}, func() error {
			enginetest.RunQueryWithContext(t, e, harness, nil, "insert into t10(pk) values (1)")
			return nil
		})
		// enginetest.TestQueryWithContext(t, ctx, e, harness, "select * from t10 order by 1", []sql.Row{
		// 	{1, now, now, now, now},
		// }, nil, nil, nil)
	})

	// TODO: zero timestamps work slightly differently than they do in MySQL, where the zero time is "0000-00-00 00:00:00"
	//  We use "0000-01-01 00:00:00"
	t.Run("DATETIME/TIMESTAMP NOW/CURRENT_TIMESTAMP literals", func(t *testing.T) {
		if enginetest.IsServerEngine(e) {
			t.Skip("TODO: fix result formatting for server engine tests")
		}
		enginetest.TestQueryWithContext(t, ctx, e, harness, "CREATE TABLE t10zero(pk BIGINT PRIMARY KEY, v1 DATETIME DEFAULT '2020-01-01 01:02:03', v2 DATETIME DEFAULT '2020-01-01 01:02:03',"+
			"v3 TIMESTAMP DEFAULT '2020-01-01 01:02:03', v4 TIMESTAMP DEFAULT '2020-01-01 01:02:03')", []sql.Row{{types.NewOkResult(0)}}, nil, nil, nil)

		enginetest.RunQueryWithContext(t, e, harness, ctx, "insert into t10zero(pk) values (1)")

		// TODO: the string conversion does not transform to UTC like other NOW() calls, fix this
		enginetest.TestQueryWithContext(t, ctx, e, harness, "select * from t10zero order by 1", []sql.Row{{1, time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC), time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC), time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC), time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC)}}, nil, nil, nil)
	})
}

func TestAlterTable(t *testing.T) {

	harness := NewDefaultDuckHarness()

	// patch the test script since we don't support column as default value yet
	replaceQueryInScriptTest(queries.AlterTableScripts, "variety of alter column statements in a single statement",
		"CREATE TABLE t32(pk BIGINT PRIMARY KEY, v1 int, v2 int, v3 int default (v1), toRename int)",
		"CREATE TABLE t32(pk BIGINT PRIMARY KEY, v1 int, v2 int, v3 int default (10), toRename int)",
	)

	// patch the test script since we don't support check constraints yet
	replaceQueryInScriptTest(queries.AlterTableScripts, "drop column drops check constraint",
		`ALTER TABLE t34 ADD CONSTRAINT test_check CHECK (j < 12345)`,
		``,
	)

	harness.QueriesToSkip(
		// skip "mix of alter column, add and drop constraints in one statement" since check constraints are not supported
		`CREATE TABLE t33(pk BIGINT PRIMARY KEY, v1 int, v2 int)`,
		// skip "ALTER TABLE ... ALTER ADD CHECK / DROP CHECK" since check constraints are not supported
		"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT NOT NULL DEFAULT 88);",
		// skip "multi alter with invalid schemas", we support longer varchar lengths
		"CREATE TABLE t(a int primary key)",
		// skip "alter table containing column default value expressions" since we don't support current_timestamp default value yet
		"create table t (pk int primary key, col1 timestamp(6) default current_timestamp(6), col2 varchar(1000), index idx1 (pk, col1));",
		// skip "drop check as part of alter block" since check constraints are not supported
		"create table t42 (i bigint primary key, j int, CONSTRAINT check1 CHECK (j < 12345), CONSTRAINT check2 CHECK (j > 0))",
		// skip "drop constraint as part of alter block" since check constraints are not supported
		"create table t42 (i bigint primary key, j int, CONSTRAINT check1 CHECK (j < 12345), CONSTRAINT check2 CHECK (j > 0))",
		// skip "drop column drops all relevant check constraints" since check constraints are not supported
		"ALTER TABLE t42 ADD CONSTRAINT check1 CHECK (j < 12345)",
		// skip "drop column drops correct check constraint" since check constraints are not supported
		"create table t41 (i bigint primary key, s varchar(20))",
		// skip "drop column does not drop when referenced in constraint with other column" since check constraints are not supported
		"create table t43 (i bigint primary key, s varchar(20))",
		// skip "drop column preserves indexes" since duckdb has more strict dropping rules for tables with indexes
		"create table t35 (i bigint primary key, s varchar(20), s2 varchar(20))",
		// skip "drop column prevents foreign key violations" since foreign keys are not supported
		"create table t36 (i bigint primary key, j varchar(20))",
		// skip "ALTER TABLE does not change column collations"
		"CREATE TABLE test1 (v1 VARCHAR(200), v2 ENUM('a'), v3 SET('a'));",
		// skip "ALTER TABLE MODIFY column with UNIQUE KEY" since duckdb has more strict rules for modifying columns with constraints
		"CREATE table test (pk int primary key, uk int unique)",
		// skip "ALTER TABLE MODIFY column making UNIQUE" due to differences in error messages
		"CREATE table test (pk int primary key, uk int)",
		// skip "ALTER TABLE MODIFY column with KEY"  since duckdb has more strict rules for modifying columns with constraints
		"CREATE table test (pk int primary key, mk int, index (mk))",
		// skip "Identifier lengths"
		"create table t1 (a int primary key, b int)",
		//skip some assertions in "Index case-insensitivity"
		"alter table t2 rename index myIndex2 to mySecondIndex;",
		"show indexes from t2;",
		"alter table t3 rename index MYiNDEX3 to anotherIndex;",
		"show indexes from t3;",
	)

	enginetest.TestAlterTable(t, harness)
}

func TestDateParse(t *testing.T) {
	harness := NewDefaultDuckHarness()
	if harness.IsUsingServer() {
		t.Skip("issue: https://github.com/dolthub/dolt/issues/6901")
	}
	enginetest.TestDateParse(t, NewDefaultDuckHarness())
}

func TestJsonScripts(t *testing.T) {
	harness := NewDefaultDuckHarness()
	harness.QueriesToSkip(
		// DuckDB JSON_TYPE reports the stored physical unsigned integer type.
		"select x, JSON_TYPE(y) from xy",
		"select JSON_TYPE(y) from xy where x = 1;",
		// SQLGlot does not support go-mysql-server's three-argument JSON_VALUE form.
		`select json_value(y, '$.a', 'json') from xy`,
		`select json_value(y, '$.a[0].b', 'signed') from xy where x = 2`,
		// DuckDB accepts an integer JSON_EXTRACT input where go-mysql-server rejects it.
		`select json_length(json_extract(x, "$.a")) from xy`,
		// The DuckDB execution path does not pass this assertion's positional binding.
		`SELECT * FROM users WHERE JSON_CONTAINS (languages, JSON_ARRAY(?)) ORDER BY users.id LIMIT 1`,
		// Upstream expects [], but the script name and MySQL semantics both require NULL.
		"SELECT JSON_ARRAYAGG(o_id) FROM t2",
		// DuckDB requires the ORDER BY column to be grouped or aggregated.
		"SELECT pk, JSON_ARRAYAGG(field) FROM (SELECT * FROM j ORDER BY pk) as sub GROUP BY field ORDER BY pk",
		// DuckDB JSON_GROUP_OBJECT rejects duplicate keys instead of keeping the last value.
		"SELECT JSON_OBJECTAGG(val, o_id) FROM (SELECT * FROM t2 ORDER BY o_id) as sub GROUP BY val",
		`SELECT JSON_OBJECTAGG(c0, val) from (SELECT * FROM j ORDER BY pk) as sub`,
		// SQLGlot cannot parse an unquoted column named value inside JSON_OBJECTAGG.
		"SELECT c0, JSON_OBJECTAGG(`attribute`, value) FROM (SELECT * FROM t ORDER BY o_id) as sub GROUP BY c0",
		"SELECT c0, JSON_OBJECTAGG(c0, value) FROM (SELECT * FROM t ORDER BY o_id) as sub GROUP BY c0",
		"select JSON_OBJECTAGG(c0, value) from (SELECT * FROM t ORDER BY o_id) as sub",
		"select JSON_OBJECTAGG(`attribute`, value) from (SELECT * FROM t ORDER BY o_id) as sub",
		// DuckDB reports a different error category for a NULL JSON object key.
		`SELECT JSON_OBJECTAGG(c0, val) from test`,
		// DuckDB ->> removes one quoting layer; these JSON strings contain another.
		`select col1->>'$.key2' from t;`,
		`select pk, col1 from t where col1->>'$.key2' = 'abc';`,
		// DuckDB and MySQL use different JSON value ordering.
		"select * from t order by col1 asc;",
		"select * from t order by col1 desc;",
		// DuckDB preserves the original JSON text formatting and object key order.
		"select pk, cast(col1 as char) from t order by pk asc;",
		// DuckDB wildcard extraction returns LIST<JSON> and loses missing/null distinctions.
		"select pk, json_extract(col1, '$.items.*') from t order by pk;",
		// DuckDB has no compatible invalid-mode error for JSON_CONTAINS_PATH.
		"select pk, json_contains_path(col1, 'other', '$.c.d', '$.x') from t order by pk;",
		// DuckDB 1.1.3 has no JSON_INSERT function.
		"select pk, json_insert(col1, '$.x', 1), json_insert(col1, '$.y', 2) from t order by pk;",
	)
	var skippedTests []string = nil
	enginetest.TestJsonScripts(t, harness, skippedTests)
}

func TestShowTableStatus(t *testing.T) {
	enginetest.TestShowTableStatus(t, NewDefaultDuckHarness())
}

func TestAddDropPks(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestAddDropPks(t, NewDefaultDuckHarness())
}

func TestAddAutoIncrementColumn(t *testing.T) {
	for _, script := range queries.AlterTableAddAutoIncrementScripts {
		// https://github.com/duckdb/duckdb/pull/14419
		if strings.Contains(script.Name, "no primary key") || strings.Contains(script.Name, "no key") {
			enginetest.TestScript(t, NewDefaultDuckHarness(), script)
		}
	}
}

func TestNullRanges(t *testing.T) {
	enginetest.TestNullRanges(t, NewDefaultDuckHarness())
}

func TestBlobs(t *testing.T) {
	harness := NewDefaultDuckHarness()
	// DuckDB column defaults cannot reference another column.
	harness.QueriesToSkip(
		"ALTER TABLE blobt ADD COLUMN v2 BIGINT DEFAULT (i + 2) AFTER b",
		"ALTER TABLE textt ADD COLUMN v2 BIGINT DEFAULT (i + 2) AFTER t",
	)
	enginetest.TestBlobs(t, harness)
}

func TestIndexes(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestIndexes(t, NewDefaultDuckHarness())
}

func TestIndexPrefix(t *testing.T) {
	// DuckDB indexes full values and does not implement MySQL prefix-key semantics.
	t.Skip("DuckDB does not support MySQL prefix-key semantics")
	enginetest.TestIndexPrefix(t, NewDefaultDuckHarness())
}

func TestPersist(t *testing.T) {
	harness := NewDefaultDuckHarness()
	if harness.IsUsingServer() {
		t.Skip("this test depends on Context, which ServerEngine does not depend on or update the current context")
	}
	newSess := func(_ *sql.Context) sql.PersistableSession {
		ctx := harness.NewSession()
		persistedGlobals := memory.GlobalsMap{}
		memSession := ctx.Session.(*backend.Session).SetGlobals(persistedGlobals)
		return memSession
	}
	enginetest.TestPersist(t, harness, newSess)
}

func TestValidateSession(t *testing.T) {
	count := 0
	incrementValidateCb := func() {
		count++
	}

	harness := NewDefaultDuckHarness()
	if harness.IsUsingServer() {
		t.Skip("It depends on ValidateSession() method call on context")
	}
	newSess := func(ctx *sql.Context) sql.PersistableSession {
		memSession := ctx.Session.(*backend.Session)
		memSession.SetValidationCallback(incrementValidateCb)
		return memSession
	}
	enginetest.TestValidateSession(t, harness, newSess, &count)
}

func TestPrepared(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestPrepared(t, NewDefaultDuckHarness())
}

func TestPreparedInsert(t *testing.T) {
	harness := NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	harness.QueriesToSkip(
		// Bindings are not forwarded for general INSERTs. ON DUPLICATE KEY and foreign keys are unsupported.
		"simple insert",
		"Insert on duplicate key",
		"Insert on duplicate key with row alias",
		"Out-of-order Insert on duplicate key with row alias",
		"Insert on duplicate key with row and column alias",
		"Out-of-order Insert on duplicate key with row and column alias",
		"inserts should trigger string conversion errors",
	)
	enginetest.TestPreparedInsert(t, harness)
}

func TestPreparedStatements(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestPreparedStatements(t, NewDefaultDuckHarness())
}

func TestCharsetCollationEngine(t *testing.T) {
	t.Skip("wait for fix")
	harness := NewDefaultDuckHarness()
	if harness.IsUsingServer() {
		// Note: charset introducer needs to be handled with the SQLVal when preparing
		//  e.g. what we do currently for `_utf16'hi'` is `_utf16 :v1` with v1 = "hi", instead of `:v1` with v1 = "_utf16'hi'".
		t.Skip("way we prepare the queries with injectBindVarsAndPrepare() method does not work for ServerEngine test")
	}
	enginetest.TestCharsetCollationEngine(t, harness)
}

func TestCharsetCollationWire(t *testing.T) {
	t.Skip("wait for fix")
	if _, ok := os.LookupEnv("CI_TEST"); !ok {
		t.Skip("Skipping test that requires CI_TEST=true")
	}
	harness := NewDefaultDuckHarness()
	enginetest.TestCharsetCollationWire(t, harness, harness.SessionBuilder())
}

func TestDatabaseCollationWire(t *testing.T) {
	t.Skip("wait for fix")
	if _, ok := os.LookupEnv("CI_TEST"); !ok {
		t.Skip("Skipping test that requires CI_TEST=true")
	}
	harness := NewDefaultDuckHarness()
	enginetest.TestDatabaseCollationWire(t, harness, harness.SessionBuilder())
}

func TestTypesOverWire(t *testing.T) {
	t.Skip("wait for fix")
	if _, ok := os.LookupEnv("CI_TEST"); !ok {
		t.Skip("Skipping test that requires CI_TEST=true")
	}
	harness := NewDefaultDuckHarness()
	enginetest.TestTypesOverWire(t, harness, harness.SessionBuilder())
}

func TestTransactions(t *testing.T) {
	t.Skip("wait for support")
	enginetest.TestTransactionScripts(t, NewSkippingDuckHarness())
}

func mergableIndexDriver(dbs []sql.Database) sql.IndexDriver {
	return memory.NewIndexDriver("mydb", map[string][]sql.DriverIndex{
		"mytable": {
			newMergableIndex(dbs, "mytable",
				expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "i", false)),
			newMergableIndex(dbs, "mytable",
				expression.NewGetFieldWithTable(1, 1, types.Text, "db", "mytable", "s", false)),
			newMergableIndex(dbs, "mytable",
				expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "mytable", "i", false),
				expression.NewGetFieldWithTable(1, 1, types.Text, "db", "mytable", "s", false)),
		},
		"othertable": {
			newMergableIndex(dbs, "othertable",
				expression.NewGetFieldWithTable(0, 1, types.Text, "db", "othertable", "s2", false)),
			newMergableIndex(dbs, "othertable",
				expression.NewGetFieldWithTable(1, 1, types.Text, "db", "othertable", "i2", false)),
			newMergableIndex(dbs, "othertable",
				expression.NewGetFieldWithTable(0, 1, types.Text, "db", "othertable", "s2", false),
				expression.NewGetFieldWithTable(1, 1, types.Text, "db", "othertable", "i2", false)),
		},
		"bigtable": {
			newMergableIndex(dbs, "bigtable",
				expression.NewGetFieldWithTable(0, 1, types.Text, "db", "bigtable", "t", false)),
		},
		"floattable": {
			newMergableIndex(dbs, "floattable",
				expression.NewGetFieldWithTable(2, 1, types.Text, "db", "floattable", "f64", false)),
		},
		"niltable": {
			newMergableIndex(dbs, "niltable",
				expression.NewGetFieldWithTable(0, 1, types.Int64, "db", "niltable", "i", false)),
			newMergableIndex(dbs, "niltable",
				expression.NewGetFieldWithTable(1, 1, types.Int64, "db", "niltable", "i2", true)),
		},
		"one_pk": {
			newMergableIndex(dbs, "one_pk",
				expression.NewGetFieldWithTable(0, 1, types.Int8, "db", "one_pk", "pk", false)),
		},
		"two_pk": {
			newMergableIndex(dbs, "two_pk",
				expression.NewGetFieldWithTable(0, 1, types.Int8, "db", "two_pk", "pk1", false),
				expression.NewGetFieldWithTable(1, 1, types.Int8, "db", "two_pk", "pk2", false),
			),
		},
	})
}

func newMergableIndex(dbs []sql.Database, tableName string, exprs ...sql.Expression) *memory.Index {
	db, table := findTable(dbs, tableName)
	if db == nil {
		return nil
	}
	return &memory.Index{
		DB:         db.Name(),
		DriverName: memory.IndexDriverId,
		TableName:  tableName,
		Tbl:        table.(*memory.Table),
		Exprs:      exprs,
	}
}

func findTable(dbs []sql.Database, tableName string) (sql.Database, sql.Table) {
	for _, db := range dbs {
		names, err := db.GetTableNames(sql.NewEmptyContext())
		if err != nil {
			panic(err)
		}
		for _, name := range names {
			if name == tableName {
				table, _, _ := db.GetTableInsensitive(sql.NewEmptyContext(), name)
				return db, table
			}
		}
	}
	return nil, nil
}

func TestSQLLogicTests(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestSQLLogicTests(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

// func TestSQLLogicTestFiles(t *testing.T) {
// 	t.Skip()
// 	h := memharness.NewMemoryHarness(NewDefaultMetaHarness())
// 	paths := []string{
// 		"./sqllogictest/testdata/join/join.txt",
// 		"./sqllogictest/testdata/join/subquery_correlated.txt",
// 	}
// 	logictest.RunTestFiles(h, paths...)
// }

func replaceQueryTestByQuery(tests []queries.QueryTest, targetQuery string, updatedTest queries.QueryTest) {
	for i, test := range tests {
		if test.Query == targetQuery {
			tests[i] = updatedTest
			return
		}
	}
}

func replaceQueryInScriptTest(tests []queries.ScriptTest, targetScript string, targetQuery string, updatedQuery string) {
	for _, test := range tests {
		if test.Name == targetScript {
			for i, setUp := range test.SetUpScript {
				if setUp == targetQuery {
					test.SetUpScript[i] = updatedQuery
					return
				}
			}

			for _, assertion := range test.Assertions {
				if assertion.Query == targetQuery {
					assertion.Query = updatedQuery
					return
				}
			}
		}
	}
}
