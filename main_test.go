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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
	_ "github.com/dolthub/go-mysql-server/sql/variables"
)

type indexBehaviorTestParams struct {
	name              string
	driverInitializer IndexDriverInitializer
	nativeIndexes     bool
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

func TestDebugHarness(t *testing.T) {
	// t.Skip("only used for debugging")

	harness := NewDuckHarness("debug", 1, 1, true, nil)

	setupData := []setup.SetupScript{{
		`create database if not exists mydb`,
		`use mydb`,
		"create table mytable (i bigint primary key, s varchar(20) comment 'column s' NOT NULL)",
	}}

	harness.Setup(setupData)
	engine, err := harness.NewEngine(t)
	require.NoError(t, err)

	engine.EngineAnalyzer().Debug = true
	engine.EngineAnalyzer().Verbose = true

	ctx := enginetest.NewContext(harness)
	_, iter, _, err := engine.Query(ctx, "create index mytable_i_s on mytable (i,s)")
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

// TestQueries tests the given queries on an engine under a variety of circumstances:
// 1) Partitioned tables / non partitioned tables
// 2) Mergeable / unmergeable / native / no indexes
// 3) Parallelism on / off
func TestQueries(t *testing.T) {
	for _, numPartitions := range numPartitionsVals {
		for _, indexBehavior := range indexBehaviors {
			for _, parallelism := range parallelVals {
				if parallelism == 1 && numPartitions == testNumPartitions && indexBehavior.name == "nativeIndexes" {
					// This case is covered by TestQueriesSimple
					continue
				}
				testName := fmt.Sprintf("partitions=%d,indexes=%v,parallelism=%v", numPartitions, indexBehavior.name, parallelism)
				harness := NewDuckHarness(testName, parallelism, numPartitions, indexBehavior.nativeIndexes, indexBehavior.driverInitializer)

				t.Run(testName, func(t *testing.T) {
					enginetest.TestQueries(t, harness)
				})
			}
		}
	}
}

// TestQueriesPreparedSimple runs the canonical test queries against a single threaded index enabled harness.
func TestQueriesPreparedSimple(t *testing.T) {
	harness := NewDefaultDuckHarness()
	if harness.IsUsingServer() {
		t.Skip("issue: https://github.com/dolthub/dolt/issues/6904 and https://github.com/dolthub/dolt/issues/6901")
	}
	enginetest.TestQueriesPrepared(t, harness)
}

// TestQueriesSimple runs the canonical test queries against a single threaded index enabled harness.
func TestQueriesSimple(t *testing.T) {
	harness := NewDefaultDuckHarness()
	enginetest.TestQueries(t, harness)
}

// TestJoinQueries runs the canonical test queries against a single threaded index enabled harness.
func TestJoinQueries(t *testing.T) {
	enginetest.TestJoinQueries(t, NewDefaultDuckHarness())
}

func TestLateralJoin(t *testing.T) {
	enginetest.TestLateralJoinQueries(t, NewDefaultDuckHarness())
}

// TestJoinPlanning runs join-specific tests for merge
func TestJoinPlanning(t *testing.T) {
	enginetest.TestJoinPlanning(t, NewDefaultDuckHarness())
}

// TestJoinOps runs join-specific tests for merge
func TestJoinOps(t *testing.T) {
	enginetest.TestJoinOps(t, NewDefaultDuckHarness(), enginetest.DefaultJoinOpTests)
}

func TestJoinStats(t *testing.T) {
	harness := NewDefaultDuckHarness()
	if harness.IsUsingServer() {
		t.Skip("join stats don't work with bindvars")
	}
	enginetest.TestJoinStats(t, harness)
}

// TestJSONTableQueries runs the canonical test queries against a single threaded index enabled harness.
func TestJSONTableQueries(t *testing.T) {
	enginetest.TestJSONTableQueries(t, NewDefaultDuckHarness())
}

// TestJSONTableScripts runs the canonical test queries against a single threaded index enabled harness.
func TestJSONTableScripts(t *testing.T) {
	enginetest.TestJSONTableScripts(t, NewDefaultDuckHarness())
}

// TestBrokenJSONTableScripts runs the canonical test queries against a single threaded index enabled harness.
func TestBrokenJSONTableScripts(t *testing.T) {
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
	enginetest.TestBrokenQueries(t, enginetest.NewSkippingMemoryHarness())
}

func TestQueryPlanTODOs(t *testing.T) {
	harness := enginetest.NewSkippingMemoryHarness()
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
	enginetest.TestAnsiQuotesSqlMode(t, NewDefaultDuckHarness())
}

func TestAnsiQuotesSqlModePrepared(t *testing.T) {
	harness := NewDefaultDuckHarness()
	if harness.IsUsingServer() {
		t.Skip("prepared test depend on context for current sql_mode information, but it does not get updated when using ServerEngine")
	}
	enginetest.TestAnsiQuotesSqlModePrepared(t, NewDefaultDuckHarness())
}

// Tests of choosing the correct execution plan independent of result correctness. Mostly useful for confirming that
// the right indexes are being used for joining tables.
func TestQueryPlans(t *testing.T) {
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
	t.Skip("tests are too slow")
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
	t.Skip("missing features")
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

func TestParallelismQueries(t *testing.T) {
	enginetest.TestParallelismQueries(t, NewDuckHarness("default", 2, testNumPartitions, true, nil))
}

func TestQueryErrors(t *testing.T) {
	enginetest.TestQueryErrors(t, NewDefaultDuckHarness())
}

func TestInfoSchema(t *testing.T) {
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
	enginetest.TestColumnAliases(t, NewDefaultDuckHarness())
}

func TestDerivedTableOuterScopeVisibility(t *testing.T) {
	harness := NewDefaultDuckHarness()
	enginetest.TestDerivedTableOuterScopeVisibility(t, harness)
}

func TestOrderByGroupBy(t *testing.T) {
	// TODO: window validation expecting error message
	enginetest.TestOrderByGroupBy(t, NewDefaultDuckHarness())
}

func TestAmbiguousColumnResolution(t *testing.T) {
	enginetest.TestAmbiguousColumnResolution(t, NewDefaultDuckHarness())
}

func TestInsertInto(t *testing.T) {
	harness := NewDefaultDuckHarness()
	enginetest.TestInsertInto(t, harness)
}

func TestInsertIgnoreInto(t *testing.T) {
	enginetest.TestInsertIgnoreInto(t, NewDefaultDuckHarness())
}

func TestInsertDuplicateKeyKeyless(t *testing.T) {
	enginetest.TestInsertDuplicateKeyKeyless(t, NewDefaultDuckHarness())
}

func TestIgnoreIntoWithDuplicateUniqueKeyKeyless(t *testing.T) {
	enginetest.TestIgnoreIntoWithDuplicateUniqueKeyKeyless(t, NewDefaultDuckHarness())
}

func TestInsertIntoErrors(t *testing.T) {
	enginetest.TestInsertIntoErrors(t, NewDefaultDuckHarness())
}

func TestBrokenInsertScripts(t *testing.T) {
	enginetest.TestBrokenInsertScripts(t, NewDefaultDuckHarness())
}

func TestGeneratedColumns(t *testing.T) {
	enginetest.TestGeneratedColumns(t, NewDefaultDuckHarness())
}

func TestGeneratedColumnPlans(t *testing.T) {
	enginetest.TestGeneratedColumnPlans(t, NewDefaultDuckHarness())
}

func TestSysbenchPlans(t *testing.T) {
	enginetest.TestSysbenchPlans(t, NewDefaultDuckHarness())
}

func TestStatistics(t *testing.T) {
	enginetest.TestStatistics(t, NewDefaultDuckHarness())
}

func TestStatisticIndexFilters(t *testing.T) {
	enginetest.TestStatisticIndexFilters(t, NewDefaultDuckHarness())
}

func TestSpatialInsertInto(t *testing.T) {
	enginetest.TestSpatialInsertInto(t, NewDefaultDuckHarness())
}

func TestLoadData(t *testing.T) {
	enginetest.TestLoadData(t, NewDefaultDuckHarness())
}

func TestLoadDataErrors(t *testing.T) {
	enginetest.TestLoadDataErrors(t, NewDefaultDuckHarness())
}

func TestLoadDataFailing(t *testing.T) {
	enginetest.TestLoadDataFailing(t, NewDefaultDuckHarness())
}

func TestSelectIntoFile(t *testing.T) {
	enginetest.TestSelectIntoFile(t, NewDefaultDuckHarness())
}

func TestReplaceInto(t *testing.T) {
	enginetest.TestReplaceInto(t, NewDefaultDuckHarness())
}

func TestReplaceIntoErrors(t *testing.T) {
	enginetest.TestReplaceIntoErrors(t, NewDefaultDuckHarness())
}

func TestUpdate(t *testing.T) {
	enginetest.TestUpdate(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestUpdateIgnore(t *testing.T) {
	enginetest.TestUpdateIgnore(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestUpdateErrors(t *testing.T) {
	// TODO different errors
	enginetest.TestUpdateErrors(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestOnUpdateExprScripts(t *testing.T) {
	enginetest.TestOnUpdateExprScripts(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialUpdate(t *testing.T) {
	enginetest.TestSpatialUpdate(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestDeleteFromErrors(t *testing.T) {
	enginetest.TestDeleteErrors(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialDeleteFrom(t *testing.T) {
	enginetest.TestSpatialDelete(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestTruncate(t *testing.T) {
	enginetest.TestTruncate(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestDeleteFrom(t *testing.T) {
	enginetest.TestDelete(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestConvert(t *testing.T) {
	enginetest.TestConvert(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestScripts(t *testing.T) {
	enginetest.TestScripts(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialScripts(t *testing.T) {
	enginetest.TestSpatialScripts(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialIndexScripts(t *testing.T) {
	enginetest.TestSpatialIndexScripts(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialIndexPlans(t *testing.T) {
	enginetest.TestSpatialIndexPlans(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestNumericErrorScripts(t *testing.T) {
	enginetest.TestNumericErrorScripts(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestUserPrivileges(t *testing.T) {
	harness := NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	if harness.IsUsingServer() {
		t.Skip("TestUserPrivileges test depend on Context to switch the user to run test queries")
	}
	enginetest.TestUserPrivileges(t, harness)
}

func TestUserAuthentication(t *testing.T) {
	harness := NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	if harness.IsUsingServer() {
		t.Skip("TestUserPrivileges test depend on Context to switch the user to run test queries")
	}
	enginetest.TestUserAuthentication(t, harness)
}

func TestPrivilegePersistence(t *testing.T) {
	enginetest.TestPrivilegePersistence(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestComplexIndexQueries(t *testing.T) {
	harness := NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver)
	enginetest.TestComplexIndexQueries(t, harness)
}

func TestTriggers(t *testing.T) {
	enginetest.TestTriggers(t, NewDefaultDuckHarness())
}

func TestShowTriggers(t *testing.T) {
	enginetest.TestShowTriggers(t, NewDefaultDuckHarness())
}

func TestBrokenTriggers(t *testing.T) {
	h := enginetest.NewSkippingMemoryHarness()
	for _, script := range queries.BrokenTriggerQueries {
		enginetest.TestScript(t, h, script)
	}
}

func TestStoredProcedures(t *testing.T) {
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
	enginetest.TestEvents(t, NewDefaultDuckHarness())
}

func TestTriggersErrors(t *testing.T) {
	enginetest.TestTriggerErrors(t, NewDefaultDuckHarness())
}

func TestCreateTable(t *testing.T) {
	enginetest.TestCreateTable(t, NewDefaultDuckHarness())
}

func TestRowLimit(t *testing.T) {
	enginetest.TestRowLimit(t, NewDefaultDuckHarness())
}

func TestDropTable(t *testing.T) {
	enginetest.TestDropTable(t, NewDefaultDuckHarness())
}

func TestRenameTable(t *testing.T) {
	enginetest.TestRenameTable(t, NewDefaultDuckHarness())
}

func TestRenameColumn(t *testing.T) {
	enginetest.TestRenameColumn(t, NewDefaultDuckHarness())
}

func TestAddColumn(t *testing.T) {
	enginetest.TestAddColumn(t, NewDefaultDuckHarness())
}

func TestModifyColumn(t *testing.T) {
	enginetest.TestModifyColumn(t, NewDefaultDuckHarness())
}

func TestDropColumn(t *testing.T) {
	enginetest.TestDropColumn(t, NewDefaultDuckHarness())
}

func TestDropColumnKeylessTables(t *testing.T) {
	enginetest.TestDropColumnKeylessTables(t, NewDefaultDuckHarness())
}

func TestCreateDatabase(t *testing.T) {
	enginetest.TestCreateDatabase(t, NewDefaultDuckHarness())
}

func TestPkOrdinalsDDL(t *testing.T) {
	enginetest.TestPkOrdinalsDDL(t, NewDefaultDuckHarness())
}

func TestPkOrdinalsDML(t *testing.T) {
	enginetest.TestPkOrdinalsDML(t, NewDefaultDuckHarness())
}

func TestDropDatabase(t *testing.T) {
	enginetest.TestDropDatabase(t, NewDefaultDuckHarness())
}

func TestCreateForeignKeys(t *testing.T) {
	enginetest.TestCreateForeignKeys(t, NewDefaultDuckHarness())
}

func TestDropForeignKeys(t *testing.T) {
	enginetest.TestDropForeignKeys(t, NewDefaultDuckHarness())
}

func TestForeignKeys(t *testing.T) {
	enginetest.TestForeignKeys(t, NewDefaultDuckHarness())
}

func TestFulltextIndexes(t *testing.T) {
	enginetest.TestFulltextIndexes(t, NewDefaultDuckHarness())
}

func TestCreateCheckConstraints(t *testing.T) {
	enginetest.TestCreateCheckConstraints(t, NewDefaultDuckHarness())
}

func TestChecksOnInsert(t *testing.T) {
	enginetest.TestChecksOnInsert(t, NewDefaultDuckHarness())
}

func TestChecksOnUpdate(t *testing.T) {
	enginetest.TestChecksOnUpdate(t, NewDefaultDuckHarness())
}

func TestDisallowedCheckConstraints(t *testing.T) {
	enginetest.TestDisallowedCheckConstraints(t, NewDefaultDuckHarness())
}

func TestDropCheckConstraints(t *testing.T) {
	enginetest.TestDropCheckConstraints(t, NewDefaultDuckHarness())
}

func TestReadOnly(t *testing.T) {
	enginetest.TestReadOnly(t, NewDefaultDuckHarness(), true /* testStoredProcedures */)
}

func TestViews(t *testing.T) {
	enginetest.TestViews(t, NewDefaultDuckHarness())
}

// func TestVersionedViews(t *testing.T) {
// 	enginetest.TestVersionedViews(t, NewDefaultMetaHarness())
// }

func TestNaturalJoin(t *testing.T) {
	enginetest.TestNaturalJoin(t, NewDefaultDuckHarness())
}

func TestWindowFunctions(t *testing.T) {
	enginetest.TestWindowFunctions(t, NewDefaultDuckHarness())
}

func TestWindowRangeFrames(t *testing.T) {
	enginetest.TestWindowRangeFrames(t, NewDefaultDuckHarness())
}

func TestNamedWindows(t *testing.T) {
	enginetest.TestNamedWindows(t, NewDefaultDuckHarness())
}

func TestNaturalJoinEqual(t *testing.T) {
	enginetest.TestNaturalJoinEqual(t, NewDefaultDuckHarness())
}

func TestNaturalJoinDisjoint(t *testing.T) {
	enginetest.TestNaturalJoinDisjoint(t, NewDefaultDuckHarness())
}

func TestInnerNestedInNaturalJoins(t *testing.T) {
	enginetest.TestInnerNestedInNaturalJoins(t, NewDefaultDuckHarness())
}

func TestColumnDefaults(t *testing.T) {
	enginetest.TestColumnDefaults(t, NewDefaultDuckHarness())
}

func TestAlterTable(t *testing.T) {
	enginetest.TestAlterTable(t, NewDefaultDuckHarness())
}

func TestDateParse(t *testing.T) {
	harness := NewDefaultDuckHarness()
	if harness.IsUsingServer() {
		t.Skip("issue: https://github.com/dolthub/dolt/issues/6901")
	}
	enginetest.TestDateParse(t, NewDefaultDuckHarness())
}

func TestJsonScripts(t *testing.T) {
	var skippedTests []string = nil
	enginetest.TestJsonScripts(t, NewDefaultDuckHarness(), skippedTests)
}

func TestShowTableStatus(t *testing.T) {
	enginetest.TestShowTableStatus(t, NewDefaultDuckHarness())
}

func TestAddDropPks(t *testing.T) {
	enginetest.TestAddDropPks(t, NewDefaultDuckHarness())
}

func TestAddAutoIncrementColumn(t *testing.T) {
	for _, script := range queries.AlterTableAddAutoIncrementScripts {
		enginetest.TestScript(t, NewDefaultDuckHarness(), script)
	}
}

func TestNullRanges(t *testing.T) {
	enginetest.TestNullRanges(t, NewDefaultDuckHarness())
}

func TestBlobs(t *testing.T) {
	enginetest.TestBlobs(t, NewDefaultDuckHarness())
}

func TestIndexes(t *testing.T) {
	enginetest.TestIndexes(t, NewDefaultDuckHarness())
}

func TestIndexPrefix(t *testing.T) {
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
		memSession := ctx.Session.(*memory.Session).SetGlobals(persistedGlobals)
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
		memSession := ctx.Session.(*memory.Session)
		memSession.SetValidationCallback(incrementValidateCb)
		return memSession
	}
	enginetest.TestValidateSession(t, harness, newSess, &count)
}

func TestPrepared(t *testing.T) {
	enginetest.TestPrepared(t, NewDefaultDuckHarness())
}

func TestPreparedInsert(t *testing.T) {
	enginetest.TestPreparedInsert(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestPreparedStatements(t *testing.T) {
	enginetest.TestPreparedStatements(t, NewDefaultDuckHarness())
}

func TestCharsetCollationEngine(t *testing.T) {
	harness := NewDefaultDuckHarness()
	if harness.IsUsingServer() {
		// Note: charset introducer needs to be handled with the SQLVal when preparing
		//  e.g. what we do currently for `_utf16'hi'` is `_utf16 :v1` with v1 = "hi", instead of `:v1` with v1 = "_utf16'hi'".
		t.Skip("way we prepare the queries with injectBindVarsAndPrepare() method does not work for ServerEngine test")
	}
	enginetest.TestCharsetCollationEngine(t, harness)
}

func TestCharsetCollationWire(t *testing.T) {
	if _, ok := os.LookupEnv("CI_TEST"); !ok {
		t.Skip("Skipping test that requires CI_TEST=true")
	}
	harness := NewDefaultDuckHarness()
	enginetest.TestCharsetCollationWire(t, harness, harness.SessionBuilder())
}

func TestDatabaseCollationWire(t *testing.T) {
	if _, ok := os.LookupEnv("CI_TEST"); !ok {
		t.Skip("Skipping test that requires CI_TEST=true")
	}
	harness := NewDefaultDuckHarness()
	enginetest.TestDatabaseCollationWire(t, harness, harness.SessionBuilder())
}

func TestTypesOverWire(t *testing.T) {
	if _, ok := os.LookupEnv("CI_TEST"); !ok {
		t.Skip("Skipping test that requires CI_TEST=true")
	}
	harness := NewDefaultDuckHarness()
	enginetest.TestTypesOverWire(t, harness, harness.SessionBuilder())
}

func TestTransactions(t *testing.T) {
	enginetest.TestTransactionScripts(t, enginetest.NewSkippingMemoryHarness())
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
