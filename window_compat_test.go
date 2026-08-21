package main

import (
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
	errors "gopkg.in/src-d/go-errors.v1"
)

type windowCompatCase struct {
	query  string
	expect []sql.Row
	err    *errors.Kind
	skip   string
}

func runWindowCompatCases(t *testing.T, e enginetest.QueryEngine, harness enginetest.Harness, ctx *sql.Context, cases []windowCompatCase) {
	for _, tc := range cases {
		tc := tc
		t.Run(tc.query, func(t *testing.T) {
			if tc.skip != "" {
				t.Skip(tc.skip)
			}
			if tc.err != nil {
				enginetest.AssertErr(t, e, harness, tc.query, nil, tc.err)
				return
			}
			enginetest.TestQueryWithContext(t, ctx, e, harness, tc.query, tc.expect, nil, nil, nil)
		})
	}
}

// The upstream frame suites call assertions directly, without a query skip hook.
// Keep their setup but run each assertion independently so incompatible cases stay visible.
func testWindowRangeFramesCompat(t *testing.T) {
	harness := NewDefaultDuckHarness()
	harness.Setup(setup.MydbData, setup.MytableData)
	e, err := harness.NewEngine(t)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx := enginetest.NewContext(harness)
	for _, query := range []string{
		"CREATE TABLE a (x INTEGER PRIMARY KEY, y INTEGER, z INTEGER)",
		"INSERT INTO a VALUES (0,0,0), (1,1,0), (2,2,0), (3,0,0), (4,1,0), (5,3,0)",
	} {
		enginetest.RunQueryWithContext(t, e, harness, ctx, query)
	}
	integerSumSkip := "DuckDB returns integer SUM window results as int64; upstream expects float64"
	cases := make([]windowCompatCase, 0, 32)
	for _, query := range []string{
		`SELECT sum(y) over (partition by z order by x range unbounded preceding) FROM a order by x`,
		`SELECT sum(y) over (partition by z order by x range current row) FROM a order by x`,
		`SELECT sum(y) over (partition by z order by x range 2 preceding) FROM a order by x`,
		`SELECT sum(y) over (partition by z order by x range between current row and 1 following) FROM a order by x`,
		`SELECT sum(y) over (partition by z order by x range between 1 preceding and current row) FROM a order by x`,
		`SELECT sum(y) over (partition by z order by x range between current row and 2 following) FROM a order by x`,
		`SELECT sum(y) over (partition by z order by x range between current row and current row) FROM a order by x`,
		`SELECT sum(y) over (partition by z order by x range between current row and unbounded following) FROM a order by x`,
		`SELECT sum(y) over (partition by z order by x range between 1 preceding and 1 following) FROM a order by x`,
		`SELECT sum(y) over (partition by z order by x range between 1 preceding and unbounded following) FROM a order by x`,
		`SELECT sum(y) over (partition by z order by x range between unbounded preceding and unbounded following) FROM a order by x`,
		`SELECT sum(y) over (partition by z order by x range between 2 preceding and 1 preceding) FROM a order by x`,
		`SELECT sum(y) over (partition by y range between unbounded preceding and unbounded following) FROM a order by x`,
		`SELECT sum(y) over (partition by y range between unbounded preceding and current row) FROM a order by x`,
		`SELECT sum(y) over (partition by y range between current row and unbounded following) FROM a order by x`,
		`SELECT sum(y) over (partition by y range between current row and current row) FROM a order by x`,
	} {
		cases = append(cases, windowCompatCase{query: query, skip: integerSumSkip})
	}
	for _, query := range []string{
		"CREATE TABLE b (x INTEGER PRIMARY KEY, y INTEGER, z INTEGER, date DATE)",
		"INSERT INTO b VALUES (0,0,0,'2022-01-26'), (1,0,0,'2022-01-27'), (2,0,0, '2022-01-28'), (3,1,0,'2022-01-29'), (4,1,0,'2022-01-30'), (5,3,0,'2022-01-31')",
	} {
		enginetest.RunQueryWithContext(t, e, harness, ctx, query)
	}
	for _, query := range []string{
		`SELECT sum(y) over (partition by z order by date range between interval 2 DAY preceding and interval 1 DAY preceding) FROM b order by x`,
		`SELECT sum(y) over (partition by z order by date range between interval 1 DAY preceding and interval 1 DAY following) FROM b order by x`,
		`SELECT sum(y) over (partition by z order by date range between interval 1 DAY following and interval 2 DAY following) FROM b order by x`,
		`SELECT sum(y) over (partition by z order by date range interval 1 DAY preceding) FROM b order by x`,
		`SELECT sum(y) over (partition by z order by date range between interval 1 DAY preceding and current row) FROM b order by x`,
		`SELECT sum(y) over (partition by z order by date range between interval 1 DAY preceding and unbounded following) FROM b order by x`,
		`SELECT sum(y) over (partition by z order by date range between unbounded preceding and interval 1 DAY following) FROM b order by x`,
	} {
		cases = append(cases, windowCompatCase{query: query, skip: integerSumSkip})
	}
	for _, query := range []string{
		"CREATE TABLE c (x INTEGER PRIMARY KEY, y INTEGER, z INTEGER, date DATE)",
		"INSERT INTO c VALUES (0,0,0,'2022-01-26'), (1,0,0,'2022-01-26'), (2,0,0, '2022-01-26'), (3,1,0,'2022-01-27'), (4,1,0,'2022-01-29'), (5,3,0,'2022-01-30'), (6,0,0, '2022-02-03'), (7,1,0,'2022-02-03'), (8,1,0,'2022-02-04'), (9,3,0,'2022-02-04')",
	} {
		enginetest.RunQueryWithContext(t, e, harness, ctx, query)
	}
	for _, query := range []string{
		`SELECT sum(y) over (partition by z order by date range between interval '2' DAY preceding and interval '1' DAY preceding) FROM c order by x`,
		`SELECT sum(y) over (partition by z order by date range between interval '1' DAY preceding and interval '1' DAY following) FROM c order by x`,
		`SELECT sum(y) over (partition by z order by date range between interval '1' DAY preceding and current row) FROM c order by x`,
		`SELECT sum(y) over (partition by z order by date range between unbounded preceding and interval '1' DAY following) FROM c order by x`,
	} {
		cases = append(cases, windowCompatCase{query: query, skip: integerSumSkip})
	}
	cases = append(cases,
		windowCompatCase{query: `SELECT avg(y) over (partition by z order by date range between interval '1' DAY preceding and unbounded following) FROM c order by x`, skip: "DuckDB truncates integer AVG window results to int64; MySQL expects fractional values"},
		windowCompatCase{query: `SELECT count(y) over (partition by z order by date range between interval '1' DAY following and interval '2' DAY following) FROM c order by x`, expect: []sql.Row{{1}, {1}, {1}, {1}, {1}, {0}, {2}, {2}, {0}, {0}}},
		windowCompatCase{query: `SELECT count(y) over (partition by z order by date range between interval '1' DAY preceding and interval '2' DAY following) FROM c order by x`, expect: []sql.Row{{4}, {4}, {4}, {5}, {2}, {2}, {4}, {4}, {4}, {4}}},
		windowCompatCase{query: `SELECT sum(y) over (partition by z range between unbounded preceding and interval '1' DAY following) FROM c order by x`, skip: "DuckDB rejects RANGE frames without an ORDER BY expression with a different binder error"},
		windowCompatCase{query: `SELECT sum(y) over (partition by z order by date range interval 'e' DAY preceding) FROM c order by x`, skip: "DuckDB reports invalid interval text as a conversion error instead of MySQL's ErrInvalidValue"},
	)
	runWindowCompatCases(t, e, harness, ctx, cases)
}

func testNamedWindowsCompat(t *testing.T) {
	harness := NewDefaultDuckHarness()
	harness.Setup(setup.MydbData)
	e, err := harness.NewEngine(t)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx := enginetest.NewContext(harness)
	for _, query := range []string{
		"CREATE TABLE a (x INTEGER PRIMARY KEY, y INTEGER, z INTEGER)",
		"INSERT INTO a VALUES (0,0,0), (1,1,0), (2,2,0), (3,0,0), (4,1,0), (5,3,0)",
	} {
		enginetest.RunQueryWithContext(t, e, harness, ctx, query)
	}
	integerSumSkip := "DuckDB returns integer SUM window results as int64; upstream expects float64"
	cases := []windowCompatCase{
		{query: `SELECT sum(y) over (w1) FROM a WINDOW w1 as (order by z) order by x`, skip: integerSumSkip},
		{query: `SELECT sum(y) over (w1) FROM a WINDOW w1 as (partition by z) order by x`, skip: integerSumSkip},
		{query: `SELECT sum(y) over w FROM a WINDOW w as (partition by z order by x rows unbounded preceding) order by x`, skip: integerSumSkip},
		{query: `SELECT sum(y) over w FROM a WINDOW w as (partition by z order by x rows current row) order by x`, skip: integerSumSkip},
		{query: `SELECT sum(y) over (w) FROM a WINDOW w as (partition by z order by x rows 2 preceding) order by x`, skip: integerSumSkip},
		{query: `SELECT row_number() over (w3) FROM a WINDOW w3 as (w2), w2 as (w1), w1 as (partition by z order by x) order by x`, skip: "DuckDB parser rejects multi-level named window inheritance syntax"},
		{query: `SELECT sum(y) over (w1 partition by x) FROM a WINDOW w1 as (partition by z) order by x`, err: sql.ErrInvalidWindowInheritance},
		{query: `SELECT sum(y) over (w1 order by x) FROM a WINDOW w1 as (order by z) order by x`, err: sql.ErrInvalidWindowInheritance},
		{query: `SELECT sum(y) over (w1 rows unbounded preceding) FROM a WINDOW w1 as (range unbounded preceding) order by x`, err: sql.ErrInvalidWindowInheritance},
		{query: `SELECT sum(y) over (w3) FROM a WINDOW w1 as (w2), w2 as (w3), w3 as (w1) order by x`, err: sql.ErrCircularWindowInheritance},
	}
	runWindowCompatCases(t, e, harness, ctx, cases)
}
