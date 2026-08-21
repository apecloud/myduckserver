// Copyright 2023 Dolthub, Inc.
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
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func testWindowFunctionsCompat(t *testing.T) {
	harness := NewDefaultDuckHarness()
	harness.Setup(setup.MydbData)
	e := mustNewEngine(t, harness)
	defer e.Close()
	ctx := enginetest.NewContext(harness)

	enginetest.RunQueryWithContext(t, e, harness, ctx, "CREATE TABLE empty_tbl (a int, b int)")
	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, rank() over (order by b) FROM empty_tbl order by a`, expect: []sql.Row{}}})
	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, dense_rank() over (order by b) FROM empty_tbl order by a`, expect: []sql.Row{}}})
	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, percent_rank() over (order by b) FROM empty_tbl order by a`, expect: []sql.Row{}}})

	enginetest.RunQueryWithContext(t, e, harness, ctx, "CREATE TABLE results (name varchar(20), subject varchar(20), mark int)")
	enginetest.RunQueryWithContext(t, e, harness, ctx, "INSERT INTO results VALUES ('Pratibha', 'Maths', 100),('Ankita','Science',80),('Swarna','English',100),('Ankita','Maths',65),('Pratibha','Science',80),('Swarna','Science',50),('Pratibha','English',70),('Swarna','Maths',85),('Ankita','English',90)")

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT subject, name, mark, rank() OVER (partition by subject order by mark desc ) FROM results order by subject, mark desc, name`, expect: []sql.Row{
		{"English", "Swarna", 100, uint64(1)},
		{"English", "Ankita", 90, uint64(2)},
		{"English", "Pratibha", 70, uint64(3)},
		{"Maths", "Pratibha", 100, uint64(1)},
		{"Maths", "Swarna", 85, uint64(2)},
		{"Maths", "Ankita", 65, uint64(3)},
		{"Science", "Ankita", 80, uint64(1)},
		{"Science", "Pratibha", 80, uint64(1)},
		{"Science", "Swarna", 50, uint64(3)},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT subject, name, mark, dense_rank() OVER (partition by subject order by mark desc ) FROM results order by subject, mark desc, name`, expect: []sql.Row{
		{"English", "Swarna", 100, uint64(1)},
		{"English", "Ankita", 90, uint64(2)},
		{"English", "Pratibha", 70, uint64(3)},
		{"Maths", "Pratibha", 100, uint64(1)},
		{"Maths", "Swarna", 85, uint64(2)},
		{"Maths", "Ankita", 65, uint64(3)},
		{"Science", "Ankita", 80, uint64(1)},
		{"Science", "Pratibha", 80, uint64(1)},
		{"Science", "Swarna", 50, uint64(2)},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT subject, name, mark, percent_rank() OVER (partition by subject order by mark desc ) FROM results order by subject, mark desc, name`, expect: []sql.Row{
		{"English", "Swarna", 100, float64(0)},
		{"English", "Ankita", 90, float64(0.5)},
		{"English", "Pratibha", 70, float64(1)},
		{"Maths", "Pratibha", 100, float64(0)},
		{"Maths", "Swarna", 85, float64(0.5)},
		{"Maths", "Ankita", 65, float64(1)},
		{"Science", "Ankita", 80, float64(0)},
		{"Science", "Pratibha", 80, float64(0)},
		{"Science", "Swarna", 50, float64(1)},
	}}})

	enginetest.RunQueryWithContext(t, e, harness, ctx, "CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER, c integer)")
	enginetest.RunQueryWithContext(t, e, harness, ctx, "INSERT INTO t1 VALUES (0,0,0), (1,1,1), (2,2,0), (3,0,0), (4,1,0), (5,3,0)")

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, percent_rank() over (order by b) FROM t1 order by a`, expect: []sql.Row{
		{0, 0.0},
		{1, 0.4},
		{2, 0.8},
		{3, 0.0},
		{4, 0.4},
		{5, 1.0},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, rank() over (order by b) FROM t1 order by a`, expect: []sql.Row{
		{0, uint64(1)},
		{1, uint64(3)},
		{2, uint64(5)},
		{3, uint64(1)},
		{4, uint64(3)},
		{5, uint64(6)},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, dense_rank() over (order by b) FROM t1 order by a`, expect: []sql.Row{
		{0, uint64(1)},
		{1, uint64(2)},
		{2, uint64(3)},
		{3, uint64(1)},
		{4, uint64(2)},
		{5, uint64(4)},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, percent_rank() over (order by b desc) FROM t1 order by a`, expect: []sql.Row{
		{0, 0.8},
		{1, 0.4},
		{2, 0.2},
		{3, 0.8},
		{4, 0.4},
		{5, 0.0},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, rank() over (order by b desc) FROM t1 order by a`, expect: []sql.Row{
		{0, uint64(5)},
		{1, uint64(3)},
		{2, uint64(2)},
		{3, uint64(5)},
		{4, uint64(3)},
		{5, uint64(1)},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, dense_rank() over (order by b desc) FROM t1 order by a`, expect: []sql.Row{
		{0, uint64(4)},
		{1, uint64(3)},
		{2, uint64(2)},
		{3, uint64(4)},
		{4, uint64(3)},
		{5, uint64(1)},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, percent_rank() over (partition by c order by b) FROM t1 order by a`, expect: []sql.Row{
		{0, 0.0},
		{1, 0.0},
		{2, 0.75},
		{3, 0.0},
		{4, 0.5},
		{5, 1.0},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, rank() over (partition by c order by b) FROM t1 order by a`, expect: []sql.Row{
		{0, uint64(1)},
		{1, uint64(1)},
		{2, uint64(4)},
		{3, uint64(1)},
		{4, uint64(3)},
		{5, uint64(5)},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, dense_rank() over (partition by c order by b) FROM t1 order by a`, expect: []sql.Row{
		{0, uint64(1)},
		{1, uint64(1)},
		{2, uint64(3)},
		{3, uint64(1)},
		{4, uint64(2)},
		{5, uint64(4)},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, percent_rank() over (partition by b order by c) FROM t1 order by a`, expect: []sql.Row{
		{0, 0.0},
		{1, 1.0},
		{2, 0.0},
		{3, 0.0},
		{4, 0.0},
		{5, 0.0},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, rank() over (partition by b order by c) FROM t1 order by a`, expect: []sql.Row{
		{0, uint64(1)},
		{1, uint64(2)},
		{2, uint64(1)},
		{3, uint64(1)},
		{4, uint64(1)},
		{5, uint64(1)},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, dense_rank() over (partition by b order by c) FROM t1 order by a`, expect: []sql.Row{
		{0, uint64(1)},
		{1, uint64(2)},
		{2, uint64(1)},
		{3, uint64(1)},
		{4, uint64(1)},
		{5, uint64(1)},
	}}})

	// no order by clause -> all rows are peers
	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, percent_rank() over (partition by b) FROM t1 order by a`, expect: []sql.Row{
		{0, 0.0},
		{1, 0.0},
		{2, 0.0},
		{3, 0.0},
		{4, 0.0},
		{5, 0.0},
	}}})

	// no order by clause -> all rows are peers
	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, rank() over (partition by b) FROM t1 order by a`, expect: []sql.Row{
		{0, uint64(1)},
		{1, uint64(1)},
		{2, uint64(1)},
		{3, uint64(1)},
		{4, uint64(1)},
		{5, uint64(1)},
	}}})

	// no order by clause -> all rows are peers
	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, dense_rank() over (partition by b) FROM t1 order by a`, expect: []sql.Row{
		{0, uint64(1)},
		{1, uint64(1)},
		{2, uint64(1)},
		{3, uint64(1)},
		{4, uint64(1)},
		{5, uint64(1)},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, first_value(b) over (partition by c order by b) FROM t1 order by a`, expect: []sql.Row{
		{0, 0},
		{1, 1},
		{2, 0},
		{3, 0},
		{4, 0},
		{5, 0},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, first_value(a) over (partition by b order by a ASC, c ASC) FROM t1 order by a`, expect: []sql.Row{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 0},
		{4, 1},
		{5, 5},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, first_value(a-1) over (partition by b order by a ASC, c ASC) FROM t1 order by a`, expect: []sql.Row{
		{0, -1},
		{1, 0},
		{2, 1},
		{3, -1},
		{4, 0},
		{5, 4},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, first_value(c) over (partition by b order by a) FROM t1 order by a*b,a`, expect: []sql.Row{
		{0, 0},
		{3, 0},
		{1, 1},
		{2, 0},
		{4, 1},
		{5, 0},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, lead(a) over (partition by c order by a) FROM t1 order by a`, expect: []sql.Row{
		{0, 2},
		{1, nil},
		{2, 3},
		{3, 4},
		{4, 5},
		{5, nil},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, lead(a, 1) over (partition by c order by a) FROM t1 order by a`, expect: []sql.Row{
		{0, 2},
		{1, nil},
		{2, 3},
		{3, 4},
		{4, 5},
		{5, nil},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, lead(a+2) over (partition by c order by a) FROM t1 order by a`, expect: []sql.Row{
		{0, 4},
		{1, nil},
		{2, 5},
		{3, 6},
		{4, 7},
		{5, nil},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, lead(a, 1, a-1) over (partition by c order by a) FROM t1 order by a`, expect: []sql.Row{
		{0, 2},
		{1, 0},
		{2, 3},
		{3, 4},
		{4, 5},
		{5, 4},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, lead(a, 0) over (partition by c order by a) FROM t1 order by a`, expect: []sql.Row{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 3},
		{4, 4},
		{5, 5},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, lead(a, 1, -1) over (partition by c order by a) FROM t1 order by a`, expect: []sql.Row{
		{0, 2},
		{1, -1},
		{2, 3},
		{3, 4},
		{4, 5},
		{5, -1},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, lead(a, 3, -1) over (partition by c order by a) FROM t1 order by a`, expect: []sql.Row{
		{0, 4},
		{1, -1},
		{2, 5},
		{3, -1},
		{4, -1},
		{5, -1},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, lead('s') over (partition by c order by a) FROM t1 order by a`, expect: []sql.Row{
		{0, "s"},
		{1, nil},
		{2, "s"},
		{3, "s"},
		{4, "s"},
		{5, nil},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, last_value(b) over (partition by c order by b) FROM t1 order by a`, expect: []sql.Row{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 0},
		{4, 1},
		{5, 3},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, last_value(a) over (partition by b order by a ASC, c ASC) FROM t1 order by a`, expect: []sql.Row{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 3},
		{4, 4},
		{5, 5},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, last_value(a-1) over (partition by b order by a ASC, c ASC) FROM t1 order by a`, expect: []sql.Row{
		{0, -1},
		{1, 0},
		{2, 1},
		{3, 2},
		{4, 3},
		{5, 4},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, last_value(c) over (partition by b order by c) FROM t1 order by a*b,a`, expect: []sql.Row{
		{0, 0},
		{3, 0},
		{1, 1},
		{2, 0},
		{4, 0},
		{5, 0},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, lag(a) over (partition by c order by a) FROM t1 order by a`, expect: []sql.Row{
		{0, nil},
		{1, nil},
		{2, 0},
		{3, 2},
		{4, 3},
		{5, 4},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, lag(a, 1) over (partition by c order by a) FROM t1 order by a`, expect: []sql.Row{
		{0, nil},
		{1, nil},
		{2, 0},
		{3, 2},
		{4, 3},
		{5, 4},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, lag(a+2) over (partition by c order by a) FROM t1 order by a`, expect: []sql.Row{
		{0, nil},
		{1, nil},
		{2, 2},
		{3, 4},
		{4, 5},
		{5, 6},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, lag(a, 1, a-1) over (partition by c order by a) FROM t1 order by a`, expect: []sql.Row{
		{0, -1},
		{1, 0},
		{2, 0},
		{3, 2},
		{4, 3},
		{5, 4},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, lag(a, 0) over (partition by c order by a) FROM t1 order by a`, expect: []sql.Row{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 3},
		{4, 4},
		{5, 5},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, lag(a, 1, -1) over (partition by c order by a) FROM t1 order by a`, expect: []sql.Row{
		{0, -1},
		{1, -1},
		{2, 0},
		{3, 2},
		{4, 3},
		{5, 4},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, lag(a, 3, -1) over (partition by c order by a) FROM t1 order by a`, expect: []sql.Row{
		{0, -1},
		{1, -1},
		{2, -1},
		{3, -1},
		{4, 0},
		{5, 2},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, lag('s') over (partition by c order by a) FROM t1 order by a`, expect: []sql.Row{
		{0, nil},
		{1, nil},
		{2, "s"},
		{3, "s"},
		{4, "s"},
		{5, "s"},
	}}})

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: "SELECT a, lag(a, -1) over (partition by c) FROM t1", err: expression.ErrInvalidOffset}})
	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: "SELECT a, lag(a, 's') over (partition by c) FROM t1", err: expression.ErrInvalidOffset}})

	enginetest.RunQueryWithContext(t, e, harness, ctx, "CREATE TABLE t2 (a int, b int, c int)")
	enginetest.RunQueryWithContext(t, e, harness, ctx, "INSERT INTO t2 VALUES (1,1,1), (3,2,2), (7,4,5)")
	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT bit_and(a), bit_or(b), bit_xor(c) FROM t2`, skip: "DuckDB returns signed BIGINT results; MySQL expects unsigned 64-bit integers", expect: []sql.Row{
		{uint64(1), uint64(7), uint64(6)},
	}}})

	enginetest.RunQueryWithContext(t, e, harness, ctx, "CREATE TABLE t3 (x varchar(100))")
	enginetest.RunQueryWithContext(t, e, harness, ctx, "INSERT INTO t3 VALUES ('these'), ('are'), ('strings')")
	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT bit_and(x) from t3`, skip: "DuckDB rejects bit aggregates on VARCHAR; MySQL coerces strings to numbers", expect: []sql.Row{
		{uint64(0)},
	}}})
	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT bit_or(x) from t3`, skip: "DuckDB rejects bit aggregates on VARCHAR; MySQL coerces strings to numbers", expect: []sql.Row{
		{uint64(0)},
	}}})
	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT bit_xor(x) from t3`, skip: "DuckDB rejects bit aggregates on VARCHAR; MySQL coerces strings to numbers", expect: []sql.Row{
		{uint64(0)},
	}}})

	enginetest.RunQueryWithContext(t, e, harness, ctx, "CREATE TABLE t4 (x int)")
	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT bit_and(x) from t4`, skip: "DuckDB returns NULL on empty input; MySQL returns the all-bits-set identity", expect: []sql.Row{
		{^uint64(0)},
	}}})
	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT bit_or(x) from t4`, skip: "DuckDB returns NULL on empty input; MySQL returns the zero identity", expect: []sql.Row{
		{uint64(0)},
	}}})
	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT bit_xor(x) from t4`, skip: "DuckDB returns NULL on empty input; MySQL returns the zero identity", expect: []sql.Row{
		{uint64(0)},
	}}})

	enginetest.RunQueryWithContext(t, e, harness, ctx, "CREATE TABLE t5 (a INTEGER, b INTEGER)")
	enginetest.RunQueryWithContext(t, e, harness, ctx, "INSERT INTO t5 VALUES (0,0), (0,1), (1,0), (1,1)")

	runWindowCompatCases(t, e, harness, ctx, []windowCompatCase{{query: `SELECT a, b, row_number() over (partition by a, b) FROM t5 order by a, b`, expect: []sql.Row{
		{0, 0, 1},
		{0, 1, 1},
		{1, 0, 1},
		{1, 1, 1},
	}}})
}
