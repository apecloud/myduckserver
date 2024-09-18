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
	}

	// auto-generated by dev/extract_queries_to_skip.py
	waitForFixQueries := []string{
		"SELECT_SUM(i),_i_FROM_mytable_GROUP_BY_i_ORDER_BY_1+SUM(i)_ASC",
		"SELECT_SUM(i)_as_sum,_i_FROM_mytable_GROUP_BY_i_ORDER_BY_1+SUM(i)_ASC",
		"select_sum(10)_from_mytable",
		"_Select_x_from_(select_*_from_xy)_sq1_union_all_select_u_from_(select_*_from_uv)_sq2_limit_1_offset_1;",
		"select_count(*)_from_mytable_where_s_in_(1,_'first_row');",
		"SELECT_count(*),_i,_concat(i,_i),_123,_'abc',_concat('abc',_'def')_FROM_emptytable;",
		"SELECT_pk_FROM_one_pk_WHERE_(pk,_123)_IN_(SELECT_count(*)_AS_u,_123_AS_v_FROM_emptytable);",
		"SELECT_pk_FROM_one_pk_WHERE_(pk,_123)_IN_(SELECT_count(*)_AS_u,_123_AS_v_FROM_mytable_WHERE_false);",
		"SELECT_pk_FROM_one_pk_WHERE_(pk,_123)_NOT_IN_(SELECT_count(*)_AS_u,_123_AS_v_FROM_emptytable);",
		"SELECT_pk_FROM_one_pk_WHERE_(pk,_123)_NOT_IN_(SELECT_count(*)_AS_u,_123_AS_v_FROM_mytable_WHERE_false);",
		"SELECT_pk_DIV_2,_SUM(c3)_FROM_one_pk_GROUP_BY_1_ORDER_BY_1",
		"SELECT_pk_DIV_2,_SUM(c3)_as_sum_FROM_one_pk_GROUP_BY_1_ORDER_BY_1",
		"SELECT_pk_DIV_2,_SUM(c3)_+_sum(c3)_as_sum_FROM_one_pk_GROUP_BY_1_ORDER_BY_1",
		"SELECT_pk_DIV_2,_SUM(c3)_+_min(c3)_as_sum_and_min_FROM_one_pk_GROUP_BY_1_ORDER_BY_1",
		"SELECT_pk_DIV_2,_SUM(`c3`)_+____min(_c3_)_FROM_one_pk_GROUP_BY_1_ORDER_BY_1",
		"SELECT_pk1,_SUM(c1)_FROM_two_pk_GROUP_BY_pk1_ORDER_BY_pk1;",
		"SELECT_pk1,_SUM(c1)_FROM_two_pk_WHERE_pk1_=_0",
		"SELECT_floor(i),_s_FROM_mytable_mt_ORDER_BY_floor(i)_DESC",
		"SELECT_floor(i),_avg(char_length(s))_FROM_mytable_mt_group_by_1_ORDER_BY_floor(i)_DESC",
		"SELECT_FORMAT(i,_3)_FROM_mytable;",
		"SELECT_FORMAT(i,_3,_'da_DK')_FROM_mytable;",
		"SELECT_JSON_OVERLAPS(c3,_'{\"a\":_2,_\"d\":_2}')_FROM_jsontable",
		"SELECT_JSON_MERGE(c3,_'{\"a\":_1}')_FROM_jsontable",
		"SELECT_JSON_MERGE_PRESERVE(c3,_'{\"a\":_1}')_FROM_jsontable",
		"select_json_pretty(c3)_from_jsontable",
		"SELECT_i,_sum(i)_FROM_mytable_group_by_1_having_avg(i)_>_1_order_by_1",
		"SELECT_a.column_0,_mt.s_from_(values_row(1,\"1\"),_row(2,\"2\"),_row(4,\"4\"))_a____left_join_mytable_mt_on_column_0_=_mt.i____order_by_1",
		"WITH_mt_(s,i)_as_(select_char_length(s),_sum(i)_FROM_mytable_group_by_1)_SELECT_s,i_FROM_mt_order_by_1",
		"select_i+0.0/(lag(i)_over_(order_by_s))_from_mytable_order_by_1;",
		"select_f64/f32,_f32/(lag(i)_over_(order_by_f64))_from_floattable_order_by_1,2;",
		"SELECT_s2,_i2,_i____FROM_(SELECT_*_FROM_mytable)_mytable____RIGHT_JOIN_____((SELECT_i2,_s2_FROM_othertable_ORDER_BY_i2_ASC)______UNION_ALL______SELECT_CAST(4_AS_SIGNED)_AS_i2,_\"not_found\"_AS_s2_FROM_DUAL)_othertable____ON_i2_=_i",
		"WITH_mytable_as_(select_*_FROM_mytable)_SELECT_s,i_FROM_mytable;",
		"WITH_mytable_as_(select_*_FROM_mytable_where_i_>_2)_SELECT_*_FROM_mytable;",
		"WITH_mytable_as_(select_*_FROM_mytable_where_i_>_2)_SELECT_*_FROM_mytable_union_SELECT_*_from_mytable;",
		"____WITH_RECURSIVE_included_parts(sub_part,_part,_quantity)_AS_(_____SELECT_sub_part,_part,_quantity_FROM_parts_WHERE_part_=_'pie'______UNION_ALL_____SELECT_p.sub_part,_p.part,_p.quantity_____FROM_included_parts_AS_pr,_parts_AS_p_____WHERE_p.part_=_pr.sub_part____)____SELECT_sub_part,_sum(quantity)_as_total_quantity____FROM_included_parts____GROUP_BY_sub_part",
		"____WITH_RECURSIVE_included_parts(sub_part,_part,_quantity)_AS_(_____SELECT_sub_part,_part,_quantity_FROM_parts_WHERE_lower(part)_=_'pie'______UNION_ALL_____SELECT_p.sub_part,_p.part,_p.quantity_____FROM_included_parts_AS_pr,_parts_AS_p_____WHERE_p.part_=_pr.sub_part____)____SELECT_sub_part,_sum(quantity)_as_total_quantity____FROM_included_parts____GROUP_BY_sub_part",
		"____WITH_RECURSIVE_included_parts(sub_part,_part,_quantity)_AS_(_____SELECT_sub_part,_part,_quantity_FROM_parts_WHERE_part_=_(select_part_from_parts_where_part_=_'pie'_and_sub_part_=_'crust')______UNION_ALL_____SELECT_p.sub_part,_p.part,_p.quantity_____FROM_included_parts_AS_pr,_parts_AS_p_____WHERE_p.part_=_pr.sub_part____)____SELECT_sub_part,_sum(quantity)_as_total_quantity____FROM_included_parts____GROUP_BY_sub_part",
		"SELECT_i,_1_AS_foo,_2_AS_bar_FROM_MyTable_HAVING_bar_=_2_ORDER_BY_foo,_i;",
		"SELECT_i,_1_AS_foo,_2_AS_bar_FROM_MyTable_HAVING_bar_=_1_ORDER_BY_foo,_i;",
		"SELECT_reservedWordsTable.AND,_reservedWordsTABLE.Or,_reservedwordstable.SEleCT_FROM_reservedWordsTable;",
		"SELECT_*_from_mytable_where_(i_=_1_|_false)_IN_(true)",
		"SELECT_*_from_mytable_where_(i_=_1_&_false)_IN_(true)",
		"SELECT_i_FROM_mytable_WHERE_'hello';",
		"SELECT_i_FROM_mytable_WHERE_NOT_'hello';",
		"select_i_from_datetime_table_where_date_col_=_'2019-12-31T00:00:01'",
		"select_i_from_datetime_table_where_datetime_col_=_datetime('2020-01-01T12:00:00')",
		"select_i_from_datetime_table_where_datetime_col_>_datetime('2020-01-01T12:00:00')_order_by_1",
		"select_i_from_datetime_table_where_timestamp_col_=_datetime('2020-01-02T12:00:00')",
		"select_i_from_datetime_table_where_timestamp_col_>_datetime('2020-01-02T12:00:00')_order_by_1",
		"SELECT_SUBSTRING_INDEX(mytable.s,_\"d\",_1)_AS_s_FROM_mytable_INNER_JOIN_othertable_ON_(SUBSTRING_INDEX(mytable.s,_\"d\",_1)_=_SUBSTRING_INDEX(othertable.s2,_\"d\",_1))_GROUP_BY_1_HAVING_s_=_'secon';",
		"SELECT_SUBSTRING_INDEX(mytable.s,_\"d\",_1)_AS_s_FROM_mytable_INNER_JOIN_othertable_ON_(SUBSTRING_INDEX(mytable.s,_\"d\",_1)_=_SUBSTRING_INDEX(othertable.s2,_\"d\",_1))_GROUP_BY_s_HAVING_s_=_'secon';",
		"SELECT_SUBSTRING_INDEX(mytable.s,_\"d\",_1)_AS_ss_FROM_mytable_INNER_JOIN_othertable_ON_(SUBSTRING_INDEX(mytable.s,_\"d\",_1)_=_SUBSTRING_INDEX(othertable.s2,_\"d\",_1))_GROUP_BY_s_HAVING_s_=_'secon';",
		"SELECT_SUBSTRING_INDEX(mytable.s,_\"d\",_1)_AS_ss_FROM_mytable_INNER_JOIN_othertable_ON_(SUBSTRING_INDEX(mytable.s,_\"d\",_1)_=_SUBSTRING_INDEX(othertable.s2,_\"d\",_1))_GROUP_BY_ss_HAVING_ss_=_'secon';",
		"SELECT_TRIM(mytable.s_from_\"first_row\")_AS_s_FROM_mytable",
		"SELECT_HOUR('2007-12-11_20:21:22')_FROM_mytable",
		"SELECT_MINUTE('2007-12-11_20:21:22')_FROM_mytable",
		"SELECT_SECOND('2007-12-11_20:21:22')_FROM_mytable",
		"SELECT_SECOND('2007-12-11T20:21:22Z')_FROM_mytable",
		"SELECT_DAYOFYEAR('20071211')_FROM_mytable",
		"SELECT_DISTINCT_CAST(i_AS_DECIMAL)_from_mytable;",
		"SELECT_SUM(_DISTINCT_CAST(i_AS_DECIMAL))_from_mytable;",
		"SELECT_id_FROM_typestable_WHERE_da_<_adddate('2020-01-01',_INTERVAL_1_DAY)",
		"SELECT_id_FROM_typestable_WHERE_da_<_adddate('2020-01-01',_1)",
		"SELECT_id_FROM_typestable_WHERE_da_>=_subdate('2020-01-01',_INTERVAL_1_DAY)",
		"SELECT_id_FROM_typestable_WHERE_da_>=_subdate('2020-01-01',_1)",
		"SELECT_adddate(da,_i32)_from_typestable;",
		"SELECT_adddate(da,_concat(u32))_from_typestable;",
		"SELECT_adddate(da,_f32/10)_from_typestable;",
		"SELECT_subdate(da,_i32)_from_typestable;",
		"SELECT_subdate(da,_concat(u32))_from_typestable;",
		"SELECT_subdate(da,_f32/10)_from_typestable;",
		"SELECT_i,v_from_stringandtable_WHERE_v",
		"SELECT_i,v_from_stringandtable_WHERE_v_AND_v",
		"SELECT_i,v_from_stringandtable_WHERE_v_OR_v",
		"SELECT_i,v_from_stringandtable_WHERE_NOT_v",
		"SELECT_i,v_from_stringandtable_WHERE_NOT_v_AND_NOT_v",
		"SELECT_i,v_from_stringandtable_WHERE_NOT_v_OR_NOT_v",
		"SELECT_i,v_from_stringandtable_WHERE_v_OR_NOT_v",
		"SELECT_i,v_from_stringandtable_WHERE_v_XOR_v",
		"SELECT_i,v_from_stringandtable_WHERE_NOT_v_XOR_NOT_v",
		"SELECT_i,v_from_stringandtable_WHERE_v_XOR_NOT_v",
		"select_pk,_________row_number()_over_(order_by_pk_desc),_________sum(v1)_over_(partition_by_v2_order_by_pk),_________percent_rank()_over(partition_by_v2_order_by_pk)_____from_one_pk_three_idx_order_by_pk",
		"select_pk,____________________percent_rank()_over(partition_by_v2_order_by_pk),____________________dense_rank()_over(partition_by_v2_order_by_pk),____________________rank()_over(partition_by_v2_order_by_pk)_____from_one_pk_three_idx_order_by_pk",
		"SELECT_CAST(-3_AS_UNSIGNED)_FROM_mytable",
		"SELECT_CONVERT(-3,_UNSIGNED)_FROM_mytable",
		"SELECT_s_>_2_FROM_tabletest",
		"SELECT_*_FROM_tabletest_WHERE_s_>_0",
		"SELECT_*_FROM_tabletest_WHERE_s_=_0",
		"SELECT_i_AS_foo_FROM_mytable_HAVING_foo_NOT_IN_(1,_2,_5)",
		"SELECT_SUM(i)_FROM_mytable",
		"SELECT_RAND(i)_from_mytable_order_by_i",
		"SELECT_MOD(i,_2)_from_mytable_order_by_i_limit_1",
		"SELECT_ASIN(i)_from_mytable_order_by_i_limit_1",
		"SELECT_ACOS(i)_from_mytable_order_by_i_limit_1",
		"SELECT_CRC32(i)_from_mytable_order_by_i_limit_1",
		"SELECT_ASCII(s)_from_mytable_order_by_i_limit_1",
		"SELECT_UNHEX(s)_from_mytable_order_by_i_limit_1",
		"SELECT_BIT_LENGTH(i)_from_mytable_order_by_i_limit_1",
		"select_date_format(datetime_col,_'%D')_from_datetime_table_order_by_1",
		"select_time_format(time_col,_'%h%p')_from_datetime_table_order_by_1",
		"select_from_unixtime(i)_from_mytable_order_by_1",
		"SELECT_SUM(i)_+_1,_i_FROM_mytable_GROUP_BY_i_ORDER_BY_i",
		"SELECT_SUM(i)_as_sum,_i_FROM_mytable_GROUP_BY_i_ORDER_BY_sum_ASC",
		"SELECT_i,_SUM(i)_FROM_mytable_GROUP_BY_i_ORDER_BY_sum(i)_DESC",
		"SELECT_i,_SUM(i)_as_b_FROM_mytable_GROUP_BY_i_ORDER_BY_b_DESC",
		"SELECT_i,_SUM(i)_as_`sum(i)`_FROM_mytable_GROUP_BY_i_ORDER_BY_sum(i)_DESC",
		"SELECT_CASE_WHEN_i_>_2_THEN_i_WHEN_i_<_2_THEN_i_ELSE_'two'_END_FROM_mytable",
		"SELECT_CASE_WHEN_i_>_2_THEN_'more_than_two'_WHEN_i_<_2_THEN_'less_than_two'_ELSE_2_END_FROM_mytable",
		"SELECT_substring(mytable.s,_1,_5)_AS_s_FROM_mytable_INNER_JOIN_othertable_ON_(substring(mytable.s,_1,_5)_=_SUBSTRING(othertable.s2,_1,_5))_GROUP_BY_1_HAVING_s_=_\"secon\"",
		"SELECT_s,_i_FROM_mytable_GROUP_BY_i_ORDER_BY_SUBSTRING(s,_1,_1)_DESC",
		"SELECT_s,_i_FROM_mytable_GROUP_BY_i_HAVING_count(*)_>_0_ORDER_BY_SUBSTRING(s,_1,_1)_DESC",
		"SELECT_GREATEST(i,_s)_FROM_mytable",
		"select_md5(i)_from_mytable_order_by_1",
		"select_sha1(i)_from_mytable_order_by_1",
		"select_sha2(i,_256)_from_mytable_order_by_1",
		"select_octet_length(s)_from_mytable_order_by_i",
		"select_char_length(s)_from_mytable_order_by_i",
		"select_locate(upper(\"roW\"),_upper(s),_power(10,_0))_from_mytable_order_by_i",
		"select_log2(i)_from_mytable_order_by_i",
		"select_ln(i)_from_mytable_order_by_i",
		"select_log10(i)_from_mytable_order_by_i",
		"select_log(3,_i)_from_mytable_order_by_i",
		"SELECT_LEAST(i,_s)_FROM_mytable",
		"SELECT_i_FROM_mytable_WHERE_NOT_s_ORDER_BY_1_DESC",
		"SELECT_sum(i)_as_isum,_s_FROM_mytable_GROUP_BY_i_ORDER_BY_isum_ASC_LIMIT_0,_200",
		"SELECT_pk,_(SELECT_concat(pk,_pk)_FROM_one_pk_WHERE_pk_<_opk.pk_ORDER_BY_1_DESC_LIMIT_1)_as_strpk_FROM_one_pk_opk_having_strpk_>_\"0\"_ORDER_BY_2",
		"SELECT_pk,_(SELECT_max(pk)_FROM_one_pk_WHERE_pk_<_opk.pk)_AS_x_FROM_one_pk_opk_GROUP_BY_x_ORDER_BY_x",
		"SELECT_pk,_(SELECT_max(pk)_FROM_one_pk_WHERE_pk_<_opk.pk)_AS_x_______FROM_one_pk_opk_WHERE_(SELECT_max(pk)_FROM_one_pk_WHERE_pk_<_opk.pk)_>_0_______GROUP_BY_x_ORDER_BY_x",
		"SELECT_pk,_(SELECT_max(pk)_FROM_one_pk_WHERE_pk_<_opk.pk)_AS_x_______FROM_one_pk_opk_WHERE_(SELECT_max(pk)_FROM_one_pk_WHERE_pk_<_opk.pk)_>_0_______GROUP_BY_(SELECT_max(pk)_FROM_one_pk_WHERE_pk_<_opk.pk)_ORDER_BY_x",
		"SELECT_pk,_______(SELECT_sum(pk1+pk2)_FROM_two_pk_WHERE_pk1+pk2_IN_(SELECT_pk1+pk2_FROM_two_pk_WHERE_pk1+pk2_=_pk))_AS_sum,_______(SELECT_min(pk2)_FROM_two_pk_WHERE_pk2_IN_(SELECT_pk2_FROM_two_pk_WHERE_pk2_=_pk))_AS_equal_______FROM_one_pk_ORDER_BY_pk;",
		"SELECT_pk,_______(SELECT_sum(c1)_FROM_two_pk_WHERE_c1_+_3_IN_(SELECT_c4_FROM_two_pk_WHERE_c3_>_opk.c5))_AS_sum,_______(SELECT_sum(c1)_FROM_two_pk_WHERE_pk2_IN_(SELECT_pk2_FROM_two_pk_WHERE_c1_+_1_<_opk.c2))_AS_sum2______FROM_one_pk_opk_ORDER_BY_pk",
		"SELECT_DISTINCT_n_FROM_bigtable_ORDER_BY_t",
		"SELECT_GREATEST(CAST(i_AS_CHAR),_CAST(b_AS_CHAR))_FROM_niltable_order_by_i",
		"SELECT_count(*)_FROM_people_WHERE_last_name='doe'_and_first_name='jane'_order_by_dob",
		"SELECT_VALUES(i)_FROM_mytable",
		"select_i,_row_number()_over_(order_by_i_desc)_+_3,____row_number()_over_(order_by_length(s),i)_+_0.0_/_row_number()_over_(order_by_length(s)_desc,i_desc)_+_0.0____from_mytable_order_by_1;",
		"SELECT_pk,_row_number()_over_(partition_by_v2_order_by_pk_),_max(v3)_over_(partition_by_v2_order_by_pk)_FROM_one_pk_three_idx_ORDER_BY_pk",
		"SELECT_CONVERT_TZ(datetime_col,_\"+00:00\",_\"+04:00\")_FROM_datetime_table_WHERE_i_=_1",
		"SELECT_distinct_pk1_FROM_two_pk_WHERE_EXISTS_(SELECT_pk_from_one_pk_where_pk_<=_two_pk.pk1)",
		"select_c1_from_jsontable_where_c1_LIKE_(('%'_OR_'dsads')_OR_'%')",
		"select_c1_from_jsontable_where_c1_LIKE_('%'_OR_NULL)",
		"show_function_status",
		"show_function_status_like_'foo'",
		"show_function_status_where_Db='mydb'",
		"SELECT_CONV(i,_10,_2)_FROM_mytable",
		"select_find_in_set('second_row',_s)_from_mytable;",
		"select_find_in_set(s,_'first_row,second_row,third_row')_from_mytable;",
		"select_i_from_mytable_where_find_in_set(s,_'first_row,second_row,third_row')_=_2;",
		"____SELECT_COUNT(*)__FROM_keyless__WHERE_keyless.c0_IN_(_____WITH_RECURSIVE_cte(depth,_i,_j)_AS_(_______SELECT_0,_T1.c0,_T1.c1_______FROM_keyless_T1_______WHERE_T1.c0_=_0_________UNION_ALL_________SELECT_cte.depth_+_1,_cte.i,_T2.c1_+_1_______FROM_cte,_keyless_T2_______WHERE_cte.depth_=_T2.c0___)_____SELECT_U0.c0___FROM_keyless_U0,_cte___WHERE_cte.j_=_keyless.c0____)_____ORDER_BY_c0;_",
		"____SELECT_COUNT(*)__FROM_keyless__WHERE_keyless.c0_IN_(_____WITH_RECURSIVE_cte(depth,_i,_j)_AS_(_______SELECT_0,_T1.c0,_T1.c1_______FROM_keyless_T1_______WHERE_T1.c0_=_0_________UNION_ALL_________SELECT_cte.depth_+_1,_cte.i,_T2.c1_+_1_______FROM_cte,_keyless_T2_______WHERE_cte.depth_=_T2.c0___)_____SELECT_U0.c0___FROM_cte,_keyless_U0____WHERE_cte.j_=_keyless.c0_____)_____ORDER_BY_c0;_",
		"SELECT_pk1,_SUM(c1)_FROM_two_pk",
		"_select_*_from_mytable,__lateral_(__with_recursive_cte(a)_as_(___select_y_from_xy___union___select_x_from_cte___join___(____select_*_____from_xy____where_x_=_1____)_sqa1___on_x_=_a___limit_3___)__select_*_from_cte_)_sqa2_where_i_=_a_order_by_i;",
		"_select____dayname(id),____dayname(i8),____dayname(i16),____dayname(i32),____dayname(i64),____dayname(u8),____dayname(u16),____dayname(u32),____dayname(u64),____dayname(f32),____dayname(f64),____dayname(ti),____dayname(da),____dayname(te),____dayname(bo),____dayname(js),____dayname(bl),____dayname(e1),____dayname(s1)_from_typestable",
		"select_*_from_mytable_order_by_dayname(i)",
		"select_*_from_mytable_where_(i_BETWEEN_(CASE_1_WHEN_2_THEN_1.0_ELSE_(1||2)_END)_AND_i)",
		"select_*_from_mytable_where_(i_BETWEEN_(''_BETWEEN_''_AND_(''_OR_'#'))_AND_i)",
		"select_*_from_xy_inner_join_uv_on_(xy.x_in_(false_in_('asdf')));",
		"select_length(space(i))_from_mytable;",
		"select_concat(space(i),_'a')_from_mytable;",
		"select_space(i_*_2)_from_mytable;",
		"select_atan(i),_atan2(i,_i_+_2)_from_mytable;",
		"select_elt(i,_'a',_'b')_from_mytable;",
		"select_field(i,_'1',_'2',_'3')_from_mytable;",
		"select_char(i,_i_+_10,_pi())_from_mytable;",
		"select_length(random_bytes(i))_from_mytable;",
	}

	// failed during CI
	waitForFixQueries = append(waitForFixQueries,
		"SELECT_TAN(i)_from_mytable_order_by_i_limit_1", // might be precision issue
		"SELECT_COT(i)_from_mytable_order_by_i_limit_1",
		"select_now()_=_sysdate(),_sleep(0.1),_now(6)_<_sysdate(6);")

	panicQueries := []string{
		"SELECT_JSON_KEYS(c3)_FROM_jsontable",
		"SELECT_(SELECT_s_FROM_mytable_ORDER_BY_i_ASC_LIMIT_1)_AS_x",
	}

	harness.QueriesToSkip(notApplicableQueries...)
	harness.QueriesToSkip(waitForFixQueries...)
	harness.QueriesToSkip(panicQueries...)
	enginetest.TestQueries(t, harness)
}

// TestJoinQueries runs the canonical test queries against a single threaded index enabled harness.
func TestJoinQueries(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestJoinQueries(t, NewDefaultDuckHarness())
}

func TestLateralJoin(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestLateralJoinQueries(t, NewDefaultDuckHarness())
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
	t.Skip("wait for fix")
	enginetest.TestAnsiQuotesSqlMode(t, NewDefaultDuckHarness())
}

func TestAnsiQuotesSqlModePrepared(t *testing.T) {
	t.Skip("wait for fix")
	harness := NewDefaultDuckHarness()
	if harness.IsUsingServer() {
		t.Skip("prepared test depend on context for current sql_mode information, but it does not get updated when using ServerEngine")
	}
	enginetest.TestAnsiQuotesSqlModePrepared(t, NewDefaultDuckHarness())
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

func TestParallelismQueries(t *testing.T) {
	enginetest.TestParallelismQueries(t, NewDuckHarness("default", 2, testNumPartitions, true, nil))
}

func TestQueryErrors(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestQueryErrors(t, NewDefaultDuckHarness())
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
	t.Skip("wait for fix")
	enginetest.TestColumnAliases(t, NewDefaultDuckHarness())
}

func TestDerivedTableOuterScopeVisibility(t *testing.T) {
	t.Skip("wait for fix")
	harness := NewDefaultDuckHarness()
	enginetest.TestDerivedTableOuterScopeVisibility(t, harness)
}

func TestOrderByGroupBy(t *testing.T) {
	t.Skip("wait for fix")
	// TODO: window validation expecting error message
	enginetest.TestOrderByGroupBy(t, NewDefaultDuckHarness())
}

func TestAmbiguousColumnResolution(t *testing.T) {
	enginetest.TestAmbiguousColumnResolution(t, NewDefaultDuckHarness())
}

func TestInsertInto(t *testing.T) {
	t.Skip("wait for fix")
	harness := NewDefaultDuckHarness()
	enginetest.TestInsertInto(t, harness)
}

func TestInsertIgnoreInto(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestInsertIgnoreInto(t, NewDefaultDuckHarness())
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
	t.Skip("wait for fix")
	enginetest.TestInsertIntoErrors(t, NewDefaultDuckHarness())
}

func TestBrokenInsertScripts(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestBrokenInsertScripts(t, NewDefaultDuckHarness())
}

func TestGeneratedColumns(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestGeneratedColumns(t, NewDefaultDuckHarness())
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
		"LOAD DATA INFILE './testdata/test2.csv' IGNORE INTO TABLE loadtable FIELDS TERMINATED BY ',' IGNORE 1 LINES",
		"LOAD DATA INFILE './testdata/test2.csv' REPLACE INTO TABLE loadtable FIELDS TERMINATED BY ',' IGNORE 1 LINES",
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
	t.Skip("wait for fix")
	enginetest.TestReplaceInto(t, NewDefaultDuckHarness())
}

func TestReplaceIntoErrors(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestReplaceIntoErrors(t, NewDefaultDuckHarness())
}

func TestUpdate(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestUpdate(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestUpdateIgnore(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestUpdateIgnore(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestUpdateErrors(t *testing.T) {
	t.Skip("wait for fix")
	// TODO different errors
	enginetest.TestUpdateErrors(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestOnUpdateExprScripts(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestOnUpdateExprScripts(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialUpdate(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestSpatialUpdate(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestDeleteFromErrors(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestDeleteErrors(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestSpatialDeleteFrom(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestSpatialDelete(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestTruncate(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestTruncate(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestDeleteFrom(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestDelete(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
}

func TestConvert(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestConvert(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
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
	t.Skip("wait for fix")
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
		"CREATE_TABLE_t1_(a_INTEGER,_create_time_timestamp(6)_NOT_NULL_DEFAULT_NOW(6),_primary_key_(a))",
		"CREATE_TABLE_t1_(a_INTEGER,_create_time_timestamp(6)_NOT_NULL_DEFAULT_NOW(6),_primary_key_(a))",
		"CREATE_TABLE_t1_(____pk_bigint_primary_key,____v1_bigint_default_(2)_comment_'hi_there',____index_idx_v1_(v1)_comment_'index_here'____)",
		"CREATE_TABLE_t1_(____pk_bigint_primary_key,____v1_bigint_default_(2)_comment_'hi_there',____index_idx_v1_(v1)_comment_'index_here'____)",
		"CREATE_TABLE_t1_SELECT_*_from_mytable",
		"CREATE_TABLE_t1_SELECT_*_from_mytable",
		"CREATE_TABLE_t1_(i_int_primary_key,_j_int_auto_increment_unique)",
		"CREATE_TABLE_t1_(i_int_primary_key,_j_int_auto_increment_unique)",
		"CREATE_TABLE_t1_(i_int_primary_key,_j_int_auto_increment,_index_(j))",
		"CREATE_TABLE_t1_(i_int_primary_key,_j_int_auto_increment,_index_(j))",
		"CREATE_TABLE_t1_(i_int_primary_key,_j_int_auto_increment,_k_int,_unique(j,k))",
		"CREATE_TABLE_t1_(i_int_primary_key,_j_int_auto_increment,_k_int,_unique(j,k))",
		"CREATE_TABLE_t1_(i_int_primary_key,_j_int_auto_increment,_k_int,_index_(j,k))",
		"CREATE_TABLE_t1_(i_int_primary_key,_j_int_auto_increment,_k_int,_index_(j,k))",
		"CREATE_TABLE_t1_(_____pk_int_NOT_NULL,_____col1_blob_DEFAULT_(_utf8mb4'abc'),_____col2_json_DEFAULT_(json_object(_utf8mb4'a',1)),_____col3_text_DEFAULT_(_utf8mb4'abc'),_____PRIMARY_KEY_(pk)___)",
		"CREATE_TABLE_t1_(_____pk_int_NOT_NULL,_____col1_blob_DEFAULT_(_utf8mb4'abc'),_____col2_json_DEFAULT_(json_object(_utf8mb4'a',1)),_____col3_text_DEFAULT_(_utf8mb4'abc'),_____PRIMARY_KEY_(pk)___)",
		"CREATE_TABLE_td_(_____pk_int_PRIMARY_KEY,_____col2_int_NOT_NULL_DEFAULT_2,______col3_double_NOT_NULL_DEFAULT_(round(-(1.58),0)),_____col4_varchar(10)_DEFAULT_'new_row',___________col5_float_DEFAULT_33.33,___________col6_int_DEFAULT_NULL,_____col7_timestamp_DEFAULT_NOW(),_____col8_bigint_DEFAULT_(NOW())___)",
		"CREATE_TABLE_td_(_____pk_int_PRIMARY_KEY,_____col2_int_NOT_NULL_DEFAULT_2,______col3_double_NOT_NULL_DEFAULT_(round(-(1.58),0)),_____col4_varchar(10)_DEFAULT_'new_row',___________col5_float_DEFAULT_33.33,___________col6_int_DEFAULT_NULL,_____col7_timestamp_DEFAULT_NOW(),_____col8_bigint_DEFAULT_(NOW())___)",
		"create_table_t1_(i_int_primary_key,_b1_blob,_b2_blob,_index(b1(123),_b2(456)))",
		"create_table_t1_(i_int_primary_key,_b1_blob,_b2_blob,_index(b1(123),_b2(456)))",
		"create_table_t1_(i_int_primary_key,_b1_blob,_b2_blob,_unique_index(b1(123),_b2(456)))",
		"create_table_t1_(i_int_primary_key,_b1_blob,_b2_blob,_unique_index(b1(123),_b2(456)))",
		"create_table_t1_(i_int_primary_key,_b1_blob,_b2_blob,_index(b1(10)),_index(b2(20)),_index(b1(123),_b2(456)))",
		"create_table_t1_(i_int_primary_key,_b1_blob,_b2_blob,_index(b1(10)),_index(b2(20)),_index(b1(123),_b2(456)))",
		"CREATE_TABLE_t1_as_select_*_from_mytable",
		"CREATE_TABLE_t1_as_select_*_from_mytable",
		"CREATE_TABLE_t1_as_select_*_from_mytable#01",
		"CREATE_TABLE_t1_as_select_*_from_mytable",
		"CREATE_TABLE_t1_as_select_s,_i_from_mytable",
		"CREATE_TABLE_t1_as_select_s,_i_from_mytable",
		"CREATE_TABLE_t1_as_select_distinct_s,_i_from_mytable",
		"CREATE_TABLE_t1_as_select_distinct_s,_i_from_mytable",
		"CREATE_TABLE_t1_as_select_s,_i_from_mytable_order_by_s",
		"CREATE_TABLE_t1_as_select_s,_i_from_mytable_order_by_s",
		"CREATE_TABLE_t1_as_select_s,_sum(i)_from_mytable_group_by_s",
		"CREATE_TABLE_t1_as_select_s,_sum(i)_from_mytable_group_by_s",
		"CREATE_TABLE_t1_as_select_s,_sum(i)_from_mytable_group_by_s_having_sum(i)_>_2",
		"CREATE_TABLE_t1_as_select_s,_sum(i)_from_mytable_group_by_s_having_sum(i)_>_2",
		"CREATE_TABLE_t1_as_select_s,_i_from_mytable_order_by_s_limit_1",
		"CREATE_TABLE_t1_as_select_s,_i_from_mytable_order_by_s_limit_1",
		"CREATE_TABLE_t1_as_select_concat(\"new\",_s),_i_from_mytable",
		"CREATE_TABLE_t1_as_select_concat(\"new\",_s),_i_from_mytable",
		"display_width_for_numeric_types",
		"SHOW_FULL_FIELDS_FROM_numericDisplayWidthTest;",
		"Validate_that_CREATE_LIKE_preserves_checks",
		"datetime_precision",
		"CREATE_TABLE_tt_(pk_int_primary_key,_d_datetime(6)_default_current_timestamp(6))",
		"Identifier_lengths",
		"create_table_b_(a_int_primary_key,_constraint_abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl_check_(a_>_0))",
		"create_table_d_(a_int_primary_key,_constraint_abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl_foreign_key_(a)_references_parent(a))",
		"table_charset_options",
		"show_create_table_t1",
		"show_create_table_t2",
		"show_create_table_t3",
		"show_create_table_t4",
		"create_table_with_select_preserves_default",
		"create_table_t1_select_*_from_a;",
		"create_table_t2_select_j_from_a;",
		"create_table_t3_select_j_as_i_from_a;",
		"create_table_t4_select_j_+_1_from_a;",
		"create_table_t5_select_a.j_from_a;",
		"create_table_t6_select_sqa.j_from_(select_i,_j_from_a)_sqa;",
		"show_create_table_t7;",
		"create_table_t8_select_*_from_(select_*_from_a)_a_join_(select_*_from_b)_b;",
		"show_create_table_t9;",
		"create_table_t11_select_sum(j)_over()_as_jj_from_a;",
		"create_table_t12_select_j_from_a_group_by_j;",
		"create_table_t13_select_*_from_c;",
		"event_contains_CREATE_TABLE_AS",
		"CREATE_EVENT_foo_ON_SCHEDULE_EVERY_1_YEAR_DO_CREATE_TABLE_bar_AS_SELECT_1;",
		"trigger_contains_CREATE_TABLE_AS",
		"CREATE_TRIGGER_foo_AFTER_UPDATE_ON_t_FOR_EACH_ROW_BEGIN_CREATE_TABLE_bar_AS_SELECT_1;_END;",
		"create_table_with_non_primary_auto_increment_column",
		"insert_into_t1_(b)_values_(1),_(2)",
		"show_create_table_t1",
		"select_*_from_t1_order_by_b",
		"create_table_with_non_primary_auto_increment_column,_separate_unique_key",
		"insert_into_t1_(b)_values_(1),_(2)",
		"show_create_table_t1",
		"select_*_from_t1_order_by_b",
		"table_with_auto_increment_table_option",
		"create_table_t1_(i_int)_auto_increment=10;",
		"create_table_t2_(i_int_auto_increment_primary_key)_auto_increment=10;",
		"show_create_table_t2",
		"insert_into_t2_values_(null),_(null),_(null)",
		"select_*_from_t2",
	}

	// Patch auto-generated queries that are known to fail
	waitForFixQueries = append(waitForFixQueries, []string{
		"CREATE TABLE t1 (pk int primary key, test_score int, height int CHECK (height < 10) , CONSTRAINT mycheck CHECK (test_score >= 50))",
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
	t.Skip("wait for fix")
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
				"CREATE TABLE mytable_modify_column (i bigint not null, s varchar(20) not null comment 'column s')",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:    "ALTER TABLE mytable_modify_column MODIFY COLUMN i bigint NOT NULL COMMENT 'modified'",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Query: "SHOW FULL COLUMNS FROM mytable_modify_column /* 1 */",
					Expected: []sql.Row{
						{"i", "bigint", nil, "NO", "", nil, "", "", "modified"},
						{"s", "varchar(20)", "utf8mb4_0900_bin", "NO", "", nil, "", "", "column s"},
					},
				},
				{
					Query:    "ALTER TABLE mytable_modify_column MODIFY COLUMN i TINYINT NOT NULL COMMENT 'yes'",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Query: "SHOW FULL COLUMNS FROM mytable_modify_column /* 2 */",
					Expected: []sql.Row{
						{"i", "tinyint", nil, "NO", "", nil, "", "", "yes"},
						{"s", "varchar(20)", "utf8mb4_0900_bin", "NO", "", nil, "", "", "column s"},
					},
				},
				{
					Query:    "ALTER TABLE mytable_modify_column MODIFY COLUMN i BIGINT NOT NULL COMMENT 'ok'",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Query: "SHOW FULL COLUMNS FROM mytable_modify_column /* 3 */",
					Expected: []sql.Row{
						{"i", "bigint", nil, "NO", "", nil, "", "", "ok"},
						{"s", "varchar(20)", "utf8mb4_0900_bin", "NO", "", nil, "", "", "column s"},
					},
				},
				{
					Query:    "ALTER TABLE mytable_modify_column MODIFY COLUMN s VARCHAR(20) NULL COMMENT 'changed'",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Query: "SHOW FULL COLUMNS FROM mytable_modify_column /* 4 */",
					Expected: []sql.Row{
						{"i", "bigint", nil, "NO", "", nil, "", "", "ok"},
						{"s", "varchar(20)", "utf8mb4_0900_bin", "YES", "", nil, "", "", "changed"},
					},
				},
			},
		},
		{
			Name:        "auto increment attribute",
			SetUpScript: []string{},
			Assertions: []queries.ScriptTestAssertion{
				{
					Skip:     true,
					Query:    "ALTER TABLE mytable MODIFY i BIGINT auto_increment",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Skip:  true,
					Query: "SHOW FULL COLUMNS FROM mytable /* 1 */",
					Expected: []sql.Row{
						{"i", "bigint", nil, "NO", "PRI", nil, "auto_increment", "", ""},
						{"s", "varchar(20)", "utf8mb4_0900_bin", "YES", "", nil, "", "", "changed"},
					},
				},
				{
					Skip:  true,
					Query: "insert into mytable (s) values ('new row')",
				},
				{
					Skip:        true,
					Query:       "ALTER TABLE mytable add column i2 bigint auto_increment",
					ExpectedErr: sql.ErrInvalidAutoIncCols,
				},
				{
					Skip:  true,
					Query: "alter table mytable add column i2 bigint",
				},
				{
					Skip:        true,
					Query:       "ALTER TABLE mytable modify column i2 bigint auto_increment",
					ExpectedErr: sql.ErrInvalidAutoIncCols,
				},
				{
					Skip:  true,
					Query: "SHOW FULL COLUMNS FROM mytable /* 2 */",
					Expected: []sql.Row{
						{"i", "bigint", nil, "NO", "PRI", nil, "auto_increment", "", ""},
						{"s", "varchar(20)", "utf8mb4_0900_bin", "YES", "", nil, "", "", "changed"},
						{"i2", "bigint", nil, "YES", "", nil, "", "", ""},
					},
				},
				{
					Skip:     true,
					Query:    "ALTER TABLE mytable MODIFY COLUMN i BIGINT NOT NULL COMMENT 'ok' FIRST",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Skip:  true,
					Query: "SHOW FULL COLUMNS FROM mytable /* 3 */",
					Expected: []sql.Row{
						{"i", "bigint", nil, "NO", "PRI", nil, "", "", "ok"},
						{"s", "varchar(20)", "utf8mb4_0900_bin", "YES", "", nil, "", "", "changed"},
						{"i2", "bigint", nil, "YES", "", nil, "", "", ""},
					},
				},
				{
					Skip:     true,
					Query:    "ALTER TABLE mytable MODIFY COLUMN s VARCHAR(20) NULL COMMENT 'changed'",
					Expected: []sql.Row{{types.NewOkResult(0)}},
				},
				{
					Skip:  true,
					Query: "SHOW FULL COLUMNS FROM mytable /* 4 */",
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
		enginetest.TestQueryWithContext(t, ctx, e, harness, "ALTER TABLE mydb.mytable_modify_column MODIFY COLUMN s VARCHAR(21) NULL COMMENT 'changed again'", []sql.Row{{types.NewOkResult(0)}}, nil, nil, nil)
		enginetest.TestQueryWithContext(t, ctx, e, harness, "SHOW FULL COLUMNS FROM mydb.mytable_modify_column", []sql.Row{
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
	t.Skip("wait for fix")
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
	t.Skip("wait for fix")
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
	t.Skip("wait for fix")
	enginetest.TestWindowFunctions(t, NewDefaultDuckHarness())
}

func TestWindowRangeFrames(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestWindowRangeFrames(t, NewDefaultDuckHarness())
}

func TestNamedWindows(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestNamedWindows(t, NewDefaultDuckHarness())
}

func TestNaturalJoinEqual(t *testing.T) {
	enginetest.TestNaturalJoinEqual(t, NewDefaultDuckHarness())
}

func TestNaturalJoinDisjoint(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestNaturalJoinDisjoint(t, NewDefaultDuckHarness())
}

func TestInnerNestedInNaturalJoins(t *testing.T) {
	enginetest.TestInnerNestedInNaturalJoins(t, NewDefaultDuckHarness())
}

func TestColumnDefaults(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestColumnDefaults(t, NewDefaultDuckHarness())
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
		// skip "ALTER TABLE remove AUTO_INCREMENT" since AUTO_INCREMENT is not supported yet
		"CREATE TABLE t40 (pk int AUTO_INCREMENT PRIMARY KEY, val int)",
		// skip "ALTER TABLE does not change column collations"
		"CREATE TABLE test1 (v1 VARCHAR(200), v2 ENUM('a'), v3 SET('a'));",
		// skip "ALTER TABLE AUTO INCREMENT no-ops on table with no original auto increment key"
		"CREATE table test (pk int primary key)",
		// skip "ALTER TABLE MODIFY column with UNIQUE KEY" since duckdb has more strict rules for modifying columns with constraints
		"CREATE table test (pk int primary key, uk int unique)",
		// skip "ALTER TABLE MODIFY column making UNIQUE" due to differences in error messages
		"CREATE table test (pk int primary key, uk int)",
		// skip "ALTER TABLE MODIFY column with KEY"  since duckdb has more strict rules for modifying columns with constraints
		"CREATE table test (pk int primary key, mk int, index (mk))",
		// skip "Identifier lengths"
		"create table t1 (a int primary key, b int)",
		// skip "Prefix index with same columns as another index"
		"CREATE table t (pk int primary key, col1 varchar(100));",
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
	t.Skip("wait for fix")
	var skippedTests []string = nil
	enginetest.TestJsonScripts(t, NewDefaultDuckHarness(), skippedTests)
}

func TestShowTableStatus(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestShowTableStatus(t, NewDefaultDuckHarness())
}

func TestAddDropPks(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestAddDropPks(t, NewDefaultDuckHarness())
}

func TestAddAutoIncrementColumn(t *testing.T) {
	t.Skip("wait for fix")
	for _, script := range queries.AlterTableAddAutoIncrementScripts {
		enginetest.TestScript(t, NewDefaultDuckHarness(), script)
	}
}

func TestNullRanges(t *testing.T) {
	enginetest.TestNullRanges(t, NewDefaultDuckHarness())
}

func TestBlobs(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestBlobs(t, NewDefaultDuckHarness())
}

func TestIndexes(t *testing.T) {
	t.Skip("wait for fix")
	enginetest.TestIndexes(t, NewDefaultDuckHarness())
}

func TestIndexPrefix(t *testing.T) {
	t.Skip("wait for fix")
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
	t.Skip("wait for fix")
	enginetest.TestPreparedInsert(t, NewDuckHarness("default", 1, testNumPartitions, true, mergableIndexDriver))
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
