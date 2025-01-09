package pgserver

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/apecloud/myduckserver/testutil"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

type FuncReplacementExecution struct {
	SQL      string
	Expected [][]string
	WantErr  bool
}

func TestFuncReplacement(t *testing.T) {
	tests := []struct {
		name       string
		executions []FuncReplacementExecution
	}{
		// Get Postgresql Configuration
		{
			name: "Test Metabase query on Postgresql Configuration 1",
			executions: []FuncReplacementExecution{
				{
					// The testing target is 'PG_CATALOG.PG_GET_INDEXDEF' and 'INFORMATION_SCHEMA._PG_EXPANDARRAY'
					SQL: `SELECT
    "tmp"."table-schema", "tmp"."table-name",
    TRIM(BOTH '"' FROM PG_CATALOG.PG_GET_INDEXDEF("tmp"."ci_oid", "tmp"."pos", FALSE)) AS "field-name"
FROM (SELECT
          "n"."nspname" AS "table-schema",
          "ct"."relname" AS "table-name",
          "ci"."oid" AS "ci_oid",
          (INFORMATION_SCHEMA._PG_EXPANDARRAY("i"."indkey"))."n" AS "pos"
      FROM "pg_catalog"."pg_class" AS "ct"
           INNER JOIN "pg_catalog"."pg_namespace" AS "n" ON "ct"."relnamespace" = "n"."oid"
           INNER JOIN "pg_catalog"."pg_index" AS "i" ON "ct"."oid" = "i"."indrelid"
           INNER JOIN "pg_catalog"."pg_class" AS "ci" ON "ci"."oid" = "i"."indexrelid"
      WHERE (PG_CATALOG.PG_GET_EXPR("i"."indpred", "i"."indrelid") IS NULL)
            AND n.nspname !~ '^information_schema|catalog_history|pg_') AS "tmp"
WHERE "tmp"."pos" = 1`,
					// TODO(sean): There's no data currently, we just check the query is executed without error
					Expected: [][]string{},
					WantErr:  false,
				},
			},
		},
		{
			name: "Test Metabase query on Postgresql Configuration 2",
			executions: []FuncReplacementExecution{
				{
					// The testing target is 'pg_class'::RegClass
					SQL: `SELECT
    n.nspname AS schema,
    c.relname AS name,
    CASE c.relkind
        WHEN 'r' THEN 'TABLE'
        WHEN 'p' THEN 'PARTITIONED TABLE'
        WHEN 'v' THEN 'VIEW'
        WHEN 'f' THEN 'FOREIGN TABLE'
        WHEN 'm' THEN 'MATERIALIZED VIEW'
        ELSE NULL
    END AS type,
    d.description AS description,
    stat.n_live_tup AS estimated_row_count
FROM pg_catalog.pg_class AS c
     INNER JOIN pg_catalog.pg_namespace AS n ON c.relnamespace = n.oid
     LEFT JOIN pg_catalog.pg_description AS d ON ((c.oid = d.objoid)
                                                 AND (d.objsubid = 1))
                                                 AND (d.classoid = 'pg_class'::RegClass)
     LEFT JOIN pg_stat_user_tables AS stat ON (n.nspname = stat.schemaname)
                                              AND (c.relname = stat.relname)
WHERE ((((c.relnamespace = n.oid) AND (n.nspname !~ 'information_schema'))
          AND (n.nspname != 'pg_catalog'))
          AND (c.relkind IN ('r', 'p', 'v', 'f', 'm')))
      AND (n.nspname IN ('public', 'test'))
ORDER BY type ASC, schema ASC, name ASC`,
					// There's no data currently, we just check the query is executed without error
					Expected: [][]string{},
					WantErr:  false,
				},
			},
		},
		{
			name: "Test Metabase query on Postgresql Configuration 3",
			executions: []FuncReplacementExecution{
				{
					// The testing target is 'INFORMATION_SCHEMA._PG_EXPANDARRAY'
					SQL: `SELECT
    result.TABLE_CAT,
    result.TABLE_SCHEM,
    result.TABLE_NAME,
    result.COLUMN_NAME,
    result.KEY_SEQ,
    result.PK_NAME
FROM (SELECT
          NULL AS TABLE_CAT,
          n.nspname AS TABLE_SCHEM,
          ct.relname AS TABLE_NAME,
          a.attname AS COLUMN_NAME,
          (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ,
          ci.relname AS PK_NAME,
          information_schema._pg_expandarray(i.indkey) AS KEYS,
          a.attnum AS A_ATTNUM
      FROM pg_catalog.pg_class ct
           JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)
           JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
           JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid)
           JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
      WHERE true AND n.nspname = 'public'
            AND ct.relname = 't'
            AND i.indisprimary) result
WHERE result.A_ATTNUM = (result.KEYS).x
ORDER BY result.table_name, result.pk_name, result.key_seq;`,
					// There's no data currently, we just check the query is executed without error
					Expected: [][]string{},
					WantErr:  false,
				},
			},
		},
	}

	// Setup MyDuck Server
	testDir := testutil.CreateTestDir(t)
	testEnv := testutil.NewTestEnv()
	err := testutil.StartDuckSqlServer(t, testDir, nil, testEnv)
	require.NoError(t, err)
	defer testutil.StopDuckSqlServer(t, testEnv.DuckProcess)
	dsn := "postgresql://postgres@localhost:" + strconv.Itoa(testEnv.DuckPgPort) + "/postgres"

	// https://pkg.go.dev/github.com/jackc/pgx/v5#ParseConfig
	// We should try all the possible query_exec_mode values.
	// The first four queryExecModes will use the PostgreSQL extended protocol,
	// while the last one will use the simple protocol.
	queryExecModes := []string{"cache_statement", "cache_describe", "describe_exec", "exec", "simple_protocol"}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, queryExecMode := range queryExecModes {
				// Connect to MyDuck Server
				db, err := pgx.Connect(context.Background(), dsn+"?default_query_exec_mode="+queryExecMode)
				if err != nil {
					t.Errorf("Connect failed! dsn = %v, err: %v", dsn, err)
					return
				}
				defer db.Close(context.Background())

				for _, execution := range tt.executions {
					func() {
						rows, err := db.Query(context.Background(), execution.SQL)
						if execution.WantErr {
							// When the queryExecModes is set to "exec", the error will be returned in the rows.Err() after executing rows.Next()
							// So we can not simply check the err here.
							rows.Next()
							if rows.Err() != nil {
								return
							}
							defer rows.Close()
							t.Errorf("Test expectes error but got none! queryExecMode: %v, sql = %v", queryExecMode, execution.SQL)
							return
						}
						if err != nil {
							t.Errorf("Query failed! queryExecMode: %v, sql = %v, err: %v", queryExecMode, execution.SQL, err)
							return
						}
						defer rows.Close()
						// check whether the result is as expected
						for i := 0; execution.Expected != nil && i < len(execution.Expected); i++ {
							rows.Next()
							values, err := rows.Values()
							require.NoError(t, err)
							// check whether the row length is as expected
							if len(values) != len(execution.Expected[i]) {
								t.Errorf("queryExecMode: %v, %v got = %v, want %v", queryExecMode, execution.SQL, values, execution.Expected[i])
							}
							for j := 0; j < len(values); j++ {
								valueStr := fmt.Sprintf("%v", values[j])
								if valueStr != execution.Expected[i][j] {
									t.Errorf("queryExecMode: %v, %v got = %v, want %v", queryExecMode, execution.SQL, valueStr, execution.Expected[i][j])
								}
							}
						}
					}()
				}
			}
		})
	}
}
