package catalog

import (
	"context"
	"github.com/apecloud/myduckserver/testutil"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

type Execution struct {
	SQL      string
	Expected string
	WantErr  bool
}

func TestCreateCatalog(t *testing.T) {
	tests := []struct {
		name       string
		executions []Execution
	}{
		{
			name: "create database",
			executions: []Execution{
				{
					SQL:      "CREATE DATABASE testdb1;",
					Expected: "CREATE DATABASE",
				},
				// Can not create the database with the same name
				{
					SQL:     "CREATE DATABASE testdb1;",
					WantErr: true,
				},
				{
					SQL:      "CREATE DATABASE IF NOT EXISTS testdb1;",
					Expected: "CREATE DATABASE",
				},
				{
					SQL:      "CREATE DATABASE testdb2;",
					Expected: "CREATE DATABASE",
				},
			},
		},
		{
			name: "switch database",
			executions: []Execution{
				{
					SQL:      "USE testdb1;",
					Expected: "SET",
				},
				{
					SQL:      "CREATE SCHEMA testschema1;",
					Expected: "CREATE SCHEMA",
				},
				{
					SQL:      "USE testdb1.testschema1;",
					Expected: "SET",
				},
				{
					SQL:      "USE testdb2;",
					Expected: "SET",
				},
				// Can not drop the schema as it is not in the current database
				{
					SQL:     "DROP SCHEMA testschema1;",
					WantErr: true,
				},
				{
					SQL:      "USE testdb1;",
					Expected: "SET",
				},
				{
					SQL:      "DROP SCHEMA testschema1;",
					Expected: "DROP SCHEMA",
				},
				// Can not switch to the schema that does not exist
				{
					SQL:     "USE testdb1.testschema1;",
					WantErr: true,
				},
			},
		},
		{
			name: "drop database",
			executions: []Execution{
				//{
				//	SQL:      "USE testdb1;",
				//	Expected: "SET",
				//},
				//// Can not drop the database when the current database is the one to be dropped
				//{
				//	SQL:     "DROP DATABASE testdb1;",
				//	WantErr: true,
				//},
				{
					SQL:      "USE testdb2;",
					Expected: "SET",
				},
				{
					SQL:      "DROP DATABASE testdb1;",
					Expected: "DROP DATABASE",
				},
				{
					SQL:     "DROP DATABASE testdb1;",
					WantErr: true,
				},
				{
					SQL:      "DROP DATABASE IF EXISTS testdb1;",
					Expected: "DROP DATABASE",
				},
			},
		},
	}
	testDir := testutil.CreateTestDir(t)
	testEnv := testutil.NewTestEnv()
	err := testutil.StartDuckSqlServer(t, testDir, nil, testEnv)
	require.NoError(t, err)
	defer testutil.StopDuckSqlServer(t, testEnv.DuckProcess)
	dsn := "postgresql://postgres@localhost:" + strconv.Itoa(testEnv.DuckPgPort) + "/postgres"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Connect to MyDuck Server
			db, err := pgx.Connect(context.Background(), dsn)
			if err != nil {
				t.Errorf("Connect failed! dsn = %v, err: %v", dsn, err)
				return
			}
			defer db.Close(context.Background())

			for _, execution := range tt.executions {
				func() {
					tag, err := db.Exec(context.Background(), execution.SQL)
					if execution.WantErr {
						if err != nil {
							return
						}
						t.Errorf("Test expectes error but got none! sql: %v", execution.SQL)
						return
					}
					if err != nil {
						t.Errorf("Query failed! sql: %v, err: %v", execution.SQL, err)
						return
					}
					// check whether the result is as expected
					if tag.String() != execution.Expected {
						t.Errorf("sql: %v, got %v, want %v", execution.SQL, tag.String(), execution.Expected)
					}
				}()
			}
		})
	}
}
