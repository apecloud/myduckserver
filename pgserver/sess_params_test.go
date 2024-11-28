package pgserver

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/apecloud/myduckserver/test"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

type Execution struct {
	SQL      string
	Expected [][]string
	WantErr  bool
}

func TestSessParam(t *testing.T) {
	tests := []struct {
		name       string
		executions []Execution
	}{
		// To test whether the white spaces can be handled correctly
		// The statements in these tests will all end with " \t\r\n"

		// Get PostgreSQL Configuration
		{
			name: "Show application_name",
			executions: []Execution{
				{
					SQL:      " \t\r\nSHOW application_name; \t\r\n",
					Expected: [][]string{{"psql"}},
					WantErr:  false,
				},
			},
		},
		{
			name: "Show application_NAME",
			executions: []Execution{
				{
					SQL:      " \t\r\nSHOW application_NAME; \t\r\n",
					Expected: [][]string{{"psql"}},
					WantErr:  false,
				},
			},
		},
		{
			name: "Show 'application_name'",
			executions: []Execution{
				{
					// Single quotes are not allowed in SHOW statement
					SQL:      "SHOW 'application_name';",
					Expected: nil,
					WantErr:  true,
				},
			},
		},
		{
			name: "Show \"application_name\"",
			executions: []Execution{
				{
					// Double quotes are not allowed in SHOW statement
					SQL:      "SHOW \"application_name\";",
					Expected: [][]string{{"psql"}},
					WantErr:  false,
				},
			},
		},
		{
			name: "Show ALL",
			executions: []Execution{
				{
					// SHOW ALL is not supported yet.
					SQL:      "SHOW ALL;",
					Expected: nil,
					WantErr:  true,
				},
			},
		},

		{
			name: "Select current_setting('application_name')",
			executions: []Execution{
				{
					SQL:      " \t\r\nSELECT CURRENT_SETTING('application_name'); \t\r\n",
					Expected: [][]string{{"psql"}},
					WantErr:  false,
				},
			},
		},
		{
			name: "Select current_setting('application_NAME')",
			executions: []Execution{
				{
					SQL:      " \t\r\nSELECT CURRENT_SETTING('application_NAME'); \t\r\n",
					Expected: [][]string{{"psql"}},
					WantErr:  false,
				},
			},
		},
		// Single quotes are required in CURRENT_SETTING statement
		{
			name: "Select current_setting(application_name)",
			executions: []Execution{
				{
					SQL:      "SELECT CURRENT_SETTING(application_name);",
					Expected: nil,
					WantErr:  true,
				},
			},
		},
		// Double quotes are not allowed in CURRENT_SETTING statement
		{
			name: "Select current_setting(\"application_name\")",
			executions: []Execution{
				{
					SQL:      "SELECT CURRENT_SETTING(\"application_name\");",
					Expected: nil,
					WantErr:  true,
				},
			},
		},

		// Get DuckDB Configuration
		{
			name: "Select current_setting('access_mode')",
			executions: []Execution{
				{
					SQL:      " \t\r\nSELECT CURRENT_SETTING('access_mode'); \t\r\n",
					Expected: [][]string{{"automatic"}},
					WantErr:  false,
				},
			},
		},
		{
			name: "Select current_setting('access_MODE')",
			executions: []Execution{
				{
					SQL:      " \t\r\nSELECT CURRENT_SETTING('access_MODE'); \t\r\n",
					Expected: [][]string{{"automatic"}},
					WantErr:  false,
				},
			},
		},
		// Single quotes are required in CURRENT_SETTING statement
		{
			name: "Select current_setting(access_mode)",
			executions: []Execution{
				{
					SQL:      "SELECT CURRENT_SETTING(access_mode);",
					Expected: nil,
					WantErr:  true,
				},
			},
		},
		// Double quotes are not allowed in CURRENT_SETTING statement
		{
			name: "Select current_setting(\"access_mode\")",
			executions: []Execution{
				{
					SQL:      "SELECT CURRENT_SETTING(\"access_mode\");",
					Expected: nil,
					WantErr:  true,
				},
			},
		},

		// Set PostgreSQL Configuration
		{
			name: "Set application_name with quotes",
			executions: []Execution{
				// Set the application_name to 'myDUCK'
				{
					SQL:      " \t\r\nSET application_name TO 'myDUCK'; \t\r\n",
					Expected: nil,
					WantErr:  false,
				},
				// Get the value of application_name, it should be 'myDUCK'
				{
					SQL:      " \t\r\nSELECT CURRENT_SETTING('application_name'); \t\r\n",
					Expected: [][]string{{"myDUCK"}},
					WantErr:  false,
				},
				// Set the application_name to "MYduck"
				{
					SQL:      " \t\r\nSET application_NAME TO \"MYduck\"; \t\r\n",
					Expected: nil,
					WantErr:  false,
				},
				// Get the value of application_name, it should be 'MYduck'
				{
					SQL:      " \t\r\nSELECT CURRENT_SETTING('application_name'); \t\r\n",
					Expected: [][]string{{"MYduck"}},
					WantErr:  false,
				},
			},
		},
		{
			name: "Set application_name to 'default' in quotes",
			executions: []Execution{
				// Set the application_name to 'myDUCK'
				{
					SQL:      " \t\r\nSET application_name TO 'myDUCK'; \t\r\n",
					Expected: nil,
					WantErr:  false,
				},
				// Get the value of application_name, it should be 'myDUCK'
				{
					SQL:      " \t\r\nSELECT CURRENT_SETTING('application_name'); \t\r\n",
					Expected: [][]string{{"myDUCK"}},
					WantErr:  false,
				},
				// Set the application_name to 'default'
				{
					SQL:      " \t\r\nSET application_NAME TO 'default'; \t\r\n",
					Expected: nil,
					WantErr:  false,
				},
				// Get the value of application_name, it should be 'default'
				{
					SQL:      " \t\r\nSELECT CURRENT_SETTING('application_name'); \t\r\n",
					Expected: [][]string{{"default"}},
					WantErr:  false,
				},
			},
		},
		{
			name: "Set application_name to default",
			executions: []Execution{
				// Set the application_name to myDUCK
				{
					SQL:      " \t\r\nSET application_name TO myDUCK; \t\r\n",
					Expected: nil,
					WantErr:  false,
				},
				// Get the value of application_name, it should be 'myduck'
				{
					SQL:      " \t\r\nSELECT CURRENT_SETTING('application_name'); \t\r\n",
					Expected: [][]string{{"myduck"}},
					WantErr:  false,
				},
				// Set the application_name to default
				{
					SQL:      " \t\r\nSET application_NAME TO default; \t\r\n",
					Expected: nil,
					WantErr:  false,
				},
				// Get the value of application_name, it should be default value 'psql'
				{
					SQL:      " \t\r\nSELECT CURRENT_SETTING('application_name'); \t\r\n",
					Expected: [][]string{{"psql"}},
					WantErr:  false,
				},
			},
		},

		// Set DuckDB Configuration
		{
			name: "Set http_proxy_username with quotes",
			executions: []Execution{
				// Set the application_name to 'myDUCK'
				{
					SQL:      " \t\r\nSET http_proxy_username TO 'myDUCK'; \t\r\n",
					Expected: nil,
					WantErr:  false,
				},
				// Get the value of http_proxy_username, it should be 'myDUCK'
				{
					SQL:      " \t\r\nSELECT CURRENT_SETTING('http_proxy_username'); \t\r\n",
					Expected: [][]string{{"myDUCK"}},
					WantErr:  false,
				},
				// Set the application_name to "MYduck"
				{
					SQL:      " \t\r\nSET http_proxy_USERNAME TO \"MYduck\"; \t\r\n",
					Expected: nil,
					WantErr:  false,
				},
				// Get the value of http_proxy_username, it should be 'myDUCK'
				{
					SQL:      " \t\r\nSELECT CURRENT_SETTING('http_proxy_username'); \t\r\n",
					Expected: [][]string{{"MYduck"}},
					WantErr:  false,
				},
			},
		},
		{
			name: "Set http_proxy_username to 'default' in quotes",
			executions: []Execution{
				// Set the application_name to myDUCK
				{
					SQL:      " \t\r\nSET http_proxy_username TO myDUCK; \t\r\n",
					Expected: nil,
					WantErr:  false,
				},
				// Get the value of http_proxy_username, it should be 'myDUCK'
				{
					SQL:      " \t\r\nSELECT CURRENT_SETTING('http_proxy_username'); \t\r\n",
					Expected: [][]string{{"myDUCK"}},
					WantErr:  false,
				},
				// Set the application_name to 'default'
				{
					SQL:      " \t\r\nSET http_proxy_USERNAME TO 'default'; \t\r\n",
					Expected: nil,
					WantErr:  false,
				},
				// Get the value of http_proxy_username, it should be 'default'
				{
					SQL:      " \t\r\nSELECT CURRENT_SETTING('http_proxy_username'); \t\r\n",
					Expected: [][]string{{"default"}},
					WantErr:  false,
				},
			},
		},
		{
			name: "Set http_proxy_username to default",
			executions: []Execution{
				// Set the application_name to myDUCK
				{
					SQL:      " \t\r\nSET http_proxy_username TO myDUCK; \t\r\n",
					Expected: nil,
					WantErr:  false,
				},
				// Get the value of http_proxy_username, it should be 'myDUCK'
				{
					SQL:      " \t\r\nSELECT CURRENT_SETTING('http_proxy_username'); \t\r\n",
					Expected: [][]string{{"myDUCK"}},
					WantErr:  false,
				},
				// Set the application_name to default
				{
					SQL:      " \t\r\nSET http_proxy_USERNAME TO default; \t\r\n",
					Expected: nil,
					WantErr:  false,
				},
				// Get the value of http_proxy_username, it should be ''
				{
					SQL:      " \t\r\nSELECT CURRENT_SETTING('http_proxy_username'); \t\r\n",
					Expected: [][]string{{"<nil>"}},
					WantErr:  false,
				},
			},
		},

		// Reset PostgreSQL Configuration
		{
			name: "Reset application_name to default",
			executions: []Execution{
				// Set the application_name to 'myDUCK'
				{
					SQL:      " \t\r\nSET application_name TO 'myDUCK'; \t\r\n",
					Expected: nil,
					WantErr:  false,
				},
				// Get the value of application_name, it should be 'myDUCK'
				{
					SQL:      " \t\r\nSELECT CURRENT_SETTING('application_name'); \t\r\n",
					Expected: [][]string{{"myDUCK"}},
					WantErr:  false,
				},
				// Reset the application_name to default
				{
					SQL:      " \t\r\nreSET application_NAME; \t\r\n",
					Expected: nil,
					WantErr:  false,
				},
				// Get the value of application_name, it should be default value 'psql'
				{
					SQL:      " \t\r\nSELECT CURRENT_SETTING('application_name'); \t\r\n",
					Expected: [][]string{{"psql"}},
					WantErr:  false,
				},
			},
		},

		// Reset DuckDB Configuration
		{
			name: "Reset http_proxy_username to default",
			executions: []Execution{
				// Set the application_name to myDUCK
				{
					SQL:      " \t\r\nSET http_proxy_username TO myDUCK; \t\r\n",
					Expected: nil,
					WantErr:  false,
				},
				// Get the value of http_proxy_username, it should be 'myDUCK'
				{
					SQL:      " \t\r\nSELECT CURRENT_SETTING('http_proxy_username'); \t\r\n",
					Expected: [][]string{{"myDUCK"}},
					WantErr:  false,
				},
				// Reset the application_name to default
				{
					SQL:      " \t\r\nreSET http_proxy_USERNAME; \t\r\n",
					Expected: nil,
					WantErr:  false,
				},
				// Get the value of http_proxy_username, it should be ''
				{
					SQL:      " \t\r\nSELECT CURRENT_SETTING('http_proxy_username'); \t\r\n",
					Expected: [][]string{{"<nil>"}},
					WantErr:  false,
				},
			},
		},
		{
			name: "Reset ALL",
			executions: []Execution{
				{
					// RESET ALL is not supported yet.
					SQL:      "RESET ALL;",
					Expected: nil,
					WantErr:  true,
				},
			},
		},
	}

	// Setup MyDuck Server
	testDir := test.CreateTestDir(t)
	testEnv := test.NewTestEnv()
	err := test.StartDuckSqlServer(t, testDir, nil, testEnv)
	require.NoError(t, err)
	defer test.StopDuckSqlServer(t, testEnv.DuckProcess)
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
