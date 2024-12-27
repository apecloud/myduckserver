package main

import (
	"context"
	"reflect"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Set connection options
var options = map[string]string{
	adbc.OptionKeyURI:             "grpc://localhost:47470",
	flightsql.OptionSSLSkipVerify: adbc.OptionValueEnabled,
}

// Create database connection
func createDatabaseConnection(t *testing.T) adbc.Connection {
	alloc := memory.NewGoAllocator()
	drv := flightsql.NewDriver(alloc)
	db, err := drv.NewDatabase(options)
	if err != nil {
		t.Fatalf("Error creating database: %v", err)
	}

	cnxn, err := db.Open(context.Background())
	if err != nil {
		t.Fatalf("Error opening connection: %v", err)
	}

	return cnxn
}

// Execute SQL statement
func executeSQLStatement(cnxn adbc.Connection, query string, t *testing.T) {
	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("failed to create statement: %v", err)
	}
	defer stmt.Close()

	err = stmt.SetSqlQuery(query)
	if err != nil {
		t.Fatalf("failed to set SQL query: %v", err)
	}

	_, err = stmt.ExecuteUpdate(context.Background())
	if err != nil {
		t.Fatalf("failed to execute SQL statement: %v", err)
	}
}

// Execute query and verify results
func executeQueryAndVerify(cnxn adbc.Connection, query string, expectedResults []struct {
	id    int64
	name  string
	value int64
}, t *testing.T) {
	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("failed to create statement: %v", err)
	}
	defer stmt.Close()

	err = stmt.SetSqlQuery(query)
	if err != nil {
		t.Fatalf("failed to set SQL query: %v", err)
	}

	rows, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatalf("failed to execute query: %v", err)
	}
	defer rows.Release()

	var actualResults []struct {
		id    int64
		name  string
		value int64
	}

	// Read query results and verify
	for rows.Next() {
		record := rows.Record()
		numRows := record.NumRows()

		for i := 0; i < int(numRows); i++ {
			var id, value int64
			switch idCol := record.Column(0).(type) {
			case *array.Int32:
				id = int64(idCol.Value(i))
			case *array.Int64:
				id = idCol.Value(i)
			default:
				t.Fatalf("unexpected type for id column: %T", record.Column(0))
			}

			name := record.Column(1).(*array.String)

			switch valueCol := record.Column(2).(type) {
			case *array.Int32:
				value = int64(valueCol.Value(i))
			case *array.Int64:
				value = valueCol.Value(i)
			default:
				t.Fatalf("unexpected type for value column: %T", record.Column(2))
			}

			actualResults = append(actualResults, struct {
				id    int64
				name  string
				value int64
			}{
				id:    id,
				name:  name.Value(i),
				value: value,
			})
		}
	}

	// Verify query results
	if len(actualResults) != len(expectedResults) {
		t.Errorf("Expected %d rows, but got %d", len(expectedResults), len(actualResults))
	}

	for i, result := range actualResults {
		expected := expectedResults[i]
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Row %d: Expected %+v, but got %+v", i, expected, result)
		}
	}
}

// Go test function
func TestSQLOperations(t *testing.T) {
	cnxn := createDatabaseConnection(t)
	defer cnxn.Close()

	// 1. Execute DROP TABLE IF EXISTS intTable
	executeSQLStatement(cnxn, "DROP TABLE IF EXISTS intTable", t)

	// 2. Execute CREATE TABLE IF NOT EXISTS intTable
	executeSQLStatement(cnxn, `CREATE TABLE IF NOT EXISTS intTable (
		id INTEGER PRIMARY KEY,
		name VARCHAR(50),
		value INT
	)`, t)

	// 3. Execute INSERT INTO intTable
	executeSQLStatement(cnxn, "INSERT INTO intTable (id, name, value) VALUES (1, 'TestName', 100)", t)
	executeSQLStatement(cnxn, "INSERT INTO intTable (id, name, value) VALUES (2, 'AnotherName', 200)", t)

	// 4. Query data and verify insertion was successful
	expectedResults := []struct {
		id    int64
		name  string
		value int64
	}{
		{id: 1, name: "TestName", value: 100},
		{id: 2, name: "AnotherName", value: 200},
	}
	query := "SELECT id, name, value FROM intTable"
	executeQueryAndVerify(cnxn, query, expectedResults, t)

	// 5. Execute DROP TABLE IF EXISTS intTable
	executeSQLStatement(cnxn, "DROP TABLE IF EXISTS intTable", t)
}
