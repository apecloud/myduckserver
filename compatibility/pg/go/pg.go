package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
)

type Test struct {
	query           string
	expectedResults [][]string
}

type Tests struct {
	conn  *sql.DB
	tests []Test
}

func (t *Tests) connect(ip string, port int, user, password string) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable", ip, port, user, password)
	var err error
	t.conn, err = sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
}

func (t *Tests) disconnect() {
	err := t.conn.Close()
	if err != nil {
		panic(err)
	}
}

func (t *Tests) addTest(query string, expectedResults [][]string) {
	t.tests = append(t.tests, Test{query, expectedResults})
}

func (t *Tests) readTestsFromFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}(file)

	scanner := bufio.NewScanner(file)
	var query string
	var results [][]string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			if query != "" {
				t.addTest(query, results)
				query, results = "", nil
			}
		} else if query == "" {
			query = line
		} else {
			results = append(results, strings.Split(line, ","))
		}
	}
	if query != "" {
		t.addTest(query, results)
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
}

func (t *Tests) runTests() bool {
	for _, test := range t.tests {
		if !t.runTest(test) {
			return false
		}
	}
	return true
}

func (t *Tests) runTest(test Test) bool {
	fmt.Println("Running test:", test.query)
	rows, err := t.conn.Query(test.query)
	if err != nil {
		fmt.Println("Error executing query:", err)
		return false
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			panic(err)
		}
	}(rows)

	columns, err := rows.Columns()
	if err != nil {
		fmt.Println("Error getting columns:", err)
		return false
	}

	if len(test.expectedResults) == 0 {
		if len(columns) != 0 {
			fmt.Printf("Expected 0 columns, got %d\n", len(columns))
			return false
		}
		fmt.Println("Returns 0 rows")
		return true
	}

	if len(columns) != len(test.expectedResults[0]) {
		fmt.Printf("Expected %d columns, got %d\n", len(test.expectedResults[0]), len(columns))
		return false
	}

	var rowCount int
	for rows.Next() {
		row := make([]string, len(columns))
		rowPointers := make([]interface{}, len(columns))
		for i := range row {
			rowPointers[i] = &row[i]
		}
		if err := rows.Scan(rowPointers...); err != nil {
			fmt.Println("Error scanning row:", err)
			return false
		}

		for i, expected := range test.expectedResults[rowCount] {
			if row[i] != expected {
				fmt.Printf("Mismatch at row %d, column %d: expected '%s', got '%s'\n", rowCount+1, i+1, expected, row[i])
				return false
			}
		}
		rowCount++
	}

	if rowCount != len(test.expectedResults) {
		fmt.Printf("Expected %d rows, got %d\n", len(test.expectedResults), rowCount)
		return false
	}

	fmt.Printf("Returns %d rows\n", rowCount)
	return true
}

func main() {
	if len(os.Args) < 6 {
		fmt.Println("Usage: pg_test <ip> <port> <user> <password> <testFile>")
		os.Exit(1)
	}

	ip := os.Args[1]
	port, ok := strconv.Atoi(os.Args[2])
	if ok != nil {
		fmt.Println("Invalid port:", os.Args[2])
		os.Exit(1)
	}
	user := os.Args[3]
	password := os.Args[4]
	testFile := os.Args[5]

	tests := &Tests{}
	tests.connect(ip, port, user, password)
	defer tests.disconnect()

	tests.readTestsFromFile(testFile)

	if !tests.runTests() {
		os.Exit(1)
	}
}
