package pgserver

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"github.com/apecloud/myduckserver/adapter"
	"regexp"
)

type SubscriptionConfig struct {
	SubscriptionName string
	PublicationName  string
	DBName           string
	Host             string
	Port             string
	User             string
	Password         string
	LSN              string
}

var lsnQueryIndex = 5
var lsnColumnName = "pg_current_wal_lsn"

var subscriptionRegex = regexp.MustCompile(`CREATE SUBSCRIPTION\s+(\w+)\s+CONNECTION\s+'([^']+)'\s+PUBLICATION\s+(\w+);`)
var connectionRegex = regexp.MustCompile(`(\b\w+)=([\w\.\d]*)`)

// ToDSN Format SubscriptionConfig into a DSN
func (config *SubscriptionConfig) ToDSN() string {
	return fmt.Sprintf("dbname=%s user=%s password=%s host=%s port=%s",
		config.DBName, config.User, config.Password, config.Host, config.Port)
}

func (config *SubscriptionConfig) ToDuckDBQuery() []string {
	return []string{
		"INSTALL postgres_scanner;",
		"LOAD postgres_scanner;",
		fmt.Sprintf("ATTACH '%s' AS pg_postgres (TYPE POSTGRES);", config.ToDSN()),
		"BEGIN;",
		"COPY FROM DATABASE pg_postgres TO mysql;",
		"SELECT * FROM postgres_query('pg_postgres', 'SELECT pg_current_wal_lsn()');",
		"COMMIT;",
	}
}

func parseSubscriptionSQL(sql string) (*SubscriptionConfig, error) {
	subscriptionMatch := subscriptionRegex.FindStringSubmatch(sql)
	if len(subscriptionMatch) < 4 {
		return nil, fmt.Errorf("invalid CREATE SUBSCRIPTION SQL format")
	}

	subscriptionName := subscriptionMatch[1]
	connectionString := subscriptionMatch[2]
	publicationName := subscriptionMatch[3]

	// Parse the connection string into key-value pairs
	matches := connectionRegex.FindAllStringSubmatch(connectionString, -1)
	if matches == nil {
		return nil, fmt.Errorf("no valid key-value pairs found in connection string")
	}

	// Initialize SubscriptionConfig struct
	config := &SubscriptionConfig{
		SubscriptionName: subscriptionName,
		PublicationName:  publicationName,
	}

	// Map the matches to struct fields
	for _, match := range matches {
		switch match[1] {
		case "dbname":
			config.DBName = match[2]
		case "host":
			config.Host = match[2]
		case "port":
			config.Port = match[2]
		case "user":
			config.User = match[2]
		case "password":
			config.Password = match[2]
		}
	}

	// Handle default values
	if config.DBName == "" {
		config.DBName = "postgres"
	}
	if config.Port == "" {
		config.Port = "5432"
	}

	return config, nil
}

func executeCreateSubscriptionSQL(h *ConnectionHandler, subscriptionConfig *SubscriptionConfig) error {
	duckDBQueries := subscriptionConfig.ToDuckDBQuery()

	for index, duckDBQuery := range duckDBQueries {
		// Create a new SQL context for the DuckDB query
		sqlCtx, err := h.duckHandler.sm.NewContextWithQuery(context.Background(), h.mysqlConn, duckDBQuery)
		if err != nil {
			return fmt.Errorf("failed to create context for query at index %d: %w", index, err)
		}

		// Execute the query
		rows, err := adapter.Query(sqlCtx, duckDBQuery)
		if err != nil {
			return fmt.Errorf("query execution failed at index %d: %w", index, err)
		}
		defer func() {
			closeErr := rows.Close()
			if closeErr != nil {
				err = fmt.Errorf("failed to close rows at index %d: %w", index, closeErr)
			}
		}()

		// Process LSN query only for the specific index
		if index == lsnQueryIndex {
			if err := processLSN(rows, subscriptionConfig); err != nil {
				return fmt.Errorf("failed to process LSN query at index %d: %w", index, err)
			}
		}
	}

	return nil
}

// processLSN scans the rows for the LSN value and updates the subscriptionConfig.
func processLSN(rows *stdsql.Rows, subscriptionConfig *SubscriptionConfig) error {
	for rows.Next() {
		var lsn string
		if err := rows.Scan(&lsn); err != nil {
			return fmt.Errorf("failed to scan LSN: %w", err)
		}
		subscriptionConfig.LSN = lsn
	}

	// Check for iteration errors
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error encountered during rows iteration: %w", err)
	}

	return nil
}
