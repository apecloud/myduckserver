package pgserver

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/pgserver/logrepl"
	"github.com/jackc/pglogrepl"
)

// This file implements the logic for handling CREATE SUBSCRIPTION SQL statements.
// Example usage of CREATE SUBSCRIPTION SQL:
//
// CREATE SUBSCRIPTION mysub
// CONNECTION 'dbname= host=127.0.0.1 port=15432 user=postgres password=root'
// PUBLICATION mypub;
//
// The statement creates a subscription named 'mysub' that connects to a PostgreSQL
// database and subscribes to changes published under the 'mypub' publication.

type SubscriptionConfig struct {
	SubscriptionName string
	PublicationName  string
	DBName           string
	Host             string
	Port             string
	User             string
	Password         string
	LSN              pglogrepl.LSN
}

var lsnQueryIndex = 1

var subscriptionRegex = regexp.MustCompile(`(?i)CREATE SUBSCRIPTION\s+(\w+)\s+CONNECTION\s+'([^']+)'\s+PUBLICATION\s+(\w+);`)
var connectionRegex = regexp.MustCompile(`(\b\w+)=([\w\.\d]*)`)

// ToConnectionInfo Format SubscriptionConfig into a ConnectionInfo
func (config *SubscriptionConfig) ToConnectionInfo() string {
	return fmt.Sprintf("dbname=%s user=%s password=%s host=%s port=%s",
		config.DBName, config.User, config.Password, config.Host, config.Port)
}

// ToDNS Format SubscriptionConfig into a DNS
func (config *SubscriptionConfig) ToDNS() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		config.User, config.Password, config.Host, config.Port, config.DBName)
}

func (config *SubscriptionConfig) ToDuckDBQuery() []string {
	return []string{
		fmt.Sprintf("ATTACH '%s' AS pg_postgres (TYPE POSTGRES);", config.ToConnectionInfo()),
		"SELECT * FROM postgres_query('pg_postgres', 'SELECT pg_current_wal_lsn()');",
		"BEGIN;",
		"COPY FROM DATABASE pg_postgres TO mysql;",
		"COMMIT;",
		"DETACH pg_postgres;",
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
		key := strings.ToLower(match[1])
		switch key {
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
	err := doSnapshot(h, subscriptionConfig)
	if err != nil {
		return fmt.Errorf("failed to execute snapshot in CREATE SUBSCRIPTION: %w", err)
	}

	replicator, err := doCreateSubscription(h, subscriptionConfig)
	if err != nil {
		return fmt.Errorf("failed to execute CREATE SUBSCRIPTION: %w", err)
	}

	go replicator.StartReplication(h.server.NewInternalCtx(), subscriptionConfig.PublicationName)

	return nil
}

func doSnapshot(h *ConnectionHandler, subscriptionConfig *SubscriptionConfig) error {

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

func doCreateSubscription(h *ConnectionHandler, subscriptionConfig *SubscriptionConfig) (*logrepl.LogicalReplicator, error) {
	replicator, err := logrepl.NewLogicalReplicator(subscriptionConfig.ToDNS())
	if err != nil {
		return nil, fmt.Errorf("failed to create logical replicator: %w", err)
	}

	err = logrepl.CreatePublicationIfNotExists(subscriptionConfig.ToDNS(), subscriptionConfig.PublicationName)
	if err != nil {
		return nil, fmt.Errorf("failed to create publication: %w", err)
	}

	err = replicator.CreateReplicationSlotIfNotExists(subscriptionConfig.PublicationName)
	if err != nil {
		return nil, fmt.Errorf("failed to create replication slot: %w", err)
	}

	sqlCtx, err := h.duckHandler.sm.NewContextWithQuery(context.Background(), h.mysqlConn, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create context for query: %w", err)
	}

	// `WriteWALPosition` and `WriteSubscription` execute in a transaction internally,
	// so we start a transaction here and commit it after writing the WAL position.
	tx, err := adapter.GetCatalogTxn(sqlCtx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}
	defer tx.Rollback()
	defer adapter.CloseTxn(sqlCtx)

	err = replicator.WriteWALPosition(sqlCtx, subscriptionConfig.PublicationName, subscriptionConfig.LSN)
	if err != nil {
		return nil, fmt.Errorf("failed to write WAL position: %w", err)
	}

	err = logrepl.WriteSubscription(sqlCtx, subscriptionConfig.SubscriptionName, subscriptionConfig.ToDNS(), subscriptionConfig.PublicationName)
	if err != nil {
		return nil, fmt.Errorf("failed to write subscription: %w", err)
	}

	return replicator, tx.Commit()
}

// processLSN scans the rows for the LSN value and updates the subscriptionConfig.
func processLSN(rows *stdsql.Rows, subscriptionConfig *SubscriptionConfig) error {
	for rows.Next() {
		var lsnStr string
		if err := rows.Scan(&lsnStr); err != nil {
			return fmt.Errorf("failed to scan LSN: %w", err)
		}
		lsn, err := pglogrepl.ParseLSN(lsnStr)
		if err != nil {
			return fmt.Errorf("failed to parse LSN: %w", err)
		}
		subscriptionConfig.LSN = lsn
	}

	// Check for iteration errors
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error encountered during rows iteration: %w", err)
	}

	return nil
}
