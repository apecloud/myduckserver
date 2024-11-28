package pgserver

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/pgserver/logrepl"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pglogrepl"
)

// This file implements the logic for handling CREATE SUBSCRIPTION SQL statements.
// Example usage of CREATE SUBSCRIPTION SQL:
//
// CREATE SUBSCRIPTION mysub
// CONNECTION 'dbname= host=127.0.0.1 port=5432 user=postgres password=root'
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
}

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

func (h *ConnectionHandler) executeCreateSubscriptionSQL(subscriptionConfig *SubscriptionConfig) error {
	sqlCtx, err := h.duckHandler.sm.NewContextWithQuery(context.Background(), h.mysqlConn, "")
	if err != nil {
		return fmt.Errorf("failed to create context for query: %w", err)
	}

	lsn, err := h.doSnapshot(sqlCtx, subscriptionConfig)
	if err != nil {
		return fmt.Errorf("failed to create snapshot for CREATE SUBSCRIPTION: %w", err)
	}

	// Do a checkpoint here to merge the WAL logs
	// if _, err := adapter.ExecCatalog(sqlCtx, "FORCE CHECKPOINT"); err != nil {
	// 	return fmt.Errorf("failed to execute FORCE CHECKPOINT: %w", err)
	// }
	// if _, err := adapter.ExecCatalog(sqlCtx, "PRAGMA force_checkpoint;"); err != nil {
	// 	return fmt.Errorf("failed to execute PRAGMA force_checkpoint: %w", err)
	// }

	replicator, err := h.doCreateSubscription(sqlCtx, subscriptionConfig, lsn)
	if err != nil {
		return fmt.Errorf("failed to execute CREATE SUBSCRIPTION: %w", err)
	}

	go replicator.StartReplication(h.server.NewInternalCtx(), subscriptionConfig.PublicationName)

	return nil
}

func (h *ConnectionHandler) doSnapshot(sqlCtx *sql.Context, subscriptionConfig *SubscriptionConfig) (pglogrepl.LSN, error) {
	// If there is ongoing transcation, commit it
	if txn := adapter.TryGetTxn(sqlCtx); txn != nil {
		if err := func() error {
			defer txn.Rollback()
			defer adapter.CloseTxn(sqlCtx)
			return txn.Commit()
		}(); err != nil {
			return 0, fmt.Errorf("failed to commit current transaction: %w", err)
		}
	}

	connInfo := subscriptionConfig.ToConnectionInfo()
	attachName := fmt.Sprintf("__pg_src_%d__", sqlCtx.ID())
	if _, err := adapter.ExecCatalog(sqlCtx, fmt.Sprintf("ATTACH '%s' AS %s (TYPE POSTGRES, READ_ONLY)", connInfo, attachName)); err != nil {
		return 0, fmt.Errorf("failed to attach connection: %w", err)
	}

	defer func() {
		if _, err := adapter.ExecCatalog(sqlCtx, fmt.Sprintf("DETACH %s", attachName)); err != nil {
			h.logger.Warnf("failed to detach connection: %v", err)
		}
	}()

	var currentLSN string
	err := adapter.QueryRowCatalog(
		sqlCtx,
		fmt.Sprintf("SELECT * FROM postgres_query('%s', 'SELECT pg_current_wal_lsn()')", attachName),
	).Scan(&currentLSN)
	if err != nil {
		return 0, fmt.Errorf("failed to query WAL LSN: %w", err)
	}

	lsn, err := pglogrepl.ParseLSN(currentLSN)
	if err != nil {
		return 0, fmt.Errorf("failed to parse LSN: %w", err)
	}

	// COPY DATABASE is buggy - it corrupts the WAL so the server cannot be restarted.
	// So we need to copy tables one by one.
	// if _, err := adapter.ExecCatalogInTxn(sqlCtx, fmt.Sprintf("COPY FROM DATABASE %s TO mysql", attachName)); err != nil {
	// 	return 0, fmt.Errorf("failed to copy from database: %w", err)
	// }

	type table struct {
		schema string
		name   string
	}
	var tables []table

	// Get all tables from the source database
	if err := func() error {
		rows, err := adapter.QueryCatalog(sqlCtx, `SELECT database, schema, name FROM (SHOW ALL TABLES) WHERE database = '`+attachName+`'`)
		if err != nil {
			return fmt.Errorf("failed to query tables: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var database, schema, tableName string
			if err := rows.Scan(&database, &schema, &tableName); err != nil {
				return fmt.Errorf("failed to scan table: %w", err)
			}
			tables = append(tables, table{schema: schema, name: tableName})
		}

		return nil
	}(); err != nil {
		return 0, err
	}

	// Create all schemas in the target database
	for _, t := range tables {
		if _, err := adapter.ExecCatalogInTxn(sqlCtx, `CREATE SCHEMA IF NOT EXISTS `+catalog.QuoteIdentifierANSI(t.schema)); err != nil {
			return 0, fmt.Errorf("failed to create schema: %w", err)
		}
	}

	txn, err := adapter.GetCatalogTxn(sqlCtx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to get transaction: %w", err)
	}
	defer txn.Rollback()
	defer adapter.CloseTxn(sqlCtx)

	for _, t := range tables {
		if _, err := adapter.ExecCatalogInTxn(
			sqlCtx,
			`CREATE TABLE `+catalog.ConnectIdentifiersANSI(t.schema, t.name)+` AS FROM `+catalog.ConnectIdentifiersANSI(attachName, t.schema, t.name),
		); err != nil {
			return 0, fmt.Errorf("failed to create table: %w", err)
		}
	}

	return lsn, txn.Commit()
}

func (h *ConnectionHandler) doCreateSubscription(sqlCtx *sql.Context, subscriptionConfig *SubscriptionConfig, lsn pglogrepl.LSN) (*logrepl.LogicalReplicator, error) {
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

	// `WriteWALPosition` and `WriteSubscription` execute in a transaction internally,
	// so we start a transaction here and commit it after writing the WAL position.
	tx, err := adapter.GetCatalogTxn(sqlCtx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}
	defer tx.Rollback()
	defer adapter.CloseTxn(sqlCtx)

	err = replicator.WriteWALPosition(sqlCtx, subscriptionConfig.PublicationName, lsn)
	if err != nil {
		return nil, fmt.Errorf("failed to write WAL position: %w", err)
	}

	err = logrepl.WriteSubscription(sqlCtx, subscriptionConfig.SubscriptionName, subscriptionConfig.ToDNS(), subscriptionConfig.PublicationName)
	if err != nil {
		return nil, fmt.Errorf("failed to write subscription: %w", err)
	}

	return replicator, tx.Commit()
}
