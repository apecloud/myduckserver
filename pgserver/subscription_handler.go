package pgserver

import (
	"context"
	"fmt"
	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/pgserver/logrepl"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pglogrepl"
	"regexp"
	"strings"
)

// This file handles SQL statements for managing PostgreSQL subscriptions. It supports:
//
// 1. Creating a subscription:
//    CREATE SUBSCRIPTION mysub
//    CONNECTION 'dbname= host=127.0.0.1 port=5432 user=postgres password=root'
//    PUBLICATION mypub;
//    This statement sets up a new subscription named 'mysub' that connects to a specified PostgreSQL
//    database and listens for changes published under the 'mypub' publication.
//
// 2. Altering a subscription (enable/disable):
//    ALTER SUBSCRIPTION mysub enable;
//    ALTER SUBSCRIPTION mysub disable;
//
// 3. Dropping a subscription:
//    DROP SUBSCRIPTION mysub;
//    This statement removes the specified subscription.

// Action represents the type of SQL action.
type Action string

const (
	Create       Action = "CREATE"
	Drop         Action = "DROP"
	AlterDisable Action = "DISABLE"
	AlterEnable  Action = "ENABLE"
)

// ConnectionDetails holds parsed connection string components.
type ConnectionDetails struct {
	DBName   string
	Host     string
	Port     string
	User     string
	Password string
}

// SubscriptionConfig represents the configuration of a subscription.
type SubscriptionConfig struct {
	SubscriptionName string
	PublicationName  string
	Connection       *ConnectionDetails // Embedded pointer to ConnectionDetails
	Action           Action
}

// createRegex matches and extracts components from a CREATE SUBSCRIPTION SQL statement. Example matched command:
var createRegex = regexp.MustCompile(`(?i)^CREATE\s+SUBSCRIPTION\s+([\w-]+)\s+CONNECTION\s+'([^']+)'(?:\s+PUBLICATION\s+([\w-]+))?;?$`)

// alterRegex matches ALTER SUBSCRIPTION SQL commands and captures the subscription name and the action to be taken.
var alterRegex = regexp.MustCompile(`(?i)^ALTER\s+SUBSCRIPTION\s+([\w-]+)\s+(disable|enable);?$`)

// dropRegex matches DROP SUBSCRIPTION SQL commands and captures the subscription name.
var dropRegex = regexp.MustCompile(`(?i)^DROP\s+SUBSCRIPTION\s+([\w-]+);?$`)

// connectionRegex matches and captures key-value pairs within a connection string.
var connectionRegex = regexp.MustCompile(`(\b\w+)=([\w\.\d]*)`)

// ParseSubscriptionSQL parses the given SQL statement and returns a SubscriptionConfig.
func parseSubscriptionSQL(sql string) (*SubscriptionConfig, error) {
	var config SubscriptionConfig
	switch {
	case createRegex.MatchString(sql):
		matches := createRegex.FindStringSubmatch(sql)
		config.Action = Create
		config.SubscriptionName = matches[1]
		if len(matches) > 3 {
			config.PublicationName = matches[3]
		}
		conn, err := parseConnectionString(matches[2])
		if err != nil {
			return nil, err
		}
		config.Connection = conn

	case alterRegex.MatchString(sql):
		matches := alterRegex.FindStringSubmatch(sql)
		config.SubscriptionName = matches[1]
		switch strings.ToUpper(matches[2]) {
		case string(AlterDisable):
			config.Action = AlterDisable
		case string(AlterEnable):
			config.Action = AlterEnable
		default:
			return nil, fmt.Errorf("invalid ALTER SUBSCRIPTION action: %s", matches[2])
		}

	case dropRegex.MatchString(sql):
		matches := dropRegex.FindStringSubmatch(sql)
		config.Action = Drop
		config.SubscriptionName = matches[1]

	default:
		return nil, nil
	}

	return &config, nil
}

// parseConnectionString parses the given connection string and returns a ConnectionDetails.
func parseConnectionString(connStr string) (*ConnectionDetails, error) {
	details := &ConnectionDetails{}
	pairs := connectionRegex.FindAllStringSubmatch(connStr, -1)

	if pairs == nil {
		return nil, fmt.Errorf("no valid key-value pairs found in connection string")
	}

	for _, pair := range pairs {
		key := pair[1]
		value := pair[2]
		switch key {
		case "dbname":
			details.DBName = value
		case "host":
			details.Host = value
		case "port":
			details.Port = value
		case "user":
			details.User = value
		case "password":
			details.Password = value
		}
	}

	// Handle default values
	if details.DBName == "" {
		details.DBName = "postgres"
	}
	if details.Port == "" {
		details.Port = "5432"
	}

	return details, nil
}

// ToConnectionInfo Format SubscriptionConfig into a ConnectionInfo
func (config *SubscriptionConfig) ToConnectionInfo() string {
	return fmt.Sprintf("dbname=%s user=%s password=%s host=%s port=%s",
		config.Connection.DBName, config.Connection.User, config.Connection.Password,
		config.Connection.Host, config.Connection.Port)
}

// ToDNS Format SubscriptionConfig into a DNS
func (config *SubscriptionConfig) ToDNS() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		config.Connection.User, config.Connection.Password, config.Connection.Host,
		config.Connection.Port, config.Connection.DBName)
}

func (h *ConnectionHandler) executeSubscriptionSQL(subscriptionConfig *SubscriptionConfig) error {
	switch subscriptionConfig.Action {
	case Create:
		return h.executeCreate(subscriptionConfig)
	case Drop:
		return h.executeDrop(subscriptionConfig)
	case AlterEnable:
		return h.executeEnableSubscription(subscriptionConfig)
	case AlterDisable:
		return h.executeDisableSubscription(subscriptionConfig)
	default:
		return fmt.Errorf("unsupported action: %s", subscriptionConfig.Action)
	}
}

func (h *ConnectionHandler) executeEnableSubscription(subscriptionConfig *SubscriptionConfig) error {
	sqlCtx, err := h.duckHandler.sm.NewContextWithQuery(context.Background(), h.mysqlConn, "")
	if err != nil {
		return fmt.Errorf("failed to create context for query: %w", err)
	}

	if err = logrepl.UpdateSubscriptionStatus(sqlCtx, true, subscriptionConfig.SubscriptionName); err != nil {
		return fmt.Errorf("failed to delete subscription: %w", err)
	}

	if err = adapter.CommitAndCloseTxn(sqlCtx); err != nil {
		return err
	}

	if err = logrepl.UpdateSubscriptions(sqlCtx); err != nil {
		return fmt.Errorf("failed to update subscriptions: %w", err)
	}

	return nil
}

func (h *ConnectionHandler) executeDisableSubscription(subscriptionConfig *SubscriptionConfig) error {
	sqlCtx, err := h.duckHandler.sm.NewContextWithQuery(context.Background(), h.mysqlConn, "")
	if err != nil {
		return fmt.Errorf("failed to create context for query: %w", err)
	}

	if err = logrepl.UpdateSubscriptionStatus(sqlCtx, false, subscriptionConfig.SubscriptionName); err != nil {
		return fmt.Errorf("failed to delete subscription: %w", err)
	}

	if err = adapter.CommitAndCloseTxn(sqlCtx); err != nil {
		return err
	}

	if err = logrepl.UpdateSubscriptions(sqlCtx); err != nil {
		return fmt.Errorf("failed to update subscriptions: %w", err)
	}

	return nil
}

func (h *ConnectionHandler) executeDrop(subscriptionConfig *SubscriptionConfig) error {
	sqlCtx, err := h.duckHandler.sm.NewContextWithQuery(context.Background(), h.mysqlConn, "")
	if err != nil {
		return fmt.Errorf("failed to create context for query: %w", err)
	}

	if err = logrepl.DeleteSubscription(sqlCtx, subscriptionConfig.SubscriptionName); err != nil {
		return fmt.Errorf("failed to delete subscription: %w", err)
	}

	if err = adapter.CommitAndCloseTxn(sqlCtx); err != nil {
		return err
	}

	if err = logrepl.UpdateSubscriptions(sqlCtx); err != nil {
		return fmt.Errorf("failed to update subscriptions: %w", err)
	}

	return nil
}

func (h *ConnectionHandler) executeCreate(subscriptionConfig *SubscriptionConfig) error {
	sqlCtx, err := h.duckHandler.sm.NewContextWithQuery(context.Background(), h.mysqlConn, "")
	if err != nil {
		return fmt.Errorf("failed to create context for query: %w", err)
	}

	lsn, err := h.doSnapshot(sqlCtx, subscriptionConfig)
	if err != nil {
		return fmt.Errorf("failed to create snapshot for CREATE SUBSCRIPTION: %w", err)
	}

	err = h.doCreateSubscription(sqlCtx, subscriptionConfig, lsn)
	if err != nil {
		return fmt.Errorf("failed to execute CREATE SUBSCRIPTION: %w", err)
	}

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

func (h *ConnectionHandler) doCreateSubscription(sqlCtx *sql.Context, subscriptionConfig *SubscriptionConfig, lsn pglogrepl.LSN) error {
	err := logrepl.CreatePublicationIfNotExists(subscriptionConfig.ToDNS(), subscriptionConfig.PublicationName)
	if err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	tx, err := adapter.GetCatalogTxn(sqlCtx, nil)
	if err != nil {
		return fmt.Errorf("failed to get transaction: %w", err)
	}
	defer tx.Rollback()
	defer adapter.CloseTxn(sqlCtx)

	if err = logrepl.CreateSubscription(sqlCtx, subscriptionConfig.SubscriptionName, subscriptionConfig.ToDNS(), subscriptionConfig.PublicationName, lsn.String(), true); err != nil {
		return fmt.Errorf("failed to write subscription: %w", err)
	}

	if err = adapter.CommitAndCloseTxn(sqlCtx); err != nil {
		return err
	}

	if err = logrepl.UpdateSubscriptions(sqlCtx); err != nil {
		return fmt.Errorf("failed to update subscriptions: %w", err)
	}

	return nil
}
