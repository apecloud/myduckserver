package pgserver

import (
	"context"
	"fmt"
	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/pgserver/logrepl"
	"github.com/dolthub/go-mysql-server/sql"
	"regexp"
	"strings"
)

// This file implements the logic for handling BACKUP SQL statements.
//
// Syntax:
//   BACKUP DATABASE my_database TO '<uri>'
//     ENDPOINT = '<endpoint>'
//     ACCESS_KEY_ID = '<access_key>'
//     SECRET_ACCESS_KEY = '<secret_key>'
//
// Example Usage:
//   BACKUP DATABASE my_database TO 's3://my_bucket/my_database/'
//     ENDPOINT = 's3.cn-northwest-1.amazonaws.com.cn'
//     ACCESS_KEY_ID = 'xxxxxxxxxxxxx'
//     SECRET_ACCESS_KEY = 'xxxxxxxxxxxx'

type BackupConfig struct {
	DbName          string
	Provider        string
	Path            string
	Endpoint        string
	AccessKeyId     string
	SecretAccessKey string
}

var (
	backupRegex = regexp.MustCompile(`(?i)BACKUP DATABASE (\S+) TO '(s3c?://[^']+)'(?:\s+ENDPOINT = '([^']+)')?(?:\s+ACCESS_KEY_ID = '([^']+)')?(?:\s+SECRET_ACCESS_KEY = '([^']+)')?`)
)

func parseBackupSQL(sql string) (*BackupConfig, error) {
	matches := backupRegex.FindStringSubmatch(sql)
	if matches == nil {
		return nil, nil
	}

	if len(matches) != 6 {
		return nil, fmt.Errorf("invalid number of matches: %d", len(matches))
	}

	// Check if any critical component is missing or empty
	for i, match := range matches[1:] { // Skip the full match, start with the first group
		if strings.TrimSpace(match) == "" {
			return nil, fmt.Errorf("critical backup configuration is missing or empty at position %d", i)
		}
	}

	providerRe := regexp.MustCompile(`(?i)^(s3c?)://`)
	providerMatch := providerRe.FindStringSubmatch(matches[2])
	if providerMatch == nil {
		return nil, fmt.Errorf("provider parsing failed")
	}

	return &BackupConfig{
		DbName:          matches[1],
		Provider:        providerMatch[1], // Use the captured provider
		Path:            matches[2],
		Endpoint:        matches[3],
		AccessKeyId:     matches[4],
		SecretAccessKey: matches[5],
	}, nil
}

func (h *ConnectionHandler) executeBackup(backupConfig *BackupConfig) error {
	sqlCtx, err := h.duckHandler.sm.NewContextWithQuery(context.Background(), h.mysqlConn, "")
	if err != nil {
		return fmt.Errorf("failed to create context for query: %w", err)
	}

	if err := h.stopReplication(sqlCtx); err != nil {
		return fmt.Errorf("failed to stop replication: %w", err)
	}

	if err := doCheckpoint(sqlCtx); err != nil {
		return fmt.Errorf("failed to do checkpoint: %w", err)
	}

	return nil
}

func doCheckpoint(sqlCtx *sql.Context) error {
	if _, err := adapter.ExecCatalogInTxn(sqlCtx, "CHECKPOINT"); err != nil {
		return err
	}

	if err := adapter.CommitAndCloseTxn(sqlCtx); err != nil {
		return err
	}

	return nil
}

func (h *ConnectionHandler) stopReplication(sqlCtx *sql.Context) error {
	err := logrepl.UpdateAllSubscriptionStatus(sqlCtx, false)
	if err != nil {
		return err
	}

	return logrepl.CommitAndUpdate(sqlCtx)
}
