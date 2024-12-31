package pgserver

import (
	"context"
	"fmt"
	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/pgserver/logrepl"
	"github.com/apecloud/myduckserver/storage"
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
	DbName        string
	RemotePath    string
	StorageConfig *storage.ObjectStorageConfig
}

var backupRegex = regexp.MustCompile(
	`(?i)BACKUP\s+DATABASE\s+(\S+)\s+TO\s+'(s3c?://[^']+)'` +
		`(?:\s+ENDPOINT\s*=\s*'([^']+)')?` +
		`(?:\s+ACCESS_KEY_ID\s*=\s*'([^']+)')?` +
		`(?:\s+SECRET_ACCESS_KEY\s*=\s*'([^']+)')?`)

func NewBackupConfig(dbName, remotePath string, storageConfig *storage.ObjectStorageConfig) *BackupConfig {
	return &BackupConfig{
		DbName:        dbName,
		RemotePath:    remotePath,
		StorageConfig: storageConfig,
	}
}

func parseBackupSQL(sql string) (*BackupConfig, error) {
	matches := backupRegex.FindStringSubmatch(sql)
	if matches == nil {
		// No match means the SQL doesn't follow the expected pattern
		return nil, nil
	}

	// matches:
	// [1] DbName
	// [2] RemoteUri
	// [3] Endpoint
	// [4] AccessKeyId
	// [5] SecretAccessKey
	dbName := strings.TrimSpace(matches[1])
	remoteUri := strings.TrimSpace(matches[2])
	endpoint := strings.TrimSpace(matches[3])
	accessKeyId := strings.TrimSpace(matches[4])
	secretAccessKey := strings.TrimSpace(matches[5])

	if dbName == "" {
		return nil, fmt.Errorf("missing required backup configuration: DATABASE")
	}
	if remoteUri == "" {
		return nil, fmt.Errorf("missing required backup configuration: TO '<URI>'")
	}
	if endpoint == "" {
		return nil, fmt.Errorf("missing required backup configuration: ENDPOINT")
	}
	if accessKeyId == "" {
		return nil, fmt.Errorf("missing required backup configuration: ACCESS_KEY_ID")
	}
	if secretAccessKey == "" {
		return nil, fmt.Errorf("missing required backup configuration: SECRET_ACCESS_KEY")
	}

	storageConfig, remotePath, err := storage.ConstructStorageConfig(remoteUri, endpoint, accessKeyId, secretAccessKey)
	if err != nil {
		return nil, fmt.Errorf("failed to construct storage configuration for backup: %w", err)
	}

	return NewBackupConfig(dbName, remotePath, storageConfig), nil
}

func (h *ConnectionHandler) executeBackup(backupConfig *BackupConfig) (string, error) {
	sqlCtx, err := h.duckHandler.sm.NewContextWithQuery(context.Background(), h.mysqlConn, "")
	if err != nil {
		return "", fmt.Errorf("failed to create context for query: %w", err)
	}

	if err := stopAllReplication(sqlCtx); err != nil {
		return "", fmt.Errorf("failed to stop replication: %w", err)
	}

	if err := doCheckpoint(sqlCtx); err != nil {
		return "", fmt.Errorf("failed to do checkpoint: %w", err)
	}

	err = h.restartServer(true)
	if err != nil {
		return "", err
	}

	msg, err := backupConfig.StorageConfig.UploadFile(
		h.server.Provider.DataDir(), backupConfig.DbName+".db", backupConfig.RemotePath)
	if err != nil {
		return "", err
	}

	err = h.restartServer(false)
	if err != nil {
		return "", fmt.Errorf("backup finished: %s, but failed to restart server: %w", msg, err)
	}

	if err = startAllReplication(sqlCtx); err != nil {
		return "", fmt.Errorf("backup finished: %s, but failed to start replication: %w", msg, err)
	}

	return msg, nil
}

func (h *ConnectionHandler) restartServer(readOnly bool) error {
	provider := h.server.Provider
	return provider.Restart(readOnly)
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

func stopAllReplication(sqlCtx *sql.Context) error {
	if err := logrepl.UpdateAllSubscriptionStatus(sqlCtx, false); err != nil {
		return err
	}

	if err := adapter.CommitAndCloseTxn(sqlCtx); err != nil {
		return err
	}

	if err := logrepl.UpdateSubscriptions(sqlCtx); err != nil {
		return fmt.Errorf("failed to update subscriptions: %w", err)
	}

	return nil
}

func startAllReplication(sqlCtx *sql.Context) error {
	if err := logrepl.UpdateAllSubscriptionStatus(sqlCtx, true); err != nil {
		return err
	}

	if err := adapter.CommitAndCloseTxn(sqlCtx); err != nil {
		return err
	}

	if err := logrepl.UpdateSubscriptions(sqlCtx); err != nil {
		return fmt.Errorf("failed to update subscriptions: %w", err)
	}

	return nil
}
