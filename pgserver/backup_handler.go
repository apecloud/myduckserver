package pgserver

import (
	"context"
	"fmt"
	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/environment"
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

func parseBackupSQL(sql string) (*BackupConfig, error) {
	matches := backupRegex.FindStringSubmatch(sql)
	if matches == nil {
		// No match means the SQL doesn't follow the expected pattern
		return nil, nil
	}

	// matches:
	// [0] entire match
	// [1] DbName (required)
	// [2] RemoteUri (required)
	// [3] Endpoint (required)
	// [4] AccessKeyId (required)
	// [5] SecretAccessKey (required)
	dbName := strings.TrimSpace(matches[1])
	remoteUri := strings.TrimSpace(matches[2])
	endpoint := strings.TrimSpace(matches[3])
	accessKeyId := strings.TrimSpace(matches[4])
	secretAccessKey := strings.TrimSpace(matches[5])

	if dbName == "" || remoteUri == "" {
		return nil, fmt.Errorf("missing required backup configuration (database name or path)")
	}

	storageConfig, remotePath, err := storage.ConstructStorageConfig(remoteUri, endpoint, accessKeyId, secretAccessKey)
	if err != nil {
		return nil, fmt.Errorf("failed to construct storage StorageConfig: %w", err)
	}

	return &BackupConfig{
		DbName:        dbName,
		RemotePath:    remotePath,
		StorageConfig: storageConfig,
	}, nil
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

	msg, err := backupConfig.StorageConfig.UploadLocalFile(environment.GetDataDirectory(), environment.GetDbFileName(),
		backupConfig.RemotePath)
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
	err := logrepl.UpdateAllSubscriptionStatus(sqlCtx, false)
	if err != nil {
		return err
	}

	return logrepl.CommitAndUpdate(sqlCtx)
}

func startAllReplication(sqlCtx *sql.Context) error {
	err := logrepl.UpdateAllSubscriptionStatus(sqlCtx, true)
	if err != nil {
		return err
	}

	return logrepl.CommitAndUpdate(sqlCtx)
}

func (h *ConnectionHandler) restartServer(readOnly bool) error {
	provider := h.server.Provider
	err := provider.Restart(readOnly)
	if err != nil {
		return err
	}

	return h.server.ConnPool.ResetAndStart(provider.CatalogName(), provider.Connector(), provider.Storage())
}
