package pgserver

import (
	"fmt"
	"github.com/apecloud/myduckserver/storage"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// This file implements the logic for handling RESTORE SQL statements.
//
// Syntax:
//   RESTORE DATABASE my_database FROM '<uri>'
//     ENDPOINT = '<endpoint>'
//     ACCESS_KEY_ID = '<access_key>'
//     SECRET_ACCESS_KEY = '<secret_key>'
//
// Example Usage:
//   RESTORE DATABASE my_database FROM 's3://my_bucket/my_database/'
//     ENDPOINT = 's3.cn-northwest-1.amazonaws.com.cn'
//     ACCESS_KEY_ID = 'xxxxxxxxxxxxx'
//     SECRET_ACCESS_KEY = 'xxxxxxxxxxxx'

type RestoreConfig struct {
	DbName        string
	RemoteFile    string
	StorageConfig *storage.ObjectStorageConfig
}

var restoreRegex = regexp.MustCompile(
	`(?i)RESTORE\s+DATABASE\s+(\S+)\s+FROM\s+'(s3c?://[^']+)'` +
		`(?:\s+ENDPOINT\s*=\s*'([^']+)')?` +
		`(?:\s+ACCESS_KEY_ID\s*=\s*'([^']+)')?` +
		`(?:\s+SECRET_ACCESS_KEY\s*=\s*'([^']+)')?`)

func NewRestoreConfig(dbName, remotePath string, storageConfig *storage.ObjectStorageConfig) *RestoreConfig {
	return &RestoreConfig{
		DbName:        dbName,
		RemoteFile:    remotePath,
		StorageConfig: storageConfig,
	}
}

func parseRestoreSQL(sql string) (*RestoreConfig, error) {
	matches := restoreRegex.FindStringSubmatch(sql)
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
		return nil, fmt.Errorf("missing required restore configuration: DATABASE")
	}
	if remoteUri == "" {
		return nil, fmt.Errorf("missing required restore configuration: TO '<URI>'")
	}
	if endpoint == "" {
		return nil, fmt.Errorf("missing required restore configuration: ENDPOINT")
	}
	if accessKeyId == "" {
		return nil, fmt.Errorf("missing required restore configuration: ACCESS_KEY_ID")
	}
	if secretAccessKey == "" {
		return nil, fmt.Errorf("missing required restore configuration: SECRET_ACCESS_KEY")
	}

	storageConfig, remotePath, err := storage.ConstructStorageConfig(remoteUri, endpoint, accessKeyId, secretAccessKey)
	if err != nil {
		return nil, fmt.Errorf("failed to construct storage configuration for restore: %w", err)
	}

	return NewRestoreConfig(dbName, remotePath, storageConfig), nil
}

func (h *ConnectionHandler) executeRestore(restoreConfig *RestoreConfig) (string, error) {
	provider := h.server.Provider
	msg, err := restoreConfig.StorageConfig.DownloadFile(restoreConfig.RemoteFile, provider.DataDir(), restoreConfig.DbName+".db")
	if err != nil {
		return "", fmt.Errorf("failed to download file: %w", err)
	}
	dbFile := filepath.Join(provider.DataDir(), restoreConfig.DbName+".db")
	// load dbFile as DirEntry
	file, err := os.Stat(dbFile)
	if err != nil {
		return "", fmt.Errorf("failed to stat file: %w", err)
	}
	err = provider.AttachCatalog(file, false)
	if err != nil {
		return "", fmt.Errorf("failed to attach catalog: %w", err)
	}
	return msg, nil
}

// ExecuteRestore downloads the specified file from the remote storage and restores it to the specified local directory.
// Note that this should only be called at startup, as this function does not attach the restored database to the catalog.
func ExecuteRestore(dbName, localDir, localFile, remoteUri, endpoint, accessKeyId, secretAccessKey string) (string, error) {
	storageConfig, remotePath, err := storage.ConstructStorageConfig(remoteUri, endpoint, accessKeyId, secretAccessKey)
	if err != nil {
		return "", fmt.Errorf("failed to construct storage configuration for restore: %w", err)
	}

	config := NewRestoreConfig(dbName, remotePath, storageConfig)

	msg, err := config.StorageConfig.DownloadFile(config.RemoteFile, localDir, localFile)
	if err != nil {
		return "", fmt.Errorf("failed to download file: %w", err)
	}
	return msg, nil
}
