package pgserver

import (
	"fmt"
	"github.com/apecloud/myduckserver/storage"
	"strings"
)

// Since MyDuck Server currently supports only a single database (catalog),
// restore operations are performed only at startup. Once multiple databases
// are supported, we will implement restore as a SQL command.

type RestoreConfig struct {
	DbName        string
	RemoteFile    string
	StorageConfig *storage.ObjectStorageConfig
}

func NewRestoreConfig(dbName, remoteUri, endpoint, accessKeyId, secretAccessKey string) (*RestoreConfig, error) {
	storageConfig, remotePath, err := storage.ConstructStorageConfig(remoteUri, endpoint, accessKeyId, secretAccessKey)
	if err != nil {
		return nil, fmt.Errorf("failed to construct storage configuration for restore: %w", err)
	}

	if strings.HasSuffix(remotePath, "/") {
		return nil, fmt.Errorf("remote path must be a file, not a directory")
	}

	return &RestoreConfig{
		DbName:        dbName,
		RemoteFile:    remotePath,
		StorageConfig: storageConfig,
	}, nil
}

func ExecuteRestore(dbName, localDir, localFile, remoteUri, endpoint, accessKeyId, secretAccessKey string) (string, error) {
	config, err := NewRestoreConfig(dbName, remoteUri, endpoint, accessKeyId, secretAccessKey)
	if err != nil {
		return "", fmt.Errorf("failed to create restore configuration: %w", err)
	}

	msg, err := config.StorageConfig.DownloadFile(config.RemoteFile, localDir, localFile)
	if err != nil {
		return "", fmt.Errorf("failed to download file: %w", err)
	}
	return msg, nil
}
