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
	DbName string
	Path   string
	config *storage.ObjectStorageConfig
}

// Regex Explanation:
// 1. Capture the database name after "BACKUP DATABASE".
// 2. Capture the path after "TO", which should start with s3:// or s3c://.
// 3. Optionally capture ENDPOINT.
// 4. Optionally capture ACCESS_KEY_ID.
// 5. Optionally capture SECRET_ACCESS_KEY.
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
	// [2] Path (required)
	// [3] Endpoint (optional)
	// [4] AccessKeyId (optional)
	// [5] SecretAccessKey (optional)
	dbName := strings.TrimSpace(matches[1])
	path := strings.TrimSpace(matches[2])
	if dbName == "" || path == "" {
		return nil, fmt.Errorf("missing required backup configuration (database name or path)")
	}

	provider := ""
	switch {
	case strings.HasPrefix(strings.ToLower(path), "s3c://"):
		provider = "s3c"
	case strings.HasPrefix(strings.ToLower(path), "s3://"):
		provider = "s3"
	default:
		return nil, fmt.Errorf("unsupported provider in path: %s", path)
	}

	region := ""
	if provider == "s3" {
		region = storage.ParseS3RegionCode(matches[3])
		if region == "" {
			return nil, fmt.Errorf("missing region in endpoint: %s", matches[3])
		}
	} else {
		region = storage.DefaultRegion
	}

	config := &storage.ObjectStorageConfig{
		Provider:        provider,
		Endpoint:        strings.TrimSpace(matches[3]),
		AccessKeyId:     strings.TrimSpace(matches[4]),
		SecretAccessKey: strings.TrimSpace(matches[5]),
		Region:          region,
	}

	return &BackupConfig{
		DbName: dbName,
		Path:   path,
		config: config,
	}, nil
}

func (h *ConnectionHandler) executeBackup(backupConfig *BackupConfig) error {
	sqlCtx, err := h.duckHandler.sm.NewContextWithQuery(context.Background(), h.mysqlConn, "")
	if err != nil {
		return fmt.Errorf("failed to create context for query: %w", err)
	}

	if err := stopReplication(sqlCtx); err != nil {
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

func stopReplication(sqlCtx *sql.Context) error {
	err := logrepl.UpdateAllSubscriptionStatus(sqlCtx, false)
	if err != nil {
		return err
	}

	return logrepl.CommitAndUpdate(sqlCtx)
}

// TODO(neo.zty): add content.
func uploadFileToObjectStorage() {

}

// TODO(neo.zty): add content.
func stopServer() {
}
