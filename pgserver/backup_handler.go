package pgserver

import (
	"fmt"
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
	backupRegex = regexp.MustCompile(`(?i)^\s*BACKUP\s+DATABASE\s+(\w+)\s+TO\s+'([^']+)'\s*(.*)$`)
	paramRegex  = regexp.MustCompile(`(?i)(ENDPOINT|ACCESS_KEY_ID|SECRET_ACCESS_KEY)\s*=\s*'([^']+)'`)
)

func parseBackupSQL(sql string) (BackupConfig, error) {
	var config BackupConfig

	normalizedSQL := strings.TrimSpace(sql)
	matches := backupRegex.FindStringSubmatch(normalizedSQL)

	if len(matches) != 4 {
		return config, fmt.Errorf("invalid BACKUP DATABASE syntax")
	}

	config.DbName = matches[1]
	config.Path = matches[2]

	// Infer the provider from the path (e.g., 's3' from 's3://...')
	pathParts := strings.SplitN(config.Path, "://", 2)
	if len(pathParts) != 2 {
		return config, fmt.Errorf("invalid path format: %s", config.Path)
	}
	config.Provider = strings.ToLower(pathParts[0])

	// Process the remaining parameters
	parameters := matches[3]
	paramMatches := paramRegex.FindAllStringSubmatch(parameters, -1)

	for _, pm := range paramMatches {
		key := strings.ToUpper(pm[1])
		value := pm[2]
		switch key {
		case "ENDPOINT":
			config.Endpoint = value
		case "ACCESS_KEY_ID":
			config.AccessKeyId = value
		case "SECRET_ACCESS_KEY":
			config.SecretAccessKey = value
		}
	}

	// Validate required fields
	if config.DbName == "" {
		return config, fmt.Errorf("database name is required")
	}
	if config.Path == "" {
		return config, fmt.Errorf("backup path is required")
	}
	if config.Provider == "" {
		return config, fmt.Errorf("provider could not be inferred from path")
	}

	return config, nil
}

func stopReplication() {

}
