package backend

import (
	"bytes"
	"errors"
	"testing"

	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestLegacyObjectStorageAuditRedactsQueryAndCompletionError(t *testing.T) {
	queries := []string{
		"BACKUP DATABASE app TO 's3://bucket/app/' ENDPOINT='https://s3.example.test:9443' ACCESS_KEY_ID='access-key-71' SECRET_ACCESS_KEY='secret-71'",
		"RESTORE DATABASE app FROM 's3://bucket/app/' ENDPOINT='https://s3.example.test:9443' ACCESS_KEY_ID='access-key-71' SECRET_ACCESS_KEY='secret-71'",
		"BACKUP DATABASE app TO 's3://bucket/app/' ENDPOINT='https://s3.example.test:9443' ACCESS_KEY_ID='access-key-71' SECRET_ACCESS_KEY='secret-71'; SELECT 1",
		"SELECT 1; RESTORE DATABASE app FROM 's3://bucket/app/' ENDPOINT='https://s3.example.test:9443' ACCESS_KEY_ID='access-key-71' SECRET_ACCESS_KEY='secret-71'",
	}
	forbidden := []string{"s3.example.test", "access-key-71", "secret-71", "s3://bucket/app/"}

	logger := logrus.StandardLogger()
	oldOut, oldLevel, oldFormatter := logger.Out, logger.Level, logger.Formatter
	defer func() {
		logger.Out = oldOut
		logger.Level = oldLevel
		logger.Formatter = oldFormatter
	}()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})

	for _, query := range queries {
		var output bytes.Buffer
		logger.SetOutput(&output)
		require.Equal(t, catalog.RedactedSensitiveSQL, catalog.RedactSensitiveSQL(query))

		// Pass the raw protocol text to exercise the audit boundary itself; callers
		// should not need to remember a separate redaction step.
		audit := NewQueryAudit(&mysql.Conn{}, "mysql", query)
		audit.Complete(errors.New("upload failed endpoint=https://s3.example.test:9443 access=access-key-71 secret=secret-71"))

		logged := output.String()
		require.Contains(t, logged, catalog.RedactedSensitiveSQL)
		require.Contains(t, logged, "service-managed operation failed")
		for _, value := range forbidden {
			require.NotContains(t, logged, value)
		}
	}
}
