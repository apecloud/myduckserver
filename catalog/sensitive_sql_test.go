package catalog

import (
	"strings"
	"testing"
)

func TestSensitiveSQLBoundary(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		sensitive bool
	}{
		{name: "create secret", query: "CREATE SECRET svc (TYPE S3, KEY_ID 'id', SECRET 'value')", sensitive: true},
		{name: "load extension", query: "LOAD 'httpfs.duckdb_extension'", sensitive: true},
		{name: "remote load data", query: "LOAD DATA INFILE 's3://bucket/file' INTO TABLE t", sensitive: true},
		{name: "remote http path", query: "SELECT http://bucket/object", sensitive: true},
		{name: "load extension function", query: "SELECT load_extension('x')", sensitive: true},
		{name: "dynamic current setting", query: "SELECT current_setting(?)", sensitive: true},
		{name: "parameter current setting", query: "SELECT current_setting($1)", sensitive: true},
		{name: "quoted current setting alias", query: `SELECT 'myDUCK' AS "current_setting"`, sensitive: false},
		{name: "sql prepare", query: "PREPARE p AS SELECT 1", sensitive: true},
		{name: "sql execute", query: "EXECUTE p", sensitive: true},
		{name: "unicode whitespace", query: "CREATE\u00a0SECRET svc (TYPE S3)", sensitive: true},
		{name: "multi statement", query: "SELECT 1; CREATE SECRET svc (TYPE S3)", sensitive: true},
		{name: "literal text", query: "SELECT 'CREATE SECRET is documented here'", sensitive: false},
		{name: "comment text", query: "SELECT 1 /* CREATE SECRET svc */", sensitive: false},
		{name: "nested comment cannot hide statement", query: "SELECT 1 /* outer /* inner */; CREATE SECRET svc (TYPE S3) */", sensitive: true},
		{name: "safe setting", query: "SELECT current_setting('search_path')", sensitive: false},
		{name: "application setting", query: "SELECT current_setting('application_name')", sensitive: false},
		{name: "set application", query: "SET application_name TO 'myDUCK'", sensitive: false},
		{name: "reset application", query: "RESET application_name", sensitive: false},
		{name: "set proxy username", query: "SET http_proxy_username TO 'myDUCK'", sensitive: false},
		{name: "reset proxy username", query: "RESET http_proxy_username", sensitive: false},
		{name: "comment operator leaves statement", query: "SELECT 1--x; CREATE SECRET svc (TYPE S3)", sensitive: true},
		{name: "minus operator then safe", query: "SELECT 1--2", sensitive: false},
		{name: "local load data", query: "LOAD DATA LOCAL INFILE '/tmp/file' INTO TABLE t", sensitive: false},
		{name: "standard string backslash cannot hide statement", query: "SELECT 'x\\'; CREATE SECRET svc (TYPE S3)", sensitive: true},
		{name: "quoted remote select is safe", query: "SELECT 's3://bucket/object'", sensitive: false},
		{name: "legacy backup keeps object storage path", query: "BACKUP DATABASE my_database TO 's3://bucket/my_database/' ENDPOINT = 's3.example' ACCESS_KEY_ID = 'id' SECRET_ACCESS_KEY = 'secret'", sensitive: false},
		{name: "legacy restore keeps object storage path", query: "RESTORE DATABASE my_database FROM 's3://bucket/my_database/' ENDPOINT = 's3.example' ACCESS_KEY_ID = 'id' SECRET_ACCESS_KEY = 'secret'", sensitive: false},
		{name: "legacy backup redacts trailing statement", query: "BACKUP DATABASE my_database TO 's3://bucket/my_database/' ENDPOINT = 's3.example' ACCESS_KEY_ID = 'id' SECRET_ACCESS_KEY = 'secret'; SELECT 1", sensitive: false},
		{name: "legacy restore redacts leading statement", query: "SELECT 1; RESTORE DATABASE my_database FROM 's3://bucket/my_database/' ENDPOINT = 's3.example' ACCESS_KEY_ID = 'id' SECRET_ACCESS_KEY = 'secret'", sensitive: false},
		{name: "copy sensitive option assignment", query: "COPY t TO '/tmp/out' (s3_endpoint = 's3.example')", sensitive: true},
		{name: "option-like RHS identifier is safe", query: "COPY t TO '/tmp/out' (label = s3_endpoint)", sensitive: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsSensitiveSQL(tt.query); got != tt.sensitive {
				t.Fatalf("IsSensitiveSQL(%q) = %t, want %t", tt.query, got, tt.sensitive)
			}
			if tt.sensitive {
				if got := RedactSensitiveSQL(tt.query); got == tt.query || strings.Contains(got, "SECRET") {
					t.Fatalf("sensitive query was not fully redacted: %q", got)
				}
			} else if strings.Contains(strings.ToUpper(tt.query), "BACKUP DATABASE") || strings.Contains(strings.ToUpper(tt.query), "RESTORE DATABASE") {
				if got := RedactSensitiveSQL(tt.query); got == tt.query || strings.Contains(got, "secret") {
					t.Fatalf("legacy storage query was not redacted: %q", got)
				}
			} else if got := RedactSensitiveSQL(tt.query); got != tt.query {
				t.Fatalf("safe query was unexpectedly redacted: %q", got)
			}
		})
	}
}
