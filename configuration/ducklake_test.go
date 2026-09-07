package configuration

import (
	"strings"
	"testing"
)

func TestDuckLakeConfigDefaultOffAndRedacted(t *testing.T) {
	cfg, err := (DuckLakeConfig{}).Normalize()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Enabled {
		t.Fatal("zero config must remain disabled")
	}
	if got := cfg.RedactedString(); got != "ducklake disabled" {
		t.Fatalf("unexpected disabled rendering: %q", got)
	}

	cfg = DuckLakeConfig{
		Enabled:      true,
		ExtensionDir: "/opt/myduck/extensions",
		S3: DuckLakeS3Config{
			Endpoint:       "https://s3.example.test",
			Region:         "us-east-1",
			UseSSL:         true,
			UseSSLSet:      true,
			AccessKeyIDRef: "ACCESS_REF",
			SecretKeyRef:   "SECRET_REF",
			SecretResolver: func(name string) (string, error) {
				return map[string]string{"ACCESS_REF": "access-value", "SECRET_REF": "secret-value"}[name], nil
			},
		},
	}
	normalized, err := cfg.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	if normalized.S3.AccessKeyID != "access-value" || normalized.S3.SecretAccessKey != "secret-value" {
		t.Fatalf("secret references were not resolved")
	}
	if normalized.S3.AccessKeyIDRef != "" || normalized.S3.SecretKeyRef != "" || normalized.S3.SecretResolver != nil {
		t.Fatalf("secret references remained in normalized runtime config")
	}
	redacted := normalized.RedactedString()
	if strings.Contains(redacted, "access-value") || strings.Contains(redacted, "secret-value") || strings.Contains(redacted, "ACCESS_REF") || strings.Contains(redacted, "SECRET_REF") {
		t.Fatalf("redacted config contains secret material: %q", redacted)
	}
}

func TestDuckLakeConfigRejectsInvalidEnabledValues(t *testing.T) {
	base := DuckLakeConfig{Enabled: true, ExtensionDir: "/opt/myduck/extensions", S3: DuckLakeS3Config{
		Endpoint: "https://s3.example.test", Region: "us-east-1", UseSSL: true, UseSSLSet: true,
		AccessKeyID: "id", SecretAccessKey: "secret",
	}}
	for name, mutate := range map[string]func(*DuckLakeConfig){
		"relative extension dir":     func(c *DuckLakeConfig) { c.ExtensionDir = "relative" },
		"missing endpoint":           func(c *DuckLakeConfig) { c.S3.Endpoint = "" },
		"missing region":             func(c *DuckLakeConfig) { c.S3.Region = "" },
		"bad url style":              func(c *DuckLakeConfig) { c.S3.URLStyle = "query" },
		"metadata secret identifier": func(c *DuckLakeConfig) { c.MetadataPath = "lake_secret" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := base
			mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
