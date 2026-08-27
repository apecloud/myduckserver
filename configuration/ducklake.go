package configuration

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
)

// DefaultDuckLakeExtensionDir is the directory populated by the Linux image
// with the extensions built for the DuckDB ABI used by MyDuck.
const DefaultDuckLakeExtensionDir = "/usr/local/lib/myduckserver/duckdb-extensions"

const (
	duckLakeEnabledEnv      = "MYDUCK_DUCKLAKE_ENABLED"
	duckLakeExtensionDirEnv = "MYDUCK_DUCKLAKE_EXTENSION_DIR"
	duckLakeEndpointEnv     = "MYDUCK_DUCKLAKE_S3_ENDPOINT"
	duckLakeRegionEnv       = "MYDUCK_DUCKLAKE_S3_REGION"
	duckLakeUseSSLEnv       = "MYDUCK_DUCKLAKE_S3_USE_SSL"
	duckLakeURLStyleEnv     = "MYDUCK_DUCKLAKE_S3_URL_STYLE"
	duckLakeAccessKeyEnv    = "MYDUCK_DUCKLAKE_S3_ACCESS_KEY_ID"
	duckLakeSecretKeyEnv    = "MYDUCK_DUCKLAKE_S3_SECRET_ACCESS_KEY"
	duckLakeAccessKeyRefEnv = "MYDUCK_DUCKLAKE_S3_ACCESS_KEY_ID_REF"
	duckLakeSecretKeyRefEnv = "MYDUCK_DUCKLAKE_S3_SECRET_ACCESS_KEY_REF"
)

// SecretResolver resolves a service-managed secret reference. Resolvers are
// called during provider construction and their values are kept in memory
// only; they are never rendered into SQL or logs by the provider.
type SecretResolver func(name string) (string, error)

// SecretReference describes a secret without requiring the caller to put its
// value in a config file. Value is useful for an already-resolved service
// secret and is intentionally not included in String output.
type SecretReference struct {
	Name  string
	Value string
}

// SecretRef is a short compatibility alias for SecretReference.
type SecretRef = SecretReference

// DuckLakeS3Config contains service-side settings used by httpfs and
// DuckLake. AccessKeyID and SecretAccessKey are accepted for callers that
// already resolved their service secret. The *Ref and *Secret forms allow a
// secret manager to resolve values without putting them in ordinary SQL.
type DuckLakeS3Config struct {
	Endpoint string
	Region   string
	UseSSL   bool
	// UseSSLSet distinguishes an explicitly configured false from a zero-value
	// config. It is set by LoadDuckLakeConfig and may be set by embedders.
	UseSSLSet bool
	// TLS, when non-nil, is an alternate spelling useful to config decoders.
	// It takes precedence over UseSSL.
	TLS      *bool
	URLStyle string

	AccessKeyID     string
	SecretAccessKey string
	AccessKeyIDRef  string
	SecretKeyRef    string
	// SecretAccessKeyRef is the descriptive spelling; SecretKeyRef is retained
	// as a short alias for config integrations.
	SecretAccessKeyRef string

	AccessKeyIDSecret     SecretReference
	SecretAccessKeySecret SecretReference
	SecretResolver        SecretResolver
}

// S3Config is a concise alias used by callers that prefer a generic name.
type S3Config = DuckLakeS3Config

// DuckLakeConfig is deliberately default-off. No extension is loaded and no
// S3 setting is applied unless Enabled is true and all fields validate.
type DuckLakeConfig struct {
	Enabled      bool
	ExtensionDir string
	S3           DuckLakeS3Config
}

// DuckLakeServiceConfig is an explicit alias for service configuration APIs.
type DuckLakeServiceConfig = DuckLakeConfig

// Environment variable names are exported for deployment code and tests
// without exposing any secret values.
const (
	DuckLakeEnabledEnv              = duckLakeEnabledEnv
	DuckLakeExtensionDirEnv         = duckLakeExtensionDirEnv
	DuckLakeS3EndpointEnv           = duckLakeEndpointEnv
	DuckLakeS3RegionEnv             = duckLakeRegionEnv
	DuckLakeS3UseSSLEnv             = duckLakeUseSSLEnv
	DuckLakeS3URLStyleEnv           = duckLakeURLStyleEnv
	DuckLakeS3AccessKeyIDEnv        = duckLakeAccessKeyEnv
	DuckLakeS3SecretAccessKeyEnv    = duckLakeSecretKeyEnv
	DuckLakeS3AccessKeyIDRefEnv     = duckLakeAccessKeyRefEnv
	DuckLakeS3SecretAccessKeyRefEnv = duckLakeSecretKeyRefEnv
)

// Normalize validates the enabled configuration and resolves service secret
// references. It returns a copy so callers do not have their config mutated.
func (c DuckLakeConfig) Normalize() (DuckLakeConfig, error) {
	if !c.Enabled {
		return c, nil
	}

	out := c
	if strings.TrimSpace(out.ExtensionDir) == "" {
		out.ExtensionDir = DefaultDuckLakeExtensionDir
	}
	if !isAbsolutePath(out.ExtensionDir) {
		return DuckLakeConfig{}, fmt.Errorf("ducklake extension directory must be absolute")
	}

	var err error
	out.S3.Endpoint, out.S3.UseSSL, err = normalizeEndpointAndTLS(out.S3.Endpoint, out.S3.UseSSL, out.S3.TLS, out.S3.UseSSLSet)
	if err != nil {
		return DuckLakeConfig{}, err
	}
	out.S3.TLS = nil
	out.S3.UseSSLSet = true

	out.S3.Region = strings.TrimSpace(out.S3.Region)
	if out.S3.Region == "" || strings.ContainsAny(out.S3.Region, "\r\n\t ") {
		return DuckLakeConfig{}, fmt.Errorf("ducklake S3 region is required")
	}

	out.S3.URLStyle = strings.ToLower(strings.TrimSpace(out.S3.URLStyle))
	if out.S3.URLStyle != "" && out.S3.URLStyle != "path" && out.S3.URLStyle != "vhost" {
		return DuckLakeConfig{}, fmt.Errorf("ducklake S3 URL style must be path or vhost")
	}

	out.S3.AccessKeyID, err = resolveSecret(
		out.S3.AccessKeyID,
		out.S3.AccessKeyIDRef,
		out.S3.AccessKeyIDSecret,
		out.S3.SecretResolver,
		"access key ID",
	)
	if err != nil {
		return DuckLakeConfig{}, err
	}
	secretRef := out.S3.SecretAccessKeyRef
	if secretRef == "" {
		secretRef = out.S3.SecretKeyRef
	}
	out.S3.SecretAccessKey, err = resolveSecret(
		out.S3.SecretAccessKey,
		secretRef,
		out.S3.SecretAccessKeySecret,
		out.S3.SecretResolver,
		"secret access key",
	)
	if err != nil {
		return DuckLakeConfig{}, err
	}

	// Keep references and resolver out of the runtime copy after resolution.
	out.S3.AccessKeyIDRef = ""
	out.S3.SecretKeyRef = ""
	out.S3.SecretAccessKeyRef = ""
	out.S3.AccessKeyIDSecret = SecretReference{}
	out.S3.SecretAccessKeySecret = SecretReference{}
	out.S3.SecretResolver = nil
	return out, nil
}

// Validate checks an enabled config without exposing secret values. It is
// equivalent to Normalize followed by discarding the resolved copy.
func (c DuckLakeConfig) Validate() error {
	_, err := c.Normalize()
	return err
}

func resolveSecret(value, ref string, embedded SecretReference, resolver SecretResolver, label string) (string, error) {
	value = strings.TrimSpace(value)
	ref = strings.TrimSpace(ref)
	if value != "" && (ref != "" || embedded.Name != "" || embedded.Value != "") {
		return "", fmt.Errorf("ducklake %s has both a value and a secret reference", label)
	}
	if value != "" {
		return value, nil
	}
	if embedded.Value != "" {
		return embedded.Value, nil
	}
	if ref == "" {
		ref = strings.TrimSpace(embedded.Name)
	}
	if ref == "" {
		return "", fmt.Errorf("ducklake %s is required", label)
	}
	if resolver == nil {
		return "", fmt.Errorf("ducklake %s secret resolver is required", label)
	}
	resolved, err := resolver(ref)
	if err != nil || strings.TrimSpace(resolved) == "" {
		// Do not wrap err: secret-manager errors occasionally include the
		// resolved value and must never reach a log or protocol response.
		return "", fmt.Errorf("ducklake %s secret could not be resolved", label)
	}
	return resolved, nil
}

func normalizeEndpointAndTLS(raw string, useSSL bool, tlsSetting *bool, explicit bool) (string, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false, fmt.Errorf("ducklake S3 endpoint is required")
	}
	if strings.ContainsAny(raw, "\r\n\t ") {
		return "", false, fmt.Errorf("ducklake S3 endpoint is invalid")
	}

	scheme := ""
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" || u.User != nil || u.Path != "" && u.Path != "/" || u.RawQuery != "" || u.Fragment != "" {
			return "", false, fmt.Errorf("ducklake S3 endpoint is invalid")
		}
		scheme = strings.ToLower(u.Scheme)
		if scheme != "http" && scheme != "https" {
			return "", false, fmt.Errorf("ducklake S3 endpoint must use http or https")
		}
		raw = u.Host
	}

	if tlsSetting != nil {
		useSSL = *tlsSetting
	} else if !explicit && scheme != "" {
		useSSL = scheme == "https"
	}
	if scheme == "https" && !useSSL {
		return "", false, fmt.Errorf("ducklake S3 endpoint requires TLS")
	}
	if scheme == "http" && useSSL {
		return "", false, fmt.Errorf("ducklake S3 endpoint is cleartext while TLS is enabled")
	}
	if strings.ContainsAny(raw, "/?#") {
		return "", false, fmt.Errorf("ducklake S3 endpoint is invalid")
	}
	if host, _, err := net.SplitHostPort(raw); err == nil {
		if host == "" {
			return "", false, fmt.Errorf("ducklake S3 endpoint is invalid")
		}
	} else if strings.Count(raw, ":") > 1 && !strings.HasPrefix(raw, "[") {
		return "", false, fmt.Errorf("ducklake S3 endpoint is invalid")
	}
	return raw, useSSL, nil
}

func isAbsolutePath(path string) bool {
	return strings.HasPrefix(path, "/")
}

// EnvironmentSecretResolver resolves a reference by looking up the exact
// environment variable name supplied by the service. It is opt-in: ordinary
// config loading never scans the environment for arbitrary secrets.
func EnvironmentSecretResolver(name string) (string, error) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return "", fmt.Errorf("secret is unavailable")
	}
	return value, nil
}

// LoadDuckLakeConfig reads non-secret service settings and optional secret
// values from the documented MYDUCK_DUCKLAKE_* environment variables. The
// absent/false switch returns a disabled config and performs no validation.
func LoadDuckLakeConfig() (DuckLakeConfig, error) {
	enabled, err := envBool(duckLakeEnabledEnv, false)
	if err != nil {
		return DuckLakeConfig{}, err
	}
	c := DuckLakeConfig{Enabled: enabled, ExtensionDir: os.Getenv(duckLakeExtensionDirEnv)}
	if !enabled {
		return c, nil
	}

	c.S3.Endpoint = os.Getenv(duckLakeEndpointEnv)
	c.S3.Region = os.Getenv(duckLakeRegionEnv)
	c.S3.URLStyle = os.Getenv(duckLakeURLStyleEnv)
	if raw, ok := os.LookupEnv(duckLakeUseSSLEnv); ok {
		c.S3.UseSSL, err = parseBool(raw)
		if err != nil {
			return DuckLakeConfig{}, fmt.Errorf("invalid %s", duckLakeUseSSLEnv)
		}
		c.S3.UseSSLSet = true
	} else {
		// Service deployments default to TLS. An explicit false remains
		// available for a deliberately configured local S3-compatible endpoint.
		c.S3.UseSSL = true
		c.S3.UseSSLSet = true
	}
	c.S3.AccessKeyID = os.Getenv(duckLakeAccessKeyEnv)
	c.S3.SecretAccessKey = os.Getenv(duckLakeSecretKeyEnv)
	c.S3.AccessKeyIDRef = os.Getenv(duckLakeAccessKeyRefEnv)
	c.S3.SecretAccessKeyRef = os.Getenv(duckLakeSecretKeyRefEnv)
	if c.S3.AccessKeyIDRef != "" || c.S3.SecretAccessKeyRef != "" {
		c.S3.SecretResolver = EnvironmentSecretResolver
	}
	return c.Normalize()
}

func envBool(name string, fallback bool) (bool, error) {
	raw, ok := os.LookupEnv(name)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := parseBool(raw)
	if err != nil {
		return false, fmt.Errorf("invalid %s", name)
	}
	return value, nil
}

func parseBool(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "t", "yes", "y", "on":
		return true, nil
	case "0", "false", "f", "no", "n", "off":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean")
	}
}

// RedactedString intentionally omits all credential values and references.
func (c DuckLakeConfig) RedactedString() string {
	if !c.Enabled {
		return "ducklake disabled"
	}
	return fmt.Sprintf("ducklake enabled extension_dir=%q endpoint=%q region=%q use_ssl=%t url_style=%q", c.ExtensionDir, c.S3.Endpoint, c.S3.Region, c.S3.UseSSL, c.S3.URLStyle)
}
