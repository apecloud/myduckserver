package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/apecloud/myduckserver/configuration"
	"github.com/apecloud/myduckserver/mycontext"
)

// DuckDBExtensionVersion is the ABI/version used by the Go DuckDB binding in
// this repository. Extension artifacts are never downloaded at runtime.
const DuckDBExtensionVersion = "1.5.5"

// DuckDBExtensionABI is kept as a separate field in the manifest so an
// artifact cannot be accidentally paired with a different DuckDB binding.
const DuckDBExtensionABI = DuckDBExtensionVersion

// DuckLakeSecretName is an internal, service-owned secret name. User SQL that
// mentions any SECRET object is rejected before it reaches DuckDB.
const DuckLakeSecretName = "__myduckserver_ducklake_service"

// DuckLakeCatalogName is the one service-owned catalog attached to each
// eligible physical connection. Logical MyDuck databases and schemas are
// mapped into this catalog by the provider; clients never select it directly.
const DuckLakeCatalogName = "__myduck_ducklake"

// ExtensionArtifact is one fixed, decompressed DuckDB extension binary. The
// filename is deliberately fixed so service configuration cannot select an
// arbitrary extension or trigger an INSTALL/network fallback.
type ExtensionArtifact struct {
	Name         string
	FileName     string
	SHA256       string
	Architecture string
	ABI          string
	Version      string
}

var linuxDuckLakeExtensionManifests = map[string][]ExtensionArtifact{
	"amd64": {
		{Name: "httpfs", FileName: "httpfs.duckdb_extension", SHA256: "887c392b1e49128d11667c81e3698d8b00dfdeb456771acf66d05a0f74f7b7d8", Architecture: "amd64", ABI: DuckDBExtensionABI, Version: DuckDBExtensionVersion},
		{Name: "ducklake", FileName: "ducklake.duckdb_extension", SHA256: "e51bf9e8d933d0e83780ae096455501b542cf962569a2ce5613532d702c08302", Architecture: "amd64", ABI: DuckDBExtensionABI, Version: DuckDBExtensionVersion},
	},
	"arm64": {
		{Name: "httpfs", FileName: "httpfs.duckdb_extension", SHA256: "eba6e263e395a83966090f1f11ade63630b1b21422f0f2813858d179d42ea1e9", Architecture: "arm64", ABI: DuckDBExtensionABI, Version: DuckDBExtensionVersion},
		{Name: "ducklake", FileName: "ducklake.duckdb_extension", SHA256: "d0b57c8e261b89a1ae367c7224f0857cfde72ab6cf2609f188e0de9b897b1088", Architecture: "arm64", ABI: DuckDBExtensionABI, Version: DuckDBExtensionVersion},
	},
}

// DuckLakeExtensionManifest returns a copy of the fixed manifest for a
// supported target. Only Linux amd64 and arm64 artifacts are accepted.
func DuckLakeExtensionManifest(goos, goarch string) ([]ExtensionArtifact, error) {
	if goos != "linux" {
		return nil, fmt.Errorf("ducklake extensions are only packaged for linux")
	}
	manifest, ok := linuxDuckLakeExtensionManifests[goarch]
	if !ok {
		return nil, fmt.Errorf("ducklake extensions are unavailable for linux/%s", goarch)
	}
	return append([]ExtensionArtifact(nil), manifest...), nil
}

// CurrentDuckLakeExtensionManifest resolves the manifest for the running
// binary's target platform.
func CurrentDuckLakeExtensionManifest() ([]ExtensionArtifact, error) {
	return DuckLakeExtensionManifest(runtime.GOOS, runtime.GOARCH)
}

// VerifyDuckLakeExtensions verifies every fixed artifact before it is handed
// to DuckDB. It rejects symlinks and non-regular files so a mutable service
// path cannot silently substitute a different extension.
func VerifyDuckLakeExtensions(extensionDir string, manifest []ExtensionArtifact) error {
	return verifyDuckLakeExtensions(extensionDir, manifest, "")
}

// VerifyDuckLakeExtensionsForTarget additionally binds every artifact to the
// requested Linux architecture. It is used by the runtime before LOAD so an
// arm64 artifact cannot be substituted into an amd64 image (or vice versa).
func VerifyDuckLakeExtensionsForTarget(extensionDir string, manifest []ExtensionArtifact, goos, goarch string) error {
	if goos != "linux" {
		return fmt.Errorf("ducklake extensions are only packaged for linux")
	}
	if goarch != "amd64" && goarch != "arm64" {
		return fmt.Errorf("ducklake extensions are unavailable for linux/%s", goarch)
	}
	if err := verifyDuckLakeExtensions(extensionDir, manifest, goarch); err != nil {
		return err
	}
	// Runtime manifests are immutable values from the fixed table. Compare all
	// identity fields as well as the file hash, so a caller cannot replace the
	// trusted manifest with a self-consistent but untrusted pair.
	expected, ok := linuxDuckLakeExtensionManifests[goarch]
	if !ok || len(expected) != len(manifest) {
		return fmt.Errorf("ducklake extension manifest does not match target")
	}
	for i := range expected {
		if expected[i] != manifest[i] {
			return fmt.Errorf("ducklake extension manifest does not match target")
		}
	}
	return nil
}

func verifyDuckLakeExtensions(extensionDir string, manifest []ExtensionArtifact, expectedArch string) error {
	if strings.TrimSpace(extensionDir) == "" || !filepath.IsAbs(extensionDir) {
		return fmt.Errorf("ducklake extension directory must be absolute")
	}
	if len(manifest) == 0 {
		return fmt.Errorf("ducklake extension manifest is empty")
	}
	seenNames := make(map[string]struct{}, len(manifest))
	seenFiles := make(map[string]struct{}, len(manifest))
	for _, artifact := range manifest {
		if artifact.Name == "" || artifact.FileName == "" || len(artifact.SHA256) != sha256.Size*2 || artifact.Architecture == "" || artifact.ABI == "" || artifact.Version == "" {
			return fmt.Errorf("ducklake extension manifest entry is invalid")
		}
		if _, err := hex.DecodeString(artifact.SHA256); err != nil {
			return fmt.Errorf("ducklake extension manifest hash is invalid")
		}
		if artifact.Architecture != "amd64" && artifact.Architecture != "arm64" {
			return fmt.Errorf("ducklake extension architecture is unsupported")
		}
		if expectedArch != "" && artifact.Architecture != expectedArch {
			return fmt.Errorf("ducklake extension architecture does not match target")
		}
		if artifact.ABI != DuckDBExtensionABI || artifact.Version != DuckDBExtensionVersion {
			return fmt.Errorf("ducklake extension ABI/version is unsupported")
		}
		if artifact.Name != "httpfs" && artifact.Name != "ducklake" {
			return fmt.Errorf("ducklake extension name is unsupported")
		}
		if artifact.FileName != artifact.Name+".duckdb_extension" {
			return fmt.Errorf("ducklake extension filename is unsupported")
		}
		if _, ok := seenNames[artifact.Name]; ok {
			return fmt.Errorf("ducklake extension manifest has duplicate names")
		}
		if _, ok := seenFiles[artifact.FileName]; ok {
			return fmt.Errorf("ducklake extension manifest has duplicate files")
		}
		seenNames[artifact.Name] = struct{}{}
		seenFiles[artifact.FileName] = struct{}{}
		if filepath.Base(artifact.FileName) != artifact.FileName || artifact.FileName == "." || artifact.FileName == ".." {
			return fmt.Errorf("ducklake extension filename is invalid")
		}
		path := filepath.Join(extensionDir, artifact.FileName)
		info, err := os.Lstat(path)
		if err != nil {
			return fmt.Errorf("ducklake extension %s is unavailable", artifact.Name)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("ducklake extension %s is not a regular file", artifact.Name)
		}
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("ducklake extension %s cannot be opened", artifact.Name)
		}
		hash := sha256.New()
		_, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil {
			return fmt.Errorf("ducklake extension %s cannot be read", artifact.Name)
		}
		actual := fmt.Sprintf("%x", hash.Sum(nil))
		if !strings.EqualFold(actual, artifact.SHA256) {
			return fmt.Errorf("ducklake extension %s failed integrity verification", artifact.Name)
		}
	}
	if len(seenNames) != 2 || len(seenFiles) != 2 {
		return fmt.Errorf("ducklake extension manifest is incomplete")
	}
	return nil
}

// ProviderOption configures optional provider behavior while preserving the
// historical three-argument NewDBProvider call sites.
type ProviderOption func(*providerOptions) error

type providerOptions struct {
	duckLake configuration.DuckLakeConfig
}

// WithDuckLakeConfig enables the service-managed DuckLake connection layer.
// The feature remains disabled when config.Enabled is false.
func WithDuckLakeConfig(config configuration.DuckLakeConfig) ProviderOption {
	return func(options *providerOptions) error {
		options.duckLake = config
		return nil
	}
}

// WithDuckLakeServiceConfig is a descriptive alias for WithDuckLakeConfig.
func WithDuckLakeServiceConfig(config configuration.DuckLakeServiceConfig) ProviderOption {
	return WithDuckLakeConfig(config)
}

type duckLakeRuntime struct {
	config       configuration.DuckLakeConfig
	manifest     []ExtensionArtifact
	initializeMu sync.Mutex
	// initialized keys successful setup by the underlying physical driver
	// connection. The cache avoids reissuing LOAD/CREATE SECRET on every
	// statement while the provider clears it whenever the pool generation is
	// replaced (Reset/Restart).
	initialized sync.Map // map[driver.Conn]struct{}
	attached    sync.Map // map[driver.Conn]struct{}
}

func newDuckLakeRuntime(config configuration.DuckLakeConfig) (*duckLakeRuntime, error) {
	if !config.Enabled {
		return nil, nil
	}
	normalized, err := config.Normalize()
	if err != nil {
		return nil, err
	}
	manifest, err := CurrentDuckLakeExtensionManifest()
	if err != nil {
		return nil, err
	}
	if err := VerifyDuckLakeExtensionsForTarget(normalized.ExtensionDir, manifest, runtime.GOOS, runtime.GOARCH); err != nil {
		return nil, err
	}
	return &duckLakeRuntime{config: normalized, manifest: manifest}, nil
}

func (rt *duckLakeRuntime) initialize(ctx context.Context, execer driver.ExecerContext) error {
	return rt.initializeForConn(ctx, nil, execer)
}

// initializeForConn applies service settings to one physical connection. The
// connector and ConnectionPool both reach this hook for a pooled acquisition.
// Successful setup is cached by raw driver connection identity so a reused
// connection does not execute service SQL for every statement. The provider
// clears that cache before replacing a pool generation.
func (rt *duckLakeRuntime) initializeForConn(ctx context.Context, conn driver.Conn, execer driver.ExecerContext) error {
	if rt == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !mycontext.IsDuckLakeEligibleQuery(ctx) {
		// Unknown and replication contexts intentionally perform no DuckLake
		// operation. In particular, do not use RESET/SET here: those commands
		// would still make a shared replication connection observe service state.
		return nil
	}
	var key any
	if conn != nil {
		typ := reflect.TypeOf(conn)
		if typ != nil && typ.Comparable() {
			key = conn
		}
	}
	// Keep the short initialization sequence atomic for a physical connection.
	// In normal operation database/sql serializes Connect/Raw, but the explicit
	// lock also covers the connector and pool hooks racing during recovery.
	rt.initializeMu.Lock()
	defer rt.initializeMu.Unlock()
	if key != nil {
		if _, ok := rt.initialized.Load(key); ok {
			return nil
		}
	}
	// Re-check immediately before LOAD to narrow the verify/execute window for
	// a mutable extension directory.
	if err := VerifyDuckLakeExtensionsForTarget(rt.config.ExtensionDir, rt.manifest, runtime.GOOS, runtime.GOARCH); err != nil {
		return fmt.Errorf("ducklake extension integrity check failed")
	}

	for _, artifact := range rt.manifest {
		path := filepath.Join(rt.config.ExtensionDir, artifact.FileName)
		query := "LOAD '" + strings.ReplaceAll(path, "'", "''") + "'"
		if _, err := execer.ExecContext(ctx, query, nil); err != nil && !isAlreadyLoadedError(err) {
			return fmt.Errorf("load duckdb extension %s failed", artifact.Name)
		}
	}

	// DuckDB 1.5.5 rejects parameters in CREATE SECRET expressions. This SQL
	// is issued only by the service initializer, after the protocol gate, and
	// never passes through query audit or request logging. Quote every value as
	// a SQL string literal so configuration cannot turn the internal statement
	// into a second command; errors below intentionally omit the statement.
	query := "CREATE OR REPLACE SECRET \"" + DuckLakeSecretName + "\" (TYPE S3, PROVIDER config, KEY_ID " + duckDBStringLiteral(rt.config.S3.AccessKeyID) + ", SECRET " + duckDBStringLiteral(rt.config.S3.SecretAccessKey) + ", ENDPOINT " + duckDBStringLiteral(rt.config.S3.Endpoint) + ", REGION " + duckDBStringLiteral(rt.config.S3.Region) + ", USE_SSL " + duckDBBoolLiteral(rt.config.S3.UseSSL)
	if rt.config.S3.URLStyle != "" {
		query += ", URL_STYLE " + duckDBStringLiteral(rt.config.S3.URLStyle)
	}
	query += ")"
	if _, err := execer.ExecContext(ctx, query, nil); err != nil {
		// Do not wrap the driver error: DuckDB may echo SQL fragments and a
		// service credential in an extension/provider error.
		return fmt.Errorf("create ducklake service secret failed")
	}
	// A deployment may enable only the extension/S3 service layer (the #71
	// configuration). Attach the lake lazily in that case; object-table callers
	// use EnsureAttached, which fails closed if either path is absent. When both
	// paths are configured, attaching here guarantees that every eligible
	// physical connection is ready before a transaction can begin.
	if rt.config.MetadataPath != "" && rt.config.DataPath != "" {
		if err := rt.attachLocked(ctx, key, execer); err != nil {
			return err
		}
	}
	if key != nil {
		rt.initialized.Store(key, struct{}{})
	}
	return nil
}

// EnsureAttached initializes and attaches the service lake on an eligible
// physical connection. It is the object-table boundary: incomplete service
// paths are rejected here rather than guessed from SQL or process state.
func (rt *duckLakeRuntime) EnsureAttached(ctx context.Context, conn driver.Conn, execer driver.ExecerContext) error {
	if rt == nil {
		return fmt.Errorf("ducklake is disabled")
	}
	if !mycontext.IsDuckLakeEligibleQuery(ctx) {
		return fmt.Errorf("ducklake is unavailable for this query origin")
	}
	if strings.TrimSpace(rt.config.MetadataPath) == "" || strings.TrimSpace(rt.config.DataPath) == "" {
		return fmt.Errorf("ducklake metadata and data paths are required for object tables")
	}
	if err := rt.initializeForConn(ctx, conn, execer); err != nil {
		return err
	}
	// initializeForConn attaches when both paths are configured. The fallback
	// below covers a nil/non-comparable driver identity used by unit tests.
	var key any
	if conn != nil {
		typ := reflect.TypeOf(conn)
		if typ != nil && typ.Comparable() {
			key = conn
		}
	}
	rt.initializeMu.Lock()
	defer rt.initializeMu.Unlock()
	if key != nil {
		if _, ok := rt.attached.Load(key); ok {
			return nil
		}
	}
	if err := rt.attachLocked(ctx, key, execer); err != nil {
		return err
	}
	return nil
}

func (rt *duckLakeRuntime) attachLocked(ctx context.Context, key any, execer driver.ExecerContext) error {
	if key != nil {
		if _, ok := rt.attached.Load(key); ok {
			return nil
		}
	}
	metadata := strings.TrimSpace(rt.config.MetadataPath)
	dataPath := strings.TrimSpace(rt.config.DataPath)
	if metadata == "" || dataPath == "" {
		return fmt.Errorf("ducklake metadata and data paths are required for object tables")
	}
	// Both values have already passed configuration validation. SQL-literal
	// quoting is still required because service paths can contain apostrophes.
	attach := "ATTACH IF NOT EXISTS " + duckDBStringLiteral("ducklake:"+metadata) +
		" AS " + QuoteIdentifierANSI(DuckLakeCatalogName) +
		" (DATA_PATH " + duckDBStringLiteral(dataPath) + ", DATA_INLINING_ROW_LIMIT 0)"
	if _, err := execer.ExecContext(ctx, attach, nil); err != nil {
		// Do not expose the metadata/data path or a driver error that may echo
		// the generated statement in a protocol response.
		return fmt.Errorf("attach ducklake catalog failed")
	}
	if key != nil {
		rt.attached.Store(key, struct{}{})
	}
	return nil
}

// resetInitialized forgets all physical connections from a previous
// database/sql generation. It shares initializeMu with setup so Reset cannot
// race a LOAD/CREATE SECRET sequence.
func (rt *duckLakeRuntime) resetInitialized() {
	if rt == nil {
		return
	}
	rt.initializeMu.Lock()
	rt.initialized.Clear()
	rt.attached.Clear()
	rt.initializeMu.Unlock()
}

func duckDBStringLiteral(value string) string {
	// Ordinary DuckDB string literals preserve backslashes; only a single quote
	// terminates the literal and therefore needs SQL-standard doubling.
	value = strings.ReplaceAll(value, "'", "''")
	return "'" + value + "'"
}

func duckDBBoolLiteral(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func isAlreadyLoadedError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "already loaded") || strings.Contains(message, "already been loaded")
}
