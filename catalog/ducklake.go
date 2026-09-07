package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	stderrors "errors"
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
	"github.com/duckdb/duckdb-go/v2"
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

// duckLakeInitStage identifies the service-owned SQL step that failed. Keep
// these values stable: they are the only stage detail exposed to a protocol or
// log consumer when a driver error is returned.
type duckLakeInitStage string

const (
	duckLakeStageLoad         duckLakeInitStage = "load"
	duckLakeStageCreateSecret duckLakeInitStage = "create_secret"
	duckLakeStageAttach       duckLakeInitStage = "attach"
	duckLakeStageUnknown      duckLakeInitStage = "unknown"
)

type duckLakeFailureReason string

const (
	duckLakeReasonContextCanceled      duckLakeFailureReason = "context_canceled"
	duckLakeReasonDeadlineExceeded     duckLakeFailureReason = "deadline_exceeded"
	duckLakeReasonBadConnection        duckLakeFailureReason = "bad_connection"
	duckLakeReasonPermissionDenied     duckLakeFailureReason = "permission_denied"
	duckLakeReasonHTTPFailure          duckLakeFailureReason = "http_request_failed"
	duckLakeReasonNetworkFailure       duckLakeFailureReason = "network_failure"
	duckLakeReasonMissingObject        duckLakeFailureReason = "missing_object"
	duckLakeReasonTLSFailure           duckLakeFailureReason = "tls_failure"
	duckLakeReasonIOFailure            duckLakeFailureReason = "io_failure"
	duckLakeReasonInvalidConfiguration duckLakeFailureReason = "invalid_configuration"
	duckLakeReasonExtensionUnavailable duckLakeFailureReason = "extension_unavailable"
	duckLakeReasonResourceExhaustion   duckLakeFailureReason = "resource_exhaustion"
	duckLakeReasonResourceUnavailable  duckLakeFailureReason = "resource_unavailable"
	duckLakeReasonInvalidStatement     duckLakeFailureReason = "invalid_statement"
	duckLakeReasonCatalogFailure       duckLakeFailureReason = "catalog_failure"
	duckLakeReasonInterrupted          duckLakeFailureReason = "interrupted"
	duckLakeReasonUnexpectedEOF        duckLakeFailureReason = "unexpected_eof"
	duckLakeReasonTimeout              duckLakeFailureReason = "timeout"
	duckLakeReasonDriverError          duckLakeFailureReason = "driver_error"
)

// These aliases make the minimum ATTACH diagnostic contract explicit while
// retaining the more descriptive reason names used by the existing protocol
// output. The underlying values are deliberately stable and closed.
const (
	duckLakeReasonPermission = duckLakeReasonPermissionDenied
	duckLakeReasonHTTP       = duckLakeReasonHTTPFailure
	duckLakeReasonNetwork    = duckLakeReasonNetworkFailure
	duckLakeReasonExtension  = duckLakeReasonExtensionUnavailable
	duckLakeReasonGenericIO  = duckLakeReasonIOFailure
)

// duckLakeInitError keeps the original driver error private while exposing a
// small, stable diagnostic. A raw Unwrap is deliberately not provided: the
// PostgreSQL handler uses errors.As to replace an error's safe text with a
// PgError.Message, which would otherwise re-expose SQL, paths, or credentials.
// Is preserves sentinel matching (for example context cancellation and
// driver.ErrBadConn) without making the raw error reachable through protocol
// error formatting or errors.As.
type duckLakeInitError struct {
	stage     duckLakeInitStage
	extension string
	reason    duckLakeFailureReason
	cause     error
}

func newDuckLakeInitError(stage duckLakeInitStage, extension string, cause error) error {
	if cause == nil {
		return nil
	}
	stage, extension = normalizeDuckLakeInitErrorContext(stage, extension)
	return &duckLakeInitError{
		stage:     stage,
		extension: extension,
		reason:    classifyDuckLakeFailureForStage(stage, cause),
		cause:     cause,
	}
}

func normalizeDuckLakeInitErrorContext(stage duckLakeInitStage, extension string) (duckLakeInitStage, string) {
	switch stage {
	case duckLakeStageLoad:
		if extension == "httpfs" || extension == "ducklake" {
			return stage, extension
		}
		return stage, ""
	case duckLakeStageCreateSecret, duckLakeStageAttach:
		return stage, ""
	default:
		return duckLakeStageUnknown, ""
	}
}

func (err *duckLakeInitError) operation() string {
	switch err.stage {
	case duckLakeStageLoad:
		if err.extension != "" {
			return "load duckdb extension " + err.extension + " failed"
		}
		return "load duckdb extension failed"
	case duckLakeStageCreateSecret:
		return "create ducklake service secret failed"
	case duckLakeStageAttach:
		return "attach ducklake catalog failed"
	default:
		return "ducklake initialization failed"
	}
}

func (err *duckLakeInitError) Error() string {
	if err == nil {
		return ""
	}
	message := err.operation()
	if err.extension != "" {
		return fmt.Sprintf("%s (stage=%s extension=%s reason=%s)", message, err.stage, err.extension, err.reason)
	}
	return fmt.Sprintf("%s (stage=%s reason=%s)", message, err.stage, err.reason)
}

// Format keeps every fmt verb on the same redacted representation. In
// particular, %+v must not accidentally acquire a driver-specific dump.
func (err *duckLakeInitError) Format(state fmt.State, verb rune) {
	if verb == 'q' {
		_, _ = fmt.Fprintf(state, "%q", err.Error())
		return
	}
	_, _ = io.WriteString(state, err.Error())
}

func (err *duckLakeInitError) Is(target error) bool {
	return err != nil && !isNilDuckLakeError(err.cause) && !isNilDuckLakeError(target) && stderrors.Is(err.cause, target)
}

// rawCause is intentionally package-private. Diagnostics in this package may
// inspect the exact driver value, while protocol callers can only observe the
// fixed Error/Format output above.
func (err *duckLakeInitError) rawCause() error {
	if err == nil {
		return nil
	}
	return err.cause
}

func classifyDuckLakeFailure(err error) duckLakeFailureReason {
	return classifyDuckLakeFailureForStage(duckLakeStageUnknown, err)
}

// classifyDuckLakeFailureForStage keeps the reason set closed while allowing
// ATTACH to distinguish a missing remote object from a generic HTTP or I/O
// failure. The driver category is authoritative for broad classes, but a few
// high-confidence markers (HTTP 404/NoSuchKey, for example) refine a typed
// DuckDB error before its broad category is applied.
func classifyDuckLakeFailureForStage(stage duckLakeInitStage, err error) duckLakeFailureReason {
	if err == nil || isNilDuckLakeError(err) {
		return duckLakeReasonDriverError
	}
	if stderrors.Is(err, context.Canceled) {
		return duckLakeReasonContextCanceled
	}
	if stderrors.Is(err, context.DeadlineExceeded) {
		return duckLakeReasonDeadlineExceeded
	}
	if stderrors.Is(err, driver.ErrBadConn) {
		return duckLakeReasonBadConnection
	}

	// A joined cause can contain more than one typed DuckDB error. Do not let
	// errors.As choose whichever branch happened to appear first: the stable
	// reason precedence below makes classification independent of join order.
	var typedReasons []duckLakeFailureReason
	for _, duckErr := range duckLakeTypedErrors(err) {
		if reason, ok := classifyDuckLakeTypedError(stage, duckErr); ok {
			typedReasons = append(typedReasons, reason)
		}
	}
	if reason, ok := chooseDuckLakeFailureReason(typedReasons); ok {
		return reason
	}

	// Generic wrappers do not carry a typed DuckDB category. Classification
	// examines only the message internally; Error/Format below never includes
	// it. This lexical precedence is deliberately narrow and stable.
	if reason, ok := classifyDuckLakeFailureMessage(stage, err.Error()); ok {
		return reason
	}
	return duckLakeReasonDriverError
}

func classifyDuckLakeTypedError(stage duckLakeInitStage, duckErr *duckdb.Error) (duckLakeFailureReason, bool) {
	if isNilDuckLakeError(duckErr) {
		return "", false
	}
	switch duckErr.Type {
	case duckdb.ErrorTypePermission:
		return duckLakeReasonPermissionDenied, true
	case duckdb.ErrorTypeNetwork, duckdb.ErrorTypeConnection:
		return duckLakeReasonNetworkFailure, true
	case duckdb.ErrorTypeMissingExtension, duckdb.ErrorTypeAutoLoad:
		return duckLakeReasonExtensionUnavailable, true
	case duckdb.ErrorTypeHTTP:
		// Refine only high-confidence HTTP statuses. Other statuses retain
		// the driver's typed HTTP category.
		if reason, ok := classifyDuckLakeHTTPStatus(stage, duckErr.Msg); ok {
			return reason, true
		}
		return duckLakeReasonHTTPFailure, true
	case duckdb.ErrorTypeIO:
		// DuckDB often types HTTP, TLS, permission, and network failures as
		// IO. Refine only those closed subclasses from the private message.
		// Do not run the full untyped classifier: local "no such file" is
		// resource_unavailable there, but a typed IO local miss must stay
		// generic IO. The public Error/Format text never includes this message.
		if reason, ok := classifyDuckLakeTypedIORefinement(stage, duckErr.Msg); ok {
			return reason, true
		}
		return duckLakeReasonIOFailure, true
	case duckdb.ErrorTypeInvalidConfiguration,
		duckdb.ErrorTypeSettings,
		duckdb.ErrorTypeInvalidInput,
		duckdb.ErrorTypeParameterNotAllowed,
		duckdb.ErrorTypeParameterNotResolved:
		return duckLakeReasonInvalidConfiguration, true
	case duckdb.ErrorTypeOutOfMemory, duckdb.ErrorTypeObjectSize:
		return duckLakeReasonResourceExhaustion, true
	case duckdb.ErrorTypeParser, duckdb.ErrorTypeSyntax:
		return duckLakeReasonInvalidStatement, true
	case duckdb.ErrorTypeCatalog:
		return duckLakeReasonCatalogFailure, true
	case duckdb.ErrorTypeInterrupt:
		return duckLakeReasonInterrupted, true
	default:
		return "", false
	}
}

func classifyDuckLakeTypedIORefinement(stage duckLakeInitStage, message string) (duckLakeFailureReason, bool) {
	if isDuckLakeMissingObjectMessage(stage, message) {
		return duckLakeReasonMissingObject, true
	}
	if reason, ok := classifyDuckLakeHTTPStatus(stage, message); ok {
		return reason, true
	}
	message = strings.ToLower(message)
	tokens := duckLakeMessageTokens(message)
	switch {
	case isDuckLakePermissionMessage(message):
		return duckLakeReasonPermissionDenied, true
	case duckLakeTokenSequenceOutsideURL(message, tokens, "tls"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "ssl"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "x509"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "certificate"):
		return duckLakeReasonTLSFailure, true
	case duckLakeTokenSequenceOutsideURL(message, tokens, "timeout"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "timed", "out"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "deadline", "exceeded"):
		return duckLakeReasonTimeout, true
	case isDuckLakeNetworkMessage(message):
		return duckLakeReasonNetworkFailure, true
	case isDuckLakeHTTPMessage(message):
		return duckLakeReasonHTTPFailure, true
	default:
		return "", false
	}
}

// duckLakeTypedErrors walks both ordinary wrappers and errors.Join trees. The
// visitor deliberately does not expose this tree through duckLakeInitError;
// it is an internal classification aid only.
func duckLakeTypedErrors(err error) []*duckdb.Error {
	var typed []*duckdb.Error
	seen := make(map[error]struct{})
	seenTyped := make(map[*duckdb.Error]struct{})
	appendTyped := func(duckErr *duckdb.Error) {
		if isNilDuckLakeError(duckErr) {
			return
		}
		if _, ok := seenTyped[duckErr]; ok {
			return
		}
		seenTyped[duckErr] = struct{}{}
		typed = append(typed, duckErr)
	}
	var visit func(error)
	visit = func(current error) {
		if current == nil || isNilDuckLakeError(current) {
			return
		}
		if typ := reflect.TypeOf(current); typ != nil && typ.Comparable() {
			if _, ok := seen[current]; ok {
				return
			}
			seen[current] = struct{}{}
		}
		// Preserve the parent classifier's errors.As compatibility for opaque
		// wrappers that expose a typed DuckDB error without an Unwrap method.
		// Each join branch is visited independently below, so precedence stays
		// deterministic even when the root As method returns only one branch.
		var asDuckErr *duckdb.Error
		if stderrors.As(current, &asDuckErr) {
			appendTyped(asDuckErr)
		}
		if duckErr, ok := current.(*duckdb.Error); ok {
			appendTyped(duckErr)
			return
		}
		switch unwrapped := current.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range unwrapped.Unwrap() {
				visit(child)
			}
		case interface{ Unwrap() error }:
			visit(unwrapped.Unwrap())
		}
	}
	visit(err)
	return typed
}

func chooseDuckLakeFailureReason(reasons []duckLakeFailureReason) (duckLakeFailureReason, bool) {
	var best duckLakeFailureReason
	bestPriority := int(^uint(0) >> 1)
	for _, reason := range reasons {
		priority := duckLakeFailureReasonPriority(reason)
		if priority < bestPriority {
			best = reason
			bestPriority = priority
		}
	}
	return best, bestPriority != int(^uint(0)>>1)
}

func duckLakeFailureReasonPriority(reason duckLakeFailureReason) int {
	// Keep the long-standing lexical precedence for joined typed causes. A
	// specific ATTACH missing-object signal therefore beats broad network/HTTP
	// causes regardless of errors.Join operand order.
	for priority, candidate := range []duckLakeFailureReason{
		duckLakeReasonPermissionDenied,
		duckLakeReasonMissingObject,
		duckLakeReasonExtensionUnavailable,
		duckLakeReasonTLSFailure,
		duckLakeReasonUnexpectedEOF,
		duckLakeReasonTimeout,
		duckLakeReasonNetworkFailure,
		duckLakeReasonHTTPFailure,
		duckLakeReasonResourceExhaustion,
		duckLakeReasonInvalidConfiguration,
		duckLakeReasonInvalidStatement,
		duckLakeReasonCatalogFailure,
		duckLakeReasonResourceUnavailable,
		duckLakeReasonIOFailure,
		duckLakeReasonInterrupted,
		duckLakeReasonContextCanceled,
		duckLakeReasonDeadlineExceeded,
		duckLakeReasonBadConnection,
	} {
		if reason == candidate {
			return priority
		}
	}
	return int(^uint(0) >> 1)
}

func classifyDuckLakeFailureMessage(stage duckLakeInitStage, message string) (duckLakeFailureReason, bool) {
	message = strings.ToLower(message)
	tokens := duckLakeMessageTokens(message)
	switch {
	case isDuckLakePermissionMessage(message):
		return duckLakeReasonPermissionDenied, true
	case isDuckLakeMissingObjectMessage(stage, message):
		return duckLakeReasonMissingObject, true
	case isDuckLakeExtensionMessage(stage, message):
		return duckLakeReasonExtensionUnavailable, true
	case duckLakeTokenSequenceOutsideURL(message, tokens, "tls"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "ssl"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "x509"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "certificate"):
		return duckLakeReasonTLSFailure, true
	case duckLakeTokenSequenceOutsideURL(message, tokens, "unexpected", "eof"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "unexpected", "end", "of", "file"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "end", "of", "file"):
		return duckLakeReasonUnexpectedEOF, true
	case duckLakeTokenSequenceOutsideURL(message, tokens, "timeout"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "timed", "out"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "deadline", "exceeded"):
		return duckLakeReasonTimeout, true
	case isDuckLakeNetworkMessage(message):
		return duckLakeReasonNetworkFailure, true
	case isDuckLakeHTTPMessage(message):
		return duckLakeReasonHTTPFailure, true
	case duckLakeTokenSequenceOutsideURL(message, tokens, "out", "of", "memory"),
		duckLakeLiteralOutsideURL(message, "out-of-memory"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "cannot", "allocate", "memory"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "no", "space", "left", "on", "device"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "disk", "full"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "quota", "exceeded"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "too", "many", "open", "files"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "resource", "exhausted"):
		return duckLakeReasonResourceExhaustion, true
	case duckLakeTokenSequenceOutsideURL(message, tokens, "invalid", "configuration"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "configuration", "error"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "invalid", "parameter"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "missing", "secret"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "mandatory", "setting"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "settings", "error"):
		return duckLakeReasonInvalidConfiguration, true
	case duckLakeTokenSequenceOutsideURL(message, tokens, "syntax"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "parser"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "invalid", "statement"):
		return duckLakeReasonInvalidStatement, true
	case duckLakeTokenSequenceOutsideURL(message, tokens, "catalog", "error"):
		return duckLakeReasonCatalogFailure, true
	case isDuckLakeResourceUnavailableMessage(message):
		return duckLakeReasonResourceUnavailable, true
	case duckLakeLiteralOutsideURL(message, "i/o error"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "io", "error"),
		duckLakeLiteralOutsideURL(message, "input/output error"),
		duckLakeLiteralOutsideURL(message, "read-only file system"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "read", "only", "file", "system"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "failed", "to", "open"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "failed", "to", "read"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "failed", "to", "write"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "bad", "file", "descriptor"):
		return duckLakeReasonIOFailure, true
	case duckLakeTokenSequenceOutsideURL(message, tokens, "interrupt"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "interrupted"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "interruption"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "cancel"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "canceled"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "cancelled"),
		duckLakeTokenSequenceOutsideURL(message, tokens, "cancellation"):
		return duckLakeReasonInterrupted, true
	default:
		return "", false
	}
}

func isDuckLakeMissingObjectMessage(stage duckLakeInitStage, message string) bool {
	if stage != duckLakeStageAttach {
		return false
	}
	message = strings.ToLower(message)
	tokens := duckLakeMessageTokens(message)
	for _, sequence := range [][]string{
		{"nosuchkey"},
		{"nosuchobject"},
		{"keynotfound"},
		{"objectnotfound"},
		{"no", "such", "key"},
		{"no", "such", "object"},
		{"key", "not", "found"},
		{"key", "does", "not", "exist"},
		{"specified", "key", "does", "not", "exist"},
		{"object", "not", "found"},
		{"object", "does", "not", "exist"},
		{"missing", "object"},
		{"object", "missing"},
		{"missing", "metadata"},
		{"metadata", "missing"},
		{"metadata", "not", "found"},
		{"catalog", "not", "found"},
		{"catalog", "does", "not", "exist"},
		{"database", "does", "not", "exist"},
		{"read", "only", "mode"},
	} {
		if duckLakeTokenSequenceOutsideURL(message, tokens, sequence...) {
			return true
		}
	}
	// A numeric 404 is only treated as an object miss when it is clearly an
	// HTTP status. This avoids classifying an arbitrary path containing "404".
	if hasDuckLakeHTTPStatus(message, "404") {
		return true
	}
	// A remote no-such-file response is an object miss only when the message
	// names a remote/object context. Bare local-file errors remain generic I/O.
	for _, sequence := range [][]string{
		{"no", "such", "file"},
		{"file", "not", "found"},
		{"file", "does", "not", "exist"},
	} {
		if duckLakeRemoteFileSequenceOutsideURL(message, tokens, sequence...) {
			return true
		}
	}
	return false
}

func classifyDuckLakeHTTPStatus(stage duckLakeInitStage, message string) (duckLakeFailureReason, bool) {
	message = strings.ToLower(message)
	// A typed HTTP error may put the status after an object code (for example,
	// "NoSuchKey 403"). Exact status tokens therefore take precedence without
	// depending on their position relative to the usual "HTTP Error" marker.
	if hasDuckLakeHTTPStatusToken(message, "401") || hasDuckLakeHTTPStatusToken(message, "403") {
		return duckLakeReasonPermissionDenied, true
	}
	if stage == duckLakeStageAttach && (hasDuckLakeHTTPStatusToken(message, "404") || isDuckLakeMissingObjectMessage(stage, message)) {
		return duckLakeReasonMissingObject, true
	}
	return "", false
}

func hasDuckLakeHTTPStatusToken(message, code string) bool {
	tokens := duckLakeMessageTokens(message)
	for index, token := range tokens {
		if token.value != code || duckLakeTokenInURL(message, token) || duckLakeTokenIsHyphenated(message, token) {
			continue
		}
		if duckLakeHTTPStatusTokenHasPathContext(message, tokens, index) {
			continue
		}
		if duckLakeHTTPStatusContext(message, tokens, index) {
			return true
		}
	}
	return false
}

func duckLakeHTTPStatusTokenHasPathContext(message string, tokens []duckLakeMessageToken, index int) bool {
	if duckLakeTokenIsPathLike(message, tokens[index]) {
		return true
	}
	// Query/identifier fields may contain a status-looking suffix (for
	// example, foo=status=404). Only the known status field spellings are
	// eligible for refinement; an unknown prefix must remain broad HTTP.
	if duckLakeHTTPStatusTokenHasUnknownIdentifierPrefix(message, tokens, index) {
		return true
	}
	for cursor := index - 1; cursor >= 0 && index-cursor <= 3; cursor-- {
		token := tokens[cursor]
		if token.value == "path" {
			// A path assignment such as path=404 is not an HTTP status. The
			// same applies when path appears before a status/code marker.
			return true
		}
		if token.value != "status" && token.value != "response" && token.value != "code" {
			continue
		}
		if duckLakeTokenIsPathComponent(message, token) || duckLakeTokenIsHyphenated(message, token) || duckLakeTokenInURL(message, token) {
			return true
		}
	}
	return false
}

func duckLakeHTTPStatusTokenHasUnknownIdentifierPrefix(message string, tokens []duckLakeMessageToken, index int) bool {
	if index <= 0 || index >= len(tokens) {
		return false
	}
	// Only an equals sign can bind a numeric status to a named field. A dot or
	// underscore makes the number part of an identifier instead (for example,
	// status_code_404 or status_code.404).
	numericSeparator := strings.TrimSpace(message[tokens[index-1].end:tokens[index].start])
	if numericSeparator == "" {
		return false
	}
	if numericSeparator != "=" {
		return strings.ContainsAny(numericSeparator, "_.=")
	}
	return !duckLakeHTTPStatusTokenHasKnownIdentifierPrefix(message, tokens, index)
}

func duckLakeHTTPStatusTokenHasKnownIdentifierPrefix(message string, tokens []duckLakeMessageToken, index int) bool {
	if index <= 0 || index >= len(tokens) ||
		strings.TrimSpace(message[tokens[index-1].end:tokens[index].start]) != "=" {
		return false
	}
	// Walk an exact snake_case field to the left of the assignment. Repeated
	// underscores and nested prefixes are identifiers, not known status fields.
	fieldStart := index - 1
	for fieldStart > 0 && message[tokens[fieldStart-1].end:tokens[fieldStart].start] == "_" {
		fieldStart--
	}
	if fieldStart > 0 {
		prefixSeparator := strings.TrimSpace(message[tokens[fieldStart-1].end:tokens[fieldStart].start])
		if strings.Contains(prefixSeparator, "=") {
			return false
		}
	}
	if start := tokens[fieldStart].start; start > 0 {
		switch message[start-1] {
		case '_', '.', '=':
			return false
		}
	}
	values := make([]string, 0, index-fieldStart)
	for cursor := fieldStart; cursor < index; cursor++ {
		values = append(values, tokens[cursor].value)
	}
	field := strings.Join(values, "_")
	switch field {
	case "http", "status", "response", "code", "returned", "return",
		"status_code", "response_code", "http_status", "http_code", "error_code":
		return true
	default:
		return false
	}
}

func hasDuckLakeHTTPStatus(message, code string) bool {
	message = strings.ToLower(message)
	tokens := duckLakeMessageTokens(message)
	for index, token := range tokens {
		if token.value == code && duckLakeHTTPStatusContext(message, tokens, index) {
			return true
		}
	}
	return false
}

func duckLakeHTTPStatusContext(message string, tokens []duckLakeMessageToken, index int) bool {
	if index < 0 || index >= len(tokens) || duckLakeTokenInURL(message, tokens[index]) {
		return false
	}
	if duckLakeTokenIsHyphenated(message, tokens[index]) {
		return false
	}
	if duckLakeTokenIsPathLike(message, tokens[index]) {
		return false
	}
	if duckLakeTokenHasDottedIdentifierContext(message, tokens[index]) {
		return false
	}
	if index == 0 {
		if len(tokens) == 1 || duckLakeHTTPReasonPhraseFollows(message, tokens, index) {
			if index+1 < len(tokens) && !duckLakeTokenIsMarker(message, tokens[index+1]) {
				return false
			}
			return true
		}
		return duckLakeLeadingHTTPStatusContext(message, tokens, index)
	}
	for cursor := index - 1; cursor >= 0 && index-cursor <= 5; cursor-- {
		token := tokens[cursor]
		switch token.value {
		case "http", "status", "response", "code", "returned", "return":
			if duckLakeHTTPStatusTokenHasPathContext(message, tokens, index) || duckLakeTokenIsPathComponent(message, token) {
				return false
			}
			if duckLakeTokenIsHyphenated(message, token) {
				return false
			}
			if token.value == "http" && duckLakeHTTPVersionPrecedesStatus(message, token, tokens[index]) {
				return duckLakeTokenIsPlain(message, token) &&
					!duckLakeTokenIsPathComponent(message, token) &&
					!duckLakeTokenIsIdentifierLike(message, token)
			}
			if !duckLakeTokenIsMarker(message, token) &&
				!duckLakeHTTPStatusTokenHasKnownIdentifierPrefix(message, tokens, index) {
				return false
			}
			// A literal http:// URL is not an HTTP error marker.
			if token.value == "http" && strings.HasPrefix(message[token.end:], "://") {
				return false
			}
			if !duckLakeTokenInURL(message, token) {
				return true
			}
			return false
		case "nosuchkey", "nosuchobject", "keynotfound", "objectnotfound":
			// Object-store response codes may precede the numeric status (for
			// example, "NoSuchKey 403"). The object marker itself is enough to
			// establish status context; URL/path markers were rejected above.
			if !duckLakeTokenIsMarker(message, token) {
				return false
			}
			return true
		case "error", "request":
			if !duckLakeTokenIsMarker(message, token) {
				return false
			}
			continue
		case "1", "2", "3", "0":
			continue
		default:
			return false
		}
	}
	return false
}

func duckLakeHTTPVersionPrecedesStatus(message string, httpToken, statusToken duckLakeMessageToken) bool {
	if httpToken.end > statusToken.start || statusToken.start > len(message) {
		return false
	}
	version := strings.TrimSpace(message[httpToken.end:statusToken.start])
	if len(version) < 2 || version[0] != '/' {
		return false
	}
	digitSeen := false
	for index := 1; index < len(version); index++ {
		switch value := version[index]; {
		case value >= '0' && value <= '9':
			digitSeen = true
		case value == '.' && digitSeen && index+1 < len(version):
			digitSeen = false
		default:
			return false
		}
	}
	return digitSeen
}

func duckLakeLeadingHTTPStatusContext(message string, tokens []duckLakeMessageToken, index int) bool {
	if index+1 >= len(tokens) {
		return false
	}
	if !duckLakeTokenIsMarker(message, tokens[index+1]) {
		return false
	}
	switch tokens[index+1].value {
	case "at", "from", "for", "on", "while", "during", "request", "response", "status", "error", "object", "objects", "key", "path", "catalog", "metadata", "resource", "url", "uri", "s3", "http", "https", "nosuchkey", "nosuchobject":
		return true
	default:
		return false
	}
}

func duckLakeHTTPReasonPhraseFollows(message string, tokens []duckLakeMessageToken, index int) bool {
	if index+1 >= len(tokens) {
		return false
	}
	switch tokens[index].value {
	case "401":
		return tokens[index+1].value == "unauthorized" && duckLakeTokenIsMarker(message, tokens[index+1])
	case "403":
		return tokens[index+1].value == "forbidden" && duckLakeTokenIsMarker(message, tokens[index+1])
	case "404":
		return tokens[index+1].value == "not" && index+2 < len(tokens) &&
			tokens[index+2].value == "found" && duckLakeTokenIsMarker(message, tokens[index+1]) &&
			duckLakeTokenIsMarker(message, tokens[index+2])
	default:
		return (tokens[index+1].value == "error" || tokens[index+1].value == "unavailable") &&
			duckLakeTokenIsMarker(message, tokens[index+1])
	}
}

func isDuckLakePermissionMessage(message string) bool {
	tokens := duckLakeMessageTokens(message)
	if hasDuckLakeHTTPStatus(message, "401") || hasDuckLakeHTTPStatus(message, "403") {
		return true
	}
	for _, sequence := range [][]string{{"permission", "denied"}, {"access", "denied"}} {
		if duckLakeTokenSequenceOutsideURL(message, tokens, sequence...) {
			return true
		}
	}
	for index, token := range tokens {
		if token.value != "unauthorized" && token.value != "forbidden" {
			continue
		}
		if !duckLakeTokenIsMarker(message, token) {
			continue
		}
		if len(tokens) == 1 || duckLakePermissionContext(message, tokens, index) {
			return true
		}
	}
	return false
}

func duckLakePermissionContext(message string, tokens []duckLakeMessageToken, index int) bool {
	if index > 0 {
		previous := tokens[index-1]
		if !duckLakeTokenIsMarker(message, previous) {
			previous = duckLakeMessageToken{}
		}
		switch previous.value {
		case "permission", "access", "operation", "http", "status", "code", "response", "returned", "return", "error", "request":
			return true
		}
	}
	if index+1 < len(tokens) {
		next := tokens[index+1]
		if !duckLakeTokenIsMarker(message, next) {
			next = duckLakeMessageToken{}
		}
		switch next.value {
		case "error", "response", "status", "code", "request", "access", "operation", "by", "from":
			return true
		}
	}
	return false
}

func isDuckLakeNetworkMessage(message string) bool {
	tokens := duckLakeMessageTokens(message)
	for _, sequence := range [][]string{
		{"connection", "refused"},
		{"connection", "reset"},
		{"connection", "aborted"},
		{"connection", "closed"},
		{"no", "such", "host"},
		{"no", "route", "to", "host"},
		{"broken", "pipe"},
		{"dial", "tcp"},
		{"name", "resolution"},
		{"network", "error"},
		{"network", "failure"},
		{"network", "request"},
		{"network", "connection"},
		{"network", "unavailable"},
		{"network", "unreachable"},
		{"network", "operation"},
		{"dns", "error"},
		{"dns", "failure"},
		{"dns", "resolution"},
	} {
		if duckLakeTokenSequenceOutsideURL(message, tokens, sequence...) {
			return true
		}
	}
	for index, token := range tokens {
		if token.value != "network" && token.value != "dns" {
			continue
		}
		if !duckLakeTokenIsMarker(message, token) {
			continue
		}
		if len(tokens) == 1 ||
			(index > 0 && tokens[index-1].value == "error" && duckLakeTokenIsMarker(message, tokens[index-1])) ||
			(index+1 < len(tokens) && tokens[index+1].value == "error" && duckLakeTokenIsMarker(message, tokens[index+1])) {
			return true
		}
	}
	return false
}

func isDuckLakeHTTPMessage(message string) bool {
	tokens := duckLakeMessageTokens(message)
	if hasDuckLakeHTTPStatusCode(message, tokens) {
		return true
	}
	for _, sequence := range [][]string{
		{"http", "error"},
		{"http", "failure"},
		{"http", "failed"},
		{"http", "request"},
		{"http", "response"},
		{"http", "status"},
	} {
		if duckLakeTokenSequenceOutsideURL(message, tokens, sequence...) {
			return true
		}
	}
	return false
}

func hasDuckLakeHTTPStatusCode(message string, tokens []duckLakeMessageToken) bool {
	for index, token := range tokens {
		if !isDuckLakeHTTPStatusToken(token.value) {
			continue
		}
		if duckLakeHTTPStatusContext(message, tokens, index) {
			return true
		}
	}
	return false
}

func isDuckLakeHTTPStatusToken(value string) bool {
	if len(value) != 3 {
		return false
	}
	for index := range value {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	return value[0] >= '1' && value[0] <= '5'
}

func isDuckLakeExtensionMessage(stage duckLakeInitStage, message string) bool {
	tokens := duckLakeMessageTokens(message)
	for _, sequence := range [][]string{
		{"cannot", "load", "extension"},
		{"cannot", "find", "extension"},
		{"failed", "to", "load", "extension"},
		{"autoload", "extension"},
		{"autoloading", "extension"},
	} {
		if duckLakeTokenSequenceOutsideURL(message, tokens, sequence...) {
			return true
		}
	}
	// Ordinary extension diagnostics are trusted during the service-owned
	// LOAD stage. At service stages after LOAD, require an explicit load/autoload
	// phrase so an object path such as "remote extension missing" cannot hijack
	// the ATTACH subclass. Unknown-stage generic classification retains the
	// historical lexical behavior.
	if stage == duckLakeStageLoad {
		for _, sequence := range [][]string{
			{"missing", "extension"},
		} {
			if duckLakeTokenSequenceOutsideURL(message, tokens, sequence...) {
				return true
			}
		}
	}
	for index, token := range tokens {
		if token.value != "extension" || !duckLakeTokenIsMarker(message, token) {
			continue
		}
		if (stage == duckLakeStageAttach || stage == duckLakeStageCreateSecret) && !duckLakeExtensionHasExplicitLoadContext(message, tokens, index) {
			continue
		}
		// An explicit load/autoload phrase is itself sufficient context. This
		// covers word orders such as "failed to load ducklake extension" where
		// the failure verb precedes the extension token.
		if duckLakeExtensionHasExplicitLoadContext(message, tokens, index) {
			return true
		}
		for cursor := index + 1; cursor < len(tokens) && cursor <= index+5; cursor++ {
			candidate := tokens[cursor]
			if !duckLakeTokenIsMarker(message, candidate) {
				continue
			}
			switch candidate.value {
			case "not":
				if cursor+1 < len(tokens) &&
					duckLakeTokenIsMarker(message, tokens[cursor+1]) &&
					(tokens[cursor+1].value == "loaded" || tokens[cursor+1].value == "found") {
					return true
				}
			case "unavailable", "autoload", "autoloading", "missing":
				return true
			case "cannot":
				if cursor+1 < len(tokens) &&
					duckLakeTokenIsMarker(message, tokens[cursor+1]) &&
					(tokens[cursor+1].value == "load" || tokens[cursor+1].value == "find") {
					return true
				}
			case "failed":
				return true
			}
		}
	}
	return false
}

func duckLakeExtensionHasExplicitLoadContext(message string, tokens []duckLakeMessageToken, index int) bool {
	for cursor := index + 1; cursor < len(tokens) && cursor <= index+5; cursor++ {
		if !duckLakeTokenIsMarker(message, tokens[cursor]) {
			continue
		}
		switch tokens[cursor].value {
		case "load", "loaded", "autoload", "autoloading":
			return true
		case "not", "cannot", "failed":
			if cursor+1 < len(tokens) && duckLakeTokenIsMarker(message, tokens[cursor+1]) &&
				(tokens[cursor+1].value == "loaded" || tokens[cursor+1].value == "load" || tokens[cursor+1].value == "find") {
				return true
			}
		}
	}
	for cursor := index - 1; cursor >= 0 && index-cursor <= 5; cursor-- {
		if !duckLakeTokenIsMarker(message, tokens[cursor]) {
			continue
		}
		switch tokens[cursor].value {
		case "load", "loaded", "autoload", "autoloading":
			return true
		case "cannot", "failed":
			if cursor+1 < len(tokens) && duckLakeTokenIsMarker(message, tokens[cursor+1]) &&
				(tokens[cursor+1].value == "load" || tokens[cursor+1].value == "find") {
				return true
			}
		}
	}
	return false
}

func isDuckLakeResourceUnavailableMessage(message string) bool {
	tokens := duckLakeMessageTokens(message)
	for _, sequence := range [][]string{
		{"resource", "temporarily", "unavailable"},
		{"resource", "unavailable"},
		{"no", "such", "file"},
		{"file", "not", "found"},
		{"file", "does", "not", "exist"},
		{"resource", "not", "found"},
		{"resource", "does", "not", "exist"},
		{"object", "not", "found"},
		{"object", "does", "not", "exist"},
		{"metadata", "not", "found"},
		{"catalog", "not", "found"},
		{"key", "not", "found"},
	} {
		if duckLakeTokenSequenceOutsideURL(message, tokens, sequence...) {
			return true
		}
	}
	if (len(tokens) == 2 && tokens[0].value == "not" && tokens[1].value == "found" &&
		duckLakeTokenIsMarker(message, tokens[0]) && duckLakeTokenIsMarker(message, tokens[1])) ||
		(len(tokens) == 3 && tokens[0].value == "does" && tokens[1].value == "not" && tokens[2].value == "exist" &&
			duckLakeTokenIsMarker(message, tokens[0]) && duckLakeTokenIsMarker(message, tokens[1]) &&
			duckLakeTokenIsMarker(message, tokens[2])) {
		return true
	}
	for index, token := range tokens {
		if token.value != "unavailable" && token.value != "missing" {
			continue
		}
		if !duckLakeTokenIsMarker(message, token) {
			continue
		}
		if len(tokens) == 1 ||
			(index > 0 && duckLakeTokenIsMarker(message, tokens[index-1]) && isDuckLakeResourceContextWord(tokens[index-1].value)) ||
			(index+1 < len(tokens) && duckLakeTokenIsMarker(message, tokens[index+1]) && isDuckLakeResourceContextWord(tokens[index+1].value)) {
			return true
		}
	}
	return false
}

func isDuckLakeResourceContextWord(value string) bool {
	switch value {
	case "resource", "file", "object", "metadata", "catalog", "key", "service", "path":
		return true
	default:
		return false
	}
}

func duckLakeRemoteFileSequenceOutsideURL(message string, tokens []duckLakeMessageToken, sequence ...string) bool {
	if len(sequence) == 0 || len(sequence) > len(tokens) {
		return false
	}
	for index := 0; index <= len(tokens)-len(sequence); index++ {
		if !duckLakeTokenSequenceMatchesAt(message, tokens, index, sequence...) {
			continue
		}
		if duckLakeHasRemoteObjectContextNear(message, tokens, index, index+len(sequence)) {
			return true
		}
	}
	return false
}

func duckLakeHasRemoteObjectContextNear(message string, tokens []duckLakeMessageToken, start, end int) bool {
	forwardLink := false
	for cursor := end; cursor < len(tokens) && cursor-end < 4; cursor++ {
		if duckLakeTokenIsRemoteObjectContext(message, tokens[cursor]) {
			return true
		}
		if !duckLakeTokenIsRemoteContextLink(message, tokens[cursor]) {
			if forwardLink && duckLakeTokenIsExplicitLocalTarget(message, tokens[cursor]) {
				return false
			}
			break
		}
		forwardLink = true
	}
	for cursor := start - 1; cursor >= 0 && start-cursor <= 4; cursor-- {
		if duckLakeTokenIsRemoteObjectContext(message, tokens[cursor]) {
			return true
		}
		if !duckLakeTokenIsRemoteContextLink(message, tokens[cursor]) {
			break
		}
	}
	return false
}

func duckLakeTokenIsExplicitLocalTarget(message string, token duckLakeMessageToken) bool {
	return !duckLakeTokenInRemoteURL(message, token) &&
		(duckLakeTokenIsPathLike(message, token) || duckLakeTokenIsIdentifierLike(message, token))
}

func duckLakeTokenIsRemoteObjectContext(message string, token duckLakeMessageToken) bool {
	if duckLakeTokenInRemoteURL(message, token) {
		return true
	}
	if token.value == "s3" && strings.HasPrefix(message[token.end:], "://") {
		return false
	}
	return (token.value == "remote" || token.value == "s3") && duckLakeTokenIsMarker(message, token)
}

func duckLakeTokenIsRemoteContextLink(message string, token duckLakeMessageToken) bool {
	switch token.value {
	case "at", "from", "on", "in", "for":
		return duckLakeTokenIsMarker(message, token)
	case "error", "io", "storage":
		return duckLakeTokenIsMarker(message, token)
	case "url", "uri", "endpoint", "path", "object", "key", "catalog", "metadata", "file":
		if duckLakeTokenIsMarker(message, token) {
			return true
		}
		// A known target field may bind directly to the remote URI. Keep the
		// exception local to this proximity scan so arbitrary assignments remain
		// opaque markers.
		if !duckLakeTokenHasSingleAssignmentSuffix(message, token) ||
			!duckLakeTokenIsPlain(message, token) || duckLakeTokenIsPathComponent(message, token) {
			return false
		}
		if token.start > 0 {
			switch message[token.start-1] {
			case '_', '.', '=':
				return false
			}
		}
		return true
	default:
		return false
	}
}

func duckLakeTokenHasSingleAssignmentSuffix(message string, token duckLakeMessageToken) bool {
	if token.end >= len(message) || message[token.end] != '=' {
		return false
	}
	for cursor := token.end + 1; cursor < len(message); cursor++ {
		switch message[cursor] {
		case ' ', '\t', '\r', '\n', '\'', '"', '`':
			continue
		default:
			return message[cursor] != '='
		}
	}
	return true
}

func duckLakeTokenInRemoteURL(message string, token duckLakeMessageToken) bool {
	for _, scheme := range []string{"s3://", "http://", "https://", "azure://", "gs://", "gcs://"} {
		for offset := 0; offset < len(message); {
			relative := strings.Index(message[offset:], scheme)
			if relative < 0 {
				break
			}
			start := offset + relative
			end := strings.IndexAny(message[start+len(scheme):], " \t\r\n")
			if end < 0 {
				end = len(message)
			} else {
				end += start + len(scheme)
			}
			if token.start >= start && token.start < end && duckLakeRemoteURLPrefixIsValid(message, start) {
				return true
			}
			offset = start + len(scheme)
		}
	}
	return false
}

func duckLakeRemoteURLPrefixIsValid(message string, start int) bool {
	if start <= 0 || start > len(message) {
		return start == 0
	}
	prefixTokens := duckLakeMessageTokens(message[:start])
	if len(prefixTokens) == 0 {
		return true
	}
	field := prefixTokens[len(prefixTokens)-1]
	separator := message[field.end:start]
	compact := strings.Map(func(value rune) rune {
		switch value {
		case ' ', '\t', '\r', '\n', '\'', '"', '`':
			return -1
		default:
			return value
		}
	}, separator)
	if strings.Contains(compact, "=") {
		if compact != "=" {
			return false
		}
		switch field.value {
		case "url", "uri", "endpoint", "path", "object", "key", "catalog", "metadata", "file":
		default:
			return false
		}
		if field.start > 0 {
			switch message[field.start-1] {
			case '_', '.', '=':
				return false
			}
		}
		if duckLakeTokenHasDottedIdentifierContext(message[:start], field) {
			return false
		}
		if len(prefixTokens) > 1 {
			outerSeparator := strings.TrimSpace(message[prefixTokens[len(prefixTokens)-2].end:field.start])
			if strings.Contains(outerSeparator, "=") {
				return false
			}
		}
		return true
	}
	previous := message[start-1]
	return !isDuckLakeTokenByte(previous) && !strings.ContainsRune("_-./\\?&#=", rune(previous))
}

type duckLakeMessageToken struct {
	value string
	start int
	end   int
}

func duckLakeMessageTokens(message string) []duckLakeMessageToken {
	message = strings.ToLower(message)
	tokens := make([]duckLakeMessageToken, 0, 8)
	for index := 0; index < len(message); {
		for index < len(message) && !isDuckLakeTokenByte(message[index]) {
			index++
		}
		start := index
		for index < len(message) && isDuckLakeTokenByte(message[index]) {
			index++
		}
		if start < index {
			tokens = append(tokens, duckLakeMessageToken{value: message[start:index], start: start, end: index})
		}
	}
	return tokens
}

func isDuckLakeTokenByte(value byte) bool {
	return (value >= 'a' && value <= 'z') || (value >= '0' && value <= '9')
}

// duckLakeTokenSequenceOutsideURL finds any complete sequence whose tokens are
// outside a URL. A message can contain a diagnostic-looking object URL before
// the actual error phrase; returning only the first match would hide the valid
// later phrase.
func duckLakeTokenSequenceOutsideURL(message string, tokens []duckLakeMessageToken, sequence ...string) bool {
	if len(sequence) == 0 || len(sequence) > len(tokens) {
		return false
	}
	for index := 0; index <= len(tokens)-len(sequence); index++ {
		if duckLakeTokenSequenceMatchesAt(message, tokens, index, sequence...) {
			return true
		}
	}
	return false
}

func duckLakeTokenSequenceMatchesAt(message string, tokens []duckLakeMessageToken, index int, sequence ...string) bool {
	if index < 0 || len(sequence) == 0 || index+len(sequence) > len(tokens) {
		return false
	}
	for offset, value := range sequence {
		candidate := tokens[index+offset]
		if candidate.value != value || !duckLakeTokenIsMarker(message, candidate) {
			return false
		}
	}
	return true
}

// duckLakeLiteralOutsideURL handles the few canonical diagnostics whose own
// spelling contains a slash or hyphen. Identifier, query and path adjacency is
// still rejected; a trailing period remains ordinary sentence punctuation.
func duckLakeLiteralOutsideURL(message, literal string) bool {
	for offset := 0; offset <= len(message)-len(literal); {
		relative := strings.Index(message[offset:], literal)
		if relative < 0 {
			return false
		}
		start := offset + relative
		end := start + len(literal)
		if !duckLakeLiteralHasIdentifierBoundary(message, start, end) &&
			!duckLakeTokenInURL(message, duckLakeMessageToken{start: start, end: end}) {
			return true
		}
		offset = start + 1
	}
	return false
}

func duckLakeLiteralHasIdentifierBoundary(message string, start, end int) bool {
	left := start
	skippedBoundary := false

leftScan:
	for left > 0 {
		switch message[left-1] {
		case ' ', '\t', '\r', '\n', '\'', '"', '`':
			left--
			skippedBoundary = true
		default:
			break leftScan
		}
	}
	if left > 0 {
		previous := message[left-1]
		if (skippedBoundary && previous == '=') ||
			(!skippedBoundary && (isDuckLakeTokenByte(previous) ||
				strings.ContainsRune("_-./\\?&#=", rune(previous)))) {
			return true
		}
	}
	if end >= len(message) {
		return false
	}
	next := message[end]
	if isDuckLakeTokenByte(next) || strings.ContainsRune("_-/\\?&#=", rune(next)) {
		return true
	}
	return next == '.' && duckLakeDotsLeadToIdentifier(message, end)
}

func duckLakeTokenInURL(message string, token duckLakeMessageToken) bool {
	if token.start > len(message) {
		return false
	}
	scheme := strings.LastIndex(message[:token.start], "://")
	if scheme < 0 {
		return false
	}
	end := strings.IndexAny(message[scheme+3:], " \t\r\n")
	if end < 0 {
		end = len(message) - (scheme + 3)
	}
	return token.start < scheme+3+end
}

func duckLakeTokenIsHyphenated(message string, token duckLakeMessageToken) bool {
	return (token.start > 0 && message[token.start-1] == '-') ||
		(token.end < len(message) && message[token.end] == '-')
}

// duckLakeTokenIsPlain keeps lexical markers out of URLs and hyphenated
// identifiers. Path-component checks are intentionally separate because some
// callers use ordinary path words as explicit context.
func duckLakeTokenIsPlain(message string, token duckLakeMessageToken) bool {
	return !duckLakeTokenInURL(message, token) && !duckLakeTokenIsHyphenated(message, token)
}

func duckLakeTokenIsMarker(message string, token duckLakeMessageToken) bool {
	return duckLakeTokenIsPlain(message, token) && !duckLakeTokenIsPathLike(message, token) && !duckLakeTokenIsIdentifierLike(message, token)
}

func duckLakeTokenIsIdentifierLike(message string, token duckLakeMessageToken) bool {
	if token.start > 0 {
		switch message[token.start-1] {
		case '_', '=':
			return true
		}
		for cursor := token.start; cursor > 0; {
			cursor--
			switch message[cursor] {
			case ' ', '\t', '\r', '\n', '\'', '"', '`':
				continue
			case '=':
				return true
			default:
				cursor = 0
			}
		}
	}
	if token.end < len(message) {
		switch message[token.end] {
		case '_', '=':
			return true
		}
	}
	return duckLakeTokenHasDottedIdentifierContext(message, token)
}

func duckLakeTokenHasDottedIdentifierContext(message string, token duckLakeMessageToken) bool {
	if duckLakeSpanHasDottedIdentifierContext(message, token.start, token.end) {
		return true
	}
	for _, delimiters := range [][2]byte{{'"', '"'}, {'`', '`'}, {'[', ']'}} {
		if duckLakeDelimitedTokenHasDottedIdentifierContext(message, token, delimiters[0], delimiters[1]) {
			return true
		}
	}
	return false
}

func duckLakeSpanHasDottedIdentifierContext(message string, start, end int) bool {
	if start > 0 && message[start-1] == '.' {
		return true
	}
	return end < len(message) && message[end] == '.' &&
		duckLakeDotsLeadToIdentifier(message, end)
}

func duckLakeDelimitedTokenHasDottedIdentifierContext(
	message string,
	token duckLakeMessageToken,
	openingDelimiter, closingDelimiter byte,
) bool {
	for offset := 0; offset < len(message); {
		openingOffset := strings.IndexByte(message[offset:], openingDelimiter)
		if openingOffset < 0 {
			return false
		}
		opening := offset + openingOffset
		closing := opening + 1
		for closing < len(message) {
			closingOffset := strings.IndexByte(message[closing:], closingDelimiter)
			if closingOffset < 0 {
				return false
			}
			closing += closingOffset
			if closing+1 < len(message) && message[closing+1] == closingDelimiter {
				closing += 2
				continue
			}
			break
		}
		if closing >= len(message) {
			return false
		}
		if token.start > opening && token.end <= closing &&
			duckLakeSpanHasDottedIdentifierContext(message, opening, closing+1) {
			return true
		}
		offset = closing + 1
	}
	return false
}

func duckLakeDotsLeadToIdentifier(message string, start int) bool {
	for start < len(message) && message[start] == '.' {
		start++
	}
	return start < len(message) &&
		(isDuckLakeTokenByte(message[start]) || strings.ContainsRune("_=/\\?&#-", rune(message[start])))
}

func duckLakeTokenIsPathComponent(message string, token duckLakeMessageToken) bool {
	if token.start > 0 {
		switch message[token.start-1] {
		case '/', '\\', '?', '&', '#':
			return true
		}
	}
	return false
}

func duckLakeTokenIsPathLike(message string, token duckLakeMessageToken) bool {
	if duckLakeTokenIsPathComponent(message, token) {
		return true
	}
	if token.end >= len(message) {
		return false
	}
	switch message[token.end] {
	case '/', '\\', '?', '&', '#':
		return true
	default:
		return false
	}
}

func isNilDuckLakeError(err error) bool {
	if err == nil {
		return false
	}
	value := reflect.ValueOf(err)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
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
	if err := rt.initializeSQLLocked(ctx, key, execer); err != nil {
		return err
	}
	if key != nil {
		rt.initialized.Store(key, struct{}{})
	}
	return nil
}

// initializeSQLLocked executes the fixed service setup sequence. The caller
// must hold initializeMu and must have completed extension verification. It is
// kept separate so the sequence can be tested with a scripted executor without
// weakening the production integrity gate.
func (rt *duckLakeRuntime) initializeSQLLocked(ctx context.Context, key any, execer driver.ExecerContext) error {
	for _, artifact := range rt.manifest {
		path := filepath.Join(rt.config.ExtensionDir, artifact.FileName)
		query := "LOAD '" + strings.ReplaceAll(path, "'", "''") + "'"
		if _, err := execer.ExecContext(ctx, query, nil); err != nil && !isAlreadyLoadedError(err) {
			return newDuckLakeInitError(duckLakeStageLoad, artifact.Name, err)
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
		return newDuckLakeInitError(duckLakeStageCreateSecret, "", err)
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
	return nil
}

// EnsureAttached initializes and attaches the service lake on an eligible
// physical connection. It is the object-table boundary: incomplete service
// paths are rejected here rather than guessed from SQL or process state.
func (rt *duckLakeRuntime) EnsureAttached(ctx context.Context, conn driver.Conn, execer driver.ExecerContext) error {
	if rt == nil {
		return fmt.Errorf("%w: DuckLake service configuration is disabled", ErrInvalidTableStorage)
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
	if duckLakeRemoteCatalogURI(metadata) {
		return newDuckLakeInitError(duckLakeStageAttach, "", fmt.Errorf("invalid configuration: ducklake catalog must be a local path"))
	}
	// Both values have already passed configuration validation. SQL-literal
	// quoting is still required because service paths can contain apostrophes.
	attach := "ATTACH IF NOT EXISTS " + duckDBStringLiteral("ducklake:"+metadata) +
		" AS " + QuoteIdentifierANSI(DuckLakeCatalogName) +
		" (DATA_PATH " + duckDBStringLiteral(dataPath) + ", DATA_INLINING_ROW_LIMIT 0, CREATE_IF_NOT_EXISTS true)"
	if _, err := execer.ExecContext(ctx, attach, nil); err != nil {
		return newDuckLakeInitError(duckLakeStageAttach, "", err)
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

func duckLakeRemoteCatalogURI(path string) bool {
	lower := strings.ToLower(strings.TrimSpace(path))
	return strings.HasPrefix(lower, "s3://") || strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://")
}

func localDuckLakeCatalogPath() (string, error) {
	dir := strings.TrimSpace(os.Getenv("DATA_PATH"))
	if dir == "" {
		home := strings.TrimSpace(os.Getenv("HOME"))
		if home == "" {
			return "", fmt.Errorf("ducklake local catalog directory is unavailable")
		}
		dir = filepath.Join(home, "data")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	return filepath.Join(dir, "ducklake-catalog.duckdb"), nil
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
