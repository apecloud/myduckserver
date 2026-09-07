package catalog

import (
	"context"
	"database/sql/driver"
	stderrors "errors"
	"fmt"
	"strings"
	"testing"

	"github.com/apecloud/myduckserver/configuration"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestDuckLakeInitErrorKeepsStageAndRedactsDriverDetails(t *testing.T) {
	const (
		metadata = "s3://private-bucket/catalog.ducklake"
		dataPath = "s3://private-bucket/data"
		endpoint = "https://private-storage.example:9443"
		access   = "access-key-77"
		secret   = "secret-key-77"
		query    = "ATTACH 'ducklake:" + metadata + "' AS \"__myduck_ducklake\" (DATA_PATH '" + dataPath + "')"
	)

	tests := []struct {
		name      string
		stage     duckLakeInitStage
		extension string
		cause     error
		reason    duckLakeFailureReason
		operation string
	}{
		{
			name:      "load io",
			stage:     duckLakeStageLoad,
			extension: "httpfs",
			cause: &duckdb.Error{
				Type: duckdb.ErrorTypeIO,
				Msg:  `IO Error: failed to open '/usr/local/lib/myduck/httpfs.duckdb_extension'`,
			},
			reason:    duckLakeReasonIOFailure,
			operation: "load duckdb extension httpfs failed",
		},
		{
			name:  "create secret permission",
			stage: duckLakeStageCreateSecret,
			cause: &duckdb.Error{
				Type: duckdb.ErrorTypePermission,
				Msg:  "Permission Error: endpoint=" + endpoint + " KEY_ID='" + access + "' SECRET='" + secret + "'",
			},
			reason:    duckLakeReasonPermissionDenied,
			operation: "create ducklake service secret failed",
		},
		{
			name:  "attach http",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{
				Type: duckdb.ErrorTypeHTTP,
				Msg:  "HTTP Error: request failed for " + endpoint + " while executing " + query,
			},
			reason:    duckLakeReasonHTTPFailure,
			operation: "attach ducklake catalog failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newDuckLakeInitError(tt.stage, tt.extension, tt.cause)
			require.Error(t, err)

			var stageErr *duckLakeInitError
			require.ErrorAs(t, err, &stageErr)
			require.Equal(t, tt.stage, stageErr.stage)
			require.Equal(t, tt.reason, stageErr.reason)
			require.Same(t, tt.cause, stageErr.rawCause())
			require.True(t, stderrors.Is(err, tt.cause))
			require.Contains(t, err.Error(), "stage="+string(tt.stage))
			require.Contains(t, err.Error(), "reason="+string(tt.reason))
			require.True(t, strings.HasPrefix(err.Error(), tt.operation+" "))
			require.Equal(t, err.Error(), fmt.Sprintf("%+v", err))
			require.Equal(t, err.Error(), fmt.Sprintf("%s", err))
			require.Equal(t, fmt.Sprintf("%q", err.Error()), fmt.Sprintf("%q", err))
			require.Equal(t, err.Error(), fmt.Sprintf("%#v", err))
			var rawDuck *duckdb.Error
			require.False(t, stderrors.As(err, &rawDuck))

			for _, forbidden := range []string{metadata, dataPath, endpoint, access, secret, query, "/usr/local/lib/myduck"} {
				require.NotContains(t, err.Error(), forbidden)
				require.NotContains(t, fmt.Sprintf("%+v", err), forbidden)
			}
		})
	}
}

func TestDuckLakeInitErrorDoesNotExposeProtocolErrorCause(t *testing.T) {
	raw := &pgconn.PgError{
		Severity: "ERROR",
		Code:     "XX000",
		Message:  "remote endpoint=https://private-storage.example:9443 SECRET=secret-key-77",
	}
	err := newDuckLakeInitError(duckLakeStageAttach, "", raw)

	var exposed *pgconn.PgError
	require.False(t, stderrors.As(err, &exposed))
	require.Nil(t, stderrors.Unwrap(err))
	require.True(t, stderrors.Is(err, raw))
	wrapper := fmt.Errorf("outer wrapper: %w", err)
	require.Contains(t, wrapper.Error(), err.Error())
	require.True(t, stderrors.Is(wrapper, raw))
	require.False(t, stderrors.As(wrapper, &exposed))
	require.NotContains(t, err.Error(), "private-storage.example")
	require.NotContains(t, err.Error(), "secret-key-77")
}

func TestDuckLakeInitErrorClassifiesSentinelsWithoutRawFormatting(t *testing.T) {
	tests := []struct {
		name   string
		cause  error
		reason duckLakeFailureReason
	}{
		{name: "canceled", cause: context.Canceled, reason: duckLakeReasonContextCanceled},
		{name: "deadline", cause: context.DeadlineExceeded, reason: duckLakeReasonDeadlineExceeded},
		{name: "bad connection", cause: driver.ErrBadConn, reason: duckLakeReasonBadConnection},
		{name: "unknown details", cause: fmt.Errorf("driver exploded at /var/lib/myduck with SECRET=secret-key-77"), reason: duckLakeReasonDriverError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newDuckLakeInitError(duckLakeStageAttach, "", tt.cause)
			require.Contains(t, err.Error(), "reason="+string(tt.reason))
			require.True(t, stderrors.Is(err, tt.cause))
			require.NotContains(t, err.Error(), "secret-key-77")
			require.NotContains(t, err.Error(), "/var/lib/myduck")
		})
	}
}

func TestDuckLakeAttachFailureUsesStageError(t *testing.T) {
	const (
		metadata = "/var/lib/myduck/catalog.ducklake"
		leaked   = "s3://private-bucket/catalog.ducklake"
		dataPath = "s3://private-bucket/data"
		endpoint = "https://private-storage.example:9443"
		secret   = "secret-key-77"
	)
	raw := &duckdb.Error{
		Type: duckdb.ErrorTypePermission,
		Msg:  "Permission Error: endpoint=" + endpoint + " metadata=" + leaked + " data=" + dataPath + " SECRET='" + secret + "'",
	}
	runtime := &duckLakeRuntime{config: configuration.DuckLakeConfig{
		MetadataPath: metadata,
		DataPath:     dataPath,
	}}
	execer := &recordingDuckLakeExecer{err: raw}

	err := runtime.attachLocked(context.Background(), nil, execer)
	require.Error(t, err)
	require.ErrorIs(t, err, raw)
	require.ErrorContains(t, err, "attach ducklake catalog failed")
	require.ErrorContains(t, err, "stage=attach")
	require.ErrorContains(t, err, "reason=permission_denied")
	require.Len(t, execer.queries, 1)
	for _, forbidden := range []string{leaked, dataPath, endpoint, secret} {
		require.NotContains(t, err.Error(), forbidden)
	}
}

func TestDuckLakeAttachFailureClosedSubclasses(t *testing.T) {
	const (
		metadata = "s3://private-bucket/catalog.ducklake"
		secret   = "secret-key-77"
		endpoint = "https://private-storage.example:9443"
		query    = "ATTACH 'ducklake:" + metadata + "' AS \"__myduck_ducklake\""
	)
	tests := []struct {
		name   string
		cause  error
		reason duckLakeFailureReason
	}{
		{
			name: "permission",
			cause: &duckdb.Error{
				Type: duckdb.ErrorTypePermission,
				Msg:  "Permission Error: endpoint=" + endpoint + " SECRET=" + secret,
			},
			reason: duckLakeReasonPermission,
		},
		{
			name: "permission from HTTP status",
			cause: &duckdb.Error{
				Type: duckdb.ErrorTypeHTTP,
				Msg:  "HTTP Error: 403 for " + metadata,
			},
			reason: duckLakeReasonPermission,
		},
		{
			name: "http",
			cause: &duckdb.Error{
				Type: duckdb.ErrorTypeHTTP,
				Msg:  "HTTP Error: status 503 while executing " + query,
			},
			reason: duckLakeReasonHTTP,
		},
		{
			name: "network",
			cause: &duckdb.Error{
				Type: duckdb.ErrorTypeNetwork,
				Msg:  "Network Error: dial tcp " + endpoint,
			},
			reason: duckLakeReasonNetwork,
		},
		{
			name: "missing object from HTTP 404",
			cause: &duckdb.Error{
				Type: duckdb.ErrorTypeHTTP,
				Msg:  "HTTP Error: 404 Not Found for " + metadata,
			},
			reason: duckLakeReasonMissingObject,
		},
		{
			name: "missing object from S3 code",
			cause: &duckdb.Error{
				Type: duckdb.ErrorTypeIO,
				Msg:  "IO Error: S3 NoSuchKey for " + metadata,
			},
			reason: duckLakeReasonMissingObject,
		},
		{
			name: "typed IO read-only missing catalog is missing object",
			cause: &duckdb.Error{
				Type: duckdb.ErrorTypeIO,
				Msg:  `IO Error: Failed to attach DuckLake MetaData in read-only mode: database does not exist`,
			},
			reason: duckLakeReasonMissingObject,
		},
		{
			name: "extension",
			cause: &duckdb.Error{
				Type: duckdb.ErrorTypeMissingExtension,
				Msg:  "Missing Extension Error: ducklake",
			},
			reason: duckLakeReasonExtension,
		},
		{
			name: "generic IO",
			cause: &duckdb.Error{
				Type: duckdb.ErrorTypeIO,
				Msg:  "IO Error: failed to read remote catalog at " + endpoint,
			},
			reason: duckLakeReasonGenericIO,
		},
		{
			name: "typed IO local file remains generic",
			cause: &duckdb.Error{
				Type: duckdb.ErrorTypeIO,
				Msg:  "IO Error: no such file or directory",
			},
			reason: duckLakeReasonGenericIO,
		},
		{
			name: "typed IO HTTP 403 is permission",
			cause: &duckdb.Error{
				Type: duckdb.ErrorTypeIO,
				Msg:  "IO Error: HTTP 403 Forbidden for " + metadata + " SECRET=" + secret,
			},
			reason: duckLakeReasonPermission,
		},
		{
			name: "typed IO connection refused is network",
			cause: &duckdb.Error{
				Type: duckdb.ErrorTypeIO,
				Msg:  "IO Error: Connection refused connecting to " + endpoint,
			},
			reason: duckLakeReasonNetwork,
		},
		{
			name: "typed IO TLS handshake is tls",
			cause: &duckdb.Error{
				Type: duckdb.ErrorTypeIO,
				Msg:  "IO Error: TLS handshake failed with " + endpoint,
			},
			reason: duckLakeReasonTLSFailure,
		},
		{
			name: "typed IO timeout is timeout",
			cause: &duckdb.Error{
				Type: duckdb.ErrorTypeIO,
				Msg:  "IO Error: network request timed out contacting " + endpoint,
			},
			reason: duckLakeReasonTimeout,
		},
	}

	seen := make(map[duckLakeFailureReason]struct{}, len(tests))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newDuckLakeInitError(duckLakeStageAttach, "", tt.cause)
			require.Error(t, err)

			var initErr *duckLakeInitError
			require.ErrorAs(t, err, &initErr)
			require.Equal(t, duckLakeStageAttach, initErr.stage)
			require.Equal(t, tt.reason, initErr.reason)
			require.Contains(t, err.Error(), "stage=attach")
			require.Contains(t, err.Error(), "reason="+string(tt.reason))
			require.NotContains(t, err.Error(), metadata)
			require.NotContains(t, err.Error(), endpoint)
			require.NotContains(t, err.Error(), secret)
			require.NotContains(t, err.Error(), query)
			for _, format := range []string{"%v", "%+v", "%#v", "%q"} {
				formatted := fmt.Sprintf(format, err)
				require.NotContains(t, formatted, metadata)
				require.NotContains(t, formatted, endpoint)
				require.NotContains(t, formatted, secret)
				require.NotContains(t, formatted, query)
			}
			seen[tt.reason] = struct{}{}
		})
	}
	require.Len(t, seen, 8, "typed IO must keep permission, network, TLS, timeout, HTTP, missing-object, extension, and generic IO distinct")
}

func TestDuckLakeAttachFailureClassifiesHTTPStatusesAndStage(t *testing.T) {
	tests := []struct {
		name  string
		stage duckLakeInitStage
		cause error
		want  duckLakeFailureReason
	}{
		{
			name:  "bare 404 is missing object for attach",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "HTTP Error: 404"},
			want:  duckLakeReasonMissingObject,
		},
		{
			name:  "404 not found is missing object for attach",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "404 Not Found"},
			want:  duckLakeReasonMissingObject,
		},
		{
			name:  "terminal period after 404 is punctuation",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "HTTP Error: 404."},
			want:  duckLakeReasonMissingObject,
		},
		{
			name:  "dotted 404 suffix remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "HTTP Error: 404.txt"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "identifier-like HTTP marker remains broad HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "HTTP_Error:404"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "identifier-like HTTP version remains broad HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "foo_http/1.1 404"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "status assignment with terminal period is missing object",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "status=404."},
			want:  duckLakeReasonMissingObject,
		},
		{
			name:  "status assignment with terminal ellipsis is missing object",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "status=404..."},
			want:  duckLakeReasonMissingObject,
		},
		{
			name:  "status assignment after a sentence is missing object",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "request failed. status=404"},
			want:  duckLakeReasonMissingObject,
		},
		{
			name:  "status assignment with dotted suffix remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "status=404.txt"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "status assignment with multi-dot suffix remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "status=404..txt"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "status assignment with dot path suffix remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "status=404../path"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "bare typed 404 is missing object for attach",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "404"},
			want:  duckLakeReasonMissingObject,
		},
		{
			name:  "numeric status in an unrelated identifier remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "zipcode 404"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "numeric status in a path assignment remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "path=404"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "numeric status in a path status segment remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "path/status=404"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "numeric permission status in a path code segment remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "path/code=403"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "numeric status in an unknown assignment prefix remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "foo=status=404"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "numeric permission status in an unknown assignment prefix remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "foo=code=403"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "spaced unknown status assignment prefix remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "foo = status=404"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "spaced unknown permission assignment prefix remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "foo = code=403"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "status code joined to number by underscore remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "status_code_404"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "status code joined to number by dot remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "status_code.404"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "repeated status field separator remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "status__code=404"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "prefixed status field remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "_status_code=404"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "compact object marker in an identifier remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "foo_nosuchkey"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "compact object marker assignment remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "foo=nosuchkey"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "compact object marker identifier before status remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "foo_nosuchkey 403"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "standalone code assignment is permission",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "code=403"},
			want:  duckLakeReasonPermission,
		},
		{
			name:  "leading 404 with remote payload is missing object for attach",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "404 for s3://private/catalog.ducklake"},
			want:  duckLakeReasonMissingObject,
		},
		{
			name:  "hyphenated 404 identifier remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "404-not-found"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "NoSuchKey is missing object for attach",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "NoSuchKey"},
			want:  duckLakeReasonMissingObject,
		},
		{
			name:  "NoSuchKey path component remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "/NoSuchKey"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "NoSuchKey path prefix remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "NoSuchKey/object"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "NoSuchKey dot path suffix remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "NoSuchKey../path"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "404 path prefix remains HTTP",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "HTTP Error: 404/object"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "bare 404 remains HTTP outside attach",
			stage: duckLakeStageCreateSecret,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "HTTP Error: 404"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "bare typed 404 remains HTTP outside attach",
			stage: duckLakeStageCreateSecret,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "404"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "NoSuchKey remains HTTP outside attach",
			stage: duckLakeStageLoad,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "NoSuchKey"},
			want:  duckLakeReasonHTTP,
		},
		{
			name:  "bare 403 is permission",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "HTTP Error: 403"},
			want:  duckLakeReasonPermission,
		},
		{
			name:  "403 after NoSuchKey is still permission",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "HTTP Error: NoSuchKey 403"},
			want:  duckLakeReasonPermission,
		},
		{
			name:  "401 after NoSuchKey is still permission",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "HTTP Error: NoSuchKey 401"},
			want:  duckLakeReasonPermission,
		},
		{
			name:  "remote typed IO no such file is missing object",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeIO, Msg: "IO Error: no such file at s3://private/catalog.ducklake"},
			want:  duckLakeReasonMissingObject,
		},
		{
			name:  "assigned remote typed IO target is missing object",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeIO, Msg: "IO Error: no such file at url=s3://private/catalog.ducklake"},
			want:  duckLakeReasonMissingObject,
		},
		{
			name:  "repeated remote typed IO assignment stays generic IO",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeIO, Msg: "IO Error: no such file at url==s3://private/catalog.ducklake"},
			want:  duckLakeReasonGenericIO,
		},
		{
			name:  "typed S3 error context is missing object",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeIO, Msg: "S3 Error: no such file"},
			want:  duckLakeReasonMissingObject,
		},
		{
			name:  "local typed IO no such file is generic IO",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeIO, Msg: "IO Error: no such file or directory"},
			want:  duckLakeReasonGenericIO,
		},
		{
			name:  "local path words do not imply remote object",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeIO, Msg: "IO Error: no such file at /var/lib/object/catalog"},
			want:  duckLakeReasonGenericIO,
		},
		{
			name:  "unrelated documentation URL does not make local file remote",
			stage: duckLakeStageAttach,
			cause: &duckdb.Error{Type: duckdb.ErrorTypeIO, Msg: "IO Error: no such file at /var/lib/catalog; see https://docs.example/help"},
			want:  duckLakeReasonGenericIO,
		},
		{
			name:  "extension missing lexical marker",
			stage: duckLakeStageLoad,
			cause: stderrors.New("extension httpfs missing"),
			want:  duckLakeReasonExtension,
		},
		{
			name:  "extension cannot find lexical marker",
			stage: duckLakeStageLoad,
			cause: stderrors.New("cannot find extension httpfs"),
			want:  duckLakeReasonExtension,
		},
		{
			name:  "attach extension wording is not trusted without load context",
			stage: duckLakeStageAttach,
			cause: stderrors.New("remote extension missing"),
			want:  duckLakeReasonDriverError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newDuckLakeInitError(tt.stage, "httpfs", tt.cause)
			var initErr *duckLakeInitError
			require.ErrorAs(t, err, &initErr)
			require.Equal(t, tt.want, initErr.reason)
			require.Contains(t, err.Error(), "reason="+string(tt.want))
		})
	}
}

func TestDuckLakeAttachFailureRefinesWrappedTypedMissingObject(t *testing.T) {
	const (
		metadata = "s3://private-bucket/catalog.ducklake"
		dataPath = "s3://private-bucket/data"
		secret   = "secret-key-77"
	)
	raw := &duckdb.Error{
		Type: duckdb.ErrorTypeHTTP,
		Msg:  "HTTP Error: status code: 404 (Not Found) metadata=" + metadata + " SECRET=" + secret,
	}
	wrapped := fmt.Errorf("driver wrapper: %w", raw)
	err := newDuckLakeInitError(duckLakeStageAttach, "", wrapped)

	var initErr *duckLakeInitError
	require.ErrorAs(t, err, &initErr)
	require.Equal(t, duckLakeReasonMissingObject, initErr.reason)
	require.Same(t, wrapped, initErr.rawCause())
	require.True(t, stderrors.Is(err, raw))
	require.Nil(t, stderrors.Unwrap(err))
	var exposed *duckdb.Error
	require.False(t, stderrors.As(err, &exposed))
	for _, forbidden := range []string{metadata, dataPath, secret, "driver wrapper", raw.Msg} {
		require.NotContains(t, err.Error(), forbidden)
		require.NotContains(t, fmt.Sprintf("%+v", err), forbidden)
	}
}

func TestDuckLakeAttachFailureKeepsLexicalMarkersOutOfURLs(t *testing.T) {
	tests := []struct {
		name    string
		message string
		want    duckLakeFailureReason
	}{
		{name: "zipcode is not a status marker", message: "zipcode 404", want: duckLakeReasonDriverError},
		{name: "hyphenated identifier is not a status marker", message: "zip-code 404", want: duckLakeReasonDriverError},
		{name: "hyphenated permission phrase is an identifier", message: "access-denied", want: duckLakeReasonDriverError},
		{name: "hyphenated missing phrase is an identifier", message: "object-not-found", want: duckLakeReasonDriverError},
		{name: "hyphenated network phrase is an identifier", message: "connection-refused", want: duckLakeReasonDriverError},
		{name: "hyphenated network error identifier is opaque", message: "network-error", want: duckLakeReasonDriverError},
		{name: "reversed hyphenated network identifier is opaque", message: "error-network", want: duckLakeReasonDriverError},
		{name: "hyphenated extension identifier is opaque", message: "extension-not-loaded", want: duckLakeReasonDriverError},
		{name: "hyphenated extension status suffix is opaque", message: "extension not-loaded", want: duckLakeReasonDriverError},
		{name: "reversed hyphenated extension identifier is opaque", message: "failed-to-load-extension", want: duckLakeReasonDriverError},
		{name: "hyphenated autoload identifier is opaque", message: "autoload-extension", want: duckLakeReasonDriverError},
		{name: "hyphenated resource identifier is opaque", message: "resource-unavailable", want: duckLakeReasonDriverError},
		{name: "hyphenated bare not-found phrase is opaque", message: "not-found", want: duckLakeReasonDriverError},
		{name: "single missing code with leading hyphen is opaque", message: "foo-nosuchkey", want: duckLakeReasonDriverError},
		{name: "single missing code with trailing hyphen is opaque", message: "nosuchkey-foo", want: duckLakeReasonDriverError},
		{name: "single object code with leading hyphen is opaque", message: "foo-objectnotfound", want: duckLakeReasonDriverError},
		{name: "single key code with trailing hyphen is opaque", message: "keynotfound-foo", want: duckLakeReasonDriverError},
		{name: "status code marker", message: "status_code=404", want: duckLakeReasonMissingObject},
		{name: "response code marker", message: "response_code=404", want: duckLakeReasonMissingObject},
		{name: "HTTP version marker", message: "HTTP/1.1 404 Not Found", want: duckLakeReasonMissingObject},
		{name: "leading status with payload", message: "404 for s3://private/catalog.ducklake", want: duckLakeReasonMissingObject},
		{name: "hyphenated leading status is opaque", message: "404-not-found", want: duckLakeReasonDriverError},
		{name: "plain status phrase", message: "404 Not Found", want: duckLakeReasonMissingObject},
		{name: "plain HTTP failure", message: "HTTP 503", want: duckLakeReasonHTTPFailure},
		{name: "network hostname is not a network error", message: "GET https://network.example/object", want: duckLakeReasonDriverError},
		{name: "permission hostname is not a permission error", message: "GET https://forbidden.example/object", want: duckLakeReasonDriverError},
		{name: "permission hostname with hyphen is not a permission error", message: "GET https://access-denied.example/object", want: duckLakeReasonDriverError},
		{name: "missing hostname is not an unavailable resource", message: "GET https://missing.example/object", want: duckLakeReasonDriverError},
		{name: "URL permission marker does not hide later permission", message: "GET https://access-denied.example/object; access denied while reading catalog", want: duckLakeReasonPermissionDenied},
		{name: "URL missing marker does not hide later missing object", message: "GET https://object-not-found.example/object; object not found", want: duckLakeReasonMissingObject},
		{name: "URL network marker does not hide later network error", message: "GET https://connection-refused.example/object; network error while reading catalog", want: duckLakeReasonNetworkFailure},
		{name: "network phrase", message: "network error while reading catalog", want: duckLakeReasonNetworkFailure},
		{name: "permission phrase", message: "access denied while reading catalog", want: duckLakeReasonPermissionDenied},
		{name: "unauthorized access phrase", message: "Unauthorized access", want: duckLakeReasonPermissionDenied},
		{name: "operation forbidden phrase", message: "Operation forbidden", want: duckLakeReasonPermissionDenied},
		{name: "remote extension wording is an object miss", message: "remote extension missing object", want: duckLakeReasonMissingObject},
		{name: "remote extension wording alone is opaque", message: "remote extension missing", want: duckLakeReasonDriverError},
		{name: "known remote extension wording alone is opaque", message: "remote ducklake extension unavailable", want: duckLakeReasonDriverError},
		{name: "known remote extension URL wording alone is opaque", message: "remote extension https://foo/httpfs missing", want: duckLakeReasonDriverError},
		{name: "explicit attach extension load wording is classified", message: "failed to load ducklake extension", want: duckLakeReasonExtensionUnavailable},
		{name: "local path words do not imply missing object", message: "no such file at /var/lib/object/catalog", want: duckLakeReasonResourceUnavailable},
		{name: "plain HTTP status remains an object miss", message: "404 Not Found", want: duckLakeReasonMissingObject},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newDuckLakeInitError(duckLakeStageAttach, "", stderrors.New(tt.message))
			var initErr *duckLakeInitError
			require.ErrorAs(t, err, &initErr)
			require.Equal(t, tt.want, initErr.reason)
		})
	}
}

func TestDuckLakeLexicalMarkerHyphenBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		stage   duckLakeInitStage
		message string
		want    duckLakeFailureReason
	}{
		{name: "404 reason phrase joined by hyphen", stage: duckLakeStageAttach, message: "404 not-found", want: duckLakeReasonDriverError},
		{name: "401 permission phrase joined by hyphen", stage: duckLakeStageAttach, message: "401 unauthorized-error", want: duckLakeReasonDriverError},
		{name: "403 permission phrase joined by hyphen", stage: duckLakeStageAttach, message: "403 forbidden-error", want: duckLakeReasonDriverError},
		{name: "500 error context joined by hyphen", stage: duckLakeStageAttach, message: "500 error-code", want: duckLakeReasonDriverError},
		{name: "500 unavailable context joined by hyphen", stage: duckLakeStageAttach, message: "500 unavailable-now", want: duckLakeReasonDriverError},
		{name: "leading status context joined by hyphen", stage: duckLakeStageAttach, message: "404 for-s3", want: duckLakeReasonDriverError},
		{name: "leading object context joined by hyphen", stage: duckLakeStageAttach, message: "404 object-not-found", want: duckLakeReasonDriverError},
		{name: "hyphenated object code before status", stage: duckLakeStageAttach, message: "foo-nosuchkey 403", want: duckLakeReasonDriverError},
		{name: "object code in URL before status", stage: duckLakeStageAttach, message: "https://example.test/nosuchkey 403", want: duckLakeReasonDriverError},
		{name: "permission context suffix joined by hyphen", stage: duckLakeStageAttach, message: "unauthorized error-code", want: duckLakeReasonDriverError},
		{name: "permission context prefix joined by hyphen", stage: duckLakeStageAttach, message: "error-code unauthorized", want: duckLakeReasonDriverError},
		{name: "permission marker in an absolute path is opaque", stage: duckLakeStageAttach, message: "/unauthorized", want: duckLakeReasonDriverError},
		{name: "permission marker in a trailing path is opaque", stage: duckLakeStageAttach, message: "forbidden/", want: duckLakeReasonDriverError},
		{name: "permission marker in a query is opaque", stage: duckLakeStageAttach, message: "?unauthorized", want: duckLakeReasonDriverError},
		{name: "permission marker in an identifier is opaque", stage: duckLakeStageAttach, message: "foo_unauthorized", want: duckLakeReasonDriverError},
		{name: "status marker in an unknown assignment is opaque", stage: duckLakeStageAttach, message: "foo=status=404", want: duckLakeReasonDriverError},
		{name: "permission status in an unknown assignment is opaque", stage: duckLakeStageAttach, message: "foo=code=403", want: duckLakeReasonDriverError},
		{name: "compact missing marker in an identifier is opaque", stage: duckLakeStageAttach, message: "foo_nosuchkey", want: duckLakeReasonDriverError},
		{name: "compact missing marker in an assignment is opaque", stage: duckLakeStageAttach, message: "foo=nosuchkey", want: duckLakeReasonDriverError},
		{name: "compact missing marker before permission status is opaque", stage: duckLakeStageAttach, message: "foo_nosuchkey 403", want: duckLakeReasonDriverError},
		{name: "permission access context joined by hyphen", stage: duckLakeStageAttach, message: "forbidden access-denied", want: duckLakeReasonDriverError},
		{name: "permission actor context joined by hyphen", stage: duckLakeStageAttach, message: "unauthorized by-user", want: duckLakeReasonDriverError},
		{name: "load extension status joined by hyphen", stage: duckLakeStageLoad, message: "extension unavailable-now", want: duckLakeReasonDriverError},
		{name: "load extension autoload identifier", stage: duckLakeStageLoad, message: "extension autoload-extension", want: duckLakeReasonDriverError},
		{name: "load extension failure identifier", stage: duckLakeStageLoad, message: "extension failed-to-load", want: duckLakeReasonDriverError},
		{name: "load extension cannot identifier", stage: duckLakeStageLoad, message: "extension cannot-load", want: duckLakeReasonDriverError},
		{name: "load token in URL is not extension context", stage: duckLakeStageLoad, message: "extension https://example.test/load", want: duckLakeReasonDriverError},
		{name: "attach extension status joined by hyphen", stage: duckLakeStageAttach, message: "extension not-loaded", want: duckLakeReasonDriverError},
		{name: "remote identifier is not object context", stage: duckLakeStageAttach, message: "remote-object no such file", want: duckLakeReasonResourceUnavailable},
		{name: "s3 identifier is not object context", stage: duckLakeStageAttach, message: "s3-bucket no such file", want: duckLakeReasonResourceUnavailable},
		{name: "plain permission context remains classified", stage: duckLakeStageAttach, message: "unauthorized access", want: duckLakeReasonPermissionDenied},
		{name: "plain status reason remains classified", stage: duckLakeStageAttach, message: "404 not found", want: duckLakeReasonMissingObject},
		{name: "plain extension status remains classified", stage: duckLakeStageLoad, message: "extension unavailable", want: duckLakeReasonExtensionUnavailable},
		{name: "plain remote context remains missing object", stage: duckLakeStageAttach, message: "remote no such file", want: duckLakeReasonMissingObject},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newDuckLakeInitError(tt.stage, "", stderrors.New(tt.message))
			var initErr *duckLakeInitError
			require.ErrorAs(t, err, &initErr)
			require.Equal(t, tt.want, initErr.reason)
		})
	}
}

func TestDuckLakeLexicalMarkerPathBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		stage   duckLakeInitStage
		message string
		want    duckLakeFailureReason
	}{
		{name: "leading path single missing code", stage: duckLakeStageAttach, message: "/nosuchkey", want: duckLakeReasonDriverError},
		{name: "trailing path single missing code", stage: duckLakeStageAttach, message: "nosuchkey/object", want: duckLakeReasonDriverError},
		{name: "leading path compact object code", stage: duckLakeStageAttach, message: "/objectnotfound", want: duckLakeReasonDriverError},
		{name: "relative object path", stage: duckLakeStageAttach, message: "object/not/found", want: duckLakeReasonDriverError},
		{name: "absolute object path", stage: duckLakeStageAttach, message: "/object/not/found", want: duckLakeReasonDriverError},
		{name: "windows object path", stage: duckLakeStageAttach, message: `C:\object\not\found`, want: duckLakeReasonDriverError},
		{name: "permission path", stage: duckLakeStageAttach, message: "access/denied", want: duckLakeReasonDriverError},
		{name: "network path", stage: duckLakeStageAttach, message: "connection/refused", want: duckLakeReasonDriverError},
		{name: "reversed network path", stage: duckLakeStageAttach, message: "error/network", want: duckLakeReasonDriverError},
		{name: "extension path", stage: duckLakeStageAttach, message: "cannot/load/extension", want: duckLakeReasonDriverError},
		{name: "load extension path", stage: duckLakeStageLoad, message: "extension/not/loaded", want: duckLakeReasonDriverError},
		{name: "resource path", stage: duckLakeStageAttach, message: "resource/unavailable", want: duckLakeReasonDriverError},
		{name: "remote path word is not object context", stage: duckLakeStageAttach, message: "remote/path no such file", want: duckLakeReasonResourceUnavailable},
		{name: "s3 path word is not object context", stage: duckLakeStageAttach, message: "s3/path no such file", want: duckLakeReasonResourceUnavailable},
		{name: "path marker does not hide later object marker", stage: duckLakeStageAttach, message: "/object/not/found; object not found", want: duckLakeReasonMissingObject},
		{name: "path marker does not hide later network marker", stage: duckLakeStageAttach, message: "/connection/refused; network error", want: duckLakeReasonNetworkFailure},
		{name: "status assignment remains classified", stage: duckLakeStageAttach, message: "status_code=404", want: duckLakeReasonMissingObject},
		{name: "HTTP version remains classified", stage: duckLakeStageAttach, message: "HTTP/1.1 404 Not Found", want: duckLakeReasonMissingObject},
		{name: "remote URI remains classified", stage: duckLakeStageAttach, message: "no such file at s3://private/catalog.ducklake", want: duckLakeReasonMissingObject},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newDuckLakeInitError(tt.stage, "", stderrors.New(tt.message))
			var initErr *duckLakeInitError
			require.ErrorAs(t, err, &initErr)
			require.Equal(t, tt.want, initErr.reason)
		})
	}
}

func TestDuckLakeLexicalMarkerPunctuationBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		stage   duckLakeInitStage
		message string
		want    duckLakeFailureReason
	}{
		{name: "permission sentence period", stage: duckLakeStageAttach, message: "access denied.", want: duckLakeReasonPermissionDenied},
		{name: "missing object sentence period", stage: duckLakeStageAttach, message: "object not found.", want: duckLakeReasonMissingObject},
		{name: "compact object sentence period", stage: duckLakeStageAttach, message: "NoSuchKey.", want: duckLakeReasonMissingObject},
		{name: "standalone quoted compact object marker", stage: duckLakeStageAttach, message: `"NoSuchKey"`, want: duckLakeReasonMissingObject},
		{name: "standalone quoted permission phrase", stage: duckLakeStageAttach, message: `"access denied"`, want: duckLakeReasonPermissionDenied},
		{name: "standalone quoted missing phrase", stage: duckLakeStageAttach, message: `"object not found"`, want: duckLakeReasonMissingObject},
		{name: "standalone bracketed network phrase", stage: duckLakeStageAttach, message: `[network error]`, want: duckLakeReasonNetworkFailure},
		{name: "standalone backtick extension phrase", stage: duckLakeStageLoad, message: "`extension unavailable`", want: duckLakeReasonExtensionUnavailable},
		{name: "standalone escaped quoted missing phrase", stage: duckLakeStageAttach, message: `"prefix "" object not found "" suffix"`, want: duckLakeReasonMissingObject},
		{name: "standalone nested delimited missing phrase", stage: duckLakeStageAttach, message: `[prefix "object not found" suffix]`, want: duckLakeReasonMissingObject},
		{name: "network sentence period", stage: duckLakeStageAttach, message: "network error.", want: duckLakeReasonNetworkFailure},
		{name: "connection sentence period", stage: duckLakeStageAttach, message: "connection refused.", want: duckLakeReasonNetworkFailure},
		{name: "extension sentence period", stage: duckLakeStageLoad, message: "extension unavailable.", want: duckLakeReasonExtensionUnavailable},
		{name: "resource sentence period", stage: duckLakeStageAttach, message: "resource unavailable.", want: duckLakeReasonResourceUnavailable},
		{name: "ellipsis is punctuation", stage: duckLakeStageAttach, message: "NoSuchKey...", want: duckLakeReasonMissingObject},
		{name: "dot path suffix is identifier syntax", stage: duckLakeStageAttach, message: "NoSuchKey../path", want: duckLakeReasonDriverError},
		{name: "dot query suffix is identifier syntax", stage: duckLakeStageAttach, message: "network error..?query", want: duckLakeReasonDriverError},
		{name: "period before next sentence is punctuation", stage: duckLakeStageAttach, message: "NoSuchKey. Details follow", want: duckLakeReasonMissingObject},
		{name: "leading dot is identifier syntax", stage: duckLakeStageAttach, message: ".nosuchkey", want: duckLakeReasonDriverError},
		{name: "dotted object prefix is identifier syntax", stage: duckLakeStageAttach, message: "foo.nosuchkey", want: duckLakeReasonDriverError},
		{name: "dotted object suffix is identifier syntax", stage: duckLakeStageAttach, message: "nosuchkey.foo", want: duckLakeReasonDriverError},
		{name: "dotted underscore suffix is identifier syntax", stage: duckLakeStageAttach, message: "nosuchkey._suffix", want: duckLakeReasonDriverError},
		{name: "quoted object marker with dotted suffix is identifier syntax", stage: duckLakeStageAttach, message: `"NoSuchKey".field`, want: duckLakeReasonDriverError},
		{name: "quoted object marker with dotted prefix is identifier syntax", stage: duckLakeStageAttach, message: `schema."NoSuchKey"`, want: duckLakeReasonDriverError},
		{name: "quoted dotted permission phrase is identifier syntax", stage: duckLakeStageAttach, message: `"access"."denied"`, want: duckLakeReasonDriverError},
		{name: "backtick dotted missing phrase is identifier syntax", stage: duckLakeStageAttach, message: "`object`.`not`.`found`", want: duckLakeReasonDriverError},
		{name: "bracket dotted network phrase is identifier syntax", stage: duckLakeStageAttach, message: "[network].[error]", want: duckLakeReasonDriverError},
		{name: "quoted dotted extension phrase is identifier syntax", stage: duckLakeStageLoad, message: `"extension"."unavailable"`, want: duckLakeReasonDriverError},
		{name: "quoted multiword permission identifier", stage: duckLakeStageAttach, message: `schema."access denied"`, want: duckLakeReasonDriverError},
		{name: "quoted multiword missing identifier", stage: duckLakeStageAttach, message: `"object not found".field`, want: duckLakeReasonDriverError},
		{name: "bracketed multiword network identifier", stage: duckLakeStageAttach, message: `[network error].field`, want: duckLakeReasonDriverError},
		{name: "backtick multiword extension identifier", stage: duckLakeStageLoad, message: "schema.`extension unavailable`", want: duckLakeReasonDriverError},
		{name: "escaped quoted multiword missing identifier", stage: duckLakeStageAttach, message: `"prefix "" object not found "" suffix".field`, want: duckLakeReasonDriverError},
		{name: "nested delimited multiword missing identifier", stage: duckLakeStageAttach, message: `[prefix "object not found" suffix].field`, want: duckLakeReasonDriverError},
		{name: "dotted permission phrase is identifier syntax", stage: duckLakeStageAttach, message: "access.denied", want: duckLakeReasonDriverError},
		{name: "dotted missing phrase is identifier syntax", stage: duckLakeStageAttach, message: "object.not.found", want: duckLakeReasonDriverError},
		{name: "dotted network phrase is identifier syntax", stage: duckLakeStageAttach, message: "network.error", want: duckLakeReasonDriverError},
		{name: "dotted extension phrase is identifier syntax", stage: duckLakeStageLoad, message: "extension.unavailable", want: duckLakeReasonDriverError},
		{name: "dotted resource phrase is identifier syntax", stage: duckLakeStageAttach, message: "resource.unavailable", want: duckLakeReasonDriverError},
		{name: "dotted numeric reason is identifier syntax", stage: duckLakeStageAttach, message: "404.not.found", want: duckLakeReasonDriverError},
		{name: "leading dotted numeric status is identifier syntax", stage: duckLakeStageAttach, message: ".404", want: duckLakeReasonDriverError},
		{name: "status assignment terminal period", stage: duckLakeStageAttach, message: "status=404.", want: duckLakeReasonMissingObject},
		{name: "status assignment terminal ellipsis", stage: duckLakeStageAttach, message: "status=404...", want: duckLakeReasonMissingObject},
		{name: "status assignment after sentence period", stage: duckLakeStageAttach, message: "request failed. status=404", want: duckLakeReasonMissingObject},
		{name: "status assignment dotted suffix", stage: duckLakeStageAttach, message: "status=404.txt", want: duckLakeReasonDriverError},
		{name: "status assignment multi-dot suffix", stage: duckLakeStageAttach, message: "status=404..txt", want: duckLakeReasonDriverError},
		{name: "status assignment dot path suffix", stage: duckLakeStageAttach, message: "status=404../path", want: duckLakeReasonDriverError},
		{name: "identifier-like HTTP version", stage: duckLakeStageAttach, message: "foo_http/1.1 404", want: duckLakeReasonDriverError},
		{name: "assigned HTTP version", stage: duckLakeStageAttach, message: "foo=http/1.1 403", want: duckLakeReasonDriverError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newDuckLakeInitError(tt.stage, "", stderrors.New(tt.message))
			var initErr *duckLakeInitError
			require.ErrorAs(t, err, &initErr)
			require.Equal(t, tt.want, initErr.reason)
		})
	}
}

func TestDuckLakeGenericMarkerBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		message string
		want    duckLakeFailureReason
	}{
		{name: "plain TLS diagnostic", message: "TLS error.", want: duckLakeReasonTLSFailure},
		{name: "plain timeout diagnostic", message: "request timed out.", want: duckLakeReasonTimeout},
		{name: "plain syntax diagnostic", message: "syntax error.", want: duckLakeReasonInvalidStatement},
		{name: "plain cancellation diagnostic", message: "operation canceled.", want: duckLakeReasonInterrupted},
		{name: "plain interruption diagnostic", message: "operation interrupted.", want: duckLakeReasonInterrupted},
		{name: "hyphenated TLS identifier", message: "tls-error", want: duckLakeReasonDriverError},
		{name: "hyphenated timeout identifier", message: "timeout-token", want: duckLakeReasonDriverError},
		{name: "hyphenated syntax identifier", message: "syntax-error", want: duckLakeReasonDriverError},
		{name: "hyphenated parser identifier", message: "parser-error", want: duckLakeReasonDriverError},
		{name: "hyphenated interruption identifier", message: "interrupt-token", want: duckLakeReasonDriverError},
		{name: "hyphenated cancellation identifier", message: "cancel-token", want: duckLakeReasonDriverError},
		{name: "timeout query identifier", message: "request_timeout", want: duckLakeReasonDriverError},
		{name: "timeout path", message: "path=/timeout", want: duckLakeReasonDriverError},
		{name: "timeout URL", message: "https://timeout.example", want: duckLakeReasonDriverError},
		{name: "certificate URL", message: "https://certificate.example", want: duckLakeReasonDriverError},
		{name: "unexpected EOF identifier", message: "foo_unexpected eof_bar", want: duckLakeReasonDriverError},
		{name: "resource exhaustion identifier", message: "foo_no space left on device_bar", want: duckLakeReasonDriverError},
		{name: "quota identifier", message: "quota exceeded-token", want: duckLakeReasonDriverError},
		{name: "configuration identifier", message: "foo_missing secret", want: duckLakeReasonDriverError},
		{name: "catalog identifier", message: "foo_catalog error_bar", want: duckLakeReasonDriverError},
		{name: "IO path", message: "path=/failed to open", want: duckLakeReasonDriverError},
		{name: "canonical out of memory spelling", message: "out-of-memory", want: duckLakeReasonResourceExhaustion},
		{name: "canonical out of memory in sentence", message: "driver reports out-of-memory", want: duckLakeReasonResourceExhaustion},
		{name: "canonical out of memory after sentence period", message: "request failed. out-of-memory", want: duckLakeReasonResourceExhaustion},
		{name: "canonical read only spelling", message: "read-only file system", want: duckLakeReasonIOFailure},
		{name: "canonical input output spelling", message: "input/output error", want: duckLakeReasonIOFailure},
		{name: "canonical input output in sentence", message: "write failed input/output error", want: duckLakeReasonIOFailure},
		{name: "canonical input output after sentence period", message: "request failed. input/output error", want: duckLakeReasonIOFailure},
		{name: "canonical IO spelling", message: "i/o error", want: duckLakeReasonIOFailure},
		{name: "out of memory identifier", message: "foo_out-of-memory", want: duckLakeReasonDriverError},
		{name: "read only URL", message: "https://example.test/read-only file system", want: duckLakeReasonDriverError},
		{name: "input output path", message: "path=/input/output error", want: duckLakeReasonDriverError},
		{name: "quoted read only path", message: `path="read-only file system"`, want: duckLakeReasonDriverError},
		{name: "quoted input output path", message: `path='input/output error'`, want: duckLakeReasonDriverError},
		{name: "spaced assigned compact marker", message: "foo = nosuchkey", want: duckLakeReasonDriverError},
		{name: "quoted assigned compact marker", message: `foo = "nosuchkey"`, want: duckLakeReasonDriverError},
		{name: "spaced assigned permission phrase", message: "foo = access denied", want: duckLakeReasonDriverError},
		{name: "quoted assigned network phrase", message: `foo = "network error"`, want: duckLakeReasonDriverError},
		{name: "spaced assigned syntax phrase", message: "foo = syntax error", want: duckLakeReasonDriverError},
		{name: "multi-dot out of memory identifier", message: "out-of-memory..txt", want: duckLakeReasonDriverError},
		{name: "dot path out of memory identifier", message: "out-of-memory../path", want: duckLakeReasonDriverError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newDuckLakeInitError(duckLakeStageAttach, "", stderrors.New(tt.message))
			var initErr *duckLakeInitError
			require.ErrorAs(t, err, &initErr)
			require.Equal(t, tt.want, initErr.reason)
		})
	}
}

func TestDuckLakeRemoteFileContextMustBeProximate(t *testing.T) {
	tests := []struct {
		name    string
		message string
		want    duckLakeFailureReason
	}{
		{name: "remote URL is the missing file target", message: "no such file at s3://private/catalog.ducklake", want: duckLakeReasonMissingObject},
		{name: "assigned remote URL is the missing file target", message: "no such file at url=s3://private/catalog.ducklake", want: duckLakeReasonMissingObject},
		{name: "assigned remote path is the missing file target", message: "no such file at path=https://storage.example/catalog.ducklake", want: duckLakeReasonMissingObject},
		{name: "quoted assigned remote URL is the missing file target", message: `"url"=s3://private/catalog.ducklake: no such file`, want: duckLakeReasonMissingObject},
		{name: "remote URL precedes the missing phrase", message: "s3://private/catalog.ducklake: no such file", want: duckLakeReasonMissingObject},
		{name: "remote word precedes the missing phrase", message: "remote object no such file", want: duckLakeReasonMissingObject},
		{name: "remote storage precedes the missing phrase", message: "remote storage: no such file", want: duckLakeReasonMissingObject},
		{name: "S3 error precedes the missing phrase", message: "S3 Error: no such file", want: duckLakeReasonMissingObject},
		{name: "S3 error remains remote before ordinary attach context", message: "S3 Error: no such file at attach time", want: duckLakeReasonMissingObject},
		{name: "S3 error remains remote before ordinary key context", message: "S3 Error: no such file at key foo", want: duckLakeReasonMissingObject},
		{name: "local file with unrelated later URL", message: "no such file at /var/lib/catalog; see https://docs.example/help", want: duckLakeReasonResourceUnavailable},
		{name: "local file with unrelated earlier URL", message: "see https://docs.example/help for details; no such file at /var/lib/catalog", want: duckLakeReasonResourceUnavailable},
		{name: "adjacent documentation URL does not override local target", message: "see https://docs.example/help: no such file at /var/lib/catalog", want: duckLakeReasonResourceUnavailable},
		{name: "local file with unrelated later remote word", message: "no such file at /var/lib/catalog; see remote docs", want: duckLakeReasonResourceUnavailable},
		{name: "nested remote assignment remains unrelated", message: "no such file at foo_url=s3://private/catalog.ducklake", want: duckLakeReasonResourceUnavailable},
		{name: "repeated remote assignment remains unrelated", message: "no such file at url==s3://private/catalog.ducklake", want: duckLakeReasonResourceUnavailable},
		{name: "spaced repeated remote assignment remains unrelated", message: "no such file at url = = s3://private/catalog.ducklake", want: duckLakeReasonResourceUnavailable},
		{name: "reverse nested remote identifier remains unrelated", message: "foo_url=s3://private/catalog.ducklake: no such file", want: duckLakeReasonResourceUnavailable},
		{name: "reverse nested remote assignment remains unrelated", message: "foo=url=s3://private/catalog.ducklake: no such file", want: duckLakeReasonResourceUnavailable},
		{name: "reverse quoted dotted remote assignment remains unrelated", message: `foo."url"=s3://private/catalog.ducklake: no such file`, want: duckLakeReasonResourceUnavailable},
		{name: "reverse quoted multiword remote assignment remains unrelated", message: `foo."remote url"=s3://private/catalog.ducklake: no such file`, want: duckLakeReasonResourceUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newDuckLakeInitError(duckLakeStageAttach, "", stderrors.New(tt.message))
			var initErr *duckLakeInitError
			require.ErrorAs(t, err, &initErr)
			require.Equal(t, tt.want, initErr.reason)
		})
	}
}

type duckLakeAsErrorWrapper struct {
	duckErr *duckdb.Error
}

func (wrapper duckLakeAsErrorWrapper) Error() string {
	return "opaque wrapper"
}

func (wrapper duckLakeAsErrorWrapper) As(target any) bool {
	duckTarget, ok := target.(**duckdb.Error)
	if !ok || duckTarget == nil || wrapper.duckErr == nil {
		return false
	}
	*duckTarget = wrapper.duckErr
	return true
}

func TestDuckLakeFailureClassificationPreservesAsWrapperCompatibility(t *testing.T) {
	raw := &duckdb.Error{
		Type: duckdb.ErrorTypePermission,
		Msg:  "Permission Error: opaque secret",
	}
	err := newDuckLakeInitError(duckLakeStageAttach, "", duckLakeAsErrorWrapper{duckErr: raw})

	var initErr *duckLakeInitError
	require.ErrorAs(t, err, &initErr)
	require.Equal(t, duckLakeReasonPermissionDenied, initErr.reason)
	require.True(t, stderrors.Is(err, initErr.rawCause()))
	var exposed *duckdb.Error
	require.False(t, stderrors.As(err, &exposed))
}

func TestDuckLakeInitErrorRedactsJoinedCauseAcrossFormats(t *testing.T) {
	const (
		metadata = "s3://private-bucket/catalog.ducklake"
		endpoint = "https://private-storage.example:9443"
		secret   = "secret-key-77"
	)
	rawMissing := &duckdb.Error{
		Type: duckdb.ErrorTypeHTTP,
		Msg:  "HTTP Error: 404 Not Found metadata=" + metadata + " SECRET=" + secret,
	}
	rawNetwork := fmt.Errorf("network failure endpoint=%s", endpoint)
	joined := stderrors.Join(rawNetwork, rawMissing)
	err := newDuckLakeInitError(duckLakeStageAttach, "", joined)

	var initErr *duckLakeInitError
	require.ErrorAs(t, err, &initErr)
	require.Equal(t, duckLakeReasonMissingObject, initErr.reason)
	require.Nil(t, stderrors.Unwrap(err))
	var exposed *duckdb.Error
	require.False(t, stderrors.As(err, &exposed))
	require.True(t, stderrors.Is(err, rawMissing))
	require.True(t, stderrors.Is(err, rawNetwork))
	for _, format := range []string{"%v", "%+v", "%#v", "%q"} {
		formatted := fmt.Sprintf(format, err)
		require.NotContains(t, formatted, metadata)
		require.NotContains(t, formatted, endpoint)
		require.NotContains(t, formatted, secret)
		require.NotContains(t, formatted, rawMissing.Msg)
	}
}

func TestDuckLakeInitErrorJoinedTypedCausesUseStableSpecificity(t *testing.T) {
	rawMissing := &duckdb.Error{
		Type: duckdb.ErrorTypeHTTP,
		Msg:  "HTTP Error: 404 Not Found metadata=s3://private/catalog.ducklake",
	}
	rawNetwork := &duckdb.Error{
		Type: duckdb.ErrorTypeNetwork,
		Msg:  "Network Error: dial tcp https://private-storage.example",
	}
	for _, tc := range []struct {
		name  string
		cause error
	}{
		{name: "network then missing", cause: stderrors.Join(rawNetwork, rawMissing)},
		{name: "missing then network", cause: stderrors.Join(rawMissing, rawNetwork)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := newDuckLakeInitError(duckLakeStageAttach, "", tc.cause)
			var initErr *duckLakeInitError
			require.ErrorAs(t, err, &initErr)
			require.Equal(t, duckLakeReasonMissingObject, initErr.reason)
			require.True(t, stderrors.Is(err, rawMissing))
			require.True(t, stderrors.Is(err, rawNetwork))
			require.Nil(t, stderrors.Unwrap(err))
			require.NotContains(t, err.Error(), rawMissing.Msg)
			require.NotContains(t, err.Error(), rawNetwork.Msg)
		})
	}
}

func TestDuckLakeInitErrorSanitizesStageAndExtensionContext(t *testing.T) {
	tests := []struct {
		name      string
		stage     duckLakeInitStage
		extension string
		want      string
	}{
		{
			name:      "unknown load extension is omitted",
			stage:     duckLakeStageLoad,
			extension: "httpfs'; SECRET='leaked",
			want:      "load duckdb extension failed (stage=load reason=driver_error)",
		},
		{
			name:      "non-load extension is omitted",
			stage:     duckLakeStageCreateSecret,
			extension: "ducklake",
			want:      "create ducklake service secret failed (stage=create_secret reason=driver_error)",
		},
		{
			name:      "unknown stage is fixed",
			stage:     duckLakeInitStage("attach s3://private/path SECRET=leaked"),
			extension: "ducklake",
			want:      "ducklake initialization failed (stage=unknown reason=driver_error)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newDuckLakeInitError(tt.stage, tt.extension, stderrors.New("opaque driver detail SECRET=private"))
			require.Equal(t, tt.want, err.Error())
		})
	}
}

func TestDuckLakeFailureReasonPrecedenceAndGenericCoverage(t *testing.T) {
	tests := []struct {
		name    string
		stage   duckLakeInitStage
		message string
		want    duckLakeFailureReason
	}{
		{name: "permission precedes http", stage: duckLakeStageAttach, message: "HTTP 403 forbidden", want: duckLakeReasonPermissionDenied},
		{name: "tls precedes timeout", stage: duckLakeStageAttach, message: "TLS handshake timed out", want: duckLakeReasonTLSFailure},
		{name: "eof precedes network", stage: duckLakeStageAttach, message: "network stream ended with unexpected EOF", want: duckLakeReasonUnexpectedEOF},
		{name: "timeout precedes network", stage: duckLakeStageAttach, message: "network request timed out", want: duckLakeReasonTimeout},
		{name: "network precedes http", stage: duckLakeStageAttach, message: "HTTP connection refused", want: duckLakeReasonNetworkFailure},
		{name: "http request", stage: duckLakeStageAttach, message: "HTTP request returned status 503", want: duckLakeReasonHTTPFailure},
		{name: "extension precedes unavailable", stage: duckLakeStageLoad, message: "ducklake extension httpfs is unavailable", want: duckLakeReasonExtensionUnavailable},
		{name: "resource exhaustion precedes io", stage: duckLakeStageAttach, message: "I/O error: no space left on device", want: duckLakeReasonResourceExhaustion},
		{name: "configuration precedes missing", stage: duckLakeStageAttach, message: "configuration error: missing secret", want: duckLakeReasonInvalidConfiguration},
		{name: "resource unavailable precedes io", stage: duckLakeStageAttach, message: "I/O error: no such file or directory", want: duckLakeReasonResourceUnavailable},
		{name: "read-only filesystem io", stage: duckLakeStageAttach, message: "read-only file system", want: duckLakeReasonIOFailure},
		{name: "resource temporarily unavailable", stage: duckLakeStageAttach, message: "resource temporarily unavailable", want: duckLakeReasonResourceUnavailable},
		{name: "interrupted", stage: duckLakeStageAttach, message: "operation canceled by caller", want: duckLakeReasonInterrupted},
		{name: "opaque fallback", stage: duckLakeStageAttach, message: "driver exploded at /private/path SECRET=private", want: duckLakeReasonDriverError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newDuckLakeInitError(tt.stage, "", stderrors.New(tt.message))
			require.Contains(t, err.Error(), "reason="+string(tt.want))
		})
	}
}

func TestDuckLakeFailureReasonDuckDBTypeFallbacks(t *testing.T) {
	tests := []struct {
		name string
		typ  duckdb.ErrorType
		msg  string
		want duckLakeFailureReason
	}{
		{name: "settings is configuration", typ: duckdb.ErrorTypeSettings, msg: "Settings Error: invalid setting", want: duckLakeReasonInvalidConfiguration},
		{name: "dependency remains opaque", typ: duckdb.ErrorTypeDependency, msg: "Dependency Error: internal dependency", want: duckLakeReasonDriverError},
		{name: "fatal remains opaque", typ: duckdb.ErrorTypeFatal, msg: "FATAL Error: internal failure", want: duckLakeReasonDriverError},
		{name: "internal remains opaque", typ: duckdb.ErrorTypeInternal, msg: "INTERNAL Error: internal failure", want: duckLakeReasonDriverError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newDuckLakeInitError(duckLakeStageAttach, "", &duckdb.Error{Type: tt.typ, Msg: tt.msg})
			require.Contains(t, err.Error(), "reason="+string(tt.want))
		})
	}
}

func TestDuckLakeInitErrorHandlesTypedNilDuckDBError(t *testing.T) {
	var raw *duckdb.Error
	var cause error = raw
	var err error
	require.NotPanics(t, func() {
		err = newDuckLakeInitError(duckLakeStageAttach, "", cause)
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "reason=driver_error")
	var exposed *duckdb.Error
	require.False(t, stderrors.As(err, &exposed))
	require.NotPanics(t, func() {
		require.False(t, stderrors.Is(err, context.Canceled))
	})
	var nilTarget *duckdb.Error
	var target error = nilTarget
	require.NotPanics(t, func() {
		require.False(t, stderrors.Is(err, target))
	})
}

type scriptedDuckLakeExecer struct {
	queries []string
	errors  []error
}

func (e *scriptedDuckLakeExecer) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	index := len(e.queries)
	e.queries = append(e.queries, query)
	if index < len(e.errors) && e.errors[index] != nil {
		return nil, e.errors[index]
	}
	return driver.RowsAffected(0), nil
}

func newScriptedDuckLakeRuntime() *duckLakeRuntime {
	return &duckLakeRuntime{
		config: configuration.DuckLakeConfig{
			ExtensionDir: "/opt/myduck/extensions",
			MetadataPath: "/var/lib/myduck/catalog.ducklake",
			DataPath:     "s3://test-bucket/data",
			S3: configuration.DuckLakeS3Config{
				Endpoint: "https://storage.example",
				Region:   "us-east-1",
				UseSSL:   true,
			},
		},
		manifest: []ExtensionArtifact{
			{Name: "httpfs", FileName: "httpfs.duckdb_extension"},
			{Name: "ducklake", FileName: "ducklake.duckdb_extension"},
		},
	}
}

func TestDuckLakeInitSQLStopsAtFirstFailedStage(t *testing.T) {
	tests := []struct {
		name      string
		failAt    int
		cause     error
		stage     duckLakeInitStage
		reason    duckLakeFailureReason
		queryWant int
	}{
		{
			name:      "first load",
			failAt:    0,
			cause:     &duckdb.Error{Type: duckdb.ErrorTypeIO, Msg: "I/O Error: /private/httpfs.duckdb_extension"},
			stage:     duckLakeStageLoad,
			reason:    duckLakeReasonIOFailure,
			queryWant: 1,
		},
		{
			name:      "second load",
			failAt:    1,
			cause:     &duckdb.Error{Type: duckdb.ErrorTypeIO, Msg: "I/O Error: /private/ducklake.duckdb_extension"},
			stage:     duckLakeStageLoad,
			reason:    duckLakeReasonIOFailure,
			queryWant: 2,
		},
		{
			name:      "create secret",
			failAt:    2,
			cause:     &duckdb.Error{Type: duckdb.ErrorTypePermission, Msg: "Permission Error: SECRET=private"},
			stage:     duckLakeStageCreateSecret,
			reason:    duckLakeReasonPermissionDenied,
			queryWant: 3,
		},
		{
			name:      "attach",
			failAt:    3,
			cause:     &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: "HTTP Error: https://private.example/catalog"},
			stage:     duckLakeStageAttach,
			reason:    duckLakeReasonHTTPFailure,
			queryWant: 4,
		},
		{
			name:      "all stages succeed",
			failAt:    -1,
			queryWant: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runtime := newScriptedDuckLakeRuntime()
			execer := &scriptedDuckLakeExecer{}
			if tt.failAt >= 0 {
				execer.errors = make([]error, 5)
				execer.errors[tt.failAt] = tt.cause
			}

			runtime.initializeMu.Lock()
			err := runtime.initializeSQLLocked(context.Background(), nil, execer)
			runtime.initializeMu.Unlock()

			if tt.failAt < 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), "stage="+string(tt.stage))
				require.Contains(t, err.Error(), "reason="+string(tt.reason))
			}
			require.Len(t, execer.queries, tt.queryWant)
			for index, query := range execer.queries {
				switch {
				case index < 2:
					require.True(t, strings.HasPrefix(query, "LOAD '"))
				case index == 2:
					require.True(t, strings.HasPrefix(query, "CREATE OR REPLACE SECRET "))
				case index == 3:
					require.True(t, strings.HasPrefix(query, "ATTACH IF NOT EXISTS "))
				}
			}
		})
	}
}
