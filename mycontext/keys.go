package mycontext

import "context"

type QueryOriginKey struct{}

type QueryOriginKind uint8

const (
	// UnknownQueryOrigin is the zero value. Contexts must opt in to a service
	// origin before connection-scoped DuckLake settings may be applied.
	UnknownQueryOrigin QueryOriginKind = iota
	FrontendQueryOrigin
	InternalQueryOrigin
	MySQLReplicationQueryOrigin
	PostgresReplicationQueryOrigin
	// MaintenanceQueryOrigin identifies an explicitly service-owned
	// maintenance connection (checkpoint/backup/catalog work).
	MaintenanceQueryOrigin
	// RecoveryQueryOrigin identifies an explicitly service-owned recovery or
	// restart connection.
	RecoveryQueryOrigin
)

var queryOriginKey = QueryOriginKey{}

func WithQueryOrigin(ctx context.Context, kind QueryOriginKind) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, queryOriginKey, kind)
}

func QueryOrigin(ctx context.Context) QueryOriginKind {
	if ctx == nil {
		return UnknownQueryOrigin
	}
	if kind, ok := ctx.Value(queryOriginKey).(QueryOriginKind); ok {
		return kind
	}
	return UnknownQueryOrigin
}

func IsReplicationQuery(ctx context.Context) bool {
	switch QueryOrigin(ctx) {
	case MySQLReplicationQueryOrigin, PostgresReplicationQueryOrigin:
		return true
	default:
		return false
	}
}

// IsDuckLakeEligibleQuery reports whether a context was explicitly marked as
// an ordinary frontend, maintenance, or recovery operation. Unmarked and
// replication contexts deliberately return false so a shared database pool
// cannot accidentally inherit service S3 credentials.
func IsDuckLakeEligibleQuery(ctx context.Context) bool {
	switch QueryOrigin(ctx) {
	case FrontendQueryOrigin, MaintenanceQueryOrigin, RecoveryQueryOrigin:
		return true
	default:
		return false
	}
}

// WithFrontendQuery marks an ordinary protocol query without overwriting an
// already classified context (notably replication).
func WithFrontendQuery(ctx context.Context) context.Context {
	return withExplicitOrigin(ctx, FrontendQueryOrigin)
}

// WithMaintenanceQuery marks a service-owned maintenance operation.
func WithMaintenanceQuery(ctx context.Context) context.Context {
	return withExplicitOrigin(ctx, MaintenanceQueryOrigin)
}

// WithRecoveryQuery marks a service-owned restart/recovery operation.
func WithRecoveryQuery(ctx context.Context) context.Context {
	return withExplicitOrigin(ctx, RecoveryQueryOrigin)
}

func withExplicitOrigin(ctx context.Context, kind QueryOriginKind) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	origin := QueryOrigin(ctx)
	if origin != UnknownQueryOrigin {
		return ctx
	}
	return WithQueryOrigin(ctx, kind)
}
