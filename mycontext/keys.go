package mycontext

import "context"

type QueryOriginKey struct{}

type QueryOriginKind uint8

const (
	FrontendQueryOrigin QueryOriginKind = iota
	InternalQueryOrigin
	MySQLReplicationQueryOrigin
	PostgresReplicationQueryOrigin
)

var queryOriginKey = QueryOriginKey{}

func WithQueryOrigin(ctx context.Context, kind QueryOriginKind) context.Context {
	return context.WithValue(ctx, queryOriginKey, kind)
}

func QueryOrigin(ctx context.Context) QueryOriginKind {
	if kind, ok := ctx.Value(queryOriginKey).(QueryOriginKind); ok {
		return kind
	}
	return FrontendQueryOrigin
}

func IsReplicationQuery(ctx context.Context) bool {
	switch QueryOrigin(ctx) {
	case MySQLReplicationQueryOrigin, PostgresReplicationQueryOrigin:
		return true
	default:
		return false
	}
}
