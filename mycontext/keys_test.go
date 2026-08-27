package mycontext

import (
	"context"
	"testing"
)

func TestQueryOriginDefaultsAndEligibility(t *testing.T) {
	if got := QueryOrigin(nil); got != UnknownQueryOrigin {
		t.Fatalf("QueryOrigin(nil) = %d, want UnknownQueryOrigin", got)
	}
	if got := QueryOrigin(context.Background()); got != UnknownQueryOrigin {
		t.Fatalf("QueryOrigin(background) = %d, want UnknownQueryOrigin", got)
	}
	for _, kind := range []QueryOriginKind{FrontendQueryOrigin, MaintenanceQueryOrigin, RecoveryQueryOrigin} {
		if !IsDuckLakeEligibleQuery(WithQueryOrigin(context.Background(), kind)) {
			t.Fatalf("origin %d should be DuckLake eligible", kind)
		}
	}
	for _, kind := range []QueryOriginKind{UnknownQueryOrigin, InternalQueryOrigin, MySQLReplicationQueryOrigin, PostgresReplicationQueryOrigin} {
		if IsDuckLakeEligibleQuery(WithQueryOrigin(context.Background(), kind)) {
			t.Fatalf("origin %d should not be DuckLake eligible", kind)
		}
	}
}

func TestWithFrontendPreservesReplication(t *testing.T) {
	ctx := WithQueryOrigin(context.Background(), MySQLReplicationQueryOrigin)
	if got := QueryOrigin(WithFrontendQuery(ctx)); got != MySQLReplicationQueryOrigin {
		t.Fatalf("WithFrontendQuery changed replication origin to %d", got)
	}
	if got := QueryOrigin(WithFrontendQuery(nil)); got != FrontendQueryOrigin {
		t.Fatalf("WithFrontendQuery(nil) = %d, want FrontendQueryOrigin", got)
	}
}
