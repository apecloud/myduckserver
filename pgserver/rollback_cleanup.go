package pgserver

import (
	"context"
	stdsql "database/sql"
	"fmt"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
)

// postgresRollbackOrphanCleaner is the small provider surface needed by the
// PostgreSQL protocol path. Keeping it narrow makes the rollback hook easy to
// exercise without constructing a full server in unit tests.
type postgresRollbackOrphanCleaner interface {
	DuckLakeEnabled() bool
	CleanupDuckLakeOrphansOnConn(context.Context, *stdsql.Conn) error
}

type postgresRollbackObjectStorageGate interface {
	DuckLakeObjectStorageEnabled() bool
}

// cleanupPostgresRollbackOrphans runs the product-owned DuckLake cleanup after
// a PostgreSQL ROLLBACK has completed. PostgreSQL transaction control is
// executed directly on the session connection, so it does not pass through
// backend.Session.Rollback (the MySQL cleanup hook). The same connection must
// be reused here to preserve its DuckLake attachment and session state.
func cleanupPostgresRollbackOrphans(
	ctx *sql.Context,
	statement tree.Statement,
	provider postgresRollbackOrphanCleaner,
) error {
	if ctx == nil || provider == nil || mycontext.QueryOrigin(ctx) != mycontext.FrontendQueryOrigin {
		return nil
	}
	if _, ok := statement.(*tree.RollbackTransaction); !ok {
		return nil
	}
	if !provider.DuckLakeEnabled() {
		return nil
	}
	gate, ok := provider.(postgresRollbackObjectStorageGate)
	if !ok || !gate.DuckLakeObjectStorageEnabled() {
		return nil
	}
	holder, ok := ctx.Session.(adapter.ConnectionHolder)
	if !ok {
		return fmt.Errorf("postgres rollback cleanup connection unavailable: session has no connection holder")
	}
	if holder.TryGetTxn() != nil {
		return fmt.Errorf("postgres rollback orphan cleanup requires an inactive transaction")
	}

	conn, err := holder.GetConn(ctx)
	if err != nil {
		return fmt.Errorf("postgres rollback cleanup connection unavailable: %w", err)
	}
	// Keep the concrete sql.Context so provider-side transaction guards remain
	// effective, while replacing the request origin with an explicit
	// service-owned maintenance origin.
	cleanupBase := context.WithoutCancel(ctx)
	cleanupBase = mycontext.WithQueryOrigin(cleanupBase, mycontext.MaintenanceQueryOrigin)
	cleanupCtx := ctx.WithContext(cleanupBase)
	if err := provider.CleanupDuckLakeOrphansOnConn(cleanupCtx, conn); err != nil {
		return fmt.Errorf("postgres rollback orphan cleanup failed: %w", err)
	}
	return nil
}
