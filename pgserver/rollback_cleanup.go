package pgserver

import (
	"context"
	stdsql "database/sql"
	"fmt"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"
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

// postgresRollbackCleanupReservation is implemented by the production
// provider. It is intentionally optional so the narrow provider probes used by
// unit tests do not need to model the provider's transaction barrier.
type postgresRollbackCleanupReservation interface {
	BeginDuckLakeRollbackCleanup(*stdsql.Tx) func()
}

type postgresRollbackCleanupReservationWithError interface {
	BeginDuckLakeRollbackCleanupWithError(*stdsql.Tx) (func(), error)
}

type postgresRollbackCleanupOwnerReservation interface {
	BeginDuckLakeRollbackCleanupForOwner(*stdsql.Tx, any) func()
}

type postgresRollbackCleanupOwnerReservationWithError interface {
	BeginDuckLakeRollbackCleanupForOwnerWithError(*stdsql.Tx, any) (func(), error)
}

func beginPostgresRollbackCleanupReservation(
	ctx *sql.Context,
	provider postgresRollbackOrphanCleaner,
	target *stdsql.Tx,
) func() {
	release, _ := beginPostgresRollbackCleanupReservationWithError(ctx, provider, target)
	return release
}

func beginPostgresRollbackCleanupReservationWithError(
	ctx *sql.Context,
	provider postgresRollbackOrphanCleaner,
	target *stdsql.Tx,
) (func(), error) {
	if ctx == nil || provider == nil || mycontext.QueryOrigin(ctx) != mycontext.FrontendQueryOrigin {
		return func() {}, nil
	}
	if ownerGate, ownerOK := provider.(postgresRollbackCleanupOwnerReservationWithError); ownerOK {
		owner := any(target)
		if owner == nil {
			if handler := postgresConnectionHandler(ctx); handler != nil && handler.implicitTx != nil {
				owner = handler.implicitTx
			} else if ctx.GetTransaction() != nil {
				owner = ctx.GetTransaction()
			}
		}
		return ownerGate.BeginDuckLakeRollbackCleanupForOwnerWithError(target, owner)
	}
	if gate, ok := provider.(postgresRollbackCleanupReservationWithError); ok {
		return gate.BeginDuckLakeRollbackCleanupWithError(target)
	}
	gate, ok := provider.(postgresRollbackCleanupReservation)
	if ownerGate, ownerOK := provider.(postgresRollbackCleanupOwnerReservation); ownerOK {
		// PostgreSQL transaction control owns the raw physical *sql.Tx. Pass it as
		// both identities when present so operation leases admitted before the
		// snapshot can be exempted without weakening the anonymous/other-owner
		// barrier. A GMS-only scope has no physical target; use its logical marker
		// when the handler/context exposes one.
		owner := any(target)
		if owner == nil {
			if handler := postgresConnectionHandler(ctx); handler != nil && handler.implicitTx != nil {
				owner = handler.implicitTx
			} else if ctx.GetTransaction() != nil {
				owner = ctx.GetTransaction()
			}
		}
		return ownerGate.BeginDuckLakeRollbackCleanupForOwner(target, owner), nil
	}
	if !ok {
		return func() {}, nil
	}
	release := gate.BeginDuckLakeRollbackCleanup(target)
	if release == nil {
		return func() {}, nil
	}
	return release, nil
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
	connections ...*stdsql.Conn,
) error {
	if ctx == nil || provider == nil {
		return nil
	}
	// The public entry point is a frontend rollback, while finalizer callbacks
	// derive a service-owned maintenance context before invoking this helper.
	// Accept both explicit origins, but continue to reject unknown and
	// replication contexts so a replication rollback can never trigger lake
	// cleanup.
	origin := mycontext.QueryOrigin(ctx)
	if origin != mycontext.FrontendQueryOrigin && origin != mycontext.MaintenanceQueryOrigin {
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
	var conn *stdsql.Conn
	if len(connections) > 0 {
		conn = connections[0]
	}
	ownerConn := catalog.RollbackCleanupOwner(ctx)
	if ownerConn != nil {
		if conn != nil && ownerConn != conn {
			return fmt.Errorf("postgres rollback cleanup connection is not the rollback owner")
		}
		conn = ownerConn
	}
	var holder adapter.ConnectionHolder
	if ownerConn == nil {
		var ok bool
		holder, ok = ctx.Session.(adapter.ConnectionHolder)
		if !ok {
			return fmt.Errorf("postgres rollback cleanup connection unavailable: session has no connection holder")
		}
		if _, activeTx := adapter.TryGetTxnBindingForRelease(ctx); activeTx != nil {
			return fmt.Errorf("postgres rollback orphan cleanup requires an inactive transaction")
		}
	}

	// Keep the concrete sql.Context so provider-side transaction guards remain
	// effective, while replacing the request origin with an explicit
	// service-owned maintenance origin.
	cleanupBase := context.WithoutCancel(ctx)
	if origin == mycontext.FrontendQueryOrigin {
		cleanupBase = mycontext.WithQueryOrigin(cleanupBase, mycontext.MaintenanceQueryOrigin)
	}
	if conn != nil {
		cleanupBase = catalog.WithRollbackCleanupOwner(cleanupBase, conn)
	}
	cleanupCtx := ctx.WithContext(cleanupBase)
	if conn == nil {
		var err error
		// Acquire only after cancellation has been removed. A client disconnect
		// must not prevent the post-rollback maintenance operation from running.
		conn, err = holder.GetConn(cleanupCtx)
		if err != nil {
			return fmt.Errorf("postgres rollback cleanup connection unavailable: %w", err)
		}
	}
	// A replacement transaction may have been opened while the owner was being
	// finalized. Never issue maintenance SQL through its connection.
	if catalog.RollbackCleanupOwner(cleanupCtx) == nil {
		if _, activeTx := adapter.TryGetTxnBindingForRelease(cleanupCtx); activeTx != nil {
			return fmt.Errorf("postgres rollback orphan cleanup requires an inactive transaction")
		}
	}
	if err := provider.CleanupDuckLakeOrphansOnConn(cleanupCtx, conn); err != nil {
		return fmt.Errorf("postgres rollback orphan cleanup failed: %w", err)
	}
	return nil
}
