package pgserver

import (
	stdsql "database/sql"
	"fmt"
	"strings"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
)

// rewritePostgresObjectRelations maps logical PostgreSQL table references to
// their service-owned DuckLake relations. The mapping is reread from durable
// shadow comments for every statement, so a restarted provider does not rely
// on process-local table state. CREATE/COMMENT statements intentionally remain
// on the logical shadow path because that is where GMS metadata is stored.
func (h *DuckHandler) rewritePostgresObjectRelations(ctx *sql.Context, query string) (string, error) {
	if postgresShadowOnlyStatement(query) {
		return query, nil
	}
	execer, conn, _, release, err := adapter.GetExecutionSnapshotWithLease(ctx)
	if err != nil {
		return "", err
	}
	defer release()
	return h.rewritePostgresObjectRelationsWithSnapshot(ctx, query, execer, conn)
}

// rewritePostgresObjectRelationsWithSnapshot resolves object metadata through
// the exact executor that will execute the returned SQL. It is used by the
// simple and extended PostgreSQL paths to close the metadata/execution TOCTOU
// window.
func (h *DuckHandler) rewritePostgresObjectRelationsWithSnapshot(
	ctx *sql.Context,
	query string,
	execer adapter.SQLExecutor,
	conn *stdsql.Conn,
) (string, error) {
	if h == nil || ctx == nil || query == "" {
		return query, nil
	}
	provider := h.GetCatalogProvider()
	if provider == nil || !provider.DuckLakeEnabled() || postgresShadowOnlyStatement(query) {
		return query, nil
	}
	// Resolve the namespace through the executor captured by the caller.
	// Calling adapter.GetCurrentCatalog here would re-enter ConnectionPool's
	// lifecycle lock; a pending shutdown writer can therefore deadlock a caller
	// that is already holding the retained execution lease. The executor query
	// observes the same transaction/connection snapshot as the eventual
	// statement.
	catalogName, _ := postgresCurrentNamespaceWithExecutor(ctx, execer, provider)
	if conn != nil {
		if err := h.ensurePostgresObjectConnectionOnSnapshot(ctx, execer, conn); err != nil {
			return "", err
		}
	}
	mappings, err := provider.ObjectTablesWithExecutor(ctx, execer, catalogName)
	if err != nil {
		return "", err
	}
	if len(mappings) == 0 {
		return query, nil
	}
	routes := make(map[string]string, len(mappings)*3)
	bare := make(map[string]string, len(mappings))
	ambiguous := make(map[string]bool)
	for _, mapping := range mappings {
		table := strings.ToLower(mapping.Table)
		schema := strings.ToLower(mapping.Schema)
		if schema != "" {
			routes[schema+"."+table] = mapping.PhysicalName
		}
		if previous, ok := bare[table]; ok && previous != mapping.PhysicalName {
			delete(bare, table)
			ambiguous[table] = true
		} else if !ambiguous[table] {
			bare[table] = mapping.PhysicalName
		}
	}
	for table, physical := range bare {
		routes[table] = physical
	}
	rewritten, changed := backend.RewriteSQLRelations(query, routes)
	_ = changed // retained for readability at call sites and future diagnostics
	return rewritten, nil
}

// postgresCurrentNamespaceWithExecutor returns the current catalog/schema from
// a previously captured SQL executor. It intentionally never consults the
// session holder, because callers may be retaining the pool lifecycle read
// lease while resolving object metadata.
func postgresCurrentNamespaceWithExecutor(
	ctx *sql.Context,
	execer adapter.SQLExecutor,
	provider *catalog.DatabaseProvider,
) (catalogName, schemaName string) {
	if provider != nil {
		catalogName = provider.DefaultCatalogName()
	}
	if ctx != nil {
		schemaName = ctx.GetCurrentDatabase()
	}
	if execer == nil {
		return catalogName, schemaName
	}
	var currentCatalog, currentSchema string
	if err := execer.QueryRowContext(ctx, "SELECT CURRENT_CATALOG, CURRENT_SCHEMA").Scan(&currentCatalog, &currentSchema); err == nil {
		if currentCatalog != "" {
			catalogName = currentCatalog
		}
		if currentSchema != "" {
			schemaName = currentSchema
		}
	}
	return catalogName, schemaName
}

// ensurePostgresObjectConnection initializes DuckLake on the session's
// execution connection. Object-table protocol helpers call this after they
// resolve a durable mapping; local-table paths remain untouched.
func (h *DuckHandler) ensurePostgresObjectConnection(ctx *sql.Context) error {
	if h == nil || ctx == nil {
		return nil
	}
	provider := h.GetCatalogProvider()
	if provider == nil || !provider.DuckLakeEnabled() {
		return nil
	}
	execer, conn, _, release, err := adapter.GetExecutionSnapshotWithLease(ctx)
	if err != nil {
		return err
	}
	defer release()
	return h.ensurePostgresObjectConnectionOnSnapshot(ctx, execer, conn)
}

// ensurePostgresObjectConnectionOnSnapshot initializes DuckLake using the
// executor/connection pair that selected the relation metadata. When the
// caller retains an execution lease, passing that executor is essential:
// falling back to a session binding lookup would try to acquire the pool's
// lifecycle read lock a second time while a shutdown writer is pending.
func (h *DuckHandler) ensurePostgresObjectConnectionOnSnapshot(
	ctx *sql.Context,
	execer adapter.SQLExecutor,
	conn *stdsql.Conn,
) error {
	if h == nil || ctx == nil || conn == nil {
		return nil
	}
	provider := h.GetCatalogProvider()
	if provider == nil || !provider.DuckLakeEnabled() {
		return nil
	}
	if execer != nil {
		return provider.EnsureDuckLakeConnectionWithExecutor(ctx, execer, conn)
	}
	return provider.EnsureDuckLakeConnection(ctx, conn)
}

// postgresCopySnapshotLease selects and retains the executor/physical owner
// for an asynchronous COPY exchange. Frontend DuckLake admission is acquired
// before the pool snapshot so rollback cleanup cannot overtake the accepted
// operation. The returned lease keeps those resources independent: callers
// release its snapshot half when the producer/loader closes, while the
// operation half is handed to the physical transaction finalizer when the
// session supports that lifecycle surface.
func postgresCopySnapshotLease(
	ctx *sql.Context,
	handler *DuckHandler,
	statement tree.Statement,
) (adapter.SQLExecutor, *stdsql.Conn, *stdsql.Tx, *postgresCopyLease, error) {
	if ctx == nil || ctx.Session == nil {
		return nil, nil, nil, nil, fmt.Errorf("COPY execution session is unavailable")
	}
	if _, ok := ctx.Session.(adapter.ConnectionHolder); !ok {
		return nil, nil, nil, nil, fmt.Errorf("COPY execution session has no connection holder")
	}

	// Capture the transaction owner before admitting the operation. PostgreSQL
	// opens its implicit/explicit physical transaction before COPY setup, and
	// passing that identity lets rollback cleanup exempt this operation while it
	// is finalizing. If a transaction appears in the tiny snapshot gap below,
	// the operation is reacquired with the newly observed owner.
	var initialTx *stdsql.Tx
	_, initialTx = adapter.TryGetTxnBinding(ctx)
	operationRelease := func() {}
	if handler != nil {
		var admissionErr error
		operationRelease, admissionErr = handler.beginPostgresDuckLakeOperationForOwnerWithError(ctx, statement, initialTx)
		if admissionErr != nil {
			return nil, nil, nil, nil, admissionErr
		}
	}
	execer, conn, tx, snapshotRelease, err := adapter.GetExecutionSnapshotWithLease(ctx)
	if err != nil {
		operationRelease()
		return nil, nil, tx, nil, err
	}
	if initialTx != tx {
		if initialTx != nil {
			// A transaction that disappeared or was replaced after admission is
			// not a safe target for an asynchronous COPY. Do not execute on the
			// replacement connection and do not leave either lease held.
			snapshotRelease()
			operationRelease()
			return nil, nil, tx, nil, fmt.Errorf("COPY transaction binding changed while opening")
		}
		// The operation was admitted anonymously before a concurrent transaction
		// became visible. Rebind it to the physical owner captured by the lease;
		// the active transaction itself prevents rollback cleanup from overtaking
		// this short reacquisition window.
		operationRelease()
		if handler != nil {
			var admissionErr error
			operationRelease, admissionErr = handler.beginPostgresDuckLakeOperationForOwnerWithError(ctx, statement, tx)
			if admissionErr != nil {
				snapshotRelease()
				return nil, nil, tx, nil, admissionErr
			}
		} else {
			operationRelease = func() {}
		}
	}
	lease := newPostgresCopyLease(snapshotRelease, operationRelease)
	if tx != nil {
		lease.registerPhysicalFinalization(ctx, tx)
	}
	return execer, conn, tx, lease, nil
}

// postgresCopySnapshot retains the historical snapshot-only helper for
// callers that do not span an asynchronous protocol exchange. New COPY
// producers/consumers use postgresCopySnapshotLease instead.
func postgresCopySnapshot(ctx *sql.Context) (adapter.SQLExecutor, *stdsql.Conn, error) {
	execer, conn, _, lease, err := postgresCopySnapshotLease(ctx, nil, nil)
	if lease != nil {
		lease.Release()
	}
	if err != nil {
		return nil, nil, err
	}
	return execer, conn, nil
}

// resolvePostgresCopyTarget resolves a validated GMS table to the identifier
// that DuckDB should receive. Object tables are routed to their durable
// DuckLake relation; local tables retain their catalog-qualified name. The
// caller must pass the executor/connection pair returned by
// postgresCopySnapshot so metadata and COPY execution share one snapshot.
func resolvePostgresCopyTarget(
	ctx *sql.Context,
	handler *DuckHandler,
	schema string,
	table sql.Table,
	execer adapter.SQLExecutor,
	conn *stdsql.Conn,
) (string, error) {
	if table == nil {
		return "", nil
	}
	if handler != nil {
		if provider := handler.GetCatalogProvider(); provider != nil {
			if physical, _, err := provider.PhysicalTableNameForTableWithExecutor(ctx, table, execer, conn); err != nil {
				return "", err
			} else if physical != "" {
				return physical, nil
			}
		}
	}
	if schema != "" {
		return catalog.ConnectIdentifiersANSI(schema, table.Name()), nil
	}
	return catalog.QuoteIdentifierANSI(table.Name()), nil
}

func postgresShadowOnlyStatement(query string) bool {
	trimmed := strings.TrimSpace(query)
	for len(trimmed) > 0 {
		switch {
		case strings.HasPrefix(trimmed, "--"):
			if idx := strings.IndexByte(trimmed, '\n'); idx >= 0 {
				trimmed = strings.TrimSpace(trimmed[idx+1:])
				continue
			}
			return true
		case strings.HasPrefix(trimmed, "/*"):
			if idx := strings.Index(trimmed[2:], "*/"); idx >= 0 {
				trimmed = strings.TrimSpace(trimmed[idx+4:])
				continue
			}
			return true
		}
		break
	}
	if trimmed == "" {
		return true
	}
	word := strings.ToLower(trimmed)
	for i, r := range word {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
			word = word[:i]
			break
		}
	}
	switch word {
	case "create", "comment", "show", "set", "begin", "commit", "rollback", "vacuum", "attach", "detach":
		return true
	default:
		return false
	}
}

// executePostgresObjectDDL keeps the durable local shadow table in lockstep
// with its DuckLake relation. The normal PostgreSQL handler executes SQL
// directly through database/sql and therefore bypasses GMS's catalog drop and
// alter callbacks. For an object table, execute the routed statement against
// the physical relation and then apply the same operation to the shadow using
// the exact executor snapshot supplied by the caller. Local-table statements
// return handled=false and retain their existing one-statement behavior.
//
// The helper deliberately limits the shadow statement for DROP TABLE to object
// names only. A mixed DROP containing local tables must not attempt to drop the
// local names a second time.
func (h *DuckHandler) executePostgresObjectDDL(
	ctx *sql.Context,
	query, routedQuery string,
	stmt tree.Statement,
	execer adapter.SQLExecutor,
	conn *stdsql.Conn,
	args ...any,
) (result stdsql.Result, handled bool, err error) {
	provider := h.GetCatalogProvider()
	if provider == nil || !provider.DuckLakeEnabled() || execer == nil {
		return nil, false, nil
	}
	currentCatalog, currentSchema := postgresCurrentNamespaceWithExecutor(ctx, execer, provider)
	// Resolve durable object mappings through the same executor as the DDL. A
	// GMS table lookup would call DatabaseProvider.Database and reacquire the
	// session lifecycle lock while this caller retains its execution lease.
	mappings, mappingErr := provider.ObjectTablesWithExecutor(ctx, execer, currentCatalog)
	if mappingErr != nil {
		return nil, false, mappingErr
	}

	resolve := func(name *tree.TableName) (sql.Table, bool, string, error) {
		if name == nil || name.Table() == "" {
			return nil, false, "", nil
		}
		schemaName := name.Schema()
		if schemaName == "" {
			schemaName = currentSchema
		}
		catalogName := name.Catalog()
		if catalogName == "" {
			catalogName = currentCatalog
		}
		for _, mapping := range mappings {
			if strings.EqualFold(mapping.Catalog, catalogName) &&
				strings.EqualFold(mapping.Schema, schemaName) &&
				strings.EqualFold(mapping.Table, name.Table()) {
				logical := catalog.FullTableName(catalogName, schemaName, mapping.Table)
				return nil, true, logical, nil
			}
		}
		return nil, false, "", nil
	}

	switch node := stmt.(type) {
	case *tree.DropTable:
		var shadows []string
		for i := range node.Names {
			name := &node.Names[i]
			_, object, logical, lookupErr := resolve(name)
			if lookupErr != nil {
				if node.IfExists && sql.ErrTableNotFound.Is(lookupErr) {
					continue
				}
				return nil, false, lookupErr
			}
			if object {
				shadows = append(shadows, logical)
			}
		}
		if len(shadows) == 0 {
			return nil, false, nil
		}
		result, err = execer.ExecContext(ctx, routedQuery, args...)
		if err != nil {
			return nil, true, err
		}
		var shadow strings.Builder
		shadow.WriteString("DROP TABLE ")
		if node.IfExists {
			shadow.WriteString("IF EXISTS ")
		}
		shadow.WriteString(strings.Join(shadows, ", "))
		if node.DropBehavior != tree.DropDefault {
			shadow.WriteByte(' ')
			shadow.WriteString(node.DropBehavior.String())
		}
		if _, err = execer.ExecContext(ctx, shadow.String()); err != nil && !node.IfExists {
			return result, true, err
		}
		return result, true, nil

	case *tree.AlterTable:
		if node.Table == nil {
			return nil, false, nil
		}
		name := node.Table.ToTableName()
		_, object, _, lookupErr := resolve(&name)
		if lookupErr != nil {
			if node.IfExists && sql.ErrTableNotFound.Is(lookupErr) {
				return nil, false, nil
			}
			return nil, false, lookupErr
		}
		if !object {
			return nil, false, nil
		}
		result, err = execer.ExecContext(ctx, routedQuery, args...)
		if err != nil {
			return nil, true, err
		}
		// The raw query still names the logical shadow relation. It is executed
		// only after the physical statement succeeds, and on the same executor.
		if _, err = execer.ExecContext(ctx, query, args...); err != nil {
			return result, true, err
		}
		return result, true, nil
	default:
		return nil, false, nil
	}
}
