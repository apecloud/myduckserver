package pgserver

import (
	"strings"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/backend"
	"github.com/dolthub/go-mysql-server/sql"
)

// rewritePostgresObjectRelations maps logical PostgreSQL table references to
// their service-owned DuckLake relations. The mapping is reread from durable
// shadow comments for every statement, so a restarted provider does not rely
// on process-local table state. CREATE/COMMENT statements intentionally remain
// on the logical shadow path because that is where GMS metadata is stored.
func (h *DuckHandler) rewritePostgresObjectRelations(ctx *sql.Context, query string) (string, error) {
	if h == nil || ctx == nil || query == "" {
		return query, nil
	}
	provider := h.GetCatalogProvider()
	if provider == nil || !provider.DuckLakeEnabled() || postgresShadowOnlyStatement(query) {
		return query, nil
	}
	catalogName := adapter.GetCurrentCatalog(ctx)
	if catalogName == "" {
		catalogName = provider.DefaultCatalogName()
	}
	mappings, err := provider.ObjectTables(ctx, catalogName)
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
	if changed {
		// Relation discovery is performed through the catalog connection, but
		// execution may happen on a different physical connection after a pool
		// reset. Ensure the same session connection has the DuckLake catalog
		// attached before the rewritten SQL is prepared or executed.
		if err := h.ensurePostgresObjectConnection(ctx); err != nil {
			return "", err
		}
	}
	return rewritten, nil
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
	conn, err := adapter.GetConn(ctx)
	if err != nil {
		return err
	}
	return provider.EnsureDuckLakeConnection(ctx, conn)
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
