package pgserver

import (
	"fmt"
	"strings"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
)

// postgresCreateTableStorage normalizes the table-level storage parameters
// while preserving their order (and therefore duplicate declarations). The
// Cockroach parser exposes expressions as String values; the catalog resolver
// validates that selector values are simple literals and contain no secrets or
// paths.
func postgresCreateTableStorage(stmt *tree.CreateTable) (catalog.TableStorageSelection, error) {
	if stmt == nil {
		return catalog.DefaultTableStorageSelection(), nil
	}
	options := make([]catalog.TableStorageOption, 0, len(stmt.StorageParams))
	for _, param := range stmt.StorageParams {
		var value any
		if param.Value != nil {
			value = param.Value.String()
		}
		options = append(options, catalog.TableStorageOption{
			Name:  param.Key.String(),
			Value: value,
		})
	}
	return catalog.ResolvePostgresStorageOptions(options)
}

// postgresCreateTableExecutionQuery returns a query that is safe to hand to
// DuckDB. PostgreSQL's custom WITH (myduck_storage=...) parameter is consumed
// by MyDuck and must not be forwarded as an unknown DuckDB storage option.
// Other PostgreSQL storage parameters remain in the formatted statement and
// retain the existing execution behavior.
func postgresCreateTableExecutionQuery(query string, stmt *tree.CreateTable) (string, catalog.TableStorageSelection, error) {
	selection, err := postgresCreateTableStorage(stmt)
	if err != nil {
		return "", catalog.TableStorageSelection{}, err
	}
	if stmt == nil || len(stmt.StorageParams) == 0 {
		return query, selection, nil
	}

	custom := false
	params := make(tree.StorageParams, 0, len(stmt.StorageParams))
	for _, param := range stmt.StorageParams {
		if strings.EqualFold(strings.TrimSpace(param.Key.String()), catalog.TableStorageOptionName) {
			custom = true
			continue
		}
		params = append(params, param)
	}
	if !custom {
		return query, selection, nil
	}

	copyStmt := *stmt
	if len(params) == 0 {
		copyStmt.StorageParams = nil
	} else {
		copyStmt.StorageParams = params
	}
	return tree.AsString(&copyStmt), selection, nil
}

// setPostgresCreateTableStorage validates and attaches the request-scoped
// selection used by the catalog boundary. Replication-origin DDL deliberately
// keeps its historical path and metadata behavior unchanged.
func setPostgresCreateTableStorage(ctx *sql.Context, stmt *tree.CreateTable) (catalog.TableStorageSelection, error) {
	selection, err := postgresCreateTableStorage(stmt)
	if err != nil {
		return catalog.TableStorageSelection{}, err
	}
	if err := catalog.SetTableStorageSelection(ctx, selection); err != nil {
		return catalog.TableStorageSelection{}, err
	}
	return selection, nil
}

// postgresCreateTableIdentity resolves the physical catalog coordinates used
// by the PostgreSQL protocol. Keep the fallback order in one place so the
// preflight and metadata persistence paths inspect the same table.
func postgresCreateTableIdentity(ctx *sql.Context, stmt *tree.CreateTable) (catalogName, schemaName, tableName string, err error) {
	if stmt == nil || stmt.Table.Table() == "" {
		return "", "", "", fmt.Errorf("cannot determine PostgreSQL table identity")
	}

	tableName = stmt.Table.Table()
	schemaName = stmt.Table.Schema()
	if schemaName == "" {
		schemaName = adapter.GetCurrentSchema(ctx)
	}
	if schemaName == "" {
		schemaName = ctx.GetCurrentDatabase()
	}
	if schemaName == "" {
		return "", "", "", fmt.Errorf("cannot determine PostgreSQL schema for table %q", tableName)
	}

	catalogName = stmt.Table.Catalog()
	if catalogName == "" {
		catalogName = adapter.GetCurrentCatalog(ctx)
	}
	if catalogName == "" {
		return "", "", "", fmt.Errorf("cannot determine PostgreSQL catalog for table %q", tableName)
	}
	return catalogName, schemaName, tableName, nil
}

// postgresCreateTableAlreadyExists performs the IF NOT EXISTS preflight before
// DuckDB executes the DDL. DuckDB reports a successful no-op for an existing
// table, and its DDL RowsAffected value cannot distinguish that case from a
// newly-created table. The caller uses this result to avoid rewriting the
// existing managed comment and storage selector.
func postgresCreateTableAlreadyExists(ctx *sql.Context, stmt *tree.CreateTable) (bool, error) {
	if stmt == nil || !stmt.IfNotExists || stmt.Persistence == tree.PersistenceTemporary || mycontext.IsReplicationQuery(ctx) {
		return false, nil
	}

	catalogName, schemaName, tableName, err := postgresCreateTableIdentity(ctx, stmt)
	if err != nil {
		return false, err
	}

	rows, err := adapter.QueryCatalog(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM duckdb_tables()
			WHERE database_name = ? AND schema_name = ? AND table_name = ?
		)
	`, catalogName, schemaName, tableName)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return false, err
		}
		return false, fmt.Errorf("catalog existence query returned no row for table %q", tableName)
	}

	var exists bool
	if err := rows.Scan(&exists); err != nil {
		return false, err
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return exists, nil
}

func persistPostgresCreateTableStorage(ctx *sql.Context, stmt *tree.CreateTable, selection catalog.TableStorageSelection) error {
	if stmt == nil || stmt.Persistence == tree.PersistenceTemporary || mycontext.IsReplicationQuery(ctx) {
		return nil
	}

	catalogName, schemaName, tableName, err := postgresCreateTableIdentity(ctx, stmt)
	if err != nil {
		return err
	}

	db := catalog.NewDatabase(schemaName, catalogName)
	return db.RecordTableStorageSelection(ctx, tableName, selection)
}

// postgresCreateTableQueryForPrepare sanitizes only CREATE TABLE statements;
// it is shared by simple and extended PostgreSQL protocol paths so a
// prepared DDL sees the same selector semantics as a simple query.
func postgresCreateTableQueryForPrepare(query string, parsed tree.Statement) (string, error) {
	stmt, ok := parsed.(*tree.CreateTable)
	if !ok {
		return query, nil
	}
	executionQuery, _, err := postgresCreateTableExecutionQuery(query, stmt)
	return executionQuery, err
}
