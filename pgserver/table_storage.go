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

func persistPostgresCreateTableStorage(ctx *sql.Context, stmt *tree.CreateTable, selection catalog.TableStorageSelection, provider *catalog.DatabaseProvider) error {
	if stmt == nil || stmt.Persistence == tree.PersistenceTemporary || mycontext.IsReplicationQuery(ctx) {
		return nil
	}

	schemaName := stmt.Table.Schema()
	if schemaName == "" {
		schemaName = adapter.GetCurrentSchema(ctx)
	}
	if schemaName == "" {
		schemaName = ctx.GetCurrentDatabase()
	}
	if schemaName == "" {
		return fmt.Errorf("cannot determine PostgreSQL schema for table %q", stmt.Table.Table())
	}
	catalogName := stmt.Table.Catalog()
	if catalogName == "" {
		catalogName = adapter.GetCurrentCatalog(ctx)
	}
	if catalogName == "" {
		return fmt.Errorf("cannot determine PostgreSQL catalog for table %q", stmt.Table.Table())
	}

	db := catalog.NewDatabaseWithProvider(schemaName, catalogName, provider)
	return db.RecordTableStorageSelection(ctx, stmt.Table.Table(), selection)
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
