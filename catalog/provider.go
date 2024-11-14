package catalog

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	stdsql "database/sql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/marcboeker/go-duckdb"
	_ "github.com/marcboeker/go-duckdb"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/configuration"
)

type DatabaseProvider struct {
	mu                        *sync.RWMutex
	connector                 *duckdb.Connector
	storage                   *stdsql.DB
	catalogName               string
	dataDir                   string
	externalProcedureRegistry sql.ExternalStoredProcedureRegistry
}

var _ sql.DatabaseProvider = (*DatabaseProvider)(nil)
var _ sql.MutableDatabaseProvider = (*DatabaseProvider)(nil)
var _ sql.ExternalStoredProcedureProvider = (*DatabaseProvider)(nil)
var _ configuration.DataDirProvider = (*DatabaseProvider)(nil)

func NewInMemoryDBProvider() *DatabaseProvider {
	prov, err := NewDBProvider(".", "")
	if err != nil {
		panic(err)
	}
	return prov
}

func NewDBProvider(dataDir, dbFile string) (*DatabaseProvider, error) {
	dbFile = strings.TrimSpace(dbFile)
	name := ""
	dsn := ""
	if dbFile == "" {
		// in-memory mode, mainly for testing
		name = "memory"
	} else {
		name = strings.Split(dbFile, ".")[0]
		dsn = filepath.Join(dataDir, dbFile)
	}

	connector, err := duckdb.NewConnector(dsn, nil)
	if err != nil {
		return nil, err
	}

	storage := stdsql.OpenDB(connector)

	bootQueries := []string{
		"INSTALL arrow",
		"LOAD arrow",
	}
	for _, q := range bootQueries {
		if _, err := storage.ExecContext(context.Background(), q); err != nil {
			storage.Close()
			connector.Close()
			return nil, fmt.Errorf("failed to execute boot query %q: %w", q, err)
		}
	}

	for _, t := range internalTables {
		if _, err := storage.ExecContext(
			context.Background(),
			"CREATE SCHEMA IF NOT EXISTS "+t.Schema,
		); err != nil {
			return nil, fmt.Errorf("failed to create internal schema %q: %w", t.Schema, err)
		}
		if _, err := storage.ExecContext(
			context.Background(),
			"CREATE TABLE IF NOT EXISTS "+t.QualifiedName()+"("+t.DDL+")",
		); err != nil {
			return nil, fmt.Errorf("failed to create internal table %q: %w", t.Name, err)
		}
		for _, row := range t.InitialData {
			if _, err := storage.ExecContext(
				context.Background(),
				t.UpsertStmt(),
				row...,
			); err != nil {
				return nil, fmt.Errorf("failed to insert initial data into internal table %q: %w", t.Name, err)
			}
		}
	}

	return &DatabaseProvider{
		mu:                        &sync.RWMutex{},
		connector:                 connector,
		storage:                   storage,
		catalogName:               name,
		dataDir:                   dataDir,
		externalProcedureRegistry: sql.NewExternalStoredProcedureRegistry(), // This has no effect, just to satisfy the upper layer interface
	}, nil
}

func (prov *DatabaseProvider) Close() error {
	defer prov.connector.Close()
	return prov.storage.Close()
}

func (prov *DatabaseProvider) Connector() *duckdb.Connector {
	return prov.connector
}

func (prov *DatabaseProvider) Storage() *stdsql.DB {
	return prov.storage
}

func (prov *DatabaseProvider) CatalogName() string {
	return prov.catalogName
}

func (prov *DatabaseProvider) DataDir() string {
	return prov.dataDir
}

// ExternalStoredProcedure implements sql.ExternalStoredProcedureProvider.
func (prov *DatabaseProvider) ExternalStoredProcedure(ctx *sql.Context, name string, numOfParams int) (*sql.ExternalStoredProcedureDetails, error) {
	return prov.externalProcedureRegistry.LookupByNameAndParamCount(name, numOfParams)
}

// ExternalStoredProcedures implements sql.ExternalStoredProcedureProvider.
func (prov *DatabaseProvider) ExternalStoredProcedures(ctx *sql.Context, name string) ([]sql.ExternalStoredProcedureDetails, error) {
	return prov.externalProcedureRegistry.LookupByName(name)
}

// AllDatabases implements sql.DatabaseProvider.
func (prov *DatabaseProvider) AllDatabases(ctx *sql.Context) []sql.Database {
	prov.mu.RLock()
	defer prov.mu.RUnlock()

	rows, err := adapter.QueryCatalog(ctx, "SELECT DISTINCT schema_name FROM information_schema.schemata WHERE catalog_name = ?", prov.catalogName)
	if err != nil {
		panic(ErrDuckDB.New(err))
	}
	defer rows.Close()

	all := []sql.Database{}
	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			panic(ErrDuckDB.New(err))
		}

		switch schemaName {
		case "information_schema", "pg_catalog", "__sys__":
			continue
		}

		all = append(all, NewDatabase(schemaName, prov.catalogName))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name() < all[j].Name()
	})

	return all
}

// Database implements sql.DatabaseProvider.
func (prov *DatabaseProvider) Database(ctx *sql.Context, name string) (sql.Database, error) {
	prov.mu.RLock()
	defer prov.mu.RUnlock()

	ok, err := hasDatabase(ctx, prov.catalogName, name)
	if err != nil {
		return nil, err
	}

	if ok {
		return NewDatabase(name, prov.catalogName), nil
	}
	return nil, sql.ErrDatabaseNotFound.New(name)
}

// HasDatabase implements sql.DatabaseProvider.
func (prov *DatabaseProvider) HasDatabase(ctx *sql.Context, name string) bool {
	prov.mu.RLock()
	defer prov.mu.RUnlock()

	ok, err := hasDatabase(ctx, prov.catalogName, name)
	if err != nil {
		panic(err)
	}

	return ok
}

func hasDatabase(ctx *sql.Context, catalog string, name string) (bool, error) {
	rows, err := adapter.QueryCatalog(ctx, "SELECT DISTINCT schema_name FROM information_schema.schemata WHERE catalog_name = ? AND schema_name ILIKE ?", catalog, name)
	if err != nil {
		return false, ErrDuckDB.New(err)
	}
	defer rows.Close()
	return rows.Next(), nil
}

// CreateDatabase implements sql.MutableDatabaseProvider.
func (prov *DatabaseProvider) CreateDatabase(ctx *sql.Context, name string) error {
	prov.mu.Lock()
	defer prov.mu.Unlock()

	_, err := adapter.ExecCatalog(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, FullSchemaName(prov.catalogName, name)))
	if err != nil {
		return ErrDuckDB.New(err)
	}

	return nil
}

// DropDatabase implements sql.MutableDatabaseProvider.
func (prov *DatabaseProvider) DropDatabase(ctx *sql.Context, name string) error {
	prov.mu.Lock()
	defer prov.mu.Unlock()

	_, err := adapter.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, FullSchemaName(prov.catalogName, name)))
	if err != nil {
		return ErrDuckDB.New(err)
	}

	return nil
}
