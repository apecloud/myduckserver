package catalog

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	stdsql "database/sql"

	"github.com/dolthub/go-mysql-server/sql"
	_ "github.com/marcboeker/go-duckdb"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/configuration"
)

type DatabaseProvider struct {
	mu                        *sync.RWMutex
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

	storage, err := stdsql.Open("duckdb", dsn)
	if err != nil {
		return nil, err
	}

	// install the json extension
	_, err = storage.Exec("INSTALL json")
	if err != nil {
		return nil, err
	}
	// load the json extension
	_, err = storage.Exec("LOAD json")
	if err != nil {
		return nil, err
	}

	return &DatabaseProvider{
		mu:                        &sync.RWMutex{},
		storage:                   storage,
		catalogName:               name,
		dataDir:                   dataDir,
		externalProcedureRegistry: sql.NewExternalStoredProcedureRegistry(), // This has no effect, just to satisfy the upper layer interface
	}, nil
}

func (prov *DatabaseProvider) Close() error {
	return prov.storage.Close()
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

	rows, err := adapter.QueryCatalogContext(ctx, "SELECT DISTINCT schema_name FROM information_schema.schemata WHERE catalog_name = ?", prov.catalogName)
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
		case "information_schema", "main", "pg_catalog":
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
	rows, err := adapter.QueryCatalogContext(ctx, "SELECT DISTINCT schema_name FROM information_schema.schemata WHERE catalog_name = ? AND schema_name ILIKE ?", catalog, name)
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

	_, err := adapter.ExecCatalogContext(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, FullSchemaName(prov.catalogName, name)))
	if err != nil {
		return ErrDuckDB.New(err)
	}

	return nil
}

// DropDatabase implements sql.MutableDatabaseProvider.
func (prov *DatabaseProvider) DropDatabase(ctx *sql.Context, name string) error {
	prov.mu.Lock()
	defer prov.mu.Unlock()

	_, err := adapter.ExecContext(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, FullSchemaName(prov.catalogName, name)))
	if err != nil {
		return ErrDuckDB.New(err)
	}

	return nil
}
