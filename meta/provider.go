package meta

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	stdsql "database/sql"

	"github.com/dolthub/go-mysql-server/sql"
	_ "github.com/marcboeker/go-duckdb"

	"github.com/apecloud/myduckserver/configuration"
)

type DbProvider struct {
	mu          *sync.RWMutex
	storage     *stdsql.DB
	catalogName string
	dataDir     string
}

var _ sql.DatabaseProvider = (*DbProvider)(nil)
var _ sql.MutableDatabaseProvider = (*DbProvider)(nil)
var _ configuration.DataDirProvider = (*DbProvider)(nil)

func NewInMemoryDBProvider() *DbProvider {
	prov, err := NewDBProvider(".", "")
	if err != nil {
		panic(err)
	}
	return prov
}

func NewDBProvider(dataDir, dbFile string) (*DbProvider, error) {
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
	return &DbProvider{
		mu:          &sync.RWMutex{},
		storage:     storage,
		catalogName: name,
		dataDir:     dataDir,
	}, nil
}

func (prov *DbProvider) Close() error {
	return prov.storage.Close()
}

func (prov *DbProvider) Storage() *stdsql.DB {
	return prov.storage
}

func (prov *DbProvider) CatalogName() string {
	return prov.catalogName
}

func (prov *DbProvider) DataDir() string {
	return prov.dataDir
}

// AllDatabases implements sql.DatabaseProvider.
func (prov *DbProvider) AllDatabases(ctx *sql.Context) []sql.Database {
	prov.mu.RLock()
	defer prov.mu.RUnlock()

	rows, err := prov.storage.Query("SELECT DISTINCT schema_name FROM information_schema.schemata WHERE catalog_name = ?", prov.catalogName)
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

		all = append(all, NewDatabase(schemaName, prov.storage, prov.catalogName))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name() < all[j].Name()
	})

	return all
}

// Database implements sql.DatabaseProvider.
func (prov *DbProvider) Database(ctx *sql.Context, name string) (sql.Database, error) {
	prov.mu.RLock()
	defer prov.mu.RUnlock()

	ok, err := hasDatabase(prov.storage, prov.catalogName, name)
	if err != nil {
		return nil, err
	}

	if ok {
		return NewDatabase(name, prov.storage, prov.catalogName), nil
	}
	return nil, sql.ErrDatabaseNotFound.New(name)
}

// HasDatabase implements sql.DatabaseProvider.
func (prov *DbProvider) HasDatabase(ctx *sql.Context, name string) bool {
	prov.mu.RLock()
	defer prov.mu.RUnlock()

	ok, err := hasDatabase(prov.storage, prov.catalogName, name)
	if err != nil {
		panic(err)
	}

	return ok
}

func hasDatabase(engine *stdsql.DB, dstName string, name string) (bool, error) {
	rows, err := engine.Query("SELECT DISTINCT schema_name FROM information_schema.schemata WHERE catalog_name = ? AND schema_name = ?", dstName, name)
	if err != nil {
		return false, ErrDuckDB.New(err)
	}
	defer rows.Close()
	return rows.Next(), nil
}

// CreateDatabase implements sql.MutableDatabaseProvider.
func (prov *DbProvider) CreateDatabase(ctx *sql.Context, name string) error {
	prov.mu.Lock()
	defer prov.mu.Unlock()

	_, err := prov.storage.Exec(fmt.Sprintf(`CREATE SCHEMA %s`, FullSchemaName(prov.catalogName, name)))
	if err != nil {
		return ErrDuckDB.New(err)
	}

	return nil
}

// DropDatabase implements sql.MutableDatabaseProvider.
func (prov *DbProvider) DropDatabase(ctx *sql.Context, name string) error {
	prov.mu.Lock()
	defer prov.mu.Unlock()

	_, err := prov.storage.Exec(fmt.Sprintf(`DROP SCHEMA %s CASCADE`, FullSchemaName(prov.catalogName, name)))
	if err != nil {
		return ErrDuckDB.New(err)
	}

	return nil
}
