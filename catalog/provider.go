package catalog

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
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
	defaultTimeZone           string
	connector                 *duckdb.Connector
	storage                   *stdsql.DB
	pool                      *ConnectionPool // TODO(Noy): Merge into the provider
	catalogName               string          // database name in postgres
	dataDir                   string
	dbFile                    string
	dsn                       string
	externalProcedureRegistry sql.ExternalStoredProcedureRegistry
}

var _ sql.DatabaseProvider = (*DatabaseProvider)(nil)
var _ sql.MutableDatabaseProvider = (*DatabaseProvider)(nil)
var _ sql.ExternalStoredProcedureProvider = (*DatabaseProvider)(nil)
var _ configuration.DataDirProvider = (*DatabaseProvider)(nil)

const readOnlySuffix = "?access_mode=read_only"

func NewInMemoryDBProvider() *DatabaseProvider {
	prov, err := NewDBProvider("", ".", "")
	if err != nil {
		panic(err)
	}
	return prov
}

func NewDBProvider(defaultTimeZone, dataDir, dbFile string) (*DatabaseProvider, error) {
	prov := &DatabaseProvider{
		mu:                        &sync.RWMutex{},
		defaultTimeZone:           defaultTimeZone,
		externalProcedureRegistry: sql.NewExternalStoredProcedureRegistry(), // This has no effect, just to satisfy the upper layer interface
		dsn:                       "N/A",
	}
	err := prov.CreateCatalog(dataDir, dbFile)
	if err != nil {
		return nil, err
	}
	err = prov.SwitchCatalog(dataDir, dbFile)
	if err != nil {
		return nil, err
	}
	return prov, nil
}

func (prov *DatabaseProvider) DropCatalog(dataDir, dbFile string) error {
	dbFile = strings.TrimSpace(dbFile)
	dsn := ""
	if dbFile != "" {
		dsn = filepath.Join(dataDir, dbFile)
		// if this is the current catalog, return error
		if dsn == prov.dsn {
			return fmt.Errorf("cannot drop the current catalog")
		}
		// if file does not exist, return error
		_, err := os.Stat(dsn)
		if os.IsNotExist(err) {
			return fmt.Errorf("database file %s does not exist", dsn)
		}
		// delete the file
		err = os.Remove(dsn)
		if err != nil {
			return fmt.Errorf("failed to delete database file %s: %w", dsn, err)
		}
		return nil
	} else {
		return fmt.Errorf("cannot drop the in-memory catalog")
	}
}

func (prov *DatabaseProvider) CreateCatalog(dataDir, dbFile string) error {
	dbFile = strings.TrimSpace(dbFile)
	dsn := ""
	if dbFile != "" {
		dsn = filepath.Join(dataDir, dbFile)
		// if already exists, return error
		_, err := os.Stat(dsn)
		if err == nil {
			return fmt.Errorf("database file %s already exists", dsn)
		}
	}

	connector, err := duckdb.NewConnector(dsn, nil)
	if err != nil {
		return err
	}

	storage := stdsql.OpenDB(connector)

	bootQueries := []string{
		"INSTALL arrow",
		"LOAD arrow",
		"INSTALL icu",
		"LOAD icu",
		"INSTALL postgres_scanner",
		"LOAD postgres_scanner",
	}
	for _, q := range bootQueries {
		if _, err := storage.ExecContext(context.Background(), q); err != nil {
			storage.Close()
			connector.Close()
			return fmt.Errorf("failed to execute boot query %q: %w", q, err)
		}
	}

	for _, t := range internalSchemas {
		if _, err := storage.ExecContext(
			context.Background(),
			"CREATE SCHEMA IF NOT EXISTS "+t.Schema,
		); err != nil {
			return fmt.Errorf("failed to create internal schema %q: %w", t.Schema, err)
		}
	}

	for _, t := range internalTables {
		if _, err := storage.ExecContext(
			context.Background(),
			"CREATE SCHEMA IF NOT EXISTS "+t.Schema,
		); err != nil {
			return fmt.Errorf("failed to create internal schema %q: %w", t.Schema, err)
		}
		if _, err := storage.ExecContext(
			context.Background(),
			"CREATE TABLE IF NOT EXISTS "+t.QualifiedName()+"("+t.DDL+")",
		); err != nil {
			return fmt.Errorf("failed to create internal table %q: %w", t.Name, err)
		}
		for _, row := range t.InitialData {
			if _, err := storage.ExecContext(
				context.Background(),
				t.UpsertStmt(),
				row...,
			); err != nil {
				return fmt.Errorf("failed to insert initial data into internal table %q: %w", t.Name, err)
			}
		}
	}
	return nil
}

func (prov *DatabaseProvider) SwitchCatalog(dataDir, dbFile string) error {
	dbFile = strings.TrimSpace(dbFile)
	name := ""
	dsn := ""
	if dbFile == "" || dbFile == "memory" {
		// in-memory mode, mainly for testing
		name = "memory"
	} else {
		name = strings.Split(dbFile, ".")[0]
		dsn = filepath.Join(dataDir, dbFile)
		// if file does not exist, return error
		_, err := os.Stat(dsn)
		if os.IsNotExist(err) {
			return fmt.Errorf("database file %s does not exist", dsn)
		}
	}
	if dsn == prov.dsn {
		return nil
	}

	connector, err := duckdb.NewConnector(dsn, nil)
	if err != nil {
		return err
	}

	storage := stdsql.OpenDB(connector)

	prov.mu.Lock()
	defer prov.mu.Unlock()

	prov.connector = connector
	prov.storage = storage
	prov.catalogName = name
	prov.dataDir = dataDir
	prov.dbFile = dbFile
	prov.dsn = dsn

	prov.pool = NewConnectionPool(name, connector, storage)
	if _, err := prov.pool.ExecContext(context.Background(), "PRAGMA enable_checkpoint_on_shutdown"); err != nil {
		logrus.WithError(err).Fatalln("Failed to enable checkpoint on shutdown")
	}

	if prov.defaultTimeZone != "" {
		_, err := prov.pool.ExecContext(context.Background(), fmt.Sprintf(`SET TimeZone = '%s'`, prov.defaultTimeZone))
		if err != nil {
			logrus.WithError(err).Fatalln("Failed to set the default time zone")
		}
	}

	// Postgres tables are created in the `public` schema by default.
	// Create the `public` schema if it doesn't exist.
	_, err = prov.pool.ExecContext(context.Background(), "CREATE SCHEMA IF NOT EXISTS public")
	if err != nil {
		logrus.WithError(err).Fatalln("Failed to create the `public` schema")
	}
	return nil
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

func (prov *DatabaseProvider) Pool() *ConnectionPool {
	return prov.pool
}

func (prov *DatabaseProvider) CatalogName() string {
	return prov.catalogName
}

func (prov *DatabaseProvider) DataDir() string {
	return prov.dataDir
}

func (prov *DatabaseProvider) DbFile() string {
	return prov.dbFile
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
		case "information_schema", "pg_catalog", "__sys__", "mysql":
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

func (prov *DatabaseProvider) Restart(readOnly bool) error {
	prov.mu.Lock()
	defer prov.mu.Unlock()

	err := prov.Close()
	if err != nil {
		return err
	}

	dsn := prov.dsn
	if readOnly {
		dsn += readOnlySuffix
	}

	connector, err := duckdb.NewConnector(dsn, nil)
	if err != nil {
		return err
	}
	storage := stdsql.OpenDB(connector)
	prov.connector = connector
	prov.storage = storage

	return nil
}
