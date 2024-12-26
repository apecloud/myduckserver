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
	pool                      *ConnectionPool
	catalogName               string // database name in postgres
	dataDir                   string
	dbFile                    string
	dsn                       string
	externalProcedureRegistry sql.ExternalStoredProcedureRegistry
	ready                     bool
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

func NewDBProvider(defaultTimeZone, dataDir, defaultDB string) (prov *DatabaseProvider, err error) {
	prov = &DatabaseProvider{
		mu:                        &sync.RWMutex{},
		defaultTimeZone:           defaultTimeZone,
		externalProcedureRegistry: sql.NewExternalStoredProcedureRegistry(), // This has no effect, just to satisfy the upper layer interface
		dataDir:                   dataDir,
	}

	shouldInit := true
	if defaultDB == "" || defaultDB == "memory" {
		prov.catalogName = "memory"
		prov.dbFile = ""
		prov.dsn = ""
	} else {
		prov.catalogName = defaultDB
		prov.dbFile = defaultDB + ".db"
		prov.dsn = filepath.Join(prov.dataDir, prov.dbFile)
		_, err = os.Stat(prov.dsn)
		shouldInit = os.IsNotExist(err)
	}

	prov.connector, err = duckdb.NewConnector(prov.dsn, nil)
	if err != nil {
		return nil, err
	}
	prov.storage = stdsql.OpenDB(prov.connector)
	prov.pool = NewConnectionPool(prov.catalogName, prov.connector, prov.storage)

	bootQueries := []string{
		"INSTALL arrow",
		"LOAD arrow",
		"INSTALL icu",
		"LOAD icu",
		"INSTALL postgres_scanner",
		"LOAD postgres_scanner",
	}

	for _, q := range bootQueries {
		if _, err := prov.storage.ExecContext(context.Background(), q); err != nil {
			prov.storage.Close()
			prov.connector.Close()
			return nil, fmt.Errorf("failed to execute boot query %q: %w", q, err)
		}
	}

	if shouldInit {
		err = prov.initCatalog()
		if err != nil {
			return nil, err
		}
	}

	err = prov.attachCatalogs()
	if err != nil {
		return nil, err
	}

	prov.ready = true
	return prov, nil
}

func (prov *DatabaseProvider) initCatalog() error {

	for _, t := range internalSchemas {
		if _, err := prov.storage.ExecContext(
			context.Background(),
			"CREATE SCHEMA IF NOT EXISTS "+t.Schema,
		); err != nil {
			return fmt.Errorf("failed to create internal schema %q: %w", t.Schema, err)
		}
	}

	for _, t := range internalTables {
		if _, err := prov.storage.ExecContext(
			context.Background(),
			"CREATE SCHEMA IF NOT EXISTS "+t.Schema,
		); err != nil {
			return fmt.Errorf("failed to create internal schema %q: %w", t.Schema, err)
		}
		if _, err := prov.storage.ExecContext(
			context.Background(),
			"CREATE TABLE IF NOT EXISTS "+t.QualifiedName()+"("+t.DDL+")",
		); err != nil {
			return fmt.Errorf("failed to create internal table %q: %w", t.Name, err)
		}
		for _, row := range t.InitialData {
			if _, err := prov.storage.ExecContext(
				context.Background(),
				t.UpsertStmt(),
				row...,
			); err != nil {
				return fmt.Errorf("failed to insert initial data into internal table %q: %w", t.Name, err)
			}
		}
	}

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
	_, err := prov.pool.ExecContext(context.Background(), "CREATE SCHEMA IF NOT EXISTS public")
	if err != nil {
		logrus.WithError(err).Fatalln("Failed to create the `public` schema")
	}
	return nil
}

func (prov *DatabaseProvider) IsReady() bool {
	return prov.ready
}

func (prov *DatabaseProvider) HasCatalog(name string) bool {
	name = strings.TrimSpace(name)
	// in memory database does not need to be created
	if name == "" || name == "memory" {
		return true
	}

	dsn := filepath.Join(prov.dataDir, name+".db")
	// if already exists, return error
	_, err := os.Stat(dsn)
	return os.IsExist(err)
}

// attachCatalogs attaches all the databases in the data directory
func (prov *DatabaseProvider) attachCatalogs() error {
	files, err := os.ReadDir(prov.dataDir)
	if err != nil {
		return fmt.Errorf("failed to read data directory: %w", err)
	}
	for _, file := range files {
		err := prov.AttachCatalog(file, true)
		if err != nil {
			logrus.Error(err)
		}
	}
	return nil
}

func (prov *DatabaseProvider) AttachCatalog(file interface {
	IsDir() bool
	Name() string
}, ignoreNonDB bool) error {
	if file.IsDir() {
		if ignoreNonDB {
			return nil
		}
		return fmt.Errorf("file %s is a directory", file.Name())
	}
	if !strings.HasSuffix(file.Name(), ".db") {
		if ignoreNonDB {
			return nil
		}
		return fmt.Errorf("file %s is not a database file", file.Name())
	}
	name := strings.TrimSuffix(file.Name(), ".db")
	if _, err := prov.storage.ExecContext(context.Background(), "ATTACH IF NOT EXISTS '"+filepath.Join(prov.dataDir, file.Name())+"' AS "+name); err != nil {
		return fmt.Errorf("failed to attach database %s: %w", name, err)
	}
	return nil
}

func (prov *DatabaseProvider) CreateCatalog(name string, ifNotExists bool) error {
	name = strings.TrimSpace(name)
	// in memory database does not need to be created
	if name == "" || name == "memory" {
		return nil
	}
	dsn := filepath.Join(prov.dataDir, name+".db")

	_, err := os.Stat(dsn)
	shouldInit := os.IsNotExist(err)

	// attach
	attachSQL := "ATTACH"
	if ifNotExists {
		attachSQL += " IF NOT EXISTS"
	}
	attachSQL += " '" + dsn + "' AS " + name
	_, err = prov.storage.ExecContext(context.Background(), attachSQL)
	if err != nil {
		return err
	}

	if shouldInit {
		res, err := prov.storage.QueryContext(context.Background(), "SELECT current_catalog")
		if err != nil {
			return fmt.Errorf("failed to init catalog: %w", err)
		}
		lastCatalog := ""
		for res.Next() {
			if err := res.Scan(&lastCatalog); err != nil {
				return fmt.Errorf("failed to init catalog: %w", err)
			}
		}

		if _, err := prov.storage.ExecContext(context.Background(), "USE "+name); err != nil {
			return fmt.Errorf("failed to switch to the new catalog: %w", err)
		}

		defer func() {
			if _, err := prov.storage.ExecContext(context.Background(), "USE "+lastCatalog); err != nil {
				logrus.WithError(err).Errorln("Failed to switch back to the old catalog")
			}
		}()
		err = prov.initCatalog()
		if err != nil {
			return err
		}
	}
	return nil
}

func (prov *DatabaseProvider) DropCatalog(name string, ifExists bool) error {
	name = strings.TrimSpace(name)
	// in memory database does not need to be created
	if name == "" || name == "memory" {
		return fmt.Errorf("cannot drop the in-memory catalog")
	}
	dsn := filepath.Join(prov.dataDir, name+".db")
	// if file does not exist, return error
	_, err := os.Stat(dsn)
	if os.IsNotExist(err) {
		if ifExists {
			return nil
		}
		return fmt.Errorf("database file %s does not exist", dsn)
	}
	// detach
	if _, err := prov.storage.ExecContext(context.Background(), "DETACH "+name); err != nil {
		return fmt.Errorf("failed to detach catalog %w", err)
	}
	// delete the file
	err = os.Remove(dsn)
	if err != nil {
		return fmt.Errorf("failed to delete database file %s: %w", dsn, err)
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
