package catalog

import (
	"context"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	stdsql "database/sql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/sirupsen/logrus"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/configuration"
	"github.com/apecloud/myduckserver/initialdata"
	"github.com/apecloud/myduckserver/mycontext"
)

type DatabaseProvider struct {
	mu                        *sync.RWMutex
	defaultTimeZone           string
	connector                 *duckdb.Connector
	storage                   *stdsql.DB
	pool                      *ConnectionPool
	defaultCatalogName        string // default database name in postgres
	dataDir                   string
	dbFile                    string
	dsn                       string
	externalProcedureRegistry sql.ExternalStoredProcedureRegistry
	duckLake                  *duckLakeRuntime
	ready                     bool
}

var _ sql.DatabaseProvider = (*DatabaseProvider)(nil)
var _ sql.MutableDatabaseProvider = (*DatabaseProvider)(nil)
var _ sql.ExternalStoredProcedureProvider = (*DatabaseProvider)(nil)
var _ configuration.DataDirProvider = (*DatabaseProvider)(nil)

func NewInMemoryDBProvider(options ...ProviderOption) *DatabaseProvider {
	prov, err := NewDBProvider("", ".", "", options...)
	if err != nil {
		panic(err)
	}
	return prov
}

func NewDBProvider(defaultTimeZone, dataDir, defaultDB string, options ...ProviderOption) (prov *DatabaseProvider, err error) {
	providerOptions := &providerOptions{}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(providerOptions); err != nil {
			return nil, err
		}
	}
	duckLake, err := newDuckLakeRuntime(providerOptions.duckLake)
	if err != nil {
		return nil, err
	}
	prov = &DatabaseProvider{
		mu:                        &sync.RWMutex{},
		defaultTimeZone:           defaultTimeZone,
		externalProcedureRegistry: sql.NewExternalStoredProcedureRegistry(), // This has no effect, just to satisfy the upper layer interface
		dataDir:                   dataDir,
		duckLake:                  duckLake,
	}

	if defaultDB == "" || defaultDB == "memory" {
		prov.defaultCatalogName = "memory"
		prov.dbFile = ""
		prov.dsn = ""
	} else {
		prov.defaultCatalogName = defaultDB
		prov.dbFile = defaultDB + ".db"
		prov.dsn = filepath.Join(prov.dataDir, prov.dbFile)
	}

	if err = prov.openStorage(false); err != nil {
		return nil, err
	}

	bootQueries := []string{
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

	err = prov.initCatalog()
	if err != nil {
		return nil, err
	}

	err = prov.attachCatalogs()
	if err != nil {
		return nil, err
	}

	prov.ready = true
	return prov, nil
}

func (prov *DatabaseProvider) openStorage(readOnly bool) error {
	return prov.openStorageWithContext(readOnly, context.Background())
}

func (prov *DatabaseProvider) openStorageWithContext(readOnly bool, openCtx context.Context) error {
	connectorDSN := prov.dsn
	if readOnly && prov.dsn == "" {
		connectorDSN = "?access_mode=read_only"
	}
	var attached atomic.Bool
	connInitFn := func(execer driver.ExecerContext) error {
		if prov.defaultTimeZone != "" {
			timeZone := strings.ReplaceAll(prov.defaultTimeZone, "'", "''")
			if _, err := execer.ExecContext(context.Background(), "SET TimeZone = '"+timeZone+"'", nil); err != nil {
				return err
			}
		}
		if prov.dsn != "" && attached.Load() {
			_, err := execer.ExecContext(context.Background(), "USE "+QuoteIdentifierANSI(prov.defaultCatalogName), nil)
			return err
		}
		return nil
	}
	if prov.dsn != "" {
		// WAL replay for an attached database runs after DuckDB has initialized
		// its default database. This avoids replay binding against an unset
		// default database while keeping the configured catalog as the session
		// default for every connection.
		connectorDSN = ""
	}

	connector, err := duckdb.NewConnector(connectorDSN, connInitFn)
	if err != nil {
		return err
	}
	// Keep the connector callback limited to context-free, non-secret session
	// setup above. DuckLake extensions and the service secret are initialized
	// only after an explicit origin is available at a pool or direct-storage
	// caller boundary.
	storage := stdsql.OpenDB(connector)
	if prov.duckLake != nil {
		// Storage initializes one physical connection at its explicit boundary;
		// retaining one idle connection lets direct callers reuse that initialized
		// session while pooled acquisitions still run their own origin guard.
		storage.SetMaxIdleConns(1)
	}
	if prov.pool == nil {
		prov.pool = NewConnectionPool(connector, storage, prov.defaultCatalogName)
	} else if err := prov.pool.Reset(connector, storage); err != nil {
		_ = storage.Close()
		_ = connector.Close()
		return err
	} else if prov.duckLake != nil {
		// Reset closes every old physical connection. Forget its identities so
		// a new pool generation is initialized even if the driver reuses an
		// address for a fresh connection.
		prov.duckLake.resetInitialized()
	}
	prov.pool.SetConnectionInitializer(prov.initializeConnection)
	prov.connector = connector
	prov.storage = storage

	ctx := openCtx
	if ctx == nil {
		ctx = context.Background()
	}
	recovery := mycontext.QueryOrigin(ctx) == mycontext.RecoveryQueryOrigin
	if prov.dsn == "" {
		if !recovery {
			return nil
		}
		// An in-memory provider has no ATTACH/WAL boundary, but Restart still
		// needs an explicit recovery-origin initialization on the physical
		// connection selected for that restart. Keep this connection scoped to
		// the recovery operation and let later requests use the pool hook.
		conn, err := storage.Conn(ctx)
		if err != nil {
			_ = storage.Close()
			_ = connector.Close()
			return err
		}
		if err := prov.InitializeConnection(ctx, conn); err != nil {
			_ = conn.Close()
			_ = storage.Close()
			_ = connector.Close()
			return err
		}
		if err := conn.Close(); err != nil {
			_ = storage.Close()
			_ = connector.Close()
			return err
		}
		return nil
	}

	conn, err := storage.Conn(ctx)
	if err != nil {
		_ = storage.Close()
		_ = connector.Close()
		return err
	}
	defer conn.Close()
	// Generated expressions may reference MyDuck UDFs. Register them before
	// ATTACH triggers WAL replay so the expression binder can resolve them.
	if err := prov.pool.registerMySQLUDFs(conn); err != nil {
		_ = storage.Close()
		_ = connector.Close()
		return err
	}

	attachSQL := "ATTACH '" + strings.ReplaceAll(prov.dsn, "'", "''") + "' AS " + QuoteIdentifierANSI(prov.defaultCatalogName)
	if readOnly {
		attachSQL += " (READ_ONLY)"
	}
	if _, err := conn.ExecContext(ctx, attachSQL+"; USE "+QuoteIdentifierANSI(prov.defaultCatalogName)); err != nil {
		_ = storage.Close()
		_ = connector.Close()
		return err
	}
	attached.Store(true)
	if recovery {
		// Recovery initialization must run on the same physical connection that
		// performed ATTACH and any WAL replay. Constructor/openStorage calls use
		// an unknown origin and therefore remain strictly zero-init.
		if err := prov.InitializeConnection(ctx, conn); err != nil {
			_ = storage.Close()
			_ = connector.Close()
			return err
		}
	}
	return nil
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

		initialFileContent := initialdata.InitialTableDataMap[t.Name]
		if initialFileContent != "" {
			var count int
			// Count rows in the internal table
			if err := prov.storage.QueryRow(t.CountAllStmt()).Scan(&count); err != nil {
				return fmt.Errorf("failed to count rows in internal table %q: %w", t.Name, err)
			}

			if count == 0 {
				// Create temporary file to store initial data
				tmpFile, err := os.CreateTemp("", "initial-data-"+t.Name+".csv")
				if err != nil {
					return fmt.Errorf("failed to create temporary file for initial data: %w", err)
				}
				// Ensure the temporary file is removed after usage
				defer os.Remove(tmpFile.Name())
				defer tmpFile.Close()

				// Write the initial data to the temporary file
				if _, err := tmpFile.WriteString(initialFileContent); err != nil {
					return fmt.Errorf("failed to write initial data to temporary file: %w", err)
				}

				if err = tmpFile.Sync(); err != nil {
					return fmt.Errorf("failed to sync initial data file: %w", err)
				}

				// Execute the COPY command to insert data into the table
				if _, err := prov.storage.ExecContext(
					context.Background(),
					fmt.Sprintf("COPY %s FROM '%s' (DELIMITER ',', HEADER, ESCAPE '\"')", t.QualifiedName(), tmpFile.Name()),
				); err != nil {
					return fmt.Errorf("failed to insert initial data from file into internal table %q: %w", t.Name, err)
				}
			}
		}
	}

	for _, v := range InternalViews {
		if _, err := prov.storage.ExecContext(
			context.Background(),
			"CREATE SCHEMA IF NOT EXISTS "+v.Schema,
		); err != nil {
			return fmt.Errorf("failed to create internal schema %q: %w", v.Schema, err)
		}
		if _, err := prov.storage.ExecContext(
			context.Background(),
			"CREATE VIEW IF NOT EXISTS "+v.QualifiedName()+" AS "+v.DDL,
		); err != nil {
			return fmt.Errorf("failed to create internal view %q: %w", v.Name, err)
		}
	}

	for _, m := range InternalMacros {
		if _, err := prov.storage.ExecContext(
			context.Background(),
			"CREATE SCHEMA IF NOT EXISTS "+m.Schema,
		); err != nil {
			return fmt.Errorf("failed to create internal schema %q: %w", m.Schema, err)
		}
		definitions := make([]string, 0, len(m.Definitions))
		for _, d := range m.Definitions {
			macroParams := strings.Join(d.Params, ", ")
			var asType string
			if m.IsTableMacro {
				asType = "TABLE\n"
			} else {
				asType = "\n"
			}
			definitions = append(definitions, fmt.Sprintf("\n(%s) AS %s%s", macroParams, asType, d.DDL))
		}
		if _, err := prov.storage.ExecContext(
			context.Background(),
			"CREATE OR REPLACE MACRO "+m.QualifiedName()+strings.Join(definitions, ",")+";",
		); err != nil {
			return fmt.Errorf("failed to create internal macro %q: %w", m.Name, err)
		}
	}

	if _, err := prov.pool.ExecContext(context.Background(), "PRAGMA enable_checkpoint_on_shutdown"); err != nil {
		logrus.WithError(err).Fatalln("Failed to enable checkpoint on shutdown")
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
	quoted := QuoteIdentifierANSI(name)
	if _, err := prov.storage.ExecContext(context.Background(), "ATTACH IF NOT EXISTS '"+filepath.Join(prov.dataDir, file.Name())+"' AS "+quoted); err != nil {
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
	quoted := QuoteIdentifierANSI(name)
	attachSQL += " '" + dsn + "' AS " + quoted
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

		if _, err := prov.storage.ExecContext(context.Background(), "USE "+quoted); err != nil {
			return fmt.Errorf("failed to switch to the new catalog: %w", err)
		}

		defer func() {
			if lastCatalog == "" {
				return
			}
			if _, err := prov.storage.ExecContext(context.Background(), "USE "+QuoteIdentifierANSI(lastCatalog)); err != nil {
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
	if _, err := prov.storage.ExecContext(context.Background(), "DETACH "+QuoteIdentifierANSI(name)); err != nil {
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
	return prov.pool.Close()
}

func (prov *DatabaseProvider) Connector() *duckdb.Connector {
	return prov.connector
}

func (prov *DatabaseProvider) Storage() *stdsql.DB {
	if prov == nil {
		return nil
	}
	// Storage is intentionally a passive escape hatch. It must not invent a
	// query origin: callers that own an explicit query/maintenance/recovery
	// context initialize through the provider or pool boundary before using it.
	return prov.storage
}

// InitializeStorage is retained as a compatibility wrapper for callers that
// have not yet acquired a connection. New request paths must use
// InitializeConnection on their already-owned *sql.Conn; this wrapper never
// caches a database generation and is not used to warm a different request
// connection.
func (prov *DatabaseProvider) InitializeStorage(ctx context.Context) error {
	if prov == nil || prov.duckLake == nil {
		return nil
	}
	if !mycontext.IsDuckLakeEligibleQuery(ctx) {
		return nil
	}

	storage := prov.storage
	if storage == nil {
		return fmt.Errorf("ducklake storage is unavailable")
	}

	conn, err := storage.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return prov.InitializeConnection(ctx, conn)
}

// InitializeConnection applies the service-managed DuckLake setup to an
// already acquired physical connection. Callers must provide an explicit
// frontend, maintenance, or recovery origin in ctx; unknown and replication
// origins deliberately no-op. The runtime keys successful setup by the
// underlying driver.Conn, so a reused logical *sql.Conn is cheap while every
// newly opened physical connection is initialized independently.
func (prov *DatabaseProvider) InitializeConnection(ctx context.Context, conn *stdsql.Conn) error {
	if prov == nil || prov.duckLake == nil {
		return nil
	}
	if !mycontext.IsDuckLakeEligibleQuery(ctx) {
		return nil
	}
	if conn == nil {
		return fmt.Errorf("ducklake storage connection is unavailable")
	}
	return prov.initializeConnection(ctx, conn)
}

func (prov *DatabaseProvider) Pool() *ConnectionPool {
	return prov.pool
}

func (prov *DatabaseProvider) DefaultCatalogName() string {
	return prov.defaultCatalogName
}

func (prov *DatabaseProvider) DataDir() string {
	return prov.dataDir
}

func (prov *DatabaseProvider) DbFile() string {
	return prov.dbFile
}

// DuckLakeEnabled reports whether the service-managed DuckLake connection
// layer passed validation at provider construction. It never exposes secret
// values.
func (prov *DatabaseProvider) DuckLakeEnabled() bool {
	return prov != nil && prov.duckLake != nil
}

func (prov *DatabaseProvider) initializeConnection(ctx context.Context, conn *stdsql.Conn) error {
	if prov.duckLake == nil || conn == nil {
		return nil
	}
	return conn.Raw(func(driverConn any) error {
		execer, ok := driverConn.(driver.ExecerContext)
		if !ok {
			return fmt.Errorf("duckdb connection does not support context execution")
		}
		physicalConn, _ := driverConn.(driver.Conn)
		return prov.duckLake.initializeForConn(ctx, physicalConn, execer)
	})
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

	catalogName := adapter.GetCurrentCatalog(ctx)
	rows, err := adapter.QueryCatalog(ctx, "SELECT DISTINCT schema_name FROM information_schema.schemata WHERE catalog_name = ?", catalogName)
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

		all = append(all, NewDatabase(schemaName, catalogName))
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

	catalogName := adapter.GetCurrentCatalog(ctx)
	ok, err := hasDatabase(ctx, catalogName, name)
	if err != nil {
		return nil, err
	}

	if ok {
		return NewDatabase(name, catalogName), nil
	}
	return nil, sql.ErrDatabaseNotFound.New(name)
}

// HasDatabase implements sql.DatabaseProvider.
func (prov *DatabaseProvider) HasDatabase(ctx *sql.Context, name string) bool {
	prov.mu.RLock()
	defer prov.mu.RUnlock()

	ok, err := hasDatabase(ctx, adapter.GetCurrentCatalog(ctx), name)
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

	_, err := adapter.ExecCatalog(ctx, fmt.Sprintf(`CREATE SCHEMA %s`,
		FullSchemaName(adapter.GetCurrentCatalog(ctx), name)))
	if err != nil {
		return ErrDuckDB.New(err)
	}

	return nil
}

// DropDatabase implements sql.MutableDatabaseProvider.
func (prov *DatabaseProvider) DropDatabase(ctx *sql.Context, name string) error {
	prov.mu.Lock()
	defer prov.mu.Unlock()

	_, err := adapter.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`,
		FullSchemaName(adapter.GetCurrentCatalog(ctx), name)))
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

	recoveryCtx := mycontext.WithRecoveryQuery(context.Background())
	if err := prov.openStorageWithContext(readOnly, recoveryCtx); err != nil {
		return err
	}
	// openStorageWithContext performs recovery initialization on the exact
	// attach/replay connection. No second pool connection is warmed here.
	return nil
}
