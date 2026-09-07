package catalog

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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
	mu                       *sync.RWMutex
	duckLakeCleanupMu        sync.Mutex
	duckLakeTxnMu            sync.Mutex
	duckLakeTxnCond          *sync.Cond
	duckLakeActiveTx         map[*stdsql.Tx]struct{}
	duckLakePendingTx        int
	duckLakeActiveOperations int
	duckLakeOperationOwners  map[any]int
	duckLakeOperationLeases  map[uint64]duckLakeOperationLease
	duckLakeAnonymousOps     int
	duckLakeNextOperationID  uint64
	duckLakeCleanupActive    bool
	// Lake DDL is committed on a dedicated connection because DuckDB refuses
	// writes to a second attached database in the same transaction. Relations
	// created that way stay invisible until local catalog publication commits;
	// this list is the rollback/publication-failure compensation set. Guarded
	// by duckLakeTxnMu.
	duckLakeUncommitted []uncommittedLakeRelation
	// A failed pool teardown can leave an active physical owner behind when its
	// transaction marker is malformed. Preserve that evidence, but do not let a
	// generation transition wait forever or reopen admission against the retired
	// storage. The fields are guarded by duckLakeTxnMu.
	duckLakeGenerationPoisoned bool
	duckLakeGenerationErr      error
	defaultTimeZone            string
	connector                  *duckdb.Connector
	storage                    *stdsql.DB
	pool                       *ConnectionPool
	defaultCatalogName         string // default database name in postgres
	dataDir                    string
	dbFile                     string
	dsn                        string
	externalProcedureRegistry  sql.ExternalStoredProcedureRegistry
	duckLake                   *duckLakeRuntime
	ready                      bool
}

// duckLakeOperationLease records the owner identity captured at admission.
// Comparable identities are indexed in duckLakeOperationOwners; anonymous or
// non-comparable owners are kept as blocking leases in their own count.
type duckLakeOperationLease struct {
	owner any
	owned bool
}

// uncommittedLakeRelation is a DuckLake table created outside the session
// transaction. Rollback cleanup must DROP it; a successful commit forgets it.
type uncommittedLakeRelation struct {
	tx       *stdsql.Tx
	conn     *stdsql.Conn
	physical string
}

// rollbackCleanupOwnerKey carries the physical connection reserved by a
// rollback finalizer. It is intentionally private; callers can only construct
// it through WithRollbackCleanupOwner.
type rollbackCleanupOwnerKey struct{}

type rollbackCleanupOwner struct {
	conn *stdsql.Conn
}

// duckLakeCleanupLeaseKey marks a maintenance call that already owns the
// provider cleanup barrier. It is private so a client SQL context cannot
// manufacture the exemption; only the rollback finalizer creates it.
type duckLakeCleanupLeaseKey struct{}

// WithRollbackCleanupOwner marks a maintenance context whose connection is
// reserved by the session rollback finalizer. Provider setup and cleanup can
// then avoid re-reading the session transaction map while that map's lock is
// held by the finalizer.
func WithRollbackCleanupOwner(ctx context.Context, conn *stdsql.Conn) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, rollbackCleanupOwnerKey{}, rollbackCleanupOwner{conn: conn})
}

func rollbackCleanupOwnerConn(ctx context.Context) *stdsql.Conn {
	if ctx == nil {
		return nil
	}
	owner, ok := ctx.Value(rollbackCleanupOwnerKey{}).(rollbackCleanupOwner)
	if !ok {
		return nil
	}
	return owner.conn
}

// RollbackCleanupOwner returns the physical connection reserved by a rollback
// finalizer, if the context was marked with WithRollbackCleanupOwner.
func RollbackCleanupOwner(ctx context.Context) *stdsql.Conn {
	return rollbackCleanupOwnerConn(ctx)
}

// WithDuckLakeCleanupLease marks a context used by a rollback finalizer that
// already acquired the provider cleanup barrier before entering the pool
// lifecycle lock. CleanupDuckLakeOrphansOnConn must not acquire that barrier a
// second time in this scope.
func WithDuckLakeCleanupLease(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, duckLakeCleanupLeaseKey{}, struct{}{})
}

func duckLakeCleanupLeaseHeld(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	_, ok := ctx.Value(duckLakeCleanupLeaseKey{}).(struct{})
	return ok
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
	// A failed teardown leaves the generation permanently unusable. Do not let
	// Restart (or another internal open path) silently create a fresh connector
	// while the provider still retains the malformed/unknown owner state.
	if err := prov.duckLakeGenerationPoisonError(); err != nil {
		return err
	}
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
		// Reset may fail while preserving a malformed transaction/owner mapping.
		// Poison before returning so a subsequent Restart cannot repeatedly retry
		// the same unsafe generation or admit work through a stale pool.
		prov.poisonDuckLakeGeneration(err)
		_ = storage.Close()
		_ = connector.Close()
		return err
	} else if prov.duckLake != nil {
		// Reset closes every old physical connection. Forget its identities so
		// a new pool generation is initialized even if the driver reuses an
		// address for a fresh connection.
		prov.duckLake.resetInitialized()
	}
	// Keep transaction admission/finalization coupled to this provider even
	// when callers reach the pool directly (for example connection-close and
	// replication teardown paths). Standalone pools leave these hooks unset.
	prov.pool.SetTransactionLifecycleHooksWithError(prov.beginDuckLakeTransactionWithError, prov.untrackDuckLakeTransaction)
	// A malformed teardown poisons this storage generation.  Keep the pool's
	// physical transaction admission fail-closed as well as the provider's
	// logical operation barrier; otherwise a caller that reaches GetTxn directly
	// could open an untracked transaction after the generation was retired.
	prov.pool.SetTransactionAdmissionGuard(prov.duckLakeGenerationPoisonError)
	prov.pool.SetConnectionAdmissionGuard(prov.duckLakeConnectionAdmissionError)
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
	return prov.initCatalogWithExecutor(prov.storage)
}

// initCatalogWithExecutor creates provider metadata through one selected
// database/sql executor. CREATE DATABASE supplies its session connection here
// so ATTACH, USE, and initialization cannot split across pooled connections.
func (prov *DatabaseProvider) initCatalogWithExecutor(execer adapter.SQLExecutor) error {
	if prov == nil || execer == nil {
		return fmt.Errorf("catalog initialization executor is unavailable")
	}

	for _, t := range internalSchemas {
		if _, err := execer.ExecContext(
			context.Background(),
			"CREATE SCHEMA IF NOT EXISTS "+t.Schema,
		); err != nil {
			return fmt.Errorf("failed to create internal schema %q: %w", t.Schema, err)
		}
	}

	for _, t := range internalTables {
		if _, err := execer.ExecContext(
			context.Background(),
			"CREATE SCHEMA IF NOT EXISTS "+t.Schema,
		); err != nil {
			return fmt.Errorf("failed to create internal schema %q: %w", t.Schema, err)
		}
		if _, err := execer.ExecContext(
			context.Background(),
			"CREATE TABLE IF NOT EXISTS "+t.QualifiedName()+"("+t.DDL+")",
		); err != nil {
			return fmt.Errorf("failed to create internal table %q: %w", t.Name, err)
		}
		for _, row := range t.InitialData {
			if _, err := execer.ExecContext(
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
			if err := execer.QueryRowContext(context.Background(), t.CountAllStmt()).Scan(&count); err != nil {
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
				if _, err := execer.ExecContext(
					context.Background(),
					fmt.Sprintf("COPY %s FROM '%s' (DELIMITER ',', HEADER, ESCAPE '\"')", t.QualifiedName(), tmpFile.Name()),
				); err != nil {
					return fmt.Errorf("failed to insert initial data from file into internal table %q: %w", t.Name, err)
				}
			}
		}
	}

	for _, v := range InternalViews {
		if _, err := execer.ExecContext(
			context.Background(),
			"CREATE SCHEMA IF NOT EXISTS "+v.Schema,
		); err != nil {
			return fmt.Errorf("failed to create internal schema %q: %w", v.Schema, err)
		}
		if _, err := execer.ExecContext(
			context.Background(),
			"CREATE VIEW IF NOT EXISTS "+v.QualifiedName()+" AS "+v.DDL,
		); err != nil {
			return fmt.Errorf("failed to create internal view %q: %w", v.Name, err)
		}
	}

	for _, m := range InternalMacros {
		if _, err := execer.ExecContext(
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
		if _, err := execer.ExecContext(
			context.Background(),
			"CREATE OR REPLACE MACRO "+m.QualifiedName()+strings.Join(definitions, ",")+";",
		); err != nil {
			return fmt.Errorf("failed to create internal macro %q: %w", m.Name, err)
		}
	}

	if _, err := execer.ExecContext(context.Background(), "PRAGMA enable_checkpoint_on_shutdown"); err != nil {
		logrus.WithError(err).Fatalln("Failed to enable checkpoint on shutdown")
	}

	// Postgres tables are created in the `public` schema by default.
	// Create the `public` schema if it doesn't exist.
	_, err := execer.ExecContext(context.Background(), "CREATE SCHEMA IF NOT EXISTS public")
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
func (prov *DatabaseProvider) duckLakeMetadataFile(name string) bool {
	if prov == nil || prov.duckLake == nil {
		return false
	}
	meta := strings.TrimSpace(prov.duckLake.config.MetadataPath)
	if meta == "" {
		return false
	}
	return filepath.Clean(filepath.Join(prov.dataDir, name)) == filepath.Clean(meta)
}

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
	// The DuckLake catalog file lives in dataDir but must not be ATTACHed as a
	// regular DuckDB database. Doing so occupies the file and makes the later
	// ducklake: ATTACH fail after restart.
	if prov.duckLakeMetadataFile(file.Name()) {
		return nil
	}
	name := strings.TrimSuffix(file.Name(), ".db")
	quoted := QuoteIdentifierANSI(name)
	if _, err := prov.storage.ExecContext(context.Background(), "ATTACH IF NOT EXISTS '"+filepath.Join(prov.dataDir, file.Name())+"' AS "+quoted); err != nil {
		return fmt.Errorf("failed to attach database %s: %w", name, err)
	}
	return nil
}

func (prov *DatabaseProvider) CreateCatalog(name string, ifNotExists bool) error {
	if prov == nil {
		return fmt.Errorf("database provider is unavailable")
	}
	if prov.mu != nil {
		prov.mu.Lock()
		defer prov.mu.Unlock()
	}
	if prov.storage == nil {
		return fmt.Errorf("database storage is unavailable")
	}
	ctx := context.Background()
	conn, err := prov.storage.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return prov.createCatalogOnConnLocked(ctx, conn, name, ifNotExists)
}

// CreateCatalogOnConn creates and attaches a database using the caller's
// already-owned physical connection. The provider lock serializes the whole
// ATTACH/initialization sequence with Restart, while the connection preserves
// the PostgreSQL client's session catalog state.
func (prov *DatabaseProvider) CreateCatalogOnConn(ctx context.Context, conn *stdsql.Conn, name string, ifNotExists bool) error {
	if prov == nil {
		return fmt.Errorf("database provider is unavailable")
	}
	if prov.mu != nil {
		prov.mu.Lock()
		defer prov.mu.Unlock()
	}
	return prov.createCatalogOnConnLocked(ctx, conn, name, ifNotExists)
}

func (prov *DatabaseProvider) createCatalogOnConnLocked(ctx context.Context, conn *stdsql.Conn, name string, ifNotExists bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if conn == nil {
		return fmt.Errorf("database connection is unavailable")
	}
	name = strings.TrimSpace(name)
	// The in-memory database is always present and does not need ATTACH.
	if name == "" || name == "memory" {
		return nil
	}
	dsn := filepath.Join(prov.dataDir, name+".db")

	_, err := os.Stat(dsn)
	shouldInit := os.IsNotExist(err)

	attachSQL := "ATTACH"
	if ifNotExists {
		attachSQL += " IF NOT EXISTS"
	}
	quoted := QuoteIdentifierANSI(name)
	attachSQL += " '" + dsn + "' AS " + quoted
	if _, err = conn.ExecContext(ctx, attachSQL); err != nil {
		return err
	}

	if !shouldInit {
		return nil
	}
	res, err := conn.QueryContext(ctx, "SELECT current_catalog")
	if err != nil {
		return fmt.Errorf("failed to init catalog: %w", err)
	}
	defer res.Close()
	lastCatalog := ""
	for res.Next() {
		if err := res.Scan(&lastCatalog); err != nil {
			return fmt.Errorf("failed to init catalog: %w", err)
		}
	}
	if err := res.Err(); err != nil {
		return fmt.Errorf("failed to init catalog: %w", err)
	}

	if _, err := conn.ExecContext(ctx, "USE "+quoted); err != nil {
		return fmt.Errorf("failed to switch to the new catalog: %w", err)
	}

	defer func() {
		if lastCatalog == "" {
			return
		}
		if _, err := conn.ExecContext(context.WithoutCancel(ctx), "USE "+QuoteIdentifierANSI(lastCatalog)); err != nil {
			logrus.WithError(err).Errorln("Failed to switch back to the old catalog")
		}
	}()
	return prov.initCatalogWithExecutor(conn)
}

func (prov *DatabaseProvider) DropCatalog(name string, ifExists bool) error {
	if prov == nil {
		return fmt.Errorf("database provider is unavailable")
	}
	if prov.mu != nil {
		prov.mu.Lock()
		defer prov.mu.Unlock()
	}
	if prov.storage == nil {
		return fmt.Errorf("database storage is unavailable")
	}
	ctx := context.Background()
	conn, err := prov.storage.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return prov.dropCatalogOnConnLocked(ctx, conn, name, ifExists)
}

// DropCatalogOnConn detaches and removes a database using the caller's
// physical connection. It is serialized with Restart for the full detach/file
// removal sequence.
func (prov *DatabaseProvider) DropCatalogOnConn(ctx context.Context, conn *stdsql.Conn, name string, ifExists bool) error {
	if prov == nil {
		return fmt.Errorf("database provider is unavailable")
	}
	if prov.mu != nil {
		prov.mu.Lock()
		defer prov.mu.Unlock()
	}
	return prov.dropCatalogOnConnLocked(ctx, conn, name, ifExists)
}

func (prov *DatabaseProvider) dropCatalogOnConnLocked(ctx context.Context, conn *stdsql.Conn, name string, ifExists bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if conn == nil {
		return fmt.Errorf("database connection is unavailable")
	}
	name = strings.TrimSpace(name)
	if name == "" || name == "memory" {
		return fmt.Errorf("cannot drop the in-memory catalog")
	}
	dsn := filepath.Join(prov.dataDir, name+".db")
	if _, err := os.Stat(dsn); os.IsNotExist(err) {
		if ifExists {
			return nil
		}
		return fmt.Errorf("database file %s does not exist", dsn)
	}
	if _, err := conn.ExecContext(ctx, "DETACH "+QuoteIdentifierANSI(name)); err != nil {
		return fmt.Errorf("failed to detach catalog %w", err)
	}
	if err := os.Remove(dsn); err != nil {
		return fmt.Errorf("failed to delete database file %s: %w", dsn, err)
	}
	return nil
}

func (prov *DatabaseProvider) Close() (err error) {
	if prov == nil {
		return nil
	}
	// Announce the generation transition before taking the provider mutex. An
	// already-admitted frontend operation may still need that mutex (for
	// catalog DDL/metadata) before it can release its DuckLake operation lease.
	// Holding prov.mu first would make pool teardown wait on the operation while
	// the operation waits on prov.mu.
	releaseTransition, transitionErr := prov.beginDuckLakeGenerationTransitionWithError()
	if transitionErr != nil {
		return transitionErr
	}
	defer func() {
		err = errors.Join(err, releaseTransition())
	}()
	if prov.mu != nil {
		prov.mu.Lock()
		defer prov.mu.Unlock()
	}
	err = prov.closeLocked()
	return err
}

// closeLocked retires the current storage generation. Callers that already
// hold prov.mu (notably Restart) use this helper to avoid self-deadlocking.
func (prov *DatabaseProvider) closeLocked() error {
	if prov == nil {
		return nil
	}
	var err error
	if prov.pool != nil {
		err = prov.pool.Close()
		if err != nil {
			// Pool teardown deliberately preserves malformed mappings and their
			// unknown physical owners. Mark this generation unusable so the
			// transition release can return instead of waiting on state that no
			// type-safe path can finish.
			prov.poisonDuckLakeGeneration(err)
		}
	}
	if prov.connector != nil {
		_ = prov.connector.Close()
	}
	return err
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

// DuckLakeObjectStorageEnabled reports whether the service has both sides of
// the DuckLake catalog configured. An enabled extension layer may intentionally
// omit these paths, but it cannot produce or clean lake-table files.
func (prov *DatabaseProvider) DuckLakeObjectStorageEnabled() bool {
	return prov != nil && prov.duckLakeObjectStorageEnabled()
}

// withoutCancelContext removes request cancellation while retaining the
// concrete *sql.Context type. Provider boundaries use the concrete type to
// detect a session transaction and avoid issuing *sql.Conn work alongside an
// active *sql.Tx.
func withoutCancelContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	if sqlCtx, ok := ctx.(*sql.Context); ok && sqlCtx != nil {
		return sqlCtx.WithContext(context.WithoutCancel(sqlCtx))
	}
	return context.WithoutCancel(ctx)
}

// EnsureDuckLakeConnection makes the service-owned lake available on an
// already acquired physical connection. Object-table paths call this after
// resolving persisted metadata; ordinary local and catalog operations do not
// need to invoke it explicitly.
func (prov *DatabaseProvider) EnsureDuckLakeConnection(ctx context.Context, conn *stdsql.Conn) error {
	return prov.ensureDuckLakeConnection(ctx, conn, nil, false)
}

// EnsureDuckLakeConnectionWithBinding initializes DuckLake on a connection
// selected by an already-captured execution snapshot. bindingKnown is kept
// private in the implementation so this boundary cannot accidentally perform
// a second pool lookup while the caller retains a lifecycle lease.
//
// A non-nil tx proves that the pool initializer ran before BeginTx and that
// this exact physical owner is already attached. It does not bypass provider
// admission: frontend snapshots, including active transactions, are rejected
// while the storage generation is transitioning or poisoned. A nil tx means
// an admitted connection-only snapshot may perform the driver-level attach.
func (prov *DatabaseProvider) EnsureDuckLakeConnectionWithBinding(
	ctx context.Context,
	conn *stdsql.Conn,
	tx *stdsql.Tx,
) error {
	return prov.ensureDuckLakeConnection(ctx, conn, tx, true)
}

// EnsureDuckLakeConnectionWithExecutor is the convenient form used by
// statement paths that already carry both the SQL executor and its physical
// owner. *sql.Tx is recognized without consulting the session/pool maps.
func (prov *DatabaseProvider) EnsureDuckLakeConnectionWithExecutor(
	ctx context.Context,
	execer adapter.SQLExecutor,
	conn *stdsql.Conn,
) error {
	var tx *stdsql.Tx
	if typed, ok := execer.(*stdsql.Tx); ok {
		tx = typed
	}
	return prov.ensureDuckLakeConnection(ctx, conn, tx, true)
}

func (prov *DatabaseProvider) ensureDuckLakeConnection(
	ctx context.Context,
	conn *stdsql.Conn,
	boundTx *stdsql.Tx,
	bindingKnown bool,
) error {
	if prov == nil || prov.duckLake == nil {
		return fmt.Errorf("%w: DuckLake service configuration is disabled", ErrInvalidTableStorage)
	}
	if err := prov.duckLakeConnectionAdmissionError(ctx); err != nil {
		return err
	}
	if conn == nil {
		return fmt.Errorf("ducklake storage connection is unavailable")
	}
	// For an admitted request, a session transaction owns this physical
	// connection. The pool initializer runs before BeginTx, so an active
	// transaction is already initialized and attached; calling Conn.Raw here
	// would race the transaction's driver calls (and can deadlock when the driver
	// serializes access). Object callers use their SQLExecutor (*sql.Tx) for all
	// statement work while the transaction is active.
	if ownerConn := rollbackCleanupOwnerConn(ctx); ownerConn != nil {
		if ownerConn != conn {
			return fmt.Errorf("ducklake cleanup connection is not the rollback owner")
		}
		// The rollback finalizer holds the session lifecycle lock. The explicit
		// owner marker is the atomic snapshot for this maintenance operation;
		// calling back into the holder here would deadlock.
	} else if bindingKnown {
		// The caller supplied the atomic snapshot that selected conn. Do not
		// re-enter ConnectionPool.GetTxnBinding here: a retained execution lease
		// already holds lifecycleMu.RLock, and a pending pool writer would make
		// that nested ordinary read wait on itself. A physical transaction has
		// already been initialized by the pool before it was returned.
		if boundTx != nil {
			return nil
		}
	} else if sqlCtx, ok := ctx.(*sql.Context); ok && sqlCtx != nil {
		if _, holder := sqlCtx.Session.(adapter.ConnectionHolder); holder {
			boundConn, boundTx := adapter.TryGetTxnBinding(sqlCtx)
			if boundConn != nil && boundConn != conn {
				return fmt.Errorf("ducklake connection is not owned by the session")
			}
			if boundTx != nil {
				// The transaction was initialized before BeginTx and owns this exact
				// physical connection. Do not call Conn.Raw beside its executor.
				return nil
			}
		}
	}
	return conn.Raw(func(driverConn any) error {
		execer, ok := driverConn.(driver.ExecerContext)
		if !ok {
			return fmt.Errorf("duckdb connection does not support context execution")
		}
		physicalConn, _ := driverConn.(driver.Conn)
		return prov.duckLake.EnsureAttached(ctx, physicalConn, execer)
	})
}

func (prov *DatabaseProvider) ensureDuckLakeTxnStateLocked() {
	if prov.duckLakeTxnCond == nil {
		prov.duckLakeTxnCond = sync.NewCond(&prov.duckLakeTxnMu)
	}
	if prov.duckLakeActiveTx == nil {
		prov.duckLakeActiveTx = make(map[*stdsql.Tx]struct{})
	}
	if prov.duckLakeOperationOwners == nil {
		prov.duckLakeOperationOwners = make(map[any]int)
	}
	if prov.duckLakeOperationLeases == nil {
		prov.duckLakeOperationLeases = make(map[uint64]duckLakeOperationLease)
	}
}

var errDuckLakeGenerationPoisoned = errors.New("DuckLake storage generation is poisoned")
var errDuckLakeGenerationTransition = errors.New("DuckLake storage generation is transitioning")

// duckLakeGenerationPoisonErrorLocked returns the stable error for a retired
// generation. Callers must hold duckLakeTxnMu; keeping this small locked helper
// avoids trying to reacquire the mutex from an admission path that is already
// checking the poison bit under the condition variable.
func (prov *DatabaseProvider) duckLakeGenerationPoisonErrorLocked() error {
	if prov == nil || !prov.duckLakeGenerationPoisoned {
		return nil
	}
	if prov.duckLakeGenerationErr == nil {
		return errDuckLakeGenerationPoisoned
	}
	return errors.Join(errDuckLakeGenerationPoisoned, prov.duckLakeGenerationErr)
}

// poisonDuckLakeGeneration records a teardown failure without discarding the
// mappings that caused it. A poisoned provider cannot safely reopen or admit
// new DuckLake work, but late physical finalizers may still untrack the exact
// transaction they own.
func (prov *DatabaseProvider) poisonDuckLakeGeneration(err error) {
	if prov == nil || err == nil || !prov.duckLakeObjectStorageEnabled() {
		return
	}
	prov.duckLakeTxnMu.Lock()
	defer prov.duckLakeTxnMu.Unlock()
	prov.ensureDuckLakeTxnStateLocked()
	if !prov.duckLakeGenerationPoisoned {
		prov.duckLakeGenerationPoisoned = true
		prov.duckLakeGenerationErr = err
	} else if prov.duckLakeGenerationErr == nil {
		prov.duckLakeGenerationErr = err
	}
	prov.duckLakeTxnCond.Broadcast()
}

// duckLakeGenerationPoisonError returns the stable teardown error, if this
// provider can no longer safely admit or reopen a DuckLake generation.
func (prov *DatabaseProvider) duckLakeGenerationPoisonError() error {
	if prov == nil {
		return nil
	}
	prov.duckLakeTxnMu.Lock()
	defer prov.duckLakeTxnMu.Unlock()
	return prov.duckLakeGenerationPoisonErrorLocked()
}

// duckLakeConnectionAdmissionError protects every ordinary pool connection and
// snapshot entry point, not only transaction admission. A provider transition
// keeps cleanupActive set across pool Close/Reset and recovery reopen; reject
// frontend work during that interval so it cannot slip into the retired pool
// after the lifecycle writer unlocks but before poison/reopen is published.
// Provider-owned rollback cleanup and recovery carry explicit context markers
// and may continue on their already-reserved generation.
func (prov *DatabaseProvider) duckLakeConnectionAdmissionError(ctx context.Context) error {
	if prov == nil || !prov.duckLakeObjectStorageEnabled() {
		return nil
	}
	prov.duckLakeTxnMu.Lock()
	defer prov.duckLakeTxnMu.Unlock()
	if poisonErr := prov.duckLakeGenerationPoisonErrorLocked(); poisonErr != nil {
		return poisonErr
	}
	origin := mycontext.QueryOrigin(ctx)
	cleanupOwner := duckLakeCleanupLeaseHeld(ctx) && origin == mycontext.MaintenanceQueryOrigin
	if prov.duckLakeCleanupActive && !cleanupOwner && origin != mycontext.RecoveryQueryOrigin {
		return errDuckLakeGenerationTransition
	}
	return nil
}

// comparableDuckLakeOwner returns an owner only when it can be used safely as
// a map key and compared through an interface. A non-comparable (or typed-nil)
// owner is deliberately treated as anonymous, which keeps rollback cleanup
// conservative instead of risking a comparison panic or an accidental
// exemption.
func comparableDuckLakeOwner(owner any) (any, bool) {
	if owner == nil {
		return nil, false
	}
	v := reflect.ValueOf(owner)
	if !v.IsValid() || !v.Comparable() {
		return nil, false
	}
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.Slice, reflect.UnsafePointer:
		if v.IsNil() {
			return nil, false
		}
	}
	return owner, true
}

// duckLakeOperationOwnerFromContext derives the logical GMS transaction when
// the legacy API is called with a concrete SQL context. Contexts without a
// session/transaction remain anonymous and therefore continue to block a
// rollback cleanup reservation.
func duckLakeOperationOwnerFromContext(ctx context.Context) any {
	sqlCtx, ok := ctx.(*sql.Context)
	if !ok || sqlCtx == nil || sqlCtx.Session == nil {
		return nil
	}
	// Prefer the physical transaction identity when the session exposes an
	// atomic binding snapshot. PostgreSQL and explicit MySQL transactions can
	// then be released by pool generation teardown without waiting on a logical
	// lease whose callback lives only in the GMS transaction object. Implicit
	// autocommit scopes still have no physical owner here and remain conservative
	// logical/anonymous leases.
	if holder, ok := sqlCtx.Session.(adapter.TransactionBindingHolder); ok {
		if _, tx := holder.GetTxnBinding(); tx != nil {
			return tx
		}
	}
	return sqlCtx.GetTransaction()
}

// duckLakeOperationBlocksCleanupLocked reports whether any admitted operation
// remains that the rollback caller cannot prove it owns. The caller must hold
// duckLakeTxnMu. The aggregate count is retained as a conservative fallback if
// bookkeeping was populated by an older generation or a legacy test double.
func (prov *DatabaseProvider) duckLakeOperationBlocksCleanupLocked(target *stdsql.Tx, owner any) bool {
	ownerKey, ownerOK := comparableDuckLakeOwner(owner)
	targetKey, targetOK := comparableDuckLakeOwner(target)

	if prov.duckLakeAnonymousOps > 0 {
		return true
	}
	tracked := 0
	for operationOwner, count := range prov.duckLakeOperationOwners {
		if count <= 0 {
			continue
		}
		tracked += count
		ownedByRollback := (ownerOK && operationOwner == ownerKey) ||
			(targetOK && operationOwner == targetKey)
		if !ownedByRollback {
			return true
		}
	}
	// If an aggregate count exceeds the owner-indexed records, retain the old
	// fail-closed behavior: the unclassified leases must still block cleanup.
	return prov.duckLakeActiveOperations > tracked
}

// beginDuckLakeTransaction reserves admission for a transaction before the
// pool starts the driver transaction. The reservation closes the small window
// in which cleanup could otherwise begin between BeginTx and registration. The
// returned completion function is idempotent and must be called with the
// transaction on success or nil on failure.
func (prov *DatabaseProvider) beginDuckLakeTransaction() func(*stdsql.Tx) {
	release, _ := prov.beginDuckLakeTransactionWithError()
	return release
}

// beginDuckLakeTransactionWithError reserves admission for a physical
// transaction and preserves a poisoned-generation error for callers that can
// report it. The legacy closure-only wrapper above intentionally remains for
// compatibility with existing pool/test hooks; production pools install this
// error-bearing form so a failed reservation cannot be mistaken for success.
func (prov *DatabaseProvider) beginDuckLakeTransactionWithError() (func(*stdsql.Tx), error) {
	if prov == nil || !prov.duckLakeObjectStorageEnabled() {
		return func(*stdsql.Tx) {}, nil
	}
	prov.duckLakeTxnMu.Lock()
	prov.ensureDuckLakeTxnStateLocked()
	if poisonErr := prov.duckLakeGenerationPoisonErrorLocked(); poisonErr != nil {
		prov.duckLakeTxnMu.Unlock()
		return func(*stdsql.Tx) {}, poisonErr
	}
	for prov.duckLakeCleanupActive && !prov.duckLakeGenerationPoisoned {
		prov.duckLakeTxnCond.Wait()
	}
	if poisonErr := prov.duckLakeGenerationPoisonErrorLocked(); poisonErr != nil {
		prov.duckLakeTxnMu.Unlock()
		return func(*stdsql.Tx) {}, poisonErr
	}
	prov.duckLakePendingTx++
	prov.duckLakeTxnMu.Unlock()

	var once sync.Once
	return func(tx *stdsql.Tx) {
		once.Do(func() {
			prov.duckLakeTxnMu.Lock()
			if prov.duckLakePendingTx > 0 {
				prov.duckLakePendingTx--
			}
			if tx != nil {
				prov.duckLakeActiveTx[tx] = struct{}{}
			}
			prov.duckLakeTxnCond.Broadcast()
			prov.duckLakeTxnMu.Unlock()
		})
	}, nil
}

// BeginDuckLakeOperation reserves admission for one top-level frontend
// operation that may read or write the service-managed DuckLake relation.
//
// GMS creates a logical autocommit transaction without a physical *sql.Tx
// when @@autocommit=1. The transaction barrier therefore cannot observe that
// work through duckLakeActiveTx alone. This lease covers the interval from
// execution-build through iterator consumption, allowing rollback cleanup to
// wait for the operation even when no physical transaction exists.
//
// The returned release function is idempotent. Callers must release it on
// build failure and from every terminal iterator path (EOF, error, or Close).
func (prov *DatabaseProvider) BeginDuckLakeOperation(ctx context.Context) func() {
	release, _ := prov.BeginDuckLakeOperationWithError(ctx)
	return release
}

// BeginDuckLakeOperationWithError is the error-preserving admission surface.
// Legacy callers may continue to use BeginDuckLakeOperation, but new protocol
// paths should use this form so a poisoned generation cannot be mistaken for a
// successful no-op admission.
func (prov *DatabaseProvider) BeginDuckLakeOperationWithError(ctx context.Context) (func(), error) {
	return prov.BeginDuckLakeOperationForOwnerWithError(ctx, duckLakeOperationOwnerFromContext(ctx))
}

// BeginDuckLakeOperationForOwner is the owner-aware form of
// BeginDuckLakeOperation. owner should be the logical transaction identity
// used by the caller's rollback path (for example, the concrete GMS
// transaction). A physical *sql.Tx is also accepted and is matched against a
// rollback target. Owners that are not safely comparable are retained as
// anonymous blocking leases.
func (prov *DatabaseProvider) BeginDuckLakeOperationForOwner(ctx context.Context, owner any) func() {
	release, _ := prov.BeginDuckLakeOperationForOwnerWithError(ctx, owner)
	return release
}

// BeginDuckLakeOperationForOwnerWithError is the owner-aware, error-preserving
// admission surface. A non-nil error means no lease was admitted and the
// returned release function is a safe no-op.
func (prov *DatabaseProvider) BeginDuckLakeOperationForOwnerWithError(ctx context.Context, owner any) (func(), error) {
	if prov == nil || !prov.duckLakeObjectStorageEnabled() ||
		mycontext.QueryOrigin(ctx) != mycontext.FrontendQueryOrigin {
		return func() {}, nil
	}
	ownerKey, owned := comparableDuckLakeOwner(owner)

	prov.duckLakeTxnMu.Lock()
	prov.ensureDuckLakeTxnStateLocked()
	if poisonErr := prov.duckLakeGenerationPoisonErrorLocked(); poisonErr != nil {
		prov.duckLakeTxnMu.Unlock()
		return func() {}, poisonErr
	}
	for prov.duckLakeCleanupActive && !prov.duckLakeGenerationPoisoned {
		prov.duckLakeTxnCond.Wait()
	}
	if poisonErr := prov.duckLakeGenerationPoisonErrorLocked(); poisonErr != nil {
		prov.duckLakeTxnMu.Unlock()
		return func() {}, poisonErr
	}
	prov.duckLakeNextOperationID++
	leaseID := prov.duckLakeNextOperationID
	prov.duckLakeOperationLeases[leaseID] = duckLakeOperationLease{
		owner: ownerKey,
		owned: owned,
	}
	if owned {
		prov.duckLakeOperationOwners[ownerKey]++
	} else {
		prov.duckLakeAnonymousOps++
	}
	prov.duckLakeActiveOperations++
	prov.duckLakeTxnMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			prov.duckLakeTxnMu.Lock()
			prov.ensureDuckLakeTxnStateLocked()
			lease, present := prov.duckLakeOperationLeases[leaseID]
			if !present {
				prov.duckLakeTxnMu.Unlock()
				return
			}
			delete(prov.duckLakeOperationLeases, leaseID)
			if lease.owned {
				if count := prov.duckLakeOperationOwners[lease.owner]; count <= 1 {
					delete(prov.duckLakeOperationOwners, lease.owner)
				} else {
					prov.duckLakeOperationOwners[lease.owner] = count - 1
				}
			} else if prov.duckLakeAnonymousOps > 0 {
				prov.duckLakeAnonymousOps--
			}
			if prov.duckLakeActiveOperations > 0 {
				prov.duckLakeActiveOperations--
			}
			prov.duckLakeTxnCond.Broadcast()
			prov.duckLakeTxnMu.Unlock()
		})
	}, nil
}

// BeginDuckLakeRollbackCleanup reserves the provider-wide DuckLake cleanup
// barrier for a rollback finalizer. Unlike beginDuckLakeCleanup, the target
// transaction is allowed to remain active while the reservation is acquired:
// the caller must still run the driver's Rollback before maintenance can use
// the owner's physical connection. Every other physical transaction, pending
// admission, and logical frontend operation is drained first, and no new work
// can enter until the returned release function is called.
//
// The reservation must be obtained before entering a pool/session finalizer.
// That ordering prevents a finalizer holding the pool release lock from
// waiting on an operation that itself needs the pool lock to finish. The
// returned callback is idempotent and safe to defer on all paths.
func (prov *DatabaseProvider) BeginDuckLakeRollbackCleanup(target *stdsql.Tx) func() {
	release, _ := prov.BeginDuckLakeRollbackCleanupWithError(target)
	return release
}

// BeginDuckLakeRollbackCleanupWithError is the error-preserving counterpart to
// BeginDuckLakeRollbackCleanup. A non-nil error means the provider did not
// reserve its cleanup barrier; callers must still finalize the driver
// transaction, but must skip DuckLake maintenance for this poisoned generation.
func (prov *DatabaseProvider) BeginDuckLakeRollbackCleanupWithError(target *stdsql.Tx) (func(), error) {
	return prov.BeginDuckLakeRollbackCleanupForOwnerWithError(target, nil)
}

// BeginDuckLakeRollbackCleanupForOwner is the owner-aware rollback barrier.
// The physical target and logical owner are both considered when deciding
// which already-admitted operation leases belong to this rollback caller. Only
// those leases are exempt; every other owner, plus anonymous leases, keeps the
// reservation blocked.
func (prov *DatabaseProvider) BeginDuckLakeRollbackCleanupForOwner(target *stdsql.Tx, owner any) func() {
	release, _ := prov.BeginDuckLakeRollbackCleanupForOwnerWithError(target, owner)
	return release
}

// BeginDuckLakeRollbackCleanupForOwnerWithError is the owner-aware,
// error-preserving rollback barrier. The returned release is always safe to
// call, including when admission fails before the provider mutexes are held.
func (prov *DatabaseProvider) BeginDuckLakeRollbackCleanupForOwnerWithError(target *stdsql.Tx, owner any) (func(), error) {
	if prov == nil || !prov.duckLakeObjectStorageEnabled() {
		return func() {}, nil
	}

	prov.duckLakeCleanupMu.Lock()
	prov.duckLakeTxnMu.Lock()
	prov.ensureDuckLakeTxnStateLocked()
	if poisonErr := prov.duckLakeGenerationPoisonErrorLocked(); poisonErr != nil {
		prov.duckLakeTxnMu.Unlock()
		prov.duckLakeCleanupMu.Unlock()
		return func() {}, poisonErr
	}
	prov.duckLakeCleanupActive = true
	for {
		if poisonErr := prov.duckLakeGenerationPoisonErrorLocked(); poisonErr != nil {
			prov.duckLakeCleanupActive = false
			prov.duckLakeTxnCond.Broadcast()
			prov.duckLakeTxnMu.Unlock()
			prov.duckLakeCleanupMu.Unlock()
			return func() {}, poisonErr
		}
		otherTransactions := len(prov.duckLakeActiveTx)
		if target != nil {
			if _, present := prov.duckLakeActiveTx[target]; present {
				otherTransactions--
			}
		}
		if otherTransactions <= 0 &&
			prov.duckLakePendingTx == 0 &&
			!prov.duckLakeOperationBlocksCleanupLocked(target, owner) {
			break
		}
		prov.duckLakeTxnCond.Wait()
	}
	prov.duckLakeTxnMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			prov.duckLakeTxnMu.Lock()
			prov.ensureDuckLakeTxnStateLocked()
			prov.duckLakeCleanupActive = false
			prov.duckLakeTxnCond.Broadcast()
			prov.duckLakeTxnMu.Unlock()
			prov.duckLakeCleanupMu.Unlock()
		})
	}, nil
}

// trackDuckLakeTransaction records a transaction that was admitted by the
// provider barrier. It is kept as a small idempotent primitive for lifecycle
// callers that already hold a transaction and need to repair bookkeeping.
func (prov *DatabaseProvider) trackDuckLakeTransaction(tx *stdsql.Tx) {
	if prov == nil || tx == nil || !prov.duckLakeObjectStorageEnabled() {
		return
	}
	prov.duckLakeTxnMu.Lock()
	defer prov.duckLakeTxnMu.Unlock()
	prov.ensureDuckLakeTxnStateLocked()
	prov.duckLakeActiveTx[tx] = struct{}{}
}

// untrackDuckLakeTransaction is idempotent so both normal finalization and a
// connection-close fallback may safely release the same transaction.
func (prov *DatabaseProvider) untrackDuckLakeTransaction(tx *stdsql.Tx) {
	if prov == nil || tx == nil || !prov.duckLakeObjectStorageEnabled() {
		return
	}
	prov.duckLakeTxnMu.Lock()
	defer prov.duckLakeTxnMu.Unlock()
	prov.ensureDuckLakeTxnStateLocked()
	if _, exists := prov.duckLakeActiveTx[tx]; exists {
		delete(prov.duckLakeActiveTx, tx)
		prov.duckLakeTxnCond.Broadcast()
	}
}

func (prov *DatabaseProvider) registerUncommittedLakeRelation(tx *stdsql.Tx, conn *stdsql.Conn, physical string) {
	if prov == nil || tx == nil || strings.TrimSpace(physical) == "" {
		return
	}
	prov.duckLakeTxnMu.Lock()
	prov.ensureDuckLakeTxnStateLocked()
	prov.duckLakeUncommitted = append(prov.duckLakeUncommitted, uncommittedLakeRelation{
		tx:       tx,
		conn:     conn,
		physical: physical,
	})
	prov.duckLakeTxnMu.Unlock()
	if prov.pool != nil {
		prov.pool.RegisterTransactionCompletion(tx, func(success bool) {
			if success {
				prov.forgetUncommittedLakeRelation(tx, physical)
			}
		})
	}
}

func (prov *DatabaseProvider) forgetUncommittedLakeRelation(tx *stdsql.Tx, physical string) {
	if prov == nil || tx == nil {
		return
	}
	prov.duckLakeTxnMu.Lock()
	defer prov.duckLakeTxnMu.Unlock()
	prov.ensureDuckLakeTxnStateLocked()
	kept := prov.duckLakeUncommitted[:0]
	for _, rel := range prov.duckLakeUncommitted {
		if rel.tx == tx && (physical == "" || rel.physical == physical) {
			continue
		}
		kept = append(kept, rel)
	}
	prov.duckLakeUncommitted = kept
}

func (prov *DatabaseProvider) takeUncommittedLakeRelationsForConn(conn *stdsql.Conn) []string {
	if prov == nil || conn == nil {
		return nil
	}
	prov.duckLakeTxnMu.Lock()
	defer prov.duckLakeTxnMu.Unlock()
	prov.ensureDuckLakeTxnStateLocked()
	var taken []string
	kept := prov.duckLakeUncommitted[:0]
	for _, rel := range prov.duckLakeUncommitted {
		if rel.conn == conn {
			taken = append(taken, rel.physical)
			continue
		}
		kept = append(kept, rel)
	}
	prov.duckLakeUncommitted = kept
	return taken
}

// beginDuckLakeCleanup obtains an exclusive provider barrier. It waits for
// existing physical transactions and logical frontend operations to finish,
// and blocks admission of new ones until the returned release function is
// called.
func (prov *DatabaseProvider) beginDuckLakeCleanup() func() {
	release, _ := prov.beginDuckLakeCleanupWithError()
	return release
}

// beginDuckLakeCleanupWithError is the error-preserving maintenance barrier.
// It is intentionally private: public cleanup callers use
// CleanupDuckLakeOrphansOnConn, which turns a poisoned generation into an
// actionable error before issuing any SQL.
func (prov *DatabaseProvider) beginDuckLakeCleanupWithError() (func(), error) {
	if prov == nil || !prov.duckLakeObjectStorageEnabled() {
		return func() {}, nil
	}
	prov.duckLakeCleanupMu.Lock()
	prov.duckLakeTxnMu.Lock()
	prov.ensureDuckLakeTxnStateLocked()
	if poisonErr := prov.duckLakeGenerationPoisonErrorLocked(); poisonErr != nil {
		prov.duckLakeTxnMu.Unlock()
		prov.duckLakeCleanupMu.Unlock()
		return func() {}, poisonErr
	}
	prov.duckLakeCleanupActive = true
	for !prov.duckLakeGenerationPoisoned &&
		(len(prov.duckLakeActiveTx) > 0 || prov.duckLakePendingTx > 0 || prov.duckLakeActiveOperations > 0) {
		prov.duckLakeTxnCond.Wait()
	}
	if poisonErr := prov.duckLakeGenerationPoisonErrorLocked(); poisonErr != nil {
		prov.duckLakeCleanupActive = false
		prov.duckLakeTxnCond.Broadcast()
		prov.duckLakeTxnMu.Unlock()
		prov.duckLakeCleanupMu.Unlock()
		return func() {}, poisonErr
	}
	prov.duckLakeTxnMu.Unlock()
	return func() {
		prov.duckLakeTxnMu.Lock()
		prov.duckLakeCleanupActive = false
		prov.duckLakeTxnCond.Broadcast()
		prov.duckLakeTxnMu.Unlock()
		prov.duckLakeCleanupMu.Unlock()
	}, nil
}

func (prov *DatabaseProvider) resetDuckLakeTransactions() {
	if prov == nil {
		return
	}
	// Keep this compatibility helper on the same generation-transition path as
	// provider Close/Restart. The transition drains anonymous/non-physical
	// operations before pool teardown, then lets physical transaction callbacks
	// drain before it resets bookkeeping.
	releaseTransition := prov.beginDuckLakeGenerationTransition()
	releaseTransition()
}

// duckLakeGenerationOperationsBlockLocked reports whether an admitted
// operation must finish before a storage-generation transition can enter pool
// teardown. A physical *sql.Tx owner is intentionally allowed through: pool
// teardown retires that transaction and the pool completion bridge releases
// the operation lease. Anonymous and logical/non-physical owners have no such
// physical completion boundary, so waiting for them here avoids entering a
// transition that could otherwise strand their lease.
//
// The caller must hold duckLakeTxnMu. Missing or inconsistent per-lease
// bookkeeping is treated conservatively as blocking rather than allowing a
// transition to reset state underneath an untracked operation.
func (prov *DatabaseProvider) duckLakeGenerationOperationsBlockLocked() bool {
	if prov == nil {
		return false
	}
	if prov.duckLakeAnonymousOps > 0 {
		return true
	}
	tracked := 0
	for _, lease := range prov.duckLakeOperationLeases {
		tracked++
		if !lease.owned {
			return true
		}
		owner, physical := lease.owner.(*stdsql.Tx)
		if !physical || owner == nil {
			return true
		}
	}
	// An aggregate count without a corresponding lease record can only come
	// from legacy/partially initialized state. Fail closed until that count is
	// released rather than silently dropping an operation during reset.
	return prov.duckLakeActiveOperations != tracked
}

// beginDuckLakeGenerationTransition announces an exclusive provider storage
// generation transition. It blocks admission of new DuckLake transactions and
// logical frontend operations, then waits for operations already admitted to
// finish. The returned release function must remain held across pool
// close/reset and reopen. Once those lifecycle calls have retired physical
// transactions, release waits for their finished hooks, resets the old
// generation bookkeeping, and reopens admission.
func (prov *DatabaseProvider) beginDuckLakeGenerationTransition() func() {
	release, _ := prov.beginDuckLakeGenerationTransitionWithError()
	return func() {
		if release != nil {
			_ = release()
		}
	}
}

// beginDuckLakeGenerationTransitionWithError is the error-preserving form used
// by Close and Restart. A poisoned generation is never reopened or reused; the
// release callback also reports a poison discovered during pool teardown.
func (prov *DatabaseProvider) beginDuckLakeGenerationTransitionWithError() (func() error, error) {
	if prov == nil || !prov.duckLakeObjectStorageEnabled() {
		return func() error { return nil }, nil
	}

	prov.duckLakeCleanupMu.Lock()
	prov.duckLakeTxnMu.Lock()
	prov.ensureDuckLakeTxnStateLocked()
	if poisonErr := prov.duckLakeGenerationPoisonErrorLocked(); poisonErr != nil {
		prov.duckLakeTxnMu.Unlock()
		prov.duckLakeCleanupMu.Unlock()
		return func() error { return nil }, poisonErr
	}
	prov.duckLakeCleanupActive = true
	// Do not wait for physical-owner operation leases here. Pool close/reset is
	// the operation that rolls those transactions back and invokes their
	// completion callbacks (COPY relies on this deferred boundary). Anonymous
	// and logical/non-physical leases have no pool callback that can release
	// them, so they must drain before the provider enters pool teardown.
	for !prov.duckLakeGenerationPoisoned &&
		(prov.duckLakePendingTx > 0 || prov.duckLakeGenerationOperationsBlockLocked()) {
		prov.duckLakeTxnCond.Wait()
	}
	if poisonErr := prov.duckLakeGenerationPoisonErrorLocked(); poisonErr != nil {
		prov.duckLakeCleanupActive = false
		prov.duckLakeTxnCond.Broadcast()
		prov.duckLakeTxnMu.Unlock()
		prov.duckLakeCleanupMu.Unlock()
		return func() error { return nil }, poisonErr
	}
	prov.duckLakeTxnMu.Unlock()

	var once sync.Once
	var releaseErr error
	release := func() error {
		once.Do(func() {
			prov.duckLakeTxnMu.Lock()
			prov.ensureDuckLakeTxnStateLocked()
			// Pool teardown should have rolled back and untracked every physical
			// transaction and completed every operation callback. Keep the wait here
			// so a delayed finished hook cannot be mistaken for a transaction or
			// operation in the replacement generation.
			for !prov.duckLakeGenerationPoisoned &&
				(len(prov.duckLakeActiveTx) > 0 || prov.duckLakeActiveOperations > 0 || prov.duckLakePendingTx > 0) {
				prov.duckLakeTxnCond.Wait()
			}
			if poisonErr := prov.duckLakeGenerationPoisonErrorLocked(); poisonErr != nil {
				// Keep malformed mappings and active-owner bookkeeping for recovery.
				// This generation is permanently closed, so wake admission waiters
				// without resetting state underneath a late finalizer.
				releaseErr = poisonErr
				prov.duckLakeCleanupActive = false
				prov.duckLakeTxnCond.Broadcast()
				prov.duckLakeTxnMu.Unlock()
				prov.duckLakeCleanupMu.Unlock()
				return
			}
			prov.duckLakeActiveTx = make(map[*stdsql.Tx]struct{})
			prov.duckLakePendingTx = 0
			prov.duckLakeActiveOperations = 0
			prov.duckLakeOperationOwners = make(map[any]int)
			prov.duckLakeOperationLeases = make(map[uint64]duckLakeOperationLease)
			prov.duckLakeAnonymousOps = 0
			// Keep operation IDs monotonic across generations. A late idempotent
			// release closure from the retired generation must never collide with a
			// newly admitted lease after admission reopens.
			prov.duckLakeCleanupActive = false
			prov.duckLakeTxnCond.Broadcast()
			prov.duckLakeTxnMu.Unlock()
			prov.duckLakeCleanupMu.Unlock()
		})
		return releaseErr
	}
	return release, nil
}

// duckLakeOrphanCleanupSQL is the product-owned cleanup form.  The table
// function returns one path column; selecting that column keeps this call
// stable across DuckLake extension revisions and, importantly, performs the
// cleanup rather than the dry-run/reporting variant.
const duckLakeOrphanCleanupSQL = `SELECT path
FROM ducklake_delete_orphaned_files('__myduck_ducklake', cleanup_all => true)`

// CleanupDuckLakeOrphans runs the product-owned orphan cleanup function on a
// newly acquired service connection. It is a compatibility entry point for
// maintenance callers that do not already hold a session connection.
func (prov *DatabaseProvider) CleanupDuckLakeOrphans(ctx context.Context) error {
	if prov == nil || prov.duckLake == nil || !prov.duckLakeObjectStorageEnabled() {
		return nil
	}
	if poisonErr := prov.duckLakeGenerationPoisonError(); poisonErr != nil {
		return poisonErr
	}
	if !mycontext.IsDuckLakeEligibleQuery(ctx) {
		return nil
	}
	if prov.storage == nil {
		return fmt.Errorf("ducklake storage is unavailable")
	}
	cleanupCtx := withoutCancelContext(ctx)
	conn, err := prov.storage.Conn(cleanupCtx)
	if err != nil {
		return fmt.Errorf("ducklake cleanup connection is unavailable")
	}
	cleanupErr := prov.CleanupDuckLakeOrphansOnConn(cleanupCtx, conn)
	if closeErr := conn.Close(); closeErr != nil && !errors.Is(closeErr, stdsql.ErrConnDone) {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("ducklake cleanup connection close failed"))
	}
	return cleanupErr
}

// CleanupDuckLakeOrphansOnConn performs orphan cleanup on the supplied
// physical connection. Callers that own a session transaction must invoke it
// only after that transaction has become inactive, and should pass the same
// connection so cleanup cannot acquire a second pooled connection.
func (prov *DatabaseProvider) CleanupDuckLakeOrphansOnConn(ctx context.Context, conn *stdsql.Conn) error {
	if prov == nil || prov.duckLake == nil || !prov.duckLakeObjectStorageEnabled() {
		return nil
	}
	if poisonErr := prov.duckLakeGenerationPoisonError(); poisonErr != nil {
		return poisonErr
	}
	if !mycontext.IsDuckLakeEligibleQuery(ctx) {
		return nil
	}
	if conn == nil {
		return fmt.Errorf("ducklake cleanup connection is unavailable")
	}
	cleanupCtx := withoutCancelContext(ctx)
	// Check the caller before entering the provider-wide barrier. The rollback
	// finalizer may invoke this method while its transaction is still represented
	// by the session, and beginDuckLakeCleanup would otherwise wait for that
	// transaction forever. A finalizer supplies an explicit owner marker after
	// it has removed the transaction; ordinary callers with an active session
	// transaction are rejected immediately.
	if ownerConn := rollbackCleanupOwnerConn(cleanupCtx); ownerConn != nil {
		if ownerConn != conn {
			return fmt.Errorf("ducklake cleanup connection is not the rollback owner")
		}
	} else if sqlCtx, ok := cleanupCtx.(*sql.Context); ok && sqlCtx != nil {
		if _, holderOK := sqlCtx.Session.(adapter.ConnectionHolder); holderOK {
			if _, activeTx := adapter.TryGetTxnBindingForRelease(sqlCtx); activeTx != nil {
				return fmt.Errorf("ducklake orphan cleanup requires an inactive transaction")
			}
		}
	}
	// ducklake_delete_orphaned_files(cleanup_all => true) mutates shared
	// catalog/data state. Exclude all active/new DuckLake transactions while it
	// discovers and deletes files, so another session's uncommitted files cannot
	// be classified as orphans. The admission check above must happen first so
	// this call cannot wait on its own active transaction.
	if !duckLakeCleanupLeaseHeld(cleanupCtx) {
		releaseCleanup, admissionErr := prov.beginDuckLakeCleanupWithError()
		if admissionErr != nil {
			return admissionErr
		}
		defer releaseCleanup()
		// The barrier above made cleanupActive visible to the connection guard.
		// Mark this already-reserved maintenance scope before Ensure enters Raw so
		// it does not reject its own cleanup transition.
		cleanupCtx = WithDuckLakeCleanupLease(cleanupCtx)
	}
	if err := prov.EnsureDuckLakeConnection(cleanupCtx, conn); err != nil {
		return fmt.Errorf("ducklake orphan cleanup setup failed: %w", err)
	}
	var cleanupErr error
	for _, physical := range prov.takeUncommittedLakeRelationsForConn(conn) {
		if _, err := conn.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS "+physical); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("ducklake uncommitted relation cleanup failed"))
		}
	}
	rows, err := conn.QueryContext(cleanupCtx, duckLakeOrphanCleanupSQL)
	if err != nil {
		return errors.Join(cleanupErr, fmt.Errorf("ducklake orphan cleanup query failed"))
	}
	columns, columnsErr := rows.Columns()
	if columnsErr != nil {
		cleanupErr = fmt.Errorf("ducklake orphan cleanup result inspection failed")
	} else {
		values := make([]any, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		for rows.Next() {
			if err := rows.Scan(dest...); err != nil {
				cleanupErr = fmt.Errorf("ducklake orphan cleanup result read failed")
				break
			}
		}
	}
	if err := rows.Err(); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("ducklake orphan cleanup execution failed"))
	}
	if err := rows.Close(); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("ducklake orphan cleanup result close failed"))
	}
	return cleanupErr
}

// duckLakeObjectStorageEnabled reports whether the service has a complete
// metadata/data pair. Extension-only configuration is valid for callers that
// need the DuckDB extensions but has no lake catalog for orphan cleanup.
func (prov *DatabaseProvider) duckLakeObjectStorageEnabled() bool {
	if prov == nil || prov.duckLake == nil {
		return false
	}
	return strings.TrimSpace(prov.duckLake.config.MetadataPath) != "" &&
		strings.TrimSpace(prov.duckLake.config.DataPath) != ""
}

// LakeSchemaName returns the deterministic physical schema used for a logical
// MyDuck catalog/schema pair. The default database retains its schema names so
// the attached catalog remains inspectable with the normal DuckLake helpers;
// additional database files are prefixed to avoid collisions in the single
// service lake catalog.
func (prov *DatabaseProvider) LakeSchemaName(catalogName, schemaName string) string {
	if prov == nil || catalogName == "" || catalogName == prov.defaultCatalogName {
		return schemaName
	}
	return "__myduck_" + lakeIdentifierPart(catalogName) + "__" + lakeIdentifierPart(schemaName)
}

func lakeIdentifierPart(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "default"
	}
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "default"
	}
	return b.String()
}

// ObjectTableName resolves a persisted object-table comment to its physical
// DuckLake relation. It deliberately consults the durable local shadow table
// on every call, so a provider restart cannot retain stale in-memory routing.
func (prov *DatabaseProvider) ObjectTableName(ctx *sql.Context, catalogName, schemaName, tableName string) (string, bool, error) {
	if prov == nil || prov.duckLake == nil {
		return "", false, nil
	}
	if strings.TrimSpace(catalogName) == "" || strings.TrimSpace(schemaName) == "" || strings.TrimSpace(tableName) == "" {
		return "", false, nil
	}
	execer, conn, _, release, err := adapter.GetCatalogExecutionSnapshotWithLease(ctx)
	if err != nil {
		return "", false, err
	}
	defer release()
	if err := prov.EnsureDuckLakeConnectionWithExecutor(ctx, execer, conn); err != nil {
		return "", false, err
	}
	return prov.objectTableNameWithExecutor(ctx, execer, catalogName, schemaName, tableName)
}

// ObjectTableNameWithExecutor resolves a persisted object-table comment using
// the caller's execution snapshot. The executor and connection must come from
// the same adapter.GetExecutionSnapshot call; this is important while a
// session transaction is active because metadata written in that transaction
// is only visible through its *sql.Tx.
func (prov *DatabaseProvider) ObjectTableNameWithExecutor(
	ctx *sql.Context,
	execer adapter.SQLExecutor,
	catalogName, schemaName, tableName string,
) (string, bool, error) {
	if prov == nil || prov.duckLake == nil {
		return "", false, nil
	}
	if strings.TrimSpace(catalogName) == "" || strings.TrimSpace(schemaName) == "" || strings.TrimSpace(tableName) == "" {
		return "", false, nil
	}
	if execer == nil {
		return "", false, fmt.Errorf("object-table metadata executor is unavailable")
	}
	return prov.objectTableNameWithExecutor(ctx, execer, catalogName, schemaName, tableName)
}

func (prov *DatabaseProvider) objectTableNameWithExecutor(
	ctx *sql.Context,
	execer adapter.SQLExecutor,
	catalogName, schemaName, tableName string,
) (string, bool, error) {
	rows, err := execer.QueryContext(ctx, `
		SELECT comment
		FROM duckdb_tables()
		WHERE database_name = ? AND schema_name = ? AND table_name = ?
	`, catalogName, schemaName, tableName)
	if err != nil {
		return "", false, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", false, err
		}
		return "", false, nil
	}
	var comment stdsql.NullString
	if err := rows.Scan(&comment); err != nil {
		return "", false, err
	}
	info := DecodeComment[ExtraTableInfo](comment.String).Meta
	if info.StorageKind() != TableStorageObject {
		return "", false, nil
	}
	return FullTableName(DuckLakeCatalogName, prov.LakeSchemaName(catalogName, schemaName), tableName), true, nil
}

// ObjectTables returns all durable object-table mappings for a logical
// catalog. The result is used by the protocol SQL rewriter and is intentionally
// sourced from duckdb_tables() rather than process-local state.
type ObjectTableMapping struct {
	Catalog        string
	Schema         string
	Table          string
	PhysicalName   string
	PhysicalSchema string
}

func (prov *DatabaseProvider) ObjectTables(ctx *sql.Context, catalogName string) ([]ObjectTableMapping, error) {
	if prov == nil || prov.duckLake == nil {
		return nil, nil
	}
	execer, conn, _, release, err := adapter.GetCatalogExecutionSnapshotWithLease(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := prov.EnsureDuckLakeConnectionWithExecutor(ctx, execer, conn); err != nil {
		return nil, err
	}
	return prov.objectTablesWithExecutor(ctx, execer, catalogName)
}

// ObjectTablesWithExecutor returns durable object-table mappings through an
// already selected execution snapshot. It never acquires another connection.
func (prov *DatabaseProvider) ObjectTablesWithExecutor(
	ctx *sql.Context,
	execer adapter.SQLExecutor,
	catalogName string,
) ([]ObjectTableMapping, error) {
	if prov == nil || prov.duckLake == nil {
		return nil, nil
	}
	if execer == nil {
		return nil, fmt.Errorf("object-table metadata executor is unavailable")
	}
	return prov.objectTablesWithExecutor(ctx, execer, catalogName)
}

func (prov *DatabaseProvider) objectTablesWithExecutor(
	ctx *sql.Context,
	execer adapter.SQLExecutor,
	catalogName string,
) ([]ObjectTableMapping, error) {
	rows, err := execer.QueryContext(ctx, `
		SELECT schema_name, table_name, comment
		FROM duckdb_tables()
		WHERE database_name = ?
	`, catalogName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var mappings []ObjectTableMapping
	for rows.Next() {
		var schemaName, tableName string
		var comment stdsql.NullString
		if err := rows.Scan(&schemaName, &tableName, &comment); err != nil {
			return nil, err
		}
		info := DecodeComment[ExtraTableInfo](comment.String).Meta
		if info.StorageKind() != TableStorageObject {
			continue
		}
		physicalSchema := prov.LakeSchemaName(catalogName, schemaName)
		mappings = append(mappings, ObjectTableMapping{
			Catalog: catalogName, Schema: schemaName, Table: tableName,
			PhysicalSchema: physicalSchema,
			PhysicalName:   FullTableName(DuckLakeCatalogName, physicalSchema, tableName),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return mappings, nil
}

func (prov *DatabaseProvider) initializeConnection(ctx context.Context, conn *stdsql.Conn) error {
	if prov.duckLake == nil || conn == nil {
		return nil
	}
	if err := prov.duckLakeConnectionAdmissionError(ctx); err != nil {
		return err
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
		return []sql.Database{}
	}
	defer rows.Close()

	all := []sql.Database{}
	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			return []sql.Database{}
		}

		switch schemaName {
		case "information_schema", "pg_catalog", "__sys__", "mysql":
			continue
		}

		all = append(all, newDatabase(schemaName, catalogName, prov))
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
		return newDatabase(name, catalogName, prov), nil
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

func (prov *DatabaseProvider) Restart(readOnly bool) (err error) {
	if prov == nil {
		return fmt.Errorf("database provider is unavailable")
	}
	// Keep the generation transition ahead of prov.mu for the same reason as
	// Close: admitted frontend work must be able to finish provider catalog
	// operations before pool teardown waits on its lifecycle.
	releaseTransition, transitionErr := prov.beginDuckLakeGenerationTransitionWithError()
	if transitionErr != nil {
		return transitionErr
	}
	defer func() {
		err = errors.Join(err, releaseTransition())
	}()
	if prov.mu != nil {
		prov.mu.Lock()
		defer prov.mu.Unlock()
	}

	err = prov.closeLocked()
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
