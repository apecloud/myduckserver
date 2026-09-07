package catalog

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/configuration"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

var errMetadataPublicationInjected = errors.New("metadata publication failure injected by test")

// metadataFailureConnector delegates to DuckDB while giving the test a single
// deterministic failure point after physical relation creation.
type metadataFailureConnector struct {
	inner driver.Connector
	fail  bool
}

func (c *metadataFailureConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.inner.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &metadataFailureConn{inner: conn, owner: c}, nil
}

func (c *metadataFailureConnector) Driver() driver.Driver {
	return c.inner.Driver()
}

type metadataFailureConn struct {
	inner driver.Conn
	owner *metadataFailureConnector
}

func (c *metadataFailureConn) Prepare(query string) (driver.Stmt, error) {
	return c.inner.Prepare(query)
}

func (c *metadataFailureConn) Close() error {
	return c.inner.Close()
}

func (c *metadataFailureConn) Begin() (driver.Tx, error) {
	return c.inner.Begin()
}

func (c *metadataFailureConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if begin, ok := c.inner.(driver.ConnBeginTx); ok {
		return begin.BeginTx(ctx, opts)
	}
	return c.inner.Begin()
}

func (c *metadataFailureConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.shouldFail(query) {
		return nil, errMetadataPublicationInjected
	}
	execer, ok := c.inner.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return execer.ExecContext(ctx, query, args)
}

func (c *metadataFailureConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := c.inner.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return queryer.QueryContext(ctx, query, args)
}

func (c *metadataFailureConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if preparer, ok := c.inner.(driver.ConnPrepareContext); ok {
		return preparer.PrepareContext(ctx, query)
	}
	return c.inner.Prepare(query)
}

func (c *metadataFailureConn) CheckNamedValue(value *driver.NamedValue) error {
	if checker, ok := c.inner.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(value)
	}
	return nil
}

func (c *metadataFailureConn) Ping(ctx context.Context) error {
	if pinger, ok := c.inner.(driver.Pinger); ok {
		return pinger.Ping(ctx)
	}
	return nil
}

func (c *metadataFailureConn) ResetSession(ctx context.Context) error {
	if resetter, ok := c.inner.(driver.SessionResetter); ok {
		return resetter.ResetSession(ctx)
	}
	return nil
}

func (c *metadataFailureConn) IsValid() bool {
	if validator, ok := c.inner.(driver.Validator); ok {
		return validator.IsValid()
	}
	return true
}

func (c *metadataFailureConn) shouldFail(query string) bool {
	if c.owner == nil || !c.owner.fail {
		return false
	}
	if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "COMMENT ON TABLE") {
		return false
	}
	c.owner.fail = false
	return true
}

// metadataFailureSession is the smallest real session surface needed by the
// catalog bridge. It retains one physical connection and exposes its active
// transaction as an atomic pair to adapter helpers.
type metadataFailureSession struct {
	*memory.Session
	mu   sync.Mutex
	conn *stdsql.Conn
	tx   *stdsql.Tx
}

var _ adapter.ConnectionHolder = (*metadataFailureSession)(nil)
var _ adapter.TransactionBindingHolder = (*metadataFailureSession)(nil)
var _ adapter.ExecutionSnapshotHolder = (*metadataFailureSession)(nil)
var _ adapter.IdentityTransactionCloser = (*metadataFailureSession)(nil)

func (s *metadataFailureSession) GetConn(context.Context) (*stdsql.Conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn, nil
}

func (s *metadataFailureSession) GetCatalogConn(context.Context) (*stdsql.Conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn, nil
}

func (s *metadataFailureSession) GetTxn(ctx context.Context, options *stdsql.TxOptions) (*stdsql.Tx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tx != nil {
		return s.tx, nil
	}
	tx, err := s.conn.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	s.tx = tx
	return tx, nil
}

func (s *metadataFailureSession) GetCatalogTxn(ctx context.Context, options *stdsql.TxOptions) (*stdsql.Tx, error) {
	return s.GetTxn(ctx, options)
}

func (s *metadataFailureSession) TryGetTxn() *stdsql.Tx {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tx
}

func (s *metadataFailureSession) GetTxnBinding() (*stdsql.Conn, *stdsql.Tx) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn, s.tx
}

func (s *metadataFailureSession) GetExecutionSnapshot(context.Context, bool) (*stdsql.Conn, *stdsql.Tx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn, s.tx, nil
}

func (s *metadataFailureSession) GetCurrentCatalog() string { return "memory" }

func (s *metadataFailureSession) GetCurrentSchema() string { return "main" }

func (s *metadataFailureSession) CloseTxn() {
	s.mu.Lock()
	s.tx = nil
	s.mu.Unlock()
}

func (s *metadataFailureSession) CloseTxnIf(tx *stdsql.Tx) {
	s.mu.Lock()
	if s.tx == tx {
		s.tx = nil
	}
	s.mu.Unlock()
}

func (s *metadataFailureSession) CloseConn() {}

func TestRecordTableStorageSelectionRollsBackPhysicalRelationOnMetadataFailure(t *testing.T) {
	inner, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	failing := &metadataFailureConnector{inner: inner}
	db := stdsql.OpenDB(failing)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, inner.Close())
	})

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	// Use a normal attached DuckDB catalog as a transaction-aware stand-in for
	// the DuckLake relation. The provider runtime cache is marked initialized so
	// this test exercises catalog publication/rollback without extension files.
	_, err = conn.ExecContext(ctx, `ATTACH ':memory:' AS "__myduck_ducklake"`)
	require.NoError(t, err)
	provider := &DatabaseProvider{duckLake: &duckLakeRuntime{config: configuration.DuckLakeConfig{
		MetadataPath: t.TempDir() + "/catalog.ducklake",
		DataPath:     t.TempDir(),
	}}}
	physicalSchema := provider.LakeSchemaName("memory", "main")
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS "__myduck_ducklake"."`+physicalSchema+`"`)
	require.NoError(t, err)

	const tableName = "metadata_failure_table"
	shadowName := FullTableName("memory", "main", tableName)
	_, err = conn.ExecContext(ctx, `CREATE TABLE `+shadowName+` (id INTEGER NULL)`)
	require.NoError(t, err)
	shadowComment := NewCommentWithMeta("keep this comment", ExtraTableInfo{
		Storage: TableStorageLocal,
	}).Encode()
	_, err = conn.ExecContext(ctx, `COMMENT ON TABLE `+shadowName+` IS '`+shadowComment+`'`)
	require.NoError(t, err)

	runtime := provider.duckLake
	require.NoError(t, conn.Raw(func(raw any) error {
		runtime.initialized.Store(raw, struct{}{})
		runtime.attached.Store(raw, struct{}{})
		return nil
	}))
	session := &metadataFailureSession{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		conn:    conn,
	}
	sqlCtx := sql.NewContext(mycontext.WithFrontendQuery(ctx), sql.WithSession(session))
	database := NewDatabaseWithProvider("main", "memory", provider)
	failing.fail = true

	err = database.RecordTableStorageSelection(sqlCtx, tableName, TableStorageSelection{
		Kind:     TableStorageObject,
		Explicit: true,
		Source:   "test",
	})
	require.ErrorContains(t, err, errMetadataPublicationInjected.Error())
	require.Nil(t, session.TryGetTxn(), "scoped transaction must be released after publication failure")

	// The physical CREATE and metadata publication share the scoped transaction.
	// A rollback must remove the relation rather than leave a routable object
	// behind when publication fails.
	var physicalCount int
	require.NoError(t, conn.QueryRowContext(ctx, `
		SELECT count(*)
		FROM duckdb_tables()
		WHERE database_name = ? AND schema_name = ? AND table_name = ?
	`, DuckLakeCatalogName, physicalSchema, tableName).Scan(&physicalCount))
	require.Zero(t, physicalCount)

	var rawComment string
	require.NoError(t, conn.QueryRowContext(ctx, `
		SELECT comment
		FROM duckdb_tables()
		WHERE database_name = ? AND schema_name = ? AND table_name = ?
	`, "memory", "main", tableName).Scan(&rawComment))
	require.Equal(t, TableStorageLocal, DecodeComment[ExtraTableInfo](rawComment).Meta.StorageKind())
}

// The PostgreSQL parser bridge supplies an already-captured executor and must
// still materialize the DuckLake relation. This exercises the executor-affine
// entry point directly; unlike RecordTableStorageSelection, it cannot open a
// second scoped transaction and therefore must not silently skip the physical
// CREATE.
func TestRecordTableStorageSelectionWithExecutorMaterializesPhysicalRelation(t *testing.T) {
	inner, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(inner)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, inner.Close())
	})

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	_, err = conn.ExecContext(ctx, `ATTACH ':memory:' AS "__myduck_ducklake"`)
	require.NoError(t, err)
	provider := &DatabaseProvider{duckLake: &duckLakeRuntime{config: configuration.DuckLakeConfig{
		MetadataPath: t.TempDir() + "/catalog.ducklake",
		DataPath:     t.TempDir(),
	}}}
	physicalSchema := provider.LakeSchemaName("memory", "main")
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS "__myduck_ducklake"."`+physicalSchema+`"`)
	require.NoError(t, err)

	const tableName = "pg_executor_object"
	shadowName := FullTableName("memory", "main", tableName)
	_, err = conn.ExecContext(ctx, `CREATE TABLE `+shadowName+` (id INTEGER NULL)`)
	require.NoError(t, err)
	shadowComment := NewCommentWithMeta("pg bridge", ExtraTableInfo{Storage: TableStorageLocal}).Encode()
	_, err = conn.ExecContext(ctx, `COMMENT ON TABLE `+shadowName+` IS '`+shadowComment+`'`)
	require.NoError(t, err)

	runtime := provider.duckLake
	require.NoError(t, conn.Raw(func(raw any) error {
		runtime.initialized.Store(raw, struct{}{})
		runtime.attached.Store(raw, struct{}{})
		return nil
	}))
	session := &metadataFailureSession{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		conn:    conn,
	}
	sqlCtx := sql.NewContext(mycontext.WithFrontendQuery(ctx), sql.WithSession(session))
	database := NewDatabaseWithProvider("main", "memory", provider)
	execer := adapter.SQLExecutorForConn(sqlCtx, conn)

	err = database.RecordTableStorageSelectionWithExecutor(sqlCtx, tableName, TableStorageSelection{
		Kind:     TableStorageObject,
		Explicit: true,
		Source:   "postgres",
	}, execer, conn)
	require.NoError(t, err)

	var physicalCount int
	require.NoError(t, conn.QueryRowContext(ctx, `
		SELECT count(*)
		FROM duckdb_tables()
		WHERE database_name = ? AND schema_name = ? AND table_name = ?
	`, DuckLakeCatalogName, physicalSchema, tableName).Scan(&physicalCount))
	require.Equal(t, 1, physicalCount)
}

func TestDuckDBRejectsWritesToSecondAttachedDatabaseInOneTransaction(t *testing.T) {
	inner, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(inner)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, inner.Close())
	})
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	_, err = conn.ExecContext(ctx, `ATTACH ':memory:' AS "__myduck_ducklake"`)
	require.NoError(t, err)

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `CREATE TABLE local_shadow (id INTEGER)`)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS "__myduck_ducklake"."main"`)
	require.Error(t, err)
	require.Contains(t, err.Error(), `already modified database`)
}

func TestDedicatedConnectionWritesLakeAfterSessionTransactionTouchedLocal(t *testing.T) {
	inner, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(inner)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, inner.Close())
	})
	ctx := context.Background()
	sessionConn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sessionConn.Close()) })
	lakeConn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, lakeConn.Close()) })

	// DuckDB refuses to attach the same file path on two connections of one
	// process. The product lake uses ATTACH IF NOT EXISTS ducklake:... per
	// connection; this stand-in only needs a second connection whose writes
	// are not in the session transaction.
	_, err = lakeConn.ExecContext(ctx, `ATTACH ':memory:' AS "__myduck_ducklake"`)
	require.NoError(t, err)

	tx, err := sessionConn.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `CREATE TABLE local_shadow (id INTEGER)`)
	require.NoError(t, err)
	_, err = lakeConn.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS "__myduck_ducklake"."main"`)
	require.NoError(t, err)
	_, err = lakeConn.ExecContext(ctx, `CREATE TABLE "__myduck_ducklake"."main"."pg_object" (id INTEGER)`)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())

	var shadowCount int
	require.NoError(t, sessionConn.QueryRowContext(ctx, `
		SELECT count(*) FROM duckdb_tables()
		WHERE database_name = current_database() AND table_name = 'local_shadow'
	`).Scan(&shadowCount))
	require.Zero(t, shadowCount, "user rollback must remove the local shadow")

	var lakeCount int
	require.NoError(t, lakeConn.QueryRowContext(ctx, `
		SELECT count(*) FROM duckdb_tables()
		WHERE database_name = '__myduck_ducklake' AND schema_name = 'main' AND table_name = 'pg_object'
	`).Scan(&lakeCount))
	require.Equal(t, 1, lakeCount, "dedicated lake create survives session rollback until compensation")

	_, err = lakeConn.ExecContext(ctx, `DROP TABLE IF EXISTS "__myduck_ducklake"."main"."pg_object"`)
	require.NoError(t, err)
	require.NoError(t, lakeConn.QueryRowContext(ctx, `
		SELECT count(*) FROM duckdb_tables()
		WHERE database_name = '__myduck_ducklake' AND schema_name = 'main' AND table_name = 'pg_object'
	`).Scan(&lakeCount))
	require.Zero(t, lakeCount)
}

func TestUncommittedLakeRelationCompensationRegistry(t *testing.T) {
	provider := &DatabaseProvider{}
	tx := new(stdsql.Tx)
	conn := new(stdsql.Conn)
	const physical = `"__myduck_ducklake"."main"."t"`
	provider.registerUncommittedLakeRelation(tx, conn, physical)
	require.Equal(t, []string{physical}, provider.takeUncommittedLakeRelationsForConn(conn))
	require.Empty(t, provider.takeUncommittedLakeRelationsForConn(conn))

	provider.registerUncommittedLakeRelation(tx, conn, physical)
	provider.forgetUncommittedLakeRelation(tx, physical)
	require.Empty(t, provider.takeUncommittedLakeRelationsForConn(conn))
}

func TestMaterializeObjectTableFailsWhenSessionTxnAlreadyWroteLocal(t *testing.T) {
	inner, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(inner)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, inner.Close())
	})
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	_, err = conn.ExecContext(ctx, `ATTACH ':memory:' AS "__myduck_ducklake"`)
	require.NoError(t, err)
	provider := &DatabaseProvider{duckLake: &duckLakeRuntime{config: configuration.DuckLakeConfig{
		MetadataPath: t.TempDir() + "/catalog.ducklake",
		DataPath:     t.TempDir(),
	}}}
	physicalSchema := provider.LakeSchemaName("memory", "main")
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS "__myduck_ducklake"."`+physicalSchema+`"`)
	require.NoError(t, err)

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	const tableName = "pg_same_txn_object"
	shadowName := FullTableName("memory", "main", tableName)
	_, err = tx.ExecContext(ctx, `CREATE TABLE `+shadowName+` (id INTEGER NULL)`)
	require.NoError(t, err)

	runtime := provider.duckLake
	require.NoError(t, conn.Raw(func(raw any) error {
		runtime.initialized.Store(raw, struct{}{})
		runtime.attached.Store(raw, struct{}{})
		return nil
	}))
	session := &metadataFailureSession{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		conn:    conn,
		tx:      tx,
	}
	sqlCtx := sql.NewContext(mycontext.WithFrontendQuery(ctx), sql.WithSession(session))
	database := NewDatabaseWithProvider("main", "memory", provider)
	err = database.materializeObjectTableWithExecutor(sqlCtx, tableName, tx, conn)
	require.Error(t, err)
	require.Contains(t, err.Error(), "create DuckLake schema failed")
}
