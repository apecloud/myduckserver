package catalog

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/configuration"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
)

// callsiteLeaseSession models the atomic session surfaces used by catalog
// callsites without involving ConnectionPool internals. The release callback
// lets each test inspect driver expectations at the exact lease boundary.
type callsiteLeaseSession struct {
	*memory.Session

	mu      sync.Mutex
	conn    *stdsql.Conn
	tx      *stdsql.Tx
	release func()
}

var _ adapter.ConnectionHolder = (*callsiteLeaseSession)(nil)
var _ adapter.TransactionBindingHolder = (*callsiteLeaseSession)(nil)
var _ adapter.ReleaseTransactionBindingHolder = (*callsiteLeaseSession)(nil)
var _ adapter.ExecutionSnapshotHolder = (*callsiteLeaseSession)(nil)
var _ adapter.ExecutionSnapshotLeaseHolder = (*callsiteLeaseSession)(nil)
var _ adapter.IdentityTransactionCloser = (*callsiteLeaseSession)(nil)

func (s *callsiteLeaseSession) GetConn(context.Context) (*stdsql.Conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn, nil
}

func (s *callsiteLeaseSession) GetCatalogConn(ctx context.Context) (*stdsql.Conn, error) {
	return s.GetConn(ctx)
}

func (s *callsiteLeaseSession) GetTxn(ctx context.Context, options *stdsql.TxOptions) (*stdsql.Tx, error) {
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

func (s *callsiteLeaseSession) GetCatalogTxn(ctx context.Context, options *stdsql.TxOptions) (*stdsql.Tx, error) {
	return s.GetTxn(ctx, options)
}

func (s *callsiteLeaseSession) TryGetTxn() *stdsql.Tx {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tx
}

func (s *callsiteLeaseSession) GetTxnBinding() (*stdsql.Conn, *stdsql.Tx) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn, s.tx
}

func (s *callsiteLeaseSession) GetTxnBindingForRelease() (*stdsql.Conn, *stdsql.Tx) {
	return s.GetTxnBinding()
}

func (s *callsiteLeaseSession) GetExecutionSnapshot(context.Context, bool) (*stdsql.Conn, *stdsql.Tx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn, s.tx, nil
}

func (s *callsiteLeaseSession) GetExecutionSnapshotLease(context.Context, bool) (*stdsql.Conn, *stdsql.Tx, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn, s.tx, s.release, nil
}

func (s *callsiteLeaseSession) GetCurrentCatalog() string { return "memory" }

func (s *callsiteLeaseSession) GetCurrentSchema() string { return "main" }

func (s *callsiteLeaseSession) CloseTxn() {
	s.mu.Lock()
	s.tx = nil
	s.mu.Unlock()
}

func (s *callsiteLeaseSession) CloseTxnIf(tx *stdsql.Tx) {
	s.mu.Lock()
	if s.tx == tx {
		s.tx = nil
	}
	s.mu.Unlock()
}

func (s *callsiteLeaseSession) CloseConn() {}

type callsiteLeaseObserver struct {
	count atomic.Int32

	mu    sync.Mutex
	err   error
	check func() error
}

func (o *callsiteLeaseObserver) release() {
	o.count.Add(1)
	var err error
	if o.check != nil {
		err = o.check()
	}
	o.mu.Lock()
	o.err = errors.Join(o.err, err)
	o.mu.Unlock()
}

func (o *callsiteLeaseObserver) requireReleasedOnce(t *testing.T) {
	t.Helper()
	require.Equal(t, int32(1), o.count.Load())
	o.mu.Lock()
	defer o.mu.Unlock()
	require.NoError(t, o.err, "lease released before all owned work completed")
}

func newCallsiteLeaseContext(t *testing.T, id uint32) (*sql.Context, *callsiteLeaseSession, sqlmock.Sqlmock, *callsiteLeaseObserver) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
		_ = db.Close()
	})

	base := memory.NewSession(sql.NewBaseSession(), nil)
	base.SetConnectionId(id)
	base.SetCurrentDatabase("memory")
	observer := &callsiteLeaseObserver{}
	session := &callsiteLeaseSession{
		Session: base,
		conn:    conn,
		release: observer.release,
	}
	sqlCtx := sql.NewContext(
		mycontext.WithFrontendQuery(context.Background()),
		sql.WithSession(session),
	)
	return sqlCtx, session, mock, observer
}

func newCallsiteRowInserter() *rowInserter {
	return &rowInserter{
		db:    "main",
		table: "lease_rows",
		schema: sql.Schema{
			&sql.Column{Name: "id", Type: types.Int32, Nullable: false},
			&sql.Column{Name: "payload", Type: types.Text, Nullable: true},
		},
	}
}

func expectRowInserterCreate(mock sqlmock.Sqlmock, id uint32) {
	tmpTable := fmt.Sprintf("main_lease_rows_%d", id)
	mock.ExpectExec(regexp.QuoteMeta(
		`CREATE TEMP TABLE IF NOT EXISTS ` + QuoteIdentifierANSI(tmpTable) +
			` AS FROM "main"."lease_rows" LIMIT 0`,
	)).WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectRowInserterDrop(mock sqlmock.Sqlmock, id uint32) {
	tmpTable := fmt.Sprintf("main_lease_rows_%d", id)
	mock.ExpectExec(regexp.QuoteMeta(
		`DROP TABLE IF EXISTS temp.main.` + QuoteIdentifierANSI(tmpTable),
	)).WillReturnResult(sqlmock.NewResult(0, 0))
}

func TestRowInserterExecutionLeaseTerminalPaths(t *testing.T) {
	const sessionID = uint32(701)
	insertSQL := `INSERT INTO "main_lease_rows_701" VALUES (?, ?)`
	flushSQL := `INSERT  INTO "main"."lease_rows" FROM "main_lease_rows_701"`

	t.Run("initialization error", func(t *testing.T) {
		sqlCtx, _, mock, observer := newCallsiteLeaseContext(t, sessionID)
		ri := newCallsiteRowInserter()
		initErr := errors.New("prepare failed")

		expectRowInserterCreate(mock, sessionID)
		mock.ExpectPrepare(regexp.QuoteMeta(insertSQL)).WillReturnError(initErr)
		expectRowInserterDrop(mock, sessionID)
		observer.check = func() error {
			if ri.stmt != nil {
				return errors.New("prepared statement remained live at lease release")
			}
			return mock.ExpectationsWereMet()
		}

		ri.StatementBegin(sqlCtx)
		require.ErrorIs(t, ri.StatementComplete(sqlCtx), initErr)
		require.ErrorIs(t, ri.DiscardChanges(sqlCtx, initErr), initErr)
		ri.releaseSnapshot()
		ri.releaseSnapshot()

		observer.requireReleasedOnce(t)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("insert and close", func(t *testing.T) {
		sqlCtx, _, mock, observer := newCallsiteLeaseContext(t, sessionID)
		ri := newCallsiteRowInserter()

		expectRowInserterCreate(mock, sessionID)
		prepared := mock.ExpectPrepare(regexp.QuoteMeta(insertSQL)).WillBeClosed()
		prepared.ExpectExec().WithArgs(int64(7), "kept").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(regexp.QuoteMeta(flushSQL)).WillReturnResult(sqlmock.NewResult(0, 1))
		expectRowInserterDrop(mock, sessionID)
		observer.check = func() error {
			if ri.stmt != nil {
				return errors.New("prepared statement remained live at lease release")
			}
			return mock.ExpectationsWereMet()
		}

		require.NoError(t, ri.Insert(sqlCtx, sql.Row{int32(7), "kept"}))
		require.NoError(t, ri.Close(sqlCtx))
		ri.releaseSnapshot()

		observer.requireReleasedOnce(t)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("discard changes", func(t *testing.T) {
		sqlCtx, _, mock, observer := newCallsiteLeaseContext(t, sessionID)
		ri := newCallsiteRowInserter()

		expectRowInserterCreate(mock, sessionID)
		prepared := mock.ExpectPrepare(regexp.QuoteMeta(insertSQL)).WillBeClosed()
		prepared.ExpectExec().WithArgs(int64(8), "discarded").WillReturnResult(sqlmock.NewResult(0, 1))
		expectRowInserterDrop(mock, sessionID)
		observer.check = func() error {
			if ri.stmt != nil {
				return errors.New("prepared statement remained live at lease release")
			}
			return mock.ExpectationsWereMet()
		}

		require.NoError(t, ri.Insert(sqlCtx, sql.Row{int32(8), "discarded"}))
		require.NoError(t, ri.DiscardChanges(sqlCtx, errors.New("statement failed")))
		ri.releaseSnapshot()

		observer.requireReleasedOnce(t)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("cleanup after transaction rollback", func(t *testing.T) {
		sqlCtx, session, mock, observer := newCallsiteLeaseContext(t, sessionID)
		ri := newCallsiteRowInserter()

		mock.ExpectBegin()
		tx, err := session.GetTxn(sqlCtx, nil)
		require.NoError(t, err)
		expectRowInserterCreate(mock, sessionID)
		prepared := mock.ExpectPrepare(regexp.QuoteMeta(insertSQL)).WillBeClosed()
		prepared.ExpectExec().WithArgs(int64(9), "rolled back").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectRollback()
		expectRowInserterDrop(mock, sessionID)
		observer.check = func() error {
			if ri.stmt != nil {
				return errors.New("prepared statement remained live at lease release")
			}
			if session.TryGetTxn() != nil {
				return errors.New("rolled-back transaction remained bound at lease release")
			}
			return mock.ExpectationsWereMet()
		}

		require.NoError(t, ri.Insert(sqlCtx, sql.Row{int32(9), "rolled back"}))
		owner, inactive, rollbackErr := adapter.FinalizeRollback(sqlCtx, tx)
		require.Same(t, session.conn, owner)
		require.True(t, inactive)
		require.NoError(t, rollbackErr)
		require.NoError(t, ri.DiscardChanges(sqlCtx, errors.New("transaction rolled back")))
		ri.releaseSnapshot()

		observer.requireReleasedOnce(t)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestRecordTableStorageSelectionScopedTransactionLease(t *testing.T) {
	const (
		sessionID = uint32(702)
		tableName = "lease_storage"
		comment   = "metadata consumed before release"
	)

	for _, test := range []struct {
		name        string
		metadataErr error
	}{
		{name: "commit"},
		{name: "rollback after metadata write failure", metadataErr: errors.New("metadata write failed")},
	} {
		t.Run(test.name, func(t *testing.T) {
			sqlCtx, session, mock, observer := newCallsiteLeaseContext(t, sessionID)
			provider := &DatabaseProvider{
				defaultCatalogName: "memory",
				duckLake: &duckLakeRuntime{config: configuration.DuckLakeConfig{
					MetadataPath: "/tmp/task77/catalog.ducklake",
					DataPath:     "/tmp/task77/data",
				}},
			}
			database := NewDatabaseWithProvider("main", "memory", provider)
			storage := TableStorageSelection{Kind: TableStorageObject, Explicit: true, Source: "test"}
			physical := database.objectPhysicalTableName(tableName)
			encoded := NewCommentWithMeta(comment, ExtraTableInfo{Storage: TableStorageObject}).Encode()

			mock.ExpectBegin()
			mock.ExpectQuery(`FROM duckdb_columns\(\)`).
				WithArgs("memory", "main", tableName).
				WillReturnRows(sqlmock.NewRows([]string{
					"column_name", "data_type", "is_nullable", "column_default",
				}).AddRow("id", "INTEGER", true, nil)).
				RowsWillBeClosed()
			mock.ExpectQuery(`FROM duckdb_constraints\(\)`).
				WithArgs("memory", "main", tableName).
				WillReturnRows(sqlmock.NewRows([]string{"constraint_type"})).
				RowsWillBeClosed()
			mock.ExpectExec(regexp.QuoteMeta(
				`CREATE SCHEMA IF NOT EXISTS "__myduck_ducklake"."main"`,
			)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec(regexp.QuoteMeta(
				`CREATE TABLE ` + physical + ` ("id" INTEGER NULL)`,
			)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(`SELECT comment FROM duckdb_tables\(\)`).
				WithArgs("memory", "main", tableName).
				WillReturnRows(sqlmock.NewRows([]string{"comment"}).AddRow(comment)).
				RowsWillBeClosed()
			metadata := mock.ExpectExec(regexp.QuoteMeta(
				`COMMENT ON TABLE ` + FullTableName("memory", "main", tableName) + ` IS '` + encoded + `'`,
			))
			if test.metadataErr != nil {
				metadata.WillReturnError(test.metadataErr)
				mock.ExpectRollback()
			} else {
				metadata.WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectCommit()
			}
			observer.check = func() error {
				if session.TryGetTxn() != nil {
					return errors.New("scoped transaction remained bound at lease release")
				}
				return mock.ExpectationsWereMet()
			}

			err := database.RecordTableStorageSelection(sqlCtx, tableName, storage)
			if test.metadataErr != nil {
				require.ErrorContains(t, err, test.metadataErr.Error())
			} else {
				require.NoError(t, err)
			}

			observer.requireReleasedOnce(t)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
