package backend

import (
	"context"
	stdsql "database/sql"
	"io"
	"sync/atomic"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/apecloud/myduckserver/adapter"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
)

// loadDataLeaseSession is a small production-shaped session double. It
// exposes the atomic execution snapshot surface while keeping the test
// independent of a live DuckLake extension.
type loadDataLeaseSession struct {
	*memory.Session
	conn         *stdsql.Conn
	releaseCalls atomic.Int32
}

var _ adapter.ConnectionHolder = (*loadDataLeaseSession)(nil)
var _ adapter.ExecutionSnapshotLeaseHolder = (*loadDataLeaseSession)(nil)

func (s *loadDataLeaseSession) GetConn(context.Context) (*stdsql.Conn, error) {
	return s.conn, nil
}

func (s *loadDataLeaseSession) GetCatalogConn(context.Context) (*stdsql.Conn, error) {
	return s.conn, nil
}

func (s *loadDataLeaseSession) GetTxn(context.Context, *stdsql.TxOptions) (*stdsql.Tx, error) {
	return nil, nil
}

func (s *loadDataLeaseSession) GetCatalogTxn(context.Context, *stdsql.TxOptions) (*stdsql.Tx, error) {
	return nil, nil
}

func (s *loadDataLeaseSession) TryGetTxn() *stdsql.Tx { return nil }

func (s *loadDataLeaseSession) GetCurrentCatalog() string { return "memory" }

func (s *loadDataLeaseSession) GetCurrentSchema() string { return "main" }

func (s *loadDataLeaseSession) CloseTxn() {}

func (s *loadDataLeaseSession) CloseConn() {}

func (s *loadDataLeaseSession) GetExecutionSnapshotLease(
	context.Context,
	bool,
) (*stdsql.Conn, *stdsql.Tx, func(), error) {
	return s.conn, nil, func() { s.releaseCalls.Add(1) }, nil
}

type loadDataTestTable struct {
	name   string
	schema sql.PrimaryKeySchema
}

var _ sql.InsertableTable = (*loadDataTestTable)(nil)

func (t *loadDataTestTable) Name() string { return t.name }

func (t *loadDataTestTable) String() string { return t.name }

func (t *loadDataTestTable) Schema(*sql.Context) sql.Schema { return t.schema.Schema }

func (t *loadDataTestTable) Collation() sql.CollationID { return sql.Collation_Default }

func (t *loadDataTestTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(), nil
}

func (t *loadDataTestTable) PartitionRows(*sql.Context, sql.Partition) (sql.RowIter, error) {
	return nil, nil
}

func (t *loadDataTestTable) Inserter(*sql.Context) sql.RowInserter { return nil }

func newLoadDataLeaseFixture(t *testing.T) (*sql.Context, *loadDataLeaseSession, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	session := &loadDataLeaseSession{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		conn:    conn,
	}
	session.SetCurrentDatabase("main")
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))
	cleanup := func() {
		_ = conn.Close()
		_ = db.Close()
	}
	return ctx, session, mock, cleanup
}

func newLoadDataTestInputs() (*plan.InsertInto, sql.InsertableTable, *plan.LoadData) {
	schema := sql.NewPrimaryKeySchema(sql.Schema{
		&sql.Column{Name: "id", Type: types.Int64},
	})
	table := &loadDataTestTable{name: "load_target", schema: schema}
	insert := plan.NewInsertInto(memory.NewDatabase("main"), nil, nil, false, nil, nil, false)
	load := plan.NewLoadData(false, "input.csv", schema.Schema, nil, nil, 0, "")
	return insert, table, load
}

func expectLoadDataExec(mock sqlmock.Sqlmock) {
	mock.ExpectExec(`(?s).*INSERT INTO.*read_csv.*`).WillReturnResult(sqlmock.NewResult(17, 3))
}

func TestExecuteLoadDataRetainsSnapshotLeaseUntilResultEOF(t *testing.T) {
	ctx, session, mock, cleanup := newLoadDataLeaseFixture(t)
	defer cleanup()
	expectLoadDataExec(mock)
	insert, table, load := newLoadDataTestInputs()

	iter, err := (&DuckBuilder{}).executeLoadData(ctx, insert, table, load, "input.csv")
	require.NoError(t, err)
	require.Zero(t, session.releaseCalls.Load(), "the snapshot must cover the returned result iterator")

	_, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Zero(t, session.releaseCalls.Load(), "the lease must remain while the OK row is being consumed")

	_, err = iter.Next(ctx)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, int32(1), session.releaseCalls.Load())
	require.NoError(t, iter.Close(ctx))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteLoadDataReleasesSnapshotLeaseOnEarlyClose(t *testing.T) {
	ctx, session, mock, cleanup := newLoadDataLeaseFixture(t)
	defer cleanup()
	expectLoadDataExec(mock)
	insert, table, load := newLoadDataTestInputs()

	iter, err := (&DuckBuilder{}).executeLoadData(ctx, insert, table, load, "input.csv")
	require.NoError(t, err)
	require.ErrorIs(t, iter.Close(ctx), errDuckLakeOperationClosedBeforeEOF)
	require.Equal(t, int32(1), session.releaseCalls.Load())
	require.NoError(t, mock.ExpectationsWereMet())
}
