package adapter_test

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/apecloud/myduckserver/adapter"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

type snapshotHolder struct {
	*memory.Session
	conn       *stdsql.Conn
	queries    []bool
	activeTx   *stdsql.Tx
	currentCat string
	currentSch string
}

// legacyHolder intentionally implements only ConnectionHolder. It models the
// small session doubles that predate the atomic transaction-owner snapshot.
type legacyHolder struct {
	*memory.Session
	conn         *stdsql.Conn
	activeTx     *stdsql.Tx
	getConnCalls int
	onGetConn    func()
}

type leaseHolder struct {
	*snapshotHolder
	leasedTx     *stdsql.Tx
	releaseCalls atomic.Int32
	onRelease    func()
	leaseErr     error
}

func (h *leaseHolder) GetExecutionSnapshotLease(_ context.Context, catalogOnly bool) (*stdsql.Conn, *stdsql.Tx, func(), error) {
	h.queries = append(h.queries, catalogOnly)
	tx := h.activeTx
	if h.leasedTx != nil {
		tx = h.leasedTx
	}
	release := func() {
		h.releaseCalls.Add(1)
		if h.onRelease != nil {
			h.onRelease()
		}
	}
	return h.conn, tx, release, h.leaseErr
}

type leaseTransitionWaiter struct {
	released chan struct{}
	started  chan struct{}
	done     chan struct{}
	once     sync.Once
}

func newLeaseTransitionWaiter() *leaseTransitionWaiter {
	return &leaseTransitionWaiter{
		released: make(chan struct{}),
		started:  make(chan struct{}),
		done:     make(chan struct{}),
	}
}

func (w *leaseTransitionWaiter) release() {
	w.once.Do(func() { close(w.released) })
}

func (w *leaseTransitionWaiter) start() {
	go func() {
		close(w.started)
		<-w.released
		close(w.done)
	}()
	<-w.started
}

type blockingDriverArgument struct {
	started chan struct{}
	allow   <-chan struct{}
	once    sync.Once
}

func (a *blockingDriverArgument) Match(driver.Value) bool {
	a.once.Do(func() { close(a.started) })
	<-a.allow
	return true
}

type blockingQueryMatcher struct {
	started  chan struct{}
	allow    <-chan struct{}
	delegate sqlmock.QueryMatcher
	once     sync.Once
}

func (m *blockingQueryMatcher) Match(expectedSQL, actualSQL string) error {
	m.once.Do(func() { close(m.started) })
	<-m.allow
	return m.delegate.Match(expectedSQL, actualSQL)
}

var _ adapter.ConnectionHolder = (*snapshotHolder)(nil)
var _ adapter.ExecutionSnapshotHolder = (*snapshotHolder)(nil)
var _ adapter.ExecutionSnapshotLeaseHolder = (*leaseHolder)(nil)

func (h *snapshotHolder) GetConn(context.Context) (*stdsql.Conn, error) { return h.conn, nil }

func (h *snapshotHolder) GetTxn(context.Context, *stdsql.TxOptions) (*stdsql.Tx, error) {
	return h.activeTx, nil
}

func (h *snapshotHolder) GetCatalogConn(context.Context) (*stdsql.Conn, error) { return h.conn, nil }

func (h *snapshotHolder) GetCatalogTxn(context.Context, *stdsql.TxOptions) (*stdsql.Tx, error) {
	return h.activeTx, nil
}

func (h *snapshotHolder) TryGetTxn() *stdsql.Tx { return h.activeTx }

func (h *snapshotHolder) GetCurrentCatalog() string { return h.currentCat }

func (h *snapshotHolder) GetCurrentSchema() string { return h.currentSch }

func (h *snapshotHolder) CloseTxn() {}

func (h *snapshotHolder) CloseConn() {}

func (h *snapshotHolder) GetExecutionSnapshot(_ context.Context, catalogOnly bool) (*stdsql.Conn, *stdsql.Tx, error) {
	h.queries = append(h.queries, catalogOnly)
	return h.conn, h.activeTx, nil
}

func (h *legacyHolder) GetConn(context.Context) (*stdsql.Conn, error) {
	h.getConnCalls++
	if h.onGetConn != nil {
		h.onGetConn()
	}
	return h.conn, nil
}

func (h *legacyHolder) GetTxn(context.Context, *stdsql.TxOptions) (*stdsql.Tx, error) {
	return h.activeTx, nil
}

func (h *legacyHolder) GetCatalogConn(context.Context) (*stdsql.Conn, error) { return h.conn, nil }

func (h *legacyHolder) GetCatalogTxn(context.Context, *stdsql.TxOptions) (*stdsql.Tx, error) {
	return h.activeTx, nil
}

func (h *legacyHolder) TryGetTxn() *stdsql.Tx { return h.activeTx }

func (h *legacyHolder) GetCurrentCatalog() string { return "memory" }

func (h *legacyHolder) GetCurrentSchema() string { return "main" }

func (h *legacyHolder) CloseTxn() {}

func (h *legacyHolder) CloseConn() {}

func newLeaseTestContext(
	t *testing.T,
	matcher sqlmock.QueryMatcher,
) (*sql.Context, *leaseHolder, sqlmock.Sqlmock) {
	t.Helper()
	if matcher == nil {
		matcher = sqlmock.QueryMatcherEqual
	}
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	holder := &leaseHolder{snapshotHolder: &snapshotHolder{
		Session:    memory.NewSession(sql.NewBaseSession(), nil),
		conn:       conn,
		currentCat: "memory",
		currentSch: "main",
	}}
	ctx := sql.NewContext(context.Background(), sql.WithSession(holder))
	return ctx, holder, mock
}

func newLeaseTxnTestContext(
	t *testing.T,
	matcher sqlmock.QueryMatcher,
) (*sql.Context, *leaseHolder, sqlmock.Sqlmock, *stdsql.Tx) {
	t.Helper()
	ctx, holder, mock := newLeaseTestContext(t, matcher)
	mock.ExpectBegin()
	tx, err := holder.conn.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	holder.activeTx = tx
	return ctx, holder, mock, tx
}

func requireTransitionWaiting(t *testing.T, waiter *leaseTransitionWaiter) {
	t.Helper()
	select {
	case <-waiter.done:
		t.Fatal("lifecycle transition completed before the execution lease was released")
	default:
	}
}

func requireSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
	}
}

func TestGetCatalogExecutorRequestsCatalogOnlySnapshot(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	holder := &snapshotHolder{
		Session:    memory.NewSession(sql.NewBaseSession(), nil),
		conn:       conn,
		currentCat: "memory",
		currentSch: "main",
	}
	ctx := sql.NewContext(context.Background(), sql.WithSession(holder))
	execer, err := adapter.GetCatalogExecutor(ctx)
	require.NoError(t, err)
	require.Same(t, conn, execer)
	require.Equal(t, []bool{true}, holder.queries)

	_, err = adapter.GetSQLExecutor(ctx)
	require.NoError(t, err)
	require.Equal(t, []bool{true, false}, holder.queries)
}

func TestGetExecutionSnapshotRejectsLegacyOwnerlessTransaction(t *testing.T) {
	holder := &legacyHolder{
		Session:  memory.NewSession(sql.NewBaseSession(), nil),
		activeTx: new(stdsql.Tx),
	}
	ctx := sql.NewContext(context.Background(), sql.WithSession(holder))

	execer, conn, tx, err := adapter.GetExecutionSnapshot(ctx)
	require.ErrorContains(t, err, "active transaction has no physical owner")
	require.Nil(t, execer)
	require.Nil(t, conn)
	require.Same(t, holder.activeTx, tx)
	require.Zero(t, holder.getConnCalls)
}

func TestGetExecutionSnapshotRejectsOwnerlessLegacyTransactionAfterAcquire(t *testing.T) {
	holder := &legacyHolder{Session: memory.NewSession(sql.NewBaseSession(), nil)}
	holder.onGetConn = func() { holder.activeTx = new(stdsql.Tx) }
	ctx := sql.NewContext(context.Background(), sql.WithSession(holder))

	execer, conn, tx, err := adapter.GetExecutionSnapshot(ctx)
	require.ErrorContains(t, err, "active transaction has no physical owner")
	require.Nil(t, execer)
	require.Nil(t, conn)
	require.Same(t, holder.activeTx, tx)
	require.Equal(t, 1, holder.getConnCalls)
}

func TestGetExecutionSnapshotWithLeaseReleasesExactlyOnce(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	holder := &leaseHolder{snapshotHolder: &snapshotHolder{
		Session:    memory.NewSession(sql.NewBaseSession(), nil),
		conn:       conn,
		currentCat: "memory",
		currentSch: "main",
	}}
	ctx := sql.NewContext(context.Background(), sql.WithSession(holder))
	execer, owner, tx, release, err := adapter.GetExecutionSnapshotWithLease(ctx)
	require.NoError(t, err)
	require.Same(t, conn, execer)
	require.Same(t, conn, owner)
	require.Nil(t, tx)
	require.NotNil(t, release)
	release()
	release()
	require.Equal(t, int32(1), holder.releaseCalls.Load())
	require.Equal(t, []bool{false}, holder.queries)
}

func TestGetExecutionSnapshotWithLeaseReleasesInvalidLeaseOnError(t *testing.T) {
	leaseErr := errors.New("snapshot failed")
	holder := &leaseHolder{
		snapshotHolder: &snapshotHolder{Session: memory.NewSession(sql.NewBaseSession(), nil)},
		leaseErr:       leaseErr,
	}
	ctx := sql.NewContext(context.Background(), sql.WithSession(holder))
	execer, owner, tx, release, err := adapter.GetExecutionSnapshotWithLease(ctx)
	require.ErrorIs(t, err, leaseErr)
	require.Nil(t, execer)
	require.Nil(t, owner)
	require.Nil(t, tx)
	require.NotNil(t, release)
	release()
	require.Equal(t, int32(1), holder.releaseCalls.Load(), "the adapter must release an admitted lease on error")
}

func TestExecInTxnKeepsLeaseUntilDriverCallReturns(t *testing.T) {
	ctx, holder, mock, _ := newLeaseTxnTestContext(t, nil)
	waiter := newLeaseTransitionWaiter()
	holder.onRelease = waiter.release

	driverStarted := make(chan struct{})
	allowDriver := make(chan struct{})
	var allowOnce sync.Once
	t.Cleanup(func() { allowOnce.Do(func() { close(allowDriver) }) })
	mock.ExpectExec("UPDATE lease_test SET value = ?").
		WithArgs(&blockingDriverArgument{started: driverStarted, allow: allowDriver}).
		WillReturnResult(sqlmock.NewResult(0, 1))

	type execResult struct {
		result stdsql.Result
		err    error
	}
	execDone := make(chan execResult, 1)
	go func() {
		result, err := adapter.ExecInTxn(ctx, "UPDATE lease_test SET value = ?", 7)
		execDone <- execResult{result: result, err: err}
	}()

	requireSignal(t, driverStarted, "driver ExecContext entry")
	waiter.start()
	requireTransitionWaiting(t, waiter)
	require.Zero(t, holder.releaseCalls.Load())

	allowOnce.Do(func() { close(allowDriver) })
	var completed execResult
	select {
	case completed = <-execDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ExecInTxn to finish")
	}
	require.NoError(t, completed.err)
	require.NotNil(t, completed.result)
	affected, err := completed.result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)
	requireSignal(t, waiter.done, "lifecycle transition after ExecContext")
	require.Equal(t, int32(1), holder.releaseCalls.Load())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLeasedRowsKeepLeaseUntilEOFOrExplicitClose(t *testing.T) {
	tests := []struct {
		name        string
		catalogOnly bool
		finish      func(*testing.T, *adapter.LeasedRows)
	}{
		{
			name: "eof",
			finish: func(t *testing.T, rows *adapter.LeasedRows) {
				var values []int
				for rows.Next() {
					var value int
					require.NoError(t, rows.Scan(&value))
					values = append(values, value)
				}
				require.NoError(t, rows.Err())
				require.Equal(t, []int{1, 2}, values)
			},
		},
		{
			name:        "explicit close",
			catalogOnly: true,
			finish: func(t *testing.T, rows *adapter.LeasedRows) {
				require.NoError(t, rows.Close())
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, holder, mock := newLeaseTestContext(t, nil)
			waiter := newLeaseTransitionWaiter()
			holder.onRelease = waiter.release
			const query = "SELECT value FROM lease_rows"
			mock.ExpectQuery(query).WillReturnRows(
				sqlmock.NewRows([]string{"value"}).AddRow(1).AddRow(2),
			)

			var (
				rows *adapter.LeasedRows
				err  error
			)
			if test.catalogOnly {
				rows, err = adapter.QueryCatalog(ctx, query)
			} else {
				rows, err = adapter.Query(ctx, query)
			}
			require.NoError(t, err)
			require.NotNil(t, rows)
			t.Cleanup(func() { _ = rows.Close() })

			waiter.start()
			requireTransitionWaiting(t, waiter)
			require.Zero(t, holder.releaseCalls.Load())

			test.finish(t, rows)
			requireSignal(t, waiter.done, "lifecycle transition after rows termination")
			require.NoError(t, rows.Close())
			require.Equal(t, int32(1), holder.releaseCalls.Load())
			require.Equal(t, []bool{test.catalogOnly}, holder.queries)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestLeasedRowKeepsLeaseUntilScan(t *testing.T) {
	ctx, holder, mock := newLeaseTestContext(t, nil)
	waiter := newLeaseTransitionWaiter()
	holder.onRelease = waiter.release
	const query = "SELECT value FROM lease_row"
	mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(42))

	row := adapter.QueryRow(ctx, query)
	require.NotNil(t, row)
	waiter.start()
	requireTransitionWaiting(t, waiter)
	require.Zero(t, holder.releaseCalls.Load())

	var value int
	require.NoError(t, row.Scan(&value))
	require.Equal(t, 42, value)
	requireSignal(t, waiter.done, "lifecycle transition after Row.Scan")
	require.Equal(t, int32(1), holder.releaseCalls.Load())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLeasedRowPreservesExecutionLeaseAdmissionError(t *testing.T) {
	for _, test := range []struct {
		name        string
		catalogOnly bool
		queryRow    func(*sql.Context) *adapter.LeasedRow
	}{
		{
			name: "ordinary query",
			queryRow: func(ctx *sql.Context) *adapter.LeasedRow {
				return adapter.QueryRow(ctx, "SELECT must_not_execute")
			},
		},
		{
			name:        "catalog query",
			catalogOnly: true,
			queryRow: func(ctx *sql.Context) *adapter.LeasedRow {
				return adapter.QueryRowCatalog(ctx, "SELECT must_not_execute")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, holder, mock := newLeaseTestContext(t, nil)
			admissionErr := errors.New("execution lease admission failed")
			holder.leaseErr = admissionErr

			row := test.queryRow(ctx)
			require.NotNil(t, row)
			var value int
			require.ErrorIs(t, row.Scan(&value), admissionErr)
			require.ErrorIs(t, row.Scan(&value), admissionErr)
			require.Equal(t, int32(1), holder.releaseCalls.Load())
			require.Equal(t, []bool{test.catalogOnly}, holder.queries)
			// There is deliberately no Query expectation. Any target driver call
			// would replace the admission sentinel with sqlmock's unexpected-call error.
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestTxnExecutionLeaseCoversPrepareContext(t *testing.T) {
	driverStarted := make(chan struct{})
	allowDriver := make(chan struct{})
	var allowOnce sync.Once
	t.Cleanup(func() { allowOnce.Do(func() { close(allowDriver) }) })
	matcher := &blockingQueryMatcher{
		started:  driverStarted,
		allow:    allowDriver,
		delegate: sqlmock.QueryMatcherEqual,
	}
	ctx, holder, mock, activeTx := newLeaseTxnTestContext(t, matcher)
	waiter := newLeaseTransitionWaiter()
	holder.onRelease = waiter.release
	const query = "INSERT INTO lease_prepare VALUES (?)"
	mock.ExpectPrepare(query)

	type prepareResult struct {
		owner *stdsql.Conn
		tx    *stdsql.Tx
		err   error
	}
	prepareDone := make(chan prepareResult, 1)
	go func() {
		owner, tx, release, err := adapter.GetTxnExecutionSnapshotWithLease(ctx, nil)
		if err != nil {
			prepareDone <- prepareResult{owner: owner, tx: tx, err: err}
			return
		}
		stmt, prepareErr := tx.PrepareContext(ctx, query)
		if stmt != nil {
			prepareErr = errors.Join(prepareErr, stmt.Close())
		}
		release()
		prepareDone <- prepareResult{owner: owner, tx: tx, err: prepareErr}
	}()

	requireSignal(t, driverStarted, "driver PrepareContext entry")
	waiter.start()
	requireTransitionWaiting(t, waiter)
	require.Zero(t, holder.releaseCalls.Load())

	allowOnce.Do(func() { close(allowDriver) })
	var completed prepareResult
	select {
	case completed = <-prepareDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for PrepareContext to finish")
	}
	require.NoError(t, completed.err)
	require.Same(t, holder.conn, completed.owner)
	require.Same(t, activeTx, completed.tx)
	requireSignal(t, waiter.done, "lifecycle transition after PrepareContext")
	require.Equal(t, int32(1), holder.releaseCalls.Load())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecInTxnAdmissionFailureStartsNoDriverCall(t *testing.T) {
	ctx, holder, mock, _ := newLeaseTxnTestContext(t, nil)
	admissionErr := errors.New("execution lease admission failed")
	holder.leaseErr = admissionErr

	result, err := adapter.ExecInTxn(ctx, "UPDATE must_not_execute SET value = 1")
	require.ErrorIs(t, err, admissionErr)
	require.Nil(t, result)
	require.Equal(t, int32(1), holder.releaseCalls.Load())
	// There is deliberately no Exec expectation. Any target driver call would
	// replace the admission sentinel with sqlmock's unexpected-call error.
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecInTxnBindingMismatchReleasesLeaseBeforeDriverCall(t *testing.T) {
	ctx, holder, mock := newLeaseTestContext(t, nil)
	holder.activeTx = new(stdsql.Tx)
	holder.leasedTx = new(stdsql.Tx)
	require.NotSame(t, holder.activeTx, holder.leasedTx)

	result, err := adapter.ExecInTxn(ctx, "UPDATE must_not_execute SET value = 1")
	require.ErrorIs(t, err, adapter.ErrTransactionBindingChanged)
	require.Nil(t, result)
	require.Equal(t, int32(1), holder.releaseCalls.Load())
	// Identity validation precedes driver dispatch, so no target expectation is
	// installed or consumed when the transaction generation has changed.
	require.NoError(t, mock.ExpectationsWereMet())
}
