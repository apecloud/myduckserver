package backend

import (
	"context"
	stdsql "database/sql"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/stretchr/testify/require"
)

var errOperationChild = errors.New("operation child failed")

type orderedOperationIter struct {
	events     *[]string
	nextErr    error
	valueErr   error
	closeErr   error
	closeCalls int
	nextCalls  int
	valueCalls int
}

func (i *orderedOperationIter) Next(*sql.Context) (sql.Row, error) {
	i.nextCalls++
	*i.events = append(*i.events, "next")
	if i.nextErr != nil {
		return nil, i.nextErr
	}
	return nil, io.EOF
}

func (i *orderedOperationIter) NextValueRow(*sql.Context) (sql.ValueRow, error) {
	i.valueCalls++
	*i.events = append(*i.events, "next-value")
	if i.valueErr != nil {
		return nil, i.valueErr
	}
	return nil, io.EOF
}

func (i *orderedOperationIter) IsValueRowIter(*sql.Context) bool { return true }

func (i *orderedOperationIter) Close(*sql.Context) error {
	i.closeCalls++
	*i.events = append(*i.events, "close")
	return i.closeErr
}

type postCleanupTestSession struct {
	*Session
	postCleanup func(bool)
}

// deterministicShutdownGate models the lifecycle gate's important property:
// once an exclusive teardown writer is waiting, ordinary readers cannot enter
// again. The explicit channels make that state deterministic in a unit test;
// a second GetTxnBinding call would remain blocked until the retained snapshot
// reader is released.
type deterministicShutdownGate struct {
	mu            sync.Mutex
	cond          *sync.Cond
	readers       int
	writerWaiting bool
	writerActive  bool
	writerReady   chan struct{}
	writerGot     chan struct{}
	writerRelease chan struct{}
	writerDone    chan struct{}
	releaseOnce   sync.Once
}

func newDeterministicShutdownGate() *deterministicShutdownGate {
	gate := &deterministicShutdownGate{
		writerReady:   make(chan struct{}),
		writerGot:     make(chan struct{}),
		writerRelease: make(chan struct{}),
		writerDone:    make(chan struct{}),
	}
	gate.cond = sync.NewCond(&gate.mu)
	return gate
}

func (gate *deterministicShutdownGate) RLock() {
	gate.mu.Lock()
	for gate.writerWaiting || gate.writerActive {
		gate.cond.Wait()
	}
	gate.readers++
	gate.mu.Unlock()
}

func (gate *deterministicShutdownGate) RUnlock() {
	gate.mu.Lock()
	if gate.readers > 0 {
		gate.readers--
	}
	gate.cond.Broadcast()
	gate.mu.Unlock()
}

func (gate *deterministicShutdownGate) startWriter() {
	gate.mu.Lock()
	gate.writerWaiting = true
	close(gate.writerReady)
	for gate.readers > 0 {
		gate.cond.Wait()
	}
	gate.writerWaiting = false
	gate.writerActive = true
	close(gate.writerGot)
	gate.mu.Unlock()

	<-gate.writerRelease

	gate.mu.Lock()
	gate.writerActive = false
	gate.cond.Broadcast()
	gate.mu.Unlock()
	close(gate.writerDone)
}

func (gate *deterministicShutdownGate) releaseWriter() {
	gate.releaseOnce.Do(func() { close(gate.writerRelease) })
}

type shutdownGateSession struct {
	*memory.Session
	gate *deterministicShutdownGate
	conn *stdsql.Conn
	tx   *stdsql.Tx

	bindingCalls atomic.Int32
	postMu       sync.Mutex
	postTx       *stdsql.Tx
	postCallback func(bool)
}

var _ adapter.ConnectionHolder = (*shutdownGateSession)(nil)
var _ adapter.ExecutionSnapshotHolder = (*shutdownGateSession)(nil)
var _ adapter.ExecutionSnapshotLeaseHolder = (*shutdownGateSession)(nil)
var _ PhysicalPostTransactionCleanupRegistrar = (*shutdownGateSession)(nil)

func (s *shutdownGateSession) GetConn(context.Context) (*stdsql.Conn, error) {
	return s.conn, nil
}

func (s *shutdownGateSession) GetTxn(context.Context, *stdsql.TxOptions) (*stdsql.Tx, error) {
	return s.tx, nil
}

func (s *shutdownGateSession) GetCatalogConn(context.Context) (*stdsql.Conn, error) {
	return s.conn, nil
}

func (s *shutdownGateSession) GetCatalogTxn(context.Context, *stdsql.TxOptions) (*stdsql.Tx, error) {
	return s.tx, nil
}

func (s *shutdownGateSession) TryGetTxn() *stdsql.Tx { return s.tx }

func (s *shutdownGateSession) GetCurrentCatalog() string { return "memory" }

func (s *shutdownGateSession) GetCurrentSchema() string { return "main" }

func (s *shutdownGateSession) CloseTxn() {}

func (s *shutdownGateSession) CloseConn() {}

func (s *shutdownGateSession) GetExecutionSnapshot(
	context.Context,
	bool,
) (*stdsql.Conn, *stdsql.Tx, error) {
	return s.conn, s.tx, nil
}

func (s *shutdownGateSession) GetExecutionSnapshotLease(
	context.Context,
	bool,
) (*stdsql.Conn, *stdsql.Tx, func(), error) {
	s.gate.RLock()
	var once sync.Once
	return s.conn, s.tx, func() {
		once.Do(s.gate.RUnlock)
	}, nil
}

func (s *shutdownGateSession) GetTxnBinding() (*stdsql.Conn, *stdsql.Tx) {
	s.bindingCalls.Add(1)
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.conn, s.tx
}

func (s *shutdownGateSession) RegisterPostPhysicalTransactionCleanup(
	_ *sql.Context,
	tx *stdsql.Tx,
	callback func(bool),
) bool {
	if tx == nil || callback == nil {
		return false
	}
	s.postMu.Lock()
	s.postTx = tx
	s.postCallback = callback
	s.postMu.Unlock()
	return true
}

func (s *postCleanupTestSession) RegisterPostTransactionCleanup(
	_ *sql.Context,
	_ sql.Transaction,
	callback func(bool),
) bool {
	s.postCleanup = callback
	return true
}

func TestDuckLakeOperationIterClosesChildBeforeReleaseOnTerminalNext(t *testing.T) {
	ctx := sql.NewEmptyContext()
	events := []string{}
	child := &orderedOperationIter{events: &events, nextErr: errOperationChild}
	iter := wrapDuckLakeOperationIter(child, func() { events = append(events, "release") })

	_, err := iter.Next(ctx)
	require.ErrorIs(t, err, errOperationChild)
	require.Equal(t, []string{"next", "close", "release"}, events)

	// The engine commonly calls Close after a terminal error. It must not close
	// the child or release the operation a second time, but it must preserve the
	// terminal failure so a transaction wrapper cannot mistake the close for a
	// successful statement.
	require.ErrorIs(t, iter.Close(ctx), errOperationChild)
	require.Equal(t, 1, child.closeCalls)
	require.Equal(t, []string{"next", "close", "release"}, events)
}

func TestDuckLakeOperationIterEarlyCloseAbortsBeforeCommit(t *testing.T) {
	ctx := sql.NewEmptyContext()
	events := []string{}
	child := &orderedOperationIter{events: &events}
	iter := wrapDuckLakeOperationIter(child, func() { events = append(events, "release") })

	require.ErrorIs(t, iter.Close(ctx), errDuckLakeOperationClosedBeforeEOF)
	require.Equal(t, []string{"close", "release"}, events)
	require.Equal(t, 1, child.closeCalls)
	require.ErrorIs(t, iter.Close(ctx), errDuckLakeOperationClosedBeforeEOF)
	require.Equal(t, 1, child.closeCalls)
}

func TestDuckLakeOperationIterGMSDoesNotCommitAfterChildError(t *testing.T) {
	sess, ctx, transaction := newFrontendImplicitSession(t)
	child := &orderedOperationIter{events: new([]string), nextErr: errOperationChild}
	iter := wrapDuckLakeOperationIterWithContext(ctx, child, nil)
	committing, err := rowexec.AddTransactionCommittingIter(ctx, nil, iter)
	require.NoError(t, err)

	_, err = committing.Next(ctx)
	require.ErrorIs(t, err, errOperationChild)
	// TransactionCommittingIter must observe the preserved child error from
	// Close and skip Session.CommitTransaction entirely.
	require.ErrorIs(t, committing.Close(ctx), errOperationChild)
	require.Same(t, transaction, sess.Session.GetTransaction())
	require.NotNil(t, sess.implicit)

	// The engine's clearAutocommitOnError boundary now performs the callback and
	// rollback, after the child has already been closed exactly once.
	ctx.SetTransaction(nil)
	require.Nil(t, sess.Session.GetTransaction())
	require.Nil(t, sess.implicit)
}

func TestDuckLakeOperationIterGMSCommitsAfterEOF(t *testing.T) {
	sess, ctx, _ := newFrontendImplicitSession(t)
	child := &orderedOperationIter{events: new([]string)}
	iter := wrapDuckLakeOperationIterWithContext(ctx, child, nil)
	committing, err := rowexec.AddTransactionCommittingIter(ctx, nil, iter)
	require.NoError(t, err)

	_, err = committing.Next(ctx)
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, committing.Close(ctx))
	require.Nil(t, sess.Session.GetTransaction())
	require.Nil(t, sess.implicit)
}

func TestDuckLakeOperationIterDoesNotTreatExplicitTransactionAsImplicit(t *testing.T) {
	sess, ctx, transaction := newFrontendImplicitSession(t)
	// Remove only the frontend autocommit marker. The logical transaction stays
	// current, modeling an explicit/DDL transaction with no implicit cleanup
	// boundary.
	sess.removeImplicitState(sess.implicit)
	require.False(t, sess.HasImplicitTransactionCleanup(transaction))

	events := []string{}
	child := &orderedOperationIter{events: &events}
	iter := wrapDuckLakeOperationIterWithLeases(
		ctx,
		child,
		func() { events = append(events, "snapshot-release") },
		nil,
	)
	// Registration must not run the callback immediately just because the
	// current transaction has no implicit marker.
	require.Equal(t, []string{}, events)

	_, err := iter.Next(ctx)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, []string{"next", "close", "snapshot-release"}, events)
	require.Equal(t, 1, child.closeCalls)
}

func TestDuckLakeOperationValueIterClosesChildBeforeReleaseOnEOF(t *testing.T) {
	ctx := sql.NewEmptyContext()
	events := []string{}
	child := &orderedOperationIter{events: &events}
	iter := wrapDuckLakeOperationIter(child, func() { events = append(events, "release") })

	valueIter, ok := iter.(sql.ValueRowIter)
	require.True(t, ok)
	_, err := valueIter.NextValueRow(ctx)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, []string{"next-value", "close", "release"}, events)
	require.NoError(t, valueIter.Close(ctx))
	require.Equal(t, 1, child.closeCalls)
}

func TestDuckLakeOperationIterPreservesChildCloseErrorForExplicitClose(t *testing.T) {
	ctx := sql.NewEmptyContext()
	events := []string{}
	child := &orderedOperationIter{
		events:   &events,
		closeErr: errOperationChild,
	}
	iter := wrapDuckLakeOperationIter(child, func() { events = append(events, "release") })

	_, err := iter.Next(ctx)
	require.ErrorIs(t, err, io.EOF)
	// EOF must stay EOF even if the eager child close reports an error.
	require.Equal(t, []string{"next", "close", "release"}, events)
	require.ErrorIs(t, iter.Close(ctx), errOperationChild)
	require.Equal(t, 1, child.closeCalls)
}

func TestDuckLakeOperationIterSharesCloseStateAcrossMutableAliases(t *testing.T) {
	ctx := sql.NewEmptyContext()
	events := []string{}
	first := &orderedOperationIter{events: &events}
	second := &orderedOperationIter{events: &events}
	iter := wrapDuckLakeOperationIter(first, func() { events = append(events, "release") })

	mutable, ok := iter.(sql.MutableRowIter)
	require.True(t, ok)
	replacement := mutable.WithChildIter(second)
	_, err := replacement.Next(ctx)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, []string{"next", "close", "release"}, events)

	// The original alias must not close its abandoned child after the
	// replacement has already released the shared lease.
	require.NoError(t, iter.Close(ctx))
	require.Zero(t, first.closeCalls)
	require.Equal(t, 1, second.closeCalls)
}

func TestDuckLakeOperationIterMutableAliasDispatchesCurrentRowChild(t *testing.T) {
	ctx := sql.NewEmptyContext()
	events := []string{}
	first := &orderedOperationIter{events: &events}
	second := &orderedOperationIter{events: &events}
	iter := wrapDuckLakeOperationIter(first, nil)

	mutable, ok := iter.(sql.MutableRowIter)
	require.True(t, ok)
	replacement := mutable.WithChildIter(second)
	require.Same(t, second, iter.(sql.MutableRowIter).GetChildIter())

	// Both the original alias and the alias returned by WithChildIter must read
	// the shared current child. Calling the original alias here is the regression
	// that used to continue driving `first` through its stale child field.
	_, err := iter.Next(ctx)
	require.ErrorIs(t, err, io.EOF)
	require.Zero(t, first.nextCalls)
	require.Equal(t, 1, second.nextCalls)
	require.Equal(t, 1, second.closeCalls)
	_, err = replacement.Next(ctx)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 1, second.nextCalls)
}

func TestDuckLakeOperationValueAliasDispatchesCurrentValueChild(t *testing.T) {
	ctx := sql.NewEmptyContext()
	events := []string{}
	first := &orderedOperationIter{events: &events}
	second := &orderedOperationIter{events: &events}
	iter := wrapDuckLakeOperationIter(first, nil)
	valueAlias, ok := iter.(sql.ValueRowIter)
	require.True(t, ok)
	mutable, ok := iter.(sql.MutableRowIter)
	require.True(t, ok)
	mutable.WithChildIter(second)
	require.True(t, valueAlias.IsValueRowIter(ctx))

	_, err := valueAlias.NextValueRow(ctx)
	require.ErrorIs(t, err, io.EOF)
	require.Zero(t, first.valueCalls)
	require.Equal(t, 1, second.valueCalls)
	require.Equal(t, 1, second.closeCalls)
}

func TestDuckLakeOperationIterSplitLeasesReleaseInOrder(t *testing.T) {
	ctx := sql.NewEmptyContext()
	events := []string{}
	child := &orderedOperationIter{events: &events}
	iter := wrapDuckLakeOperationIterWithLeases(
		nil,
		child,
		func() { events = append(events, "snapshot-release") },
		func() { events = append(events, "operation-release") },
	)

	_, err := iter.Next(ctx)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t,
		[]string{"next", "close", "snapshot-release", "operation-release"},
		events,
	)
	require.NoError(t, iter.Close(ctx))
	require.Equal(t, 1, child.closeCalls)
}

func TestDuckLakeOperationIterDefersOperationLeaseToPostFinalization(t *testing.T) {
	backendSession := NewSession(memory.NewSession(sql.NewBaseSession(), nil), nil)
	sess := &postCleanupTestSession{Session: backendSession}
	ctx := sql.NewContext(
		mycontext.WithFrontendQuery(context.Background()),
		sql.WithSession(sess),
	)
	tx, err := sess.StartTransaction(ctx, sql.ReadWrite)
	require.NoError(t, err)
	ctx.SetTransaction(tx)

	events := []string{}
	child := &orderedOperationIter{events: &events}
	iter := wrapDuckLakeOperationIterWithLeases(
		ctx,
		child,
		func() { events = append(events, "snapshot-release") },
		func() { events = append(events, "operation-release") },
	)
	require.NotNil(t, sess.postCleanup)

	_, err = iter.Next(ctx)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, []string{"next", "close", "snapshot-release"}, events)

	// The session invokes the post hook only after commit or rollback has
	// completed. It is idempotent even if both a normal terminal path and a
	// later cleanup callback try to release the same operation.
	sess.postCleanup(true)
	sess.postCleanup(false)
	require.Equal(t, []string{"next", "close", "snapshot-release", "operation-release"}, events)
}

func TestDuckLakeOperationIterUsesCapturedTxDuringPendingShutdown(t *testing.T) {
	gate := newDeterministicShutdownGate()
	holder := &shutdownGateSession{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		gate:    gate,
		conn:    new(stdsql.Conn),
		tx:      new(stdsql.Tx),
	}
	ctx := sql.NewContext(context.Background(), sql.WithSession(holder))
	logicalTx, err := holder.Session.StartTransaction(ctx, sql.ReadWrite)
	require.NoError(t, err)
	ctx.SetTransaction(logicalTx)

	_, owner, physicalTx, snapshotRelease, err := adapter.GetExecutionSnapshotWithLease(ctx)
	require.NoError(t, err)
	require.Same(t, holder.conn, owner)
	require.Same(t, holder.tx, physicalTx)
	t.Cleanup(func() {
		snapshotRelease()
		gate.releaseWriter()
		select {
		case <-gate.writerDone:
		case <-time.After(time.Second):
		}
	})

	go gate.startWriter()
	select {
	case <-gate.writerReady:
	case <-time.After(time.Second):
		t.Fatal("shutdown writer did not become pending")
	}
	select {
	case <-gate.writerGot:
		t.Fatal("shutdown writer acquired before the retained snapshot was released")
	default:
	}

	var operationCalls atomic.Int32
	child := &orderedOperationIter{events: new([]string)}
	wrappedDone := make(chan sql.RowIter, 1)
	go func() {
		wrappedDone <- wrapDuckLakeOperationIterWithLeasesAndTx(
			ctx,
			child,
			physicalTx,
			snapshotRelease,
			func() { operationCalls.Add(1) },
		)
	}()

	var wrapped sql.RowIter
	select {
	case wrapped = <-wrappedDone:
	case <-time.After(time.Second):
		t.Fatal("iterator wrapper re-entered the pending shutdown reader gate")
	}
	require.Zero(t, holder.bindingCalls.Load(), "wrapper must not perform a second GetTxnBinding lookup")
	holder.postMu.Lock()
	registeredTx, callback := holder.postTx, holder.postCallback
	holder.postMu.Unlock()
	require.Same(t, physicalTx, registeredTx)
	require.NotNil(t, callback)

	// Close releases the retained reader, allowing the pending teardown writer
	// to acquire. The deferred operation remains held until physical finalization.
	require.ErrorIs(t, wrapped.Close(ctx), errDuckLakeOperationClosedBeforeEOF)
	select {
	case <-gate.writerGot:
	case <-time.After(time.Second):
		t.Fatal("shutdown writer did not acquire after snapshot release")
	}
	require.Zero(t, operationCalls.Load())

	gate.releaseWriter()
	select {
	case <-gate.writerDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown writer did not finish")
	}
	callback(false)
	require.Equal(t, int32(1), operationCalls.Load())
}

func TestDuckLakeOperationIterNilChildReleasesSplitLeasesOnce(t *testing.T) {
	snapshotCalls, operationCalls := 0, 0
	iter := wrapDuckLakeOperationIterWithLeases(
		nil,
		nil,
		func() { snapshotCalls++ },
		func() { operationCalls++ },
	)
	require.Nil(t, iter)
	// A failed iterator construction has no child boundary, so both leases are
	// released immediately and never need a later Close call.
	require.Equal(t, 1, snapshotCalls)
	require.Equal(t, 1, operationCalls)
}
