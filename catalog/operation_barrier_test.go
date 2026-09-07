package catalog

import (
	"context"
	stdsql "database/sql"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/apecloud/myduckserver/configuration"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

func newDuckLakeOperationTestProvider() *DatabaseProvider {
	return &DatabaseProvider{duckLake: &duckLakeRuntime{config: configuration.DuckLakeConfig{
		MetadataPath: "/tmp/task77/catalog.ducklake",
		DataPath:     "/tmp/task77/data",
	}}}
}

func duckLakeOperationCount(provider *DatabaseProvider) int {
	provider.duckLakeTxnMu.Lock()
	defer provider.duckLakeTxnMu.Unlock()
	return provider.duckLakeActiveOperations
}

func TestDuckLakeOperationAdmissionBlocksCleanup(t *testing.T) {
	provider := newDuckLakeOperationTestProvider()
	release := provider.BeginDuckLakeOperation(mycontext.WithFrontendQuery(context.Background()))
	require.Equal(t, 1, duckLakeOperationCount(provider))

	cleanupDone := make(chan struct{})
	go func() {
		cleanupRelease := provider.beginDuckLakeCleanup()
		cleanupRelease()
		close(cleanupDone)
	}()

	select {
	case <-cleanupDone:
		t.Fatal("cleanup entered while a logical operation was active")
	case <-time.After(25 * time.Millisecond):
	}

	release()
	release()
	select {
	case <-cleanupDone:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not resume after logical operation release")
	}
	require.Zero(t, duckLakeOperationCount(provider))
}

func TestDuckLakeRollbackCleanupExcludesOwnedOperationLease(t *testing.T) {
	provider := newDuckLakeOperationTestProvider()
	owner := new(int)
	ctx := mycontext.WithFrontendQuery(context.Background())
	operationRelease := provider.BeginDuckLakeOperationForOwner(ctx, owner)
	defer operationRelease()

	reservationDone := make(chan func(), 1)
	go func() {
		reservationDone <- provider.BeginDuckLakeRollbackCleanupForOwner(nil, owner)
	}()
	waitForDuckLakeCleanupActive(t, provider)

	var reservationRelease func()
	select {
	case reservationRelease = <-reservationDone:
	case <-time.After(time.Second):
		t.Fatal("rollback cleanup did not exempt the caller's operation lease")
	}
	defer reservationRelease()

	// The operation remains admitted while its owner holds the reservation; the
	// exemption applies only to this caller, not by dropping the global count.
	require.Equal(t, 1, duckLakeOperationCount(provider))
	operationRelease()
	require.Zero(t, duckLakeOperationCount(provider))
}

func TestDuckLakeRollbackCleanupBlocksOtherOperationOwner(t *testing.T) {
	provider := newDuckLakeOperationTestProvider()
	ownerA := new(int)
	ownerB := new(int)
	ctx := mycontext.WithFrontendQuery(context.Background())
	operationRelease := provider.BeginDuckLakeOperationForOwner(ctx, ownerA)
	defer operationRelease()

	reservationDone := make(chan func(), 1)
	go func() {
		reservationDone <- provider.BeginDuckLakeRollbackCleanupForOwner(nil, ownerB)
	}()
	waitForDuckLakeCleanupActive(t, provider)
	select {
	case release := <-reservationDone:
		release()
		t.Fatal("rollback cleanup entered while another owner's operation was active")
	case <-time.After(50 * time.Millisecond):
	}

	operationRelease()
	var reservationRelease func()
	select {
	case reservationRelease = <-reservationDone:
	case <-time.After(time.Second):
		t.Fatal("rollback cleanup did not resume after the other owner released")
	}
	reservationRelease()
}

func TestDuckLakeRollbackCleanupBlocksAnonymousOperationLease(t *testing.T) {
	provider := newDuckLakeOperationTestProvider()
	ctx := mycontext.WithFrontendQuery(context.Background())
	// A plain context has no logical GMS transaction identity, so the legacy
	// entry point must retain a conservative anonymous lease.
	operationRelease := provider.BeginDuckLakeOperation(ctx)
	defer operationRelease()
	owner := new(int)

	reservationDone := make(chan func(), 1)
	go func() {
		reservationDone <- provider.BeginDuckLakeRollbackCleanupForOwner(nil, owner)
	}()
	waitForDuckLakeCleanupActive(t, provider)
	select {
	case release := <-reservationDone:
		release()
		t.Fatal("rollback cleanup entered while an anonymous operation was active")
	case <-time.After(50 * time.Millisecond):
	}

	operationRelease()
	select {
	case release := <-reservationDone:
		release()
	case <-time.After(time.Second):
		t.Fatal("rollback cleanup did not resume after anonymous operation release")
	}
}

func TestDuckLakeRollbackCleanupMatchesPhysicalOperationOwner(t *testing.T) {
	provider := newDuckLakeOperationTestProvider()
	target := new(stdsql.Tx)
	ctx := mycontext.WithFrontendQuery(context.Background())
	operationRelease := provider.BeginDuckLakeOperationForOwner(ctx, target)
	defer operationRelease()

	reservationDone := make(chan func(), 1)
	go func() {
		reservationDone <- provider.BeginDuckLakeRollbackCleanupForOwner(target, nil)
	}()
	waitForDuckLakeCleanupActive(t, provider)
	select {
	case release := <-reservationDone:
		release()
	case <-time.After(time.Second):
		t.Fatal("rollback cleanup did not match the physical operation owner")
	}
}

func TestDuckLakeRollbackCleanupTreatsNonComparableOwnerAsAnonymous(t *testing.T) {
	provider := newDuckLakeOperationTestProvider()
	ctx := mycontext.WithFrontendQuery(context.Background())
	operationOwner := []string{"owner"}
	operationRelease := provider.BeginDuckLakeOperationForOwner(ctx, operationOwner)
	defer operationRelease()

	reservationDone := make(chan func(), 1)
	go func() {
		reservationDone <- provider.BeginDuckLakeRollbackCleanupForOwner(nil, operationOwner)
	}()
	waitForDuckLakeCleanupActive(t, provider)
	select {
	case release := <-reservationDone:
		release()
		t.Fatal("rollback cleanup exempted a non-comparable owner")
	case <-time.After(50 * time.Millisecond):
	}

	operationRelease()
	select {
	case release := <-reservationDone:
		release()
	case <-time.After(time.Second):
		t.Fatal("rollback cleanup did not resume after non-comparable lease release")
	}
}

func TestDuckLakeOperationAdmissionBlocksDuringCleanup(t *testing.T) {
	provider := newDuckLakeOperationTestProvider()
	cleanupRelease := provider.beginDuckLakeCleanup()

	operationDone := make(chan struct{})
	go func() {
		release := provider.BeginDuckLakeOperation(mycontext.WithFrontendQuery(context.Background()))
		release()
		close(operationDone)
	}()

	select {
	case <-operationDone:
		t.Fatal("logical operation entered during cleanup")
	case <-time.After(25 * time.Millisecond):
	}

	cleanupRelease()
	select {
	case <-operationDone:
	case <-time.After(time.Second):
		t.Fatal("logical operation did not resume after cleanup release")
	}
	require.Zero(t, duckLakeOperationCount(provider))
}

func TestResetDuckLakeTransactionsDrainsLogicalOperationsBeforeReset(t *testing.T) {
	provider := newDuckLakeOperationTestProvider()
	oldRelease := provider.BeginDuckLakeOperation(mycontext.WithFrontendQuery(context.Background()))

	resetDone := make(chan struct{})
	go func() {
		provider.resetDuckLakeTransactions()
		close(resetDone)
	}()

	// Wait until reset has announced the generation transition. It must retain
	// the old lease instead of clearing its count underneath the iterator.
	deadline := time.After(time.Second)
	for {
		provider.duckLakeTxnMu.Lock()
		resetting := provider.duckLakeCleanupActive
		provider.duckLakeTxnMu.Unlock()
		if resetting {
			break
		}
		select {
		case <-deadline:
			t.Fatal("reset did not announce a cleanup barrier")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	newDone := make(chan func(), 1)
	go func() {
		newDone <- provider.BeginDuckLakeOperation(mycontext.WithFrontendQuery(context.Background()))
	}()
	select {
	case <-newDone:
		t.Fatal("new logical operation entered while reset was draining")
	case <-time.After(25 * time.Millisecond):
	}

	oldRelease()
	select {
	case <-resetDone:
	case <-time.After(time.Second):
		t.Fatal("reset did not finish after the old logical operation released")
	}

	var newRelease func()
	select {
	case newRelease = <-newDone:
	case <-time.After(time.Second):
		t.Fatal("new logical operation did not resume after reset")
	}
	require.Equal(t, 1, duckLakeOperationCount(provider))
	newRelease()
	require.Zero(t, duckLakeOperationCount(provider))
}

func TestDuckLakeGenerationTransitionDoesNotDeadlockProviderOperation(t *testing.T) {
	provider := newDuckLakeOperationTestProvider()
	provider.mu = &sync.RWMutex{}
	operationRelease := provider.BeginDuckLakeOperation(mycontext.WithFrontendQuery(context.Background()))

	transitionDone := make(chan struct{})
	go func() {
		release := provider.beginDuckLakeGenerationTransition()
		release()
		close(transitionDone)
	}()

	// Wait until the transition has announced cleanup. The admitted operation
	// must still be able to acquire the provider mutex and finish; the transition
	// itself deliberately does not take that mutex while draining operations.
	deadline := time.After(time.Second)
	for {
		provider.duckLakeTxnMu.Lock()
		active := provider.duckLakeCleanupActive
		provider.duckLakeTxnMu.Unlock()
		if active {
			break
		}
		select {
		case <-deadline:
			t.Fatal("generation transition did not announce cleanup")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	operationDone := make(chan struct{})
	go func() {
		provider.mu.Lock()
		provider.mu.Unlock()
		operationRelease()
		close(operationDone)
	}()
	select {
	case <-operationDone:
	case <-time.After(time.Second):
		t.Fatal("admitted operation could not finish while transition drained")
	}
	select {
	case <-transitionDone:
	case <-time.After(time.Second):
		t.Fatal("generation transition did not finish after operation release")
	}
}

func TestProviderCloseLetsAdmittedOperationReleaseBeforePoolTeardown(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	p := NewConnectionPool(connector, db, "memory")
	provider := &DatabaseProvider{
		mu:   &sync.RWMutex{},
		pool: p,
		duckLake: &duckLakeRuntime{config: configuration.DuckLakeConfig{
			MetadataPath: "/tmp/task77/catalog.ducklake",
			DataPath:     "/tmp/task77/data",
		}},
		connector: connector,
	}
	p.SetTransactionLifecycleHooks(provider.beginDuckLakeTransaction, provider.untrackDuckLakeTransaction)

	operationRelease := provider.BeginDuckLakeOperation(mycontext.WithFrontendQuery(context.Background()))
	closeDone := make(chan error, 1)
	go func() { closeDone <- provider.Close() }()

	deadline := time.After(time.Second)
	for {
		provider.duckLakeTxnMu.Lock()
		active := provider.duckLakeCleanupActive
		provider.duckLakeTxnMu.Unlock()
		if active {
			break
		}
		select {
		case <-deadline:
			t.Fatal("provider close did not announce the generation transition")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	operationDone := make(chan struct{})
	go func() {
		// This models an admitted frontend operation that needs the provider
		// mutex before it can release its operation lease.
		provider.mu.Lock()
		provider.mu.Unlock()
		operationRelease()
		close(operationDone)
	}()
	select {
	case <-operationDone:
	case <-time.After(time.Second):
		t.Fatal("provider close held the mutex ahead of an admitted operation")
	}
	select {
	case closeErr := <-closeDone:
		require.NoError(t, closeErr)
	case <-time.After(time.Second):
		t.Fatal("provider close did not finish after operation release")
	}
	_ = connector.Close()
}

func TestDuckLakeProviderCloseAllowsPhysicalOwnerLeaseToReleaseDuringPoolTeardown(t *testing.T) {
	provider, closeProvider := newDuckLakeOperationPoolProvider(t)
	const sessionID uint32 = 907

	tx, err := provider.Pool().GetTxn(context.Background(), sessionID, "", nil)
	require.NoError(t, err)
	require.NotNil(t, tx)

	// COPY keeps its provider operation lease until the surrounding physical
	// transaction finishes. The pool completion callback below models that
	// production registration and must be allowed to run from provider.Close.
	operationRelease := provider.BeginDuckLakeOperationForOwner(
		mycontext.WithFrontendQuery(context.Background()), tx,
	)
	callbackDone := make(chan struct{})
	var callbackOnce sync.Once
	require.True(t, provider.Pool().RegisterTransactionCompletion(tx, func(bool) {
		operationRelease()
		callbackOnce.Do(func() { close(callbackDone) })
	}))

	closeDone := make(chan error, 1)
	go func() { closeDone <- closeProvider() }()

	select {
	case closeErr := <-closeDone:
		require.NoError(t, closeErr)
	case <-time.After(time.Second):
		// Keep the test goroutine recoverable on an implementation that regresses
		// to waiting for every operation before pool teardown. Releasing here is
		// only cleanup; the timeout remains the assertion failure.
		operationRelease()
		select {
		case <-closeDone:
		case <-time.After(time.Second):
		}
		t.Fatal("provider close waited for a physical-owner operation lease before pool teardown")
	}

	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("pool teardown did not invoke the physical transaction completion callback")
	}
	require.Zero(t, duckLakeOperationCount(provider))
}

// newDuckLakeOperationPoolProvider creates the same provider/pool pairing used
// by a live configured provider, while leaving extension setup out of this
// lifecycle-only test. The non-empty service paths enable the provider's
// admission barriers; no DuckLake SQL is issued.
func newDuckLakeOperationPoolProvider(t *testing.T) (*DatabaseProvider, func() error) {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	pool := NewConnectionPool(connector, db, "memory")
	provider := &DatabaseProvider{
		mu:        &sync.RWMutex{},
		pool:      pool,
		storage:   db,
		connector: connector,
		duckLake: &duckLakeRuntime{config: configuration.DuckLakeConfig{
			MetadataPath: "/tmp/task77/catalog.ducklake",
			DataPath:     "/tmp/task77/data",
		}},
	}
	pool.SetTransactionLifecycleHooks(provider.beginDuckLakeTransaction, provider.untrackDuckLakeTransaction)

	var closeOnce sync.Once
	var closeErr error
	closeProvider := func() error {
		closeOnce.Do(func() {
			closeErr = provider.Close()
		})
		return closeErr
	}
	t.Cleanup(func() { _ = closeProvider() })
	return provider, closeProvider
}

type providerOperationChild struct {
	events       chan<- string
	nextErr      error
	closeStarted chan struct{}
	allowClose   <-chan struct{}
	closeOnce    sync.Once
}

func (c *providerOperationChild) Next(*sql.Context) (sql.Row, error) {
	c.events <- "child-next"
	return nil, c.nextErr
}

func (c *providerOperationChild) Close(*sql.Context) error {
	c.closeOnce.Do(func() {
		c.events <- "child-close-start"
		close(c.closeStarted)
		<-c.allowClose
		c.events <- "child-close-end"
	})
	return nil
}

type providerTerminalMode string

const (
	providerTerminalEOF   providerTerminalMode = "eof"
	providerTerminalError providerTerminalMode = "error"
	providerEarlyClose    providerTerminalMode = "early-close"
)

// runProviderChildTerminal is the small integration harness for the
// production iterator contract: a terminal child result is closed before the
// provider operation lease is released. The concrete wrapper's EOF/error and
// early-close behavior is covered in backend/ducklake_operation_test.go; this
// harness supplies a real provider and pool so generation finalization is
// exercised in the same ordering.
func runProviderChildTerminal(
	ctx *sql.Context,
	child *providerOperationChild,
	mode providerTerminalMode,
	release func(),
) error {
	var terminalErr error
	if mode == providerEarlyClose {
		terminalErr = errProviderEarlyClose
	} else {
		_, terminalErr = child.Next(ctx)
	}
	terminalErr = errors.Join(terminalErr, child.Close(ctx))
	release()
	return terminalErr
}

func waitForDuckLakeCleanupActive(t *testing.T, provider *DatabaseProvider) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		provider.duckLakeTxnMu.Lock()
		active := provider.duckLakeCleanupActive
		provider.duckLakeTxnMu.Unlock()
		if active {
			return
		}
		select {
		case <-deadline:
			t.Fatal("provider did not announce the cleanup barrier")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestDuckLakeProviderCloseWaitsForChildCloseBeforeLeaseRelease(t *testing.T) {
	cases := []struct {
		name      string
		mode      providerTerminalMode
		nextErr   error
		expectErr error
	}{
		{name: "eof", mode: providerTerminalEOF, nextErr: io.EOF, expectErr: io.EOF},
		{name: "child-error", mode: providerTerminalError, nextErr: errOperationProviderChild, expectErr: errOperationProviderChild},
		{name: "early-close", mode: providerEarlyClose, expectErr: errProviderEarlyClose},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			provider, closeProvider := newDuckLakeOperationPoolProvider(t)
			events := make(chan string, 32)
			allowClose := make(chan struct{})
			var allowCloseOnce sync.Once
			openChildClose := func() {
				allowCloseOnce.Do(func() { close(allowClose) })
			}
			child := &providerOperationChild{
				events:       events,
				nextErr:      tc.nextErr,
				closeStarted: make(chan struct{}),
				allowClose:   allowClose,
			}
			operationRelease := provider.BeginDuckLakeOperation(mycontext.WithFrontendQuery(context.Background()))
			t.Cleanup(func() {
				openChildClose()
				operationRelease()
			})
			release := func() {
				events <- "operation-release"
				operationRelease()
			}

			ctx := sql.NewContext(mycontext.WithFrontendQuery(context.Background()))
			terminalDone := make(chan error, 1)
			go func() {
				terminalDone <- runProviderChildTerminal(ctx, child, tc.mode, release)
			}()
			select {
			case <-child.closeStarted:
			case <-time.After(time.Second):
				t.Fatal("child close did not start")
			}

			closeDone := make(chan error, 1)
			go func() {
				closeErr := closeProvider()
				events <- "provider-close"
				closeDone <- closeErr
			}()
			waitForDuckLakeCleanupActive(t, provider)

			select {
			case closeErr := <-closeDone:
				t.Fatalf("provider closed while child close was blocked: %v", closeErr)
			case <-time.After(50 * time.Millisecond):
			}
			select {
			case terminalErr := <-terminalDone:
				t.Fatalf("terminal path completed before child close was released: %v", terminalErr)
			case <-time.After(25 * time.Millisecond):
			}

			openChildClose()
			var terminalErr error
			select {
			case terminalErr = <-terminalDone:
			case <-time.After(time.Second):
				t.Fatal("terminal path did not finish after child close release")
			}
			require.ErrorIs(t, terminalErr, tc.expectErr)

			select {
			case closeErr := <-closeDone:
				require.NoError(t, closeErr)
			case <-time.After(time.Second):
				t.Fatal("provider close did not finish after operation lease release")
			}

			got := make([]string, 0, 5)
			for {
				select {
				case event := <-events:
					got = append(got, event)
				default:
					goto drained
				}
			}
		drained:
			closeIndex := indexProviderEvent(got, "child-close-end")
			releaseIndex := indexProviderEvent(got, "operation-release")
			providerIndex := indexProviderEvent(got, "provider-close")
			require.GreaterOrEqual(t, closeIndex, 0)
			require.Greater(t, releaseIndex, closeIndex)
			require.Greater(t, providerIndex, releaseIndex)
		})
	}
}

var (
	errOperationProviderChild = errors.New("provider child failed")
	errProviderEarlyClose     = errors.New("provider child closed early")
)

func indexProviderEvent(events []string, want string) int {
	for i, event := range events {
		if event == want {
			return i
		}
	}
	return -1
}

func TestDuckLakeRollbackCleanupWaitsForChildCloseAndLeaseRelease(t *testing.T) {
	provider, closeProvider := newDuckLakeOperationPoolProvider(t)
	const sessionID uint32 = 881
	tx, err := provider.Pool().GetTxn(context.Background(), sessionID, "", nil)
	require.NoError(t, err)
	require.NotNil(t, tx)

	events := make(chan string, 32)
	allowClose := make(chan struct{})
	var allowCloseOnce sync.Once
	openChildClose := func() {
		allowCloseOnce.Do(func() { close(allowClose) })
	}
	child := &providerOperationChild{
		events:       events,
		nextErr:      io.EOF,
		closeStarted: make(chan struct{}),
		allowClose:   allowClose,
	}
	operationRelease := provider.BeginDuckLakeOperation(mycontext.WithFrontendQuery(context.Background()))
	reservationDone := make(chan func(), 1)
	var reservationRelease func()
	t.Cleanup(func() {
		openChildClose()
		operationRelease()
		if reservationRelease != nil {
			reservationRelease()
			return
		}
		select {
		case release := <-reservationDone:
			release()
		case <-time.After(time.Second):
		}
	})
	release := func() {
		events <- "operation-release"
		operationRelease()
	}

	go func() {
		reservationDone <- provider.BeginDuckLakeRollbackCleanup(tx)
	}()
	waitForDuckLakeCleanupActive(t, provider)

	ctx := sql.NewContext(mycontext.WithFrontendQuery(context.Background()))
	terminalDone := make(chan error, 1)
	go func() {
		terminalDone <- runProviderChildTerminal(ctx, child, providerTerminalEOF, release)
	}()
	select {
	case <-child.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("child close did not start")
	}
	select {
	case <-reservationDone:
		t.Fatal("rollback cleanup reservation entered while child close was blocked")
	case <-time.After(50 * time.Millisecond):
	}

	openChildClose()
	select {
	case terminalErr := <-terminalDone:
		require.ErrorIs(t, terminalErr, io.EOF)
	case <-time.After(time.Second):
		t.Fatal("terminal path did not finish")
	}

	select {
	case reservationRelease = <-reservationDone:
		events <- "rollback-reservation"
	case <-time.After(time.Second):
		t.Fatal("rollback cleanup reservation did not follow operation release")
	}

	inactive, rollbackErr := provider.Pool().RollbackTxnWithCleanup(sessionID, tx, func(*stdsql.Conn) error {
		events <- "rollback-cleanup"
		return nil
	})
	require.True(t, inactive)
	require.NoError(t, rollbackErr)
	events <- "rollback-finished"
	reservationRelease()
	require.NoError(t, closeProvider())

	got := make([]string, 0, 8)
	for {
		select {
		case event := <-events:
			got = append(got, event)
		default:
			goto drainedRollback
		}
	}

drainedRollback:
	closeIndex := indexProviderEvent(got, "child-close-end")
	releaseIndex := indexProviderEvent(got, "operation-release")
	reservationIndex := indexProviderEvent(got, "rollback-reservation")
	cleanupIndex := indexProviderEvent(got, "rollback-cleanup")
	require.GreaterOrEqual(t, closeIndex, 0)
	require.Greater(t, releaseIndex, closeIndex)
	require.Greater(t, reservationIndex, releaseIndex)
	require.Greater(t, cleanupIndex, reservationIndex)
}

func TestDuckLakeGenerationTransitionKeepsPoolWriterOutOfAdmissionWait(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := stdsql.OpenDB(connector)
	p := NewConnectionPool(connector, db, "memory")
	provider := &DatabaseProvider{duckLake: &duckLakeRuntime{config: configuration.DuckLakeConfig{
		MetadataPath: "/tmp/task77/catalog.ducklake",
		DataPath:     "/tmp/task77/data",
	}}}
	p.SetTransactionLifecycleHooks(provider.beginDuckLakeTransaction, provider.untrackDuckLakeTransaction)

	releaseTransition := provider.beginDuckLakeGenerationTransition()
	t.Cleanup(func() {
		releaseTransition()
		_ = p.Close()
		_ = connector.Close()
	})

	txnDone := make(chan error, 1)
	go func() {
		_, getErr := p.GetTxn(context.Background(), 771, "", nil)
		txnDone <- getErr
	}()

	// Pool shutdown must be able to acquire its writer even though GetTxn is
	// waiting for cleanupActive. The admission wait happens before RLock.
	closeDone := make(chan error, 1)
	go func() { closeDone <- p.Close() }()
	select {
	case closeErr := <-closeDone:
		require.NoError(t, closeErr)
	case <-time.After(time.Second):
		t.Fatal("pool close blocked behind a transaction admission wait")
	}

	releaseTransition()
	select {
	case getErr := <-txnDone:
		require.Error(t, getErr)
	case <-time.After(time.Second):
		t.Fatal("transaction admission did not resume after transition release")
	}
}

func TestDuckLakeOperationAdmissionRequiresFrontendAndCompleteConfiguration(t *testing.T) {
	provider := newDuckLakeOperationTestProvider()
	for _, ctx := range []context.Context{
		context.Background(),
		mycontext.WithMaintenanceQuery(context.Background()),
		mycontext.WithQueryOrigin(context.Background(), mycontext.MySQLReplicationQueryOrigin),
	} {
		release := provider.BeginDuckLakeOperation(ctx)
		release()
	}
	require.Zero(t, duckLakeOperationCount(provider))

	extensionOnly := &DatabaseProvider{duckLake: &duckLakeRuntime{}}
	release := extensionOnly.BeginDuckLakeOperation(mycontext.WithFrontendQuery(context.Background()))
	release()
	require.Zero(t, duckLakeOperationCount(extensionOnly))
}
