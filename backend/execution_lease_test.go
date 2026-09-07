package backend

import (
	"context"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestWrapRowIterWithReleaseKeepsExistingTerminalOrdering(t *testing.T) {
	ctx := sql.NewEmptyContext()
	events := []string{}
	child := &orderedOperationIter{events: &events}
	wrapped := WrapRowIterWithRelease(child, func() {
		events = append(events, "release")
	})

	_, err := wrapped.Next(ctx)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, []string{"next", "close", "release"}, events)
	require.NoError(t, wrapped.Close(ctx))
	require.Equal(t, 1, child.closeCalls)
}

type sessionLeaseFixture struct {
	provider *catalog.DatabaseProvider
	session  *Session
	ctx      *sql.Context
}

func newSessionLeaseFixture(t *testing.T) *sessionLeaseFixture {
	t.Helper()
	provider := catalog.NewInMemoryDBProvider()
	session := NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))
	return &sessionLeaseFixture{provider: provider, session: session, ctx: ctx}
}

func startProviderCloseAfterStorageShutdown(
	t *testing.T,
	fixture *sessionLeaseFixture,
) <-chan error {
	t.Helper()
	closeDone := make(chan error, 1)
	go func() { closeDone <- fixture.provider.Close() }()

	deadline := time.Now().Add(5 * time.Second)
	for fixture.provider.Storage().PingContext(context.Background()) == nil {
		if time.Now().After(deadline) {
			t.Fatal("provider close did not announce storage shutdown")
		}
		runtime.Gosched()
	}
	return closeDone
}

func requireProviderCloseWaiting(t *testing.T, closeDone <-chan error) {
	t.Helper()
	select {
	case err := <-closeDone:
		t.Fatalf("provider closed before the query lease reached its terminal boundary: %v", err)
	default:
	}
}

func requireProviderClosed(t *testing.T, closeDone <-chan error) {
	t.Helper()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("provider close remained blocked after query lease release")
	}
}

func TestSessionLeasedRowsKeepProviderCloseWaitingUntilEOF(t *testing.T) {
	fixture := newSessionLeaseFixture(t)
	rows, err := adapter.QueryCatalog(fixture.ctx, "SELECT value FROM (VALUES (1), (2)) AS lease_rows(value)")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rows.Close() })

	closeDone := startProviderCloseAfterStorageShutdown(t, fixture)
	requireProviderCloseWaiting(t, closeDone)

	var values []int
	for rows.Next() {
		var value int
		require.NoError(t, rows.Scan(&value))
		values = append(values, value)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []int{1, 2}, values)
	requireProviderClosed(t, closeDone)
}

func TestSessionLeasedRowsKeepProviderCloseWaitingUntilExplicitClose(t *testing.T) {
	fixture := newSessionLeaseFixture(t)
	rows, err := adapter.QueryCatalog(fixture.ctx, "SELECT value FROM (VALUES (1), (2)) AS lease_rows(value)")
	require.NoError(t, err)

	closeDone := startProviderCloseAfterStorageShutdown(t, fixture)
	requireProviderCloseWaiting(t, closeDone)
	require.NoError(t, rows.Close())
	requireProviderClosed(t, closeDone)
}

func TestSessionLeasedRowKeepsProviderCloseWaitingUntilScan(t *testing.T) {
	fixture := newSessionLeaseFixture(t)
	row := fixture.session.QueryRow(context.Background(), "SELECT 42")
	require.NotNil(t, row)

	closeDone := startProviderCloseAfterStorageShutdown(t, fixture)
	requireProviderCloseWaiting(t, closeDone)

	var value int
	require.NoError(t, row.Scan(&value))
	require.Equal(t, 42, value)
	requireProviderClosed(t, closeDone)
}

func TestSessionLeasedRowPreservesExecutionLeaseAdmissionError(t *testing.T) {
	fixture := newSessionLeaseFixture(t)
	require.NoError(t, fixture.provider.Close())

	_, _, release, expectedErr := fixture.session.GetExecutionSnapshotLease(context.Background(), true)
	release()
	require.Error(t, expectedErr)

	row := fixture.session.QueryRow(context.Background(), "SELECT must_not_execute")
	require.NotNil(t, row)
	var value int
	require.EqualError(t, row.Scan(&value), expectedErr.Error())
}

var _ interface {
	QueryRow(context.Context, string, ...any) *adapter.LeasedRow
} = (*Session)(nil)
