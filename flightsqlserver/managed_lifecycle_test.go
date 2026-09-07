package flightsqlserver

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/stretchr/testify/require"
)

type testStatementQueryTicket struct{ handle []byte }

func (t testStatementQueryTicket) GetStatementHandle() []byte { return t.handle }

type testGetTables struct {
	tableName     *string
	includeSchema bool
}

func (t testGetTables) GetCatalog() *string                { return nil }
func (t testGetTables) GetDBSchemaFilterPattern() *string  { return nil }
func (t testGetTables) GetTableNameFilterPattern() *string { return t.tableName }
func (t testGetTables) GetTableTypes() []string            { return nil }
func (t testGetTables) GetIncludeSchema() bool             { return t.includeSchema }

func newManagedFlightSQLServer(t *testing.T) (*catalog.DatabaseProvider, *SQLiteFlightSQLServer) {
	t.Helper()
	provider, err := catalog.NewDBProvider("", t.TempDir(), "managed_flight")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, provider.Close())
	})
	server, err := NewSQLiteFlightSQLServerWithProvider(provider)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, server.Close())
	})
	return provider, server
}

func managedQueryTicket(t *testing.T, query string) testStatementQueryTicket {
	t.Helper()
	return testStatementQueryTicket{handle: append([]byte{':'}, []byte(query)...)}
}

func managedQueryInt64(t *testing.T, server *SQLiteFlightSQLServer, query string) int64 {
	t.Helper()
	_, chunks, err := server.DoGetStatement(context.Background(), managedQueryTicket(t, query))
	require.NoError(t, err)
	var (
		value int64
		seen  bool
	)
	for chunk := range chunks {
		require.NoError(t, chunk.Err)
		if chunk.Data == nil {
			continue
		}
		column, ok := chunk.Data.Column(0).(*array.Int64)
		require.True(t, ok)
		if column.Len() > 0 {
			value = column.Value(0)
			seen = true
		}
		chunk.Data.Release()
	}
	require.True(t, seen)
	return value
}

func countSyncMap(values interface{ Range(func(any, any) bool) }) int {
	count := 0
	values.Range(func(any, any) bool {
		count++
		return true
	})
	return count
}

func TestManagedFlightSQLUsesReplacementProviderGeneration(t *testing.T) {
	provider, server := newManagedFlightSQLServer(t)
	ctx := context.Background()

	_, err := server.DoPutCommandStatementUpdate(ctx, testStatementUpdate{query: "CREATE TABLE managed_restart (id BIGINT)"})
	require.NoError(t, err)
	_, err = server.DoPutCommandStatementUpdate(ctx, testStatementUpdate{query: "INSERT INTO managed_restart VALUES (1)"})
	require.NoError(t, err)
	require.EqualValues(t, 1, managedQueryInt64(t, server, "SELECT COUNT(*) FROM managed_restart"))

	require.NoError(t, provider.Restart(false))
	require.EqualValues(t, 1, managedQueryInt64(t, server, "SELECT COUNT(*) FROM managed_restart"))
}

func TestManagedFlightSQLGetTablesIncludesSchemaAcrossRestart(t *testing.T) {
	provider, server := newManagedFlightSQLServer(t)
	ctx := context.Background()
	const tableName = "managed_tables_schema"
	_, err := server.DoPutCommandStatementUpdate(ctx, testStatementUpdate{
		query: "CREATE TABLE " + tableName + " (id BIGINT NOT NULL, name VARCHAR)",
	})
	require.NoError(t, err)

	check := func() {
		t.Helper()
		resultSchema, chunks, err := server.DoGetTables(ctx, testGetTables{
			tableName:     func() *string { value := tableName; return &value }(),
			includeSchema: true,
		})
		require.NoError(t, err)
		require.Len(t, resultSchema.FieldIndices("table_schema"), 1)

		matched := 0
		for chunk := range chunks {
			require.NoError(t, chunk.Err)
			if chunk.Data == nil {
				continue
			}
			tableNames := chunk.Data.Column(chunk.Data.Schema().FieldIndices("table_name")[0]).(*array.String)
			tableSchemas := chunk.Data.Column(chunk.Data.Schema().FieldIndices("table_schema")[0]).(*array.Binary)
			for row := 0; row < tableNames.Len(); row++ {
				if tableNames.Value(row) != tableName {
					continue
				}
				matched++
				included, decodeErr := flight.DeserializeSchema(tableSchemas.Value(row), memory.DefaultAllocator)
				require.NoError(t, decodeErr)
				fields := included.Fields()
				require.Len(t, fields, 2)
				require.Equal(t, "id", fields[0].Name)
				require.Equal(t, "name", fields[1].Name)
				require.False(t, fields[0].Nullable)
				require.True(t, fields[1].Nullable)
			}
			chunk.Data.Release()
		}
		require.Equal(t, 1, matched)
	}

	check()
	require.NoError(t, provider.Restart(false))
	check()
}

func TestManagedFlightSQLStreamKeepsRestartBehindExecutionLease(t *testing.T) {
	provider, server := newManagedFlightSQLServer(t)
	_, chunks, err := server.DoGetStatement(
		context.Background(),
		managedQueryTicket(t, "SELECT range AS id FROM range(10000000)"),
	)
	require.NoError(t, err)

	restartDone := make(chan error, 1)
	go func() {
		restartDone <- provider.Restart(false)
	}()
	select {
	case err := <-restartDone:
		t.Fatalf("provider restart completed before the FlightSQL stream drained: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, drainFlightChunks(chunks))
	select {
	case err := <-restartDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("provider restart did not complete after the FlightSQL stream drained")
	}
	require.EqualValues(t, 1, managedQueryInt64(t, server, "SELECT COUNT(*) FROM range(1)"))
}

func TestManagedFlightSQLRestartInvalidatesIdlePreparedAndTransactionHandles(t *testing.T) {
	provider, server := newManagedFlightSQLServer(t)
	ctx := context.Background()
	_, err := server.DoPutCommandStatementUpdate(ctx, testStatementUpdate{query: "CREATE TABLE managed_txn_restart (id BIGINT)"})
	require.NoError(t, err)

	standalone, err := server.CreatePreparedStatement(ctx, testCreatePreparedRequest{query: "SELECT 1"})
	require.NoError(t, err)
	txnID, err := server.BeginTransaction(ctx, struct{}{})
	require.NoError(t, err)
	_, err = server.DoPutCommandStatementUpdate(ctx, testStatementUpdate{
		query: "INSERT INTO managed_txn_restart VALUES (1)",
		txn:   txnID,
	})
	require.NoError(t, err)
	_, err = server.CreatePreparedStatement(ctx, testCreatePreparedRequest{
		query: "INSERT INTO managed_txn_restart VALUES (2)",
		txn:   txnID,
	})
	require.NoError(t, err)

	require.NoError(t, provider.Restart(false))
	require.Zero(t, countSyncMap(&server.prepared))
	require.Zero(t, countSyncMap(&server.openTransactions))
	_, err = server.GetFlightInfoPreparedStatement(ctx, testPreparedCommand{handle: standalone.Handle}, &flight.FlightDescriptor{})
	require.Error(t, err)
	err = server.EndTransaction(ctx, testEndTransactionRequest{txn: txnID, action: flightsql.EndTransactionCommit})
	require.Error(t, err)
	require.EqualValues(t, 0, managedQueryInt64(t, server, "SELECT COUNT(*) FROM managed_txn_restart"))

	fresh, err := server.CreatePreparedStatement(ctx, testCreatePreparedRequest{query: "SELECT 2"})
	require.NoError(t, err)
	require.NoError(t, server.ClosePreparedStatement(ctx, testPreparedCommand{handle: fresh.Handle}))
}

func TestManagedFlightSQLClosePreparedStatementAfterPythonStyleDDL(t *testing.T) {
	_, server := newManagedFlightSQLServer(t)
	ctx := context.Background()

	drop, err := server.CreatePreparedStatement(ctx, testCreatePreparedRequest{query: "DROP TABLE IF EXISTS intTable"})
	require.NoError(t, err)
	_, err = server.DoPutCommandStatementUpdate(ctx, testStatementUpdate{query: "DROP TABLE IF EXISTS intTable"})
	require.NoError(t, err)
	require.NoError(t, server.ClosePreparedStatement(ctx, testPreparedCommand{handle: drop.Handle}))
	require.NoError(t, server.ClosePreparedStatement(ctx, testPreparedCommand{handle: drop.Handle}))

	create, err := server.CreatePreparedStatement(ctx, testCreatePreparedRequest{
		query: "CREATE TABLE IF NOT EXISTS intTable (id INTEGER PRIMARY KEY, name VARCHAR(50), value INT)",
	})
	require.NoError(t, err)
	_, err = server.DoPutCommandStatementUpdate(ctx, testStatementUpdate{
		query: "CREATE TABLE IF NOT EXISTS intTable (id INTEGER PRIMARY KEY, name VARCHAR(50), value INT)",
	})
	require.NoError(t, err)
	require.NoError(t, server.ClosePreparedStatement(ctx, testPreparedCommand{handle: create.Handle}))
	require.NoError(t, server.ClosePreparedStatement(ctx, testPreparedCommand{handle: create.Handle}))
}

func TestManagedFlightSQLPreparedStreamBlocksRestartButIdleHandleDoesNot(t *testing.T) {
	provider, server := newManagedFlightSQLServer(t)
	ctx := context.Background()
	prepared, err := server.CreatePreparedStatement(ctx, testCreatePreparedRequest{
		query: "SELECT range AS id FROM range(10000000)",
	})
	require.NoError(t, err)
	command := testPreparedCommand{handle: prepared.Handle}
	_, chunks, err := server.DoGetPreparedStatement(ctx, command)
	require.NoError(t, err)

	restartDone := make(chan error, 1)
	go func() {
		restartDone <- provider.Restart(false)
	}()
	select {
	case err := <-restartDone:
		t.Fatalf("provider restart completed before the prepared stream drained: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, drainFlightChunks(chunks))
	select {
	case err := <-restartDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("provider restart did not complete after the prepared stream drained")
	}
	require.Zero(t, countSyncMap(&server.prepared))

	idle, err := server.CreatePreparedStatement(ctx, testCreatePreparedRequest{query: "SELECT 1"})
	require.NoError(t, err)
	restartDone = make(chan error, 1)
	go func() {
		restartDone <- provider.Restart(false)
	}()
	select {
	case err := <-restartDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("idle prepared handle blocked provider restart")
	}
	_, present := server.prepared.Load(string(idle.Handle))
	require.False(t, present)
}

func TestManagedFlightSQLAdmissionFailurePrecedesConnectionInitialization(t *testing.T) {
	provider, server := newManagedFlightSQLServer(t)
	sentinel := errors.New("managed admission rejected")
	var initialized atomic.Int64
	provider.Pool().SetConnectionInitializer(func(context.Context, *sql.Conn) error {
		initialized.Add(1)
		return nil
	})
	provider.Pool().SetConnectionAdmissionGuard(func(context.Context) error {
		return sentinel
	})

	_, err := server.DoPutCommandStatementUpdate(context.Background(), testStatementUpdate{query: "SELECT 1"})
	require.ErrorIs(t, err, sentinel)
	require.Zero(t, initialized.Load())
}

func TestManagedFlightSQLCloseCleansHandlesWithoutClosingProvider(t *testing.T) {
	provider, server := newManagedFlightSQLServer(t)
	ctx := context.Background()
	_, err := server.CreatePreparedStatement(ctx, testCreatePreparedRequest{query: "SELECT 1"})
	require.NoError(t, err)
	_, err = server.BeginTransaction(ctx, struct{}{})
	require.NoError(t, err)

	require.NoError(t, server.Close())
	require.NoError(t, server.Close())
	require.Zero(t, countSyncMap(&server.prepared))
	require.Zero(t, countSyncMap(&server.openTransactions))
	require.NoError(t, provider.Storage().PingContext(ctx))
	_, err = server.DoPutCommandStatementUpdate(ctx, testStatementUpdate{query: "SELECT 1"})
	require.ErrorIs(t, err, catalog.ErrExternalSessionClosed)
}
