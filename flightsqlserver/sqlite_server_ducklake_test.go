package flightsqlserver

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

type physicalConnectionRecorder struct {
	mu       sync.Mutex
	ids      []string
	origins  []mycontext.QueryOriginKind
	entered  chan struct{}
	release  chan struct{}
	blockFor int
}

func (r *physicalConnectionRecorder) initialize(ctx context.Context, conn *sql.Conn) error {
	id, err := physicalConnectionID(conn)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.ids = append(r.ids, id)
	r.origins = append(r.origins, mycontext.QueryOrigin(ctx))
	call := len(r.ids)
	r.mu.Unlock()
	if r.entered != nil && call <= r.blockFor {
		r.entered <- struct{}{}
		<-r.release
	}
	return nil
}

func (r *physicalConnectionRecorder) snapshot() (ids []string, origins []mycontext.QueryOriginKind) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ids...), append([]mycontext.QueryOriginKind(nil), r.origins...)
}

func physicalConnectionID(conn *sql.Conn) (string, error) {
	var id string
	err := conn.Raw(func(driverConn any) error {
		value := reflect.ValueOf(driverConn)
		if value.IsValid() && value.Kind() == reflect.Pointer {
			id = fmt.Sprintf("%s:%x", value.Type(), value.Pointer())
		} else {
			id = fmt.Sprintf("%T:%v", driverConn, driverConn)
		}
		return nil
	})
	return id, err
}

func newFlightSQLTestDB(t *testing.T) *sql.DB {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, connector.Close())
	})
	return db
}

func drainFlightChunks(ch <-chan flight.StreamChunk) error {
	var firstErr error
	for chunk := range ch {
		if chunk.Data != nil {
			chunk.Data.Release()
		}
		if firstErr == nil && chunk.Err != nil {
			firstErr = chunk.Err
		}
	}
	return firstErr
}

func runTableTypes(t *testing.T, srv *SQLiteFlightSQLServer) {
	t.Helper()
	_, chunks, err := srv.DoGetTableTypes(context.Background())
	require.NoError(t, err)
	require.NoError(t, drainFlightChunks(chunks))
}

func TestFlightSQLInitializesDistinctConcurrentPhysicalConnections(t *testing.T) {
	db := newFlightSQLTestDB(t)
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)

	recorder := &physicalConnectionRecorder{
		entered:  make(chan struct{}, 2),
		release:  make(chan struct{}),
		blockFor: 2,
	}
	srv, err := NewSQLiteFlightSQLServer(db, recorder.initialize)
	require.NoError(t, err)

	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, chunks, err := srv.DoGetTableTypes(context.Background())
			if err == nil {
				err = drainFlightChunks(chunks)
			}
			results <- err
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-recorder.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent requests did not acquire two physical connections")
		}
	}
	close(recorder.release)
	for i := 0; i < 2; i++ {
		require.NoError(t, <-results)
	}

	ids, origins := recorder.snapshot()
	require.Len(t, ids, 2)
	require.NotEqual(t, ids[0], ids[1])
	require.Equal(t, []mycontext.QueryOriginKind{mycontext.FrontendQueryOrigin, mycontext.FrontendQueryOrigin}, origins)
}

func TestFlightSQLInitializesAfterPhysicalConnectionEviction(t *testing.T) {
	db := newFlightSQLTestDB(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)

	recorder := &physicalConnectionRecorder{}
	srv, err := NewSQLiteFlightSQLServer(db, recorder.initialize)
	require.NoError(t, err)

	runTableTypes(t, srv)
	firstStats := db.Stats()
	runTableTypes(t, srv)

	ids, origins := recorder.snapshot()
	require.Len(t, ids, 2)
	require.NotEqual(t, ids[0], ids[1])
	require.GreaterOrEqual(t, db.Stats().MaxIdleClosed, firstStats.MaxIdleClosed+1)
	require.Equal(t, []mycontext.QueryOriginKind{mycontext.FrontendQueryOrigin, mycontext.FrontendQueryOrigin}, origins)
}

type testCreatePreparedRequest struct {
	query string
	txn   []byte
}

func (r testCreatePreparedRequest) GetQuery() string         { return r.query }
func (r testCreatePreparedRequest) GetTransactionId() []byte { return r.txn }

type testPreparedCommand struct{ handle []byte }

func (r testPreparedCommand) GetPreparedStatementHandle() []byte { return r.handle }

type testStatementUpdate struct {
	query string
	txn   []byte
}

func (r testStatementUpdate) GetQuery() string         { return r.query }
func (r testStatementUpdate) GetTransactionId() []byte { return r.txn }

type testEndTransactionRequest struct {
	txn    []byte
	action flightsql.EndTransactionRequestType
}

func (r testEndTransactionRequest) GetTransactionId() []byte                       { return r.txn }
func (r testEndTransactionRequest) GetAction() flightsql.EndTransactionRequestType { return r.action }

type testMessageReader struct {
	array.RecordReader
}

func (r *testMessageReader) Read() (arrow.RecordBatch, error) {
	if !r.Next() {
		if err := r.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	record := r.RecordBatch()
	record.Retain()
	return record, nil
}

func (r *testMessageReader) Chunk() flight.StreamChunk {
	return flight.StreamChunk{Data: r.RecordBatch()}
}

func (r *testMessageReader) LatestFlightDescriptor() *flight.FlightDescriptor { return nil }
func (r *testMessageReader) LatestAppMetadata() []byte                        { return nil }

func emptyMessageReader(t *testing.T) *testMessageReader {
	t.Helper()
	reader, err := array.NewRecordReader(arrow.NewSchema(nil, nil), nil)
	require.NoError(t, err)
	return &testMessageReader{RecordReader: reader}
}

func oneIntMessageReader(t *testing.T, value int64) *testMessageReader {
	t.Helper()
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.Append(value)
	values := builder.NewArray()
	builder.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
	values.Release()
	reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{record})
	require.NoError(t, err)
	record.Release()
	return &testMessageReader{RecordReader: reader}
}

func TestFlightSQLPreparedAndTransactionOperationsStayOnInitializedConnection(t *testing.T) {
	db := newFlightSQLTestDB(t)
	_, err := db.Exec("CREATE TABLE task71_prepared (id BIGINT)")
	require.NoError(t, err)
	recorder := &physicalConnectionRecorder{}
	srv, err := NewSQLiteFlightSQLServer(db, recorder.initialize)
	require.NoError(t, err)
	ctx := context.Background()

	queryResult, err := srv.CreatePreparedStatement(ctx, testCreatePreparedRequest{query: "SELECT id FROM task71_prepared WHERE id = ?"})
	require.NoError(t, err)
	queryHandle := testPreparedCommand{handle: queryResult.Handle}
	queryState, ok := srv.prepared.Load(string(queryResult.Handle))
	require.True(t, ok)
	queryStatement := queryState.(Statement)
	queryConnID, err := physicalConnectionID(queryStatement.conn)
	require.NoError(t, err)

	params := oneIntMessageReader(t, 7)
	_, err = srv.DoPutPreparedStatementQuery(ctx, queryHandle, params, nil)
	require.NoError(t, err)
	params.Release()
	_, chunks, err := srv.DoGetPreparedStatement(ctx, queryHandle)
	require.NoError(t, err)
	require.NoError(t, drainFlightChunks(chunks))
	ids, _ := recorder.snapshot()
	require.Len(t, ids, 1)
	require.Equal(t, queryConnID, ids[0])
	require.NoError(t, srv.ClosePreparedStatement(ctx, queryHandle))

	updateResult, err := srv.CreatePreparedStatement(ctx, testCreatePreparedRequest{query: "INSERT INTO task71_prepared VALUES (?)"})
	require.NoError(t, err)
	updateHandle := testPreparedCommand{handle: updateResult.Handle}
	updateState, ok := srv.prepared.Load(string(updateResult.Handle))
	require.True(t, ok)
	updateConnID, err := physicalConnectionID(updateState.(Statement).conn)
	require.NoError(t, err)
	updateParams := oneIntMessageReader(t, 7)
	affected, err := srv.DoPutPreparedStatementUpdate(ctx, updateHandle, updateParams)
	require.NoError(t, err)
	require.EqualValues(t, 1, affected)
	updateParams.Release()
	ids, _ = recorder.snapshot()
	require.Len(t, ids, 2)
	require.Equal(t, updateConnID, ids[1])
	require.NoError(t, srv.ClosePreparedStatement(ctx, updateHandle))

	txnID, err := srv.BeginTransaction(ctx, struct{}{})
	require.NoError(t, err)
	txnValue, ok := srv.openTransactions.Load(string(txnID))
	require.True(t, ok)
	txnState := txnValue.(transactionState)
	txnConnID, err := physicalConnectionID(txnState.conn)
	require.NoError(t, err)
	ids, origins := recorder.snapshot()
	require.Len(t, ids, 3)
	require.Equal(t, txnConnID, ids[2])
	require.Equal(t, mycontext.FrontendQueryOrigin, origins[2])
	_, err = srv.DoPutCommandStatementUpdate(ctx, testStatementUpdate{
		query: "INSERT INTO task71_prepared VALUES (8)",
		txn:   txnID,
	})
	require.NoError(t, err)
	require.NoError(t, srv.EndTransaction(ctx, testEndTransactionRequest{txn: txnID, action: flightsql.EndTransactionCommit}))
}
