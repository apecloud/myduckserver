package flightsqlserver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/duckdb/duckdb-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type arrowQuerySetup struct {
	schema *arrow.Schema
	err    error
}

func (s *SQLiteFlightSQLServer) beginManagedRequest(ctx context.Context) (context.Context, *managedFlightRequest, error) {
	if s == nil || s.provider == nil {
		return nil, nil, fmt.Errorf("FlightSQL provider lifecycle is unavailable")
	}
	ctx = mycontext.WithFrontendQuery(ctx)
	requestCtx, cancel := context.WithCancel(ctx)
	request := &managedFlightRequest{cancel: cancel, done: make(chan struct{})}
	s.requestMu.Lock()
	if s.closed {
		s.requestMu.Unlock()
		cancel()
		return nil, nil, catalog.ErrExternalSessionClosed
	}
	if s.requests == nil {
		s.requests = make(map[*managedFlightRequest]struct{})
	}
	s.requests[request] = struct{}{}
	s.requestMu.Unlock()
	return requestCtx, request, nil
}

func (s *SQLiteFlightSQLServer) finishManagedRequest(request *managedFlightRequest) {
	if request == nil {
		return
	}
	request.once.Do(func() {
		request.cancel()
		s.requestMu.Lock()
		delete(s.requests, request)
		s.requestMu.Unlock()
		close(request.done)
	})
}

func (s *SQLiteFlightSQLServer) openExternalSession(ctx context.Context) (*catalog.ExternalSession, error) {
	if s == nil || s.provider == nil || s.provider.Pool() == nil {
		return nil, fmt.Errorf("FlightSQL provider lifecycle is unavailable")
	}
	return s.provider.Pool().OpenExternalSession(ctx)
}

// acquireExternalOperation enters the provider operation barrier before the
// pool lifecycle reader. This lock order lets a provider transition announce
// cleanup without deadlocking behind an operation waiting for the pool.
func (s *SQLiteFlightSQLServer) acquireExternalOperation(
	ctx context.Context,
	session *catalog.ExternalSession,
	owner any,
) (*sql.Conn, *sql.Tx, func(), error) {
	if s == nil || s.provider == nil || session == nil {
		return nil, nil, func() {}, fmt.Errorf("FlightSQL external session is unavailable")
	}
	releaseOperation, err := s.provider.BeginDuckLakeOperationForOwnerWithError(ctx, owner)
	if err != nil {
		return nil, nil, func() {}, err
	}
	conn, tx, releaseSession, err := session.ExecutionLease(ctx)
	if err != nil {
		releaseOperation()
		return nil, nil, func() {}, err
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			releaseSession()
			releaseOperation()
		})
	}
	return conn, tx, release, nil
}

func requireExternalTransaction(actual, expected *sql.Tx) error {
	if actual != expected {
		return catalog.ErrExternalSessionTransactionMismatch
	}
	return nil
}

func (s *SQLiteFlightSQLServer) managedStatelessQuery(
	ctx context.Context,
	query string,
	args ...interface{},
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	if err := catalog.RejectSensitiveSQL(query); err != nil {
		return nil, nil, err
	}
	ctx, request, err := s.beginManagedRequest(ctx)
	if err != nil {
		return nil, nil, err
	}
	session, err := s.openExternalSession(ctx)
	if err != nil {
		s.finishManagedRequest(request)
		return nil, nil, err
	}
	conn, tx, release, err := s.acquireExternalOperation(ctx, session, session)
	if err != nil {
		_ = session.Close()
		s.finishManagedRequest(request)
		return nil, nil, err
	}
	if tx != nil {
		release()
		_ = session.Close()
		s.finishManagedRequest(request)
		return nil, nil, catalog.ErrExternalSessionTransactionMismatch
	}
	terminal := func() {
		release()
		_ = session.Close()
		s.finishManagedRequest(request)
	}
	return startArrowQuery(ctx, conn, query, [][]interface{}{args}, terminal)
}

func (s *SQLiteFlightSQLServer) managedTablesQuery(
	ctx context.Context,
	query string,
	includeSchema bool,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	ctx, request, err := s.beginManagedRequest(ctx)
	if err != nil {
		return nil, nil, err
	}
	session, err := s.openExternalSession(ctx)
	if err != nil {
		s.finishManagedRequest(request)
		return nil, nil, err
	}
	conn, tx, release, err := s.acquireExternalOperation(ctx, session, session)
	if err != nil {
		_ = session.Close()
		s.finishManagedRequest(request)
		return nil, nil, err
	}
	terminal := func() {
		release()
		_ = session.Close()
		s.finishManagedRequest(request)
	}
	if tx != nil {
		terminal()
		return nil, nil, catalog.ErrExternalSessionTransactionMismatch
	}
	if !includeSchema {
		return startArrowQuery(ctx, conn, query, [][]interface{}{nil}, terminal)
	}
	return startMaterializedTablesQuery(ctx, s.Alloc, conn, query, terminal)
}

func (s *SQLiteFlightSQLServer) managedStatementUpdate(ctx context.Context, cmd flightsql.StatementUpdate) (int64, error) {
	ctx, request, err := s.beginManagedRequest(ctx)
	if err != nil {
		return 0, err
	}
	defer s.finishManagedRequest(request)

	if len(cmd.GetTransactionId()) > 0 {
		value, loaded := s.openTransactions.Load(string(cmd.GetTransactionId()))
		state, ok := value.(*transactionState)
		if !loaded || !ok || state == nil || state.external == nil || state.tx == nil {
			return -1, status.Error(codes.InvalidArgument, "invalid transaction handle provided")
		}
		conn, tx, release, err := s.acquireExternalOperation(ctx, state.external, state.tx)
		_ = conn
		if err != nil {
			return 0, err
		}
		defer release()
		if state.closed.Load() {
			return 0, catalog.ErrExternalSessionClosed
		}
		if err := requireExternalTransaction(tx, state.tx); err != nil {
			return 0, err
		}
		result, err := tx.ExecContext(ctx, cmd.GetQuery())
		if err != nil {
			return 0, err
		}
		return result.RowsAffected()
	}

	session, err := s.openExternalSession(ctx)
	if err != nil {
		return 0, err
	}
	defer session.Close()
	conn, tx, release, err := s.acquireExternalOperation(ctx, session, session)
	if err != nil {
		return 0, err
	}
	defer release()
	if tx != nil {
		return 0, catalog.ErrExternalSessionTransactionMismatch
	}
	result, err := conn.ExecContext(ctx, cmd.GetQuery())
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *SQLiteFlightSQLServer) createManagedPreparedStatement(
	ctx context.Context,
	req flightsql.ActionCreatePreparedStatementRequest,
) (result flightsql.ActionCreatePreparedStatementResult, err error) {
	ctx, request, err := s.beginManagedRequest(ctx)
	if err != nil {
		return result, err
	}
	defer s.finishManagedRequest(request)

	handle := genRandomString()
	handleKey := string(handle)
	state := &Statement{query: req.GetQuery()}
	if len(req.GetTransactionId()) > 0 {
		value, loaded := s.openTransactions.Load(string(req.GetTransactionId()))
		txnState, ok := value.(*transactionState)
		if !loaded || !ok || txnState == nil || txnState.external == nil || txnState.tx == nil {
			return result, status.Error(codes.InvalidArgument, "invalid transaction handle provided")
		}
		conn, tx, release, leaseErr := s.acquireExternalOperation(ctx, txnState.external, txnState.tx)
		if leaseErr != nil {
			return result, leaseErr
		}
		defer release()
		if txnState.closed.Load() {
			return result, catalog.ErrExternalSessionClosed
		}
		if leaseErr = requireExternalTransaction(tx, txnState.tx); leaseErr != nil {
			return result, leaseErr
		}
		prepared, prepareErr := tx.PrepareContext(ctx, req.GetQuery())
		if prepareErr != nil {
			return result, prepareErr
		}
		state.stmt = prepared
		state.conn = conn
		state.external = txnState.external
		state.transaction = txnState
		s.prepared.Store(handleKey, state)
	} else {
		session, openErr := s.openExternalSession(context.WithoutCancel(ctx))
		if openErr != nil {
			return result, openErr
		}
		state.external = session
		state.ownsConn = true
		if hookErr := session.SetCleanupHook(func() {
			s.cleanupManagedStatement(handleKey, state)
		}); hookErr != nil {
			_ = session.Close()
			return result, hookErr
		}
		conn, tx, release, leaseErr := s.acquireExternalOperation(ctx, session, session)
		if leaseErr != nil {
			_ = session.Close()
			return result, leaseErr
		}
		if state.closed.Load() || tx != nil {
			release()
			_ = session.Close()
			if state.closed.Load() {
				return result, catalog.ErrExternalSessionClosed
			}
			return result, catalog.ErrExternalSessionTransactionMismatch
		}
		prepared, prepareErr := conn.PrepareContext(ctx, req.GetQuery())
		if prepareErr != nil {
			release()
			_ = session.Close()
			return result, prepareErr
		}
		state.stmt = prepared
		state.conn = conn
		s.prepared.Store(handleKey, state)
		release()
	}

	result.Handle = handle
	return result, nil
}

func (s *SQLiteFlightSQLServer) managedPreparedQuery(
	ctx context.Context,
	cmd flightsql.PreparedStatementQuery,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	ctx, request, err := s.beginManagedRequest(ctx)
	if err != nil {
		return nil, nil, err
	}
	value, loaded := s.prepared.Load(string(cmd.GetPreparedStatementHandle()))
	stmt, ok := value.(*Statement)
	if !loaded || !ok || stmt == nil || stmt.external == nil {
		s.finishManagedRequest(request)
		return nil, nil, status.Error(codes.InvalidArgument, "prepared statement not found")
	}
	if err := catalog.RejectSensitiveSQL(stmt.query); err != nil {
		s.finishManagedRequest(request)
		return nil, nil, err
	}
	owner := any(stmt.external)
	var expected *sql.Tx
	if stmt.transaction != nil {
		expected = stmt.transaction.tx
		owner = expected
	}
	conn, tx, release, err := s.acquireExternalOperation(ctx, stmt.external, owner)
	if err != nil {
		s.finishManagedRequest(request)
		return nil, nil, err
	}
	stmt.mu.Lock()
	if stmt.closed.Load() || stmt.stmt == nil || (stmt.transaction != nil && stmt.transaction.closed.Load()) {
		stmt.mu.Unlock()
		release()
		s.finishManagedRequest(request)
		return nil, nil, catalog.ErrExternalSessionClosed
	}
	if err := requireExternalTransaction(tx, expected); err != nil {
		stmt.mu.Unlock()
		release()
		s.finishManagedRequest(request)
		return nil, nil, err
	}
	query := stmt.query
	argSets := make([][]interface{}, len(stmt.params))
	for i := range stmt.params {
		argSets[i] = append([]interface{}(nil), stmt.params[i]...)
	}
	stmt.mu.Unlock()
	if len(argSets) == 0 {
		argSets = [][]interface{}{nil}
	}
	terminal := func() {
		release()
		s.finishManagedRequest(request)
	}
	return startArrowQuery(ctx, conn, query, argSets, terminal)
}

func (s *SQLiteFlightSQLServer) managedPreparedUpdate(
	ctx context.Context,
	cmd flightsql.PreparedStatementUpdate,
	args [][]interface{},
) (int64, error) {
	ctx, request, err := s.beginManagedRequest(ctx)
	if err != nil {
		return 0, err
	}
	defer s.finishManagedRequest(request)
	value, loaded := s.prepared.Load(string(cmd.GetPreparedStatementHandle()))
	stmt, ok := value.(*Statement)
	if !loaded || !ok || stmt == nil || stmt.external == nil {
		return 0, status.Error(codes.InvalidArgument, "prepared statement not found")
	}
	owner := any(stmt.external)
	var expected *sql.Tx
	if stmt.transaction != nil {
		expected = stmt.transaction.tx
		owner = expected
	}
	_, tx, release, err := s.acquireExternalOperation(ctx, stmt.external, owner)
	if err != nil {
		return 0, err
	}
	defer release()
	stmt.mu.Lock()
	defer stmt.mu.Unlock()
	if stmt.closed.Load() || stmt.stmt == nil || (stmt.transaction != nil && stmt.transaction.closed.Load()) {
		return 0, catalog.ErrExternalSessionClosed
	}
	if err := requireExternalTransaction(tx, expected); err != nil {
		return 0, err
	}
	if len(args) == 0 {
		result, err := stmt.stmt.ExecContext(ctx)
		if err != nil {
			return 0, err
		}
		return result.RowsAffected()
	}
	var totalAffected int64
	for _, params := range args {
		result, err := stmt.stmt.ExecContext(ctx, params...)
		if err != nil {
			return totalAffected, err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return totalAffected, err
		}
		totalAffected += affected
	}
	return totalAffected, nil
}

func (s *SQLiteFlightSQLServer) beginManagedTransaction(ctx context.Context) ([]byte, error) {
	ctx, request, err := s.beginManagedRequest(ctx)
	if err != nil {
		return nil, err
	}
	defer s.finishManagedRequest(request)
	session, err := s.openExternalSession(ctx)
	if err != nil {
		return nil, err
	}
	handle := genRandomString()
	state := &transactionState{external: session, handle: string(handle)}
	if err := session.SetCleanupHook(func() {
		s.cleanupManagedTransaction(state)
	}); err != nil {
		_ = session.Close()
		return nil, err
	}
	releaseOperation, err := s.provider.BeginDuckLakeOperationForOwnerWithError(ctx, session)
	if err != nil {
		_ = session.Close()
		return nil, err
	}
	tx, beginErr := session.BeginTx(ctx, nil)
	releaseOperation()
	if beginErr != nil {
		_ = session.Close()
		return nil, beginErr
	}
	state.tx = tx
	conn, current, release, err := s.acquireExternalOperation(ctx, session, tx)
	if err != nil {
		_ = s.rollbackManagedTransaction(ctx, state)
		_ = session.Close()
		return nil, err
	}
	if state.closed.Load() || current != tx {
		release()
		_ = s.rollbackManagedTransaction(ctx, state)
		_ = session.Close()
		if state.closed.Load() {
			return nil, catalog.ErrExternalSessionClosed
		}
		return nil, catalog.ErrExternalSessionTransactionMismatch
	}
	state.conn = conn
	s.openTransactions.Store(state.handle, state)
	release()
	return handle, nil
}

func (s *SQLiteFlightSQLServer) endManagedTransaction(ctx context.Context, req flightsql.ActionEndTransactionRequest) error {
	ctx, request, err := s.beginManagedRequest(ctx)
	if err != nil {
		return err
	}
	defer s.finishManagedRequest(request)
	if action := req.GetAction(); action != flightsql.EndTransactionCommit && action != flightsql.EndTransactionRollback {
		return status.Error(codes.InvalidArgument, "must specify Commit or Rollback to end transaction")
	}
	handle := string(req.GetTransactionId())
	value, loaded := s.openTransactions.LoadAndDelete(handle)
	state, ok := value.(*transactionState)
	if !loaded || !ok || state == nil || state.external == nil || state.tx == nil {
		return status.Error(codes.InvalidArgument, "transaction id not found")
	}
	state.closed.Store(true)
	var finalErr error
	switch req.GetAction() {
	case flightsql.EndTransactionCommit:
		finalErr = state.external.Commit(state.tx)
	case flightsql.EndTransactionRollback:
		finalErr = s.rollbackManagedTransaction(ctx, state)
	}
	return errors.Join(finalErr, state.external.Close())
}

// startArrowQuery keeps database/sql.Conn.Raw active for the complete Arrow
// reader lifetime. A driver.Conn obtained through Raw is valid only inside the
// callback; returning it and consuming a reader later can cross connection and
// storage-generation teardown.
func startArrowQuery(
	ctx context.Context,
	conn *sql.Conn,
	query string,
	argSets [][]interface{},
	terminal func(),
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	if terminal == nil {
		terminal = func() {}
	}
	setup := make(chan arrowQuerySetup, 1)
	chunks := make(chan flight.StreamChunk, 2)
	go func() {
		defer terminal()
		setupSent := false
		streamOwnsChannel := false
		defer func() {
			if recovered := recover(); recovered != nil {
				err := fmt.Errorf("panic while reading FlightSQL Arrow result: %v", recovered)
				if !setupSent {
					setup <- arrowQuerySetup{err: err}
				} else if !streamOwnsChannel {
					select {
					case chunks <- flight.StreamChunk{Err: err}:
					case <-ctx.Done():
					}
				}
			}
			if !streamOwnsChannel {
				close(chunks)
			}
		}()

		err := conn.Raw(func(driverConn any) error {
			raw, ok := driverConn.(driver.Conn)
			if !ok {
				return fmt.Errorf("unexpected DuckDB driver connection type %T", driverConn)
			}
			arrowDB, err := duckdb.NewArrowFromConn(raw)
			if err != nil {
				return err
			}
			if len(argSets) == 0 {
				argSets = [][]interface{}{nil}
			}
			readers := make([]array.RecordReader, 0, len(argSets))
			for _, args := range argSets {
				rdr, queryErr := arrowDB.QueryContext(ctx, query, args...)
				if queryErr != nil {
					for _, opened := range readers {
						opened.Release()
					}
					return queryErr
				}
				readers = append(readers, rdr)
			}
			setup <- arrowQuerySetup{schema: readers[0].Schema()}
			setupSent = true
			streamOwnsChannel = true
			streamArrowReaders(ctx, readers, chunks)
			return nil
		})
		if !setupSent {
			if err == nil {
				err = fmt.Errorf("FlightSQL Arrow query returned without a result")
			}
			setup <- arrowQuerySetup{err: err}
		}
	}()
	result := <-setup
	if result.err != nil {
		return nil, nil, result.err
	}
	return result.schema, chunks, nil
}

func streamArrowReaders(ctx context.Context, readers []array.RecordReader, chunks chan<- flight.StreamChunk) {
	defer close(chunks)
	defer func() {
		for _, rdr := range readers {
			rdr.Release()
		}
	}()
	defer func() {
		if recovered := recover(); recovered != nil {
			select {
			case chunks <- flight.StreamChunk{Err: fmt.Errorf("panic while reading FlightSQL Arrow result: %v", recovered)}:
			case <-ctx.Done():
			}
		}
	}()
	for _, rdr := range readers {
		for rdr.Next() {
			record := rdr.RecordBatch()
			record.Retain()
			select {
			case chunks <- flight.StreamChunk{Data: record}:
			case <-ctx.Done():
				record.Release()
				return
			}
		}
		if err := rdr.Err(); err != nil {
			select {
			case chunks <- flight.StreamChunk{Err: err}:
			case <-ctx.Done():
			}
			return
		}
	}
}

// DoGetTables needs additional queries while iterating its first result. Fully
// materialize that small metadata result inside Raw, then prepare the schema
// query after Raw returns while retaining the same external operation lease.
func startMaterializedTablesQuery(
	ctx context.Context,
	alloc memory.Allocator,
	conn *sql.Conn,
	query string,
	terminal func(),
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	var (
		schema  *arrow.Schema
		records []arrow.RecordBatch
	)
	err := conn.Raw(func(driverConn any) error {
		raw, ok := driverConn.(driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected DuckDB driver connection type %T", driverConn)
		}
		arrowDB, err := duckdb.NewArrowFromConn(raw)
		if err != nil {
			return err
		}
		rdr, err := arrowDB.QueryContext(ctx, query)
		if err != nil {
			return err
		}
		defer rdr.Release()
		schema = rdr.Schema()
		for rdr.Next() {
			record := rdr.RecordBatch()
			record.Retain()
			records = append(records, record)
		}
		return rdr.Err()
	})
	if err != nil {
		for _, record := range records {
			record.Release()
		}
		terminal()
		return nil, nil, err
	}
	rdr, err := array.NewRecordReader(schema, records)
	for _, record := range records {
		record.Release()
	}
	if err != nil {
		terminal()
		return nil, nil, err
	}
	tables, err := newSqliteTablesSchemaBatchReader(ctx, alloc, rdr, conn, query)
	if err != nil {
		terminal()
		return nil, nil, err
	}
	chunks := make(chan flight.StreamChunk, 2)
	go func() {
		defer terminal()
		flight.StreamChunksFromReader(ctx, tables, chunks)
	}()
	return tables.Schema(), chunks, nil
}

func (s *SQLiteFlightSQLServer) cleanupManagedStatement(handle string, stmt *Statement) {
	if stmt == nil {
		return
	}
	s.prepared.CompareAndDelete(handle, stmt)
	_ = stmt.closePrepared()
}

func (stmt *Statement) closePrepared() error {
	if stmt == nil {
		return nil
	}
	stmt.closeOnce.Do(func() {
		stmt.closed.Store(true)
		if stmt.stmt != nil {
			stmt.closeErr = stmt.stmt.Close()
		}
	})
	return stmt.closeErr
}

func (s *SQLiteFlightSQLServer) cleanupManagedTransaction(state *transactionState) {
	if state == nil {
		return
	}
	state.closed.Store(true)
	if state.handle != "" {
		s.openTransactions.CompareAndDelete(state.handle, state)
	}
	s.prepared.Range(func(key, value any) bool {
		stmt, ok := value.(*Statement)
		if !ok || stmt == nil || stmt.transaction != state {
			return true
		}
		if s.prepared.CompareAndDelete(key, stmt) {
			_ = stmt.closePrepared()
		}
		return true
	})
}

func (s *SQLiteFlightSQLServer) rollbackManagedTransaction(ctx context.Context, state *transactionState) error {
	if state == nil || state.external == nil || state.tx == nil {
		return catalog.ErrExternalSessionTransactionMismatch
	}
	releaseCleanup, reservationErr := s.provider.BeginDuckLakeRollbackCleanupForOwnerWithError(state.tx, state.tx)
	if releaseCleanup != nil {
		defer releaseCleanup()
	}
	var cleanup func(*sql.Conn) error
	if reservationErr == nil {
		cleanup = func(conn *sql.Conn) error {
			cleanupCtx := mycontext.WithMaintenanceQuery(context.WithoutCancel(ctx))
			cleanupCtx = catalog.WithDuckLakeCleanupLease(cleanupCtx)
			cleanupCtx = catalog.WithRollbackCleanupOwner(cleanupCtx, conn)
			return s.provider.CleanupDuckLakeOrphansOnConn(cleanupCtx, conn)
		}
	}
	return errors.Join(reservationErr, state.external.Rollback(state.tx, cleanup))
}

// Close rejects new provider-owned requests, cancels active streams, and
// releases every retained prepared/transaction handle. It deliberately does
// not close provider.Storage(); the provider alone owns that shared database.
func (s *SQLiteFlightSQLServer) Close() error {
	if s == nil {
		return nil
	}
	s.requestMu.Lock()
	if s.closed {
		s.requestMu.Unlock()
		return nil
	}
	s.closed = true
	requests := make([]*managedFlightRequest, 0, len(s.requests))
	for request := range s.requests {
		requests = append(requests, request)
	}
	s.requestMu.Unlock()
	for _, request := range requests {
		request.cancel()
	}

	var closeErr error
	s.prepared.Range(func(key, value any) bool {
		if !s.prepared.CompareAndDelete(key, value) {
			return true
		}
		stmt, ok := value.(*Statement)
		if !ok || stmt == nil {
			return true
		}
		if stmt.external != nil && stmt.transaction == nil {
			closeErr = errors.Join(closeErr, stmt.external.Close())
		}
		closeErr = errors.Join(closeErr, stmt.closePrepared())
		if stmt.external == nil && stmt.ownsConn && stmt.conn != nil {
			closeErr = errors.Join(closeErr, stmt.conn.Close())
		}
		return true
	})
	s.openTransactions.Range(func(key, value any) bool {
		if !s.openTransactions.CompareAndDelete(key, value) {
			return true
		}
		state, ok := value.(*transactionState)
		if !ok || state == nil {
			return true
		}
		state.closed.Store(true)
		if state.external != nil {
			closeErr = errors.Join(closeErr, s.rollbackManagedTransaction(context.Background(), state))
			closeErr = errors.Join(closeErr, state.external.Close())
		} else {
			if state.tx != nil {
				closeErr = errors.Join(closeErr, state.tx.Rollback())
			}
			if state.conn != nil {
				closeErr = errors.Join(closeErr, state.conn.Close())
			}
		}
		return true
	})
	for _, request := range requests {
		<-request.done
	}
	return closeErr
}
