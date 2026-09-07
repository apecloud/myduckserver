package pgserver

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/backend"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/duckdb/duckdb-go/v2"
)

type ArrowDataLoader struct {
	PipeDataLoader
	arrowName string
	options   string
}

var _ DataLoader = (*ArrowDataLoader)(nil)

// ErrArrowCopyInTransaction is returned before an Arrow COPY operation is
// started when the session already owns a transaction. Arrow's registration
// API requires a *sql.Conn, while normal statement execution must use the
// transaction's executor; rejecting this combination keeps the operation from
// silently splitting a statement across different physical owners.
var ErrArrowCopyInTransaction = errors.New("COPY FORMAT ARROW is not supported inside an active transaction")

// rejectArrowCopyInTransaction enforces the current Arrow COPY lifecycle
// boundary. The Arrow implementation still uses Conn.Raw and therefore may
// only run when no session transaction is bound. Check both the GMS session
// transaction and the adapter's physical transaction binding because the
// PostgreSQL frontend binds explicit BEGIN directly in the adapter pool.
func rejectArrowCopyInTransaction(ctx *sql.Context) error {
	if err := rejectArrowCopyGMSInTransaction(ctx); err != nil {
		return err
	}
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	if _, ok := ctx.Session.(adapter.ConnectionHolder); ok {
		if _, tx := adapter.TryGetTxnBinding(ctx); tx != nil {
			return ErrArrowCopyInTransaction
		}
	}
	return nil
}

// rejectArrowCopyGMSInTransaction is the pool-free portion of the Arrow
// transaction gate. Protocol setup uses it before acquiring a retained
// snapshot so an already-visible GMS transaction fails without waiting on a
// pool lifecycle writer.
func rejectArrowCopyGMSInTransaction(ctx *sql.Context) error {
	if ctx != nil && ctx.Session != nil && ctx.Session.GetTransaction() != nil {
		return ErrArrowCopyInTransaction
	}
	return nil
}

// rejectArrowCopyInTransactionForLease performs the same Arrow transaction
// gate while allowing callers that already hold a retained execution lease to
// skip a second pool lifecycle read. The lease's captured owner/transaction is
// authoritative for the asynchronous worker; a nested TryGetTxnBinding would
// otherwise block behind a pending pool shutdown writer.
func rejectArrowCopyInTransactionForLease(ctx *sql.Context, leaseHeld bool) error {
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	if ctx.Session.GetTransaction() != nil {
		return ErrArrowCopyInTransaction
	}
	if leaseHeld {
		return nil
	}
	if _, ok := ctx.Session.(adapter.ConnectionHolder); ok {
		if _, tx := adapter.TryGetTxnBinding(ctx); tx != nil {
			return ErrArrowCopyInTransaction
		}
	}
	return nil
}

// rejectArrowCopyBinding applies the Arrow transaction boundary to a binding
// that was already captured under a retained lease. The lease lets us avoid a
// second pool lookup, but a captured physical transaction is still conclusive
// evidence that Arrow's Conn.Raw-based path cannot run safely.
func rejectArrowCopyBinding(ctx *sql.Context, binding postgresCopyBinding) error {
	if binding.ownerTx != nil {
		return ErrArrowCopyInTransaction
	}
	return rejectArrowCopyInTransactionForLease(ctx, binding.leaseHeld())
}

func NewArrowDataLoader(ctx *sql.Context, handler *DuckHandler, schema string, table sql.InsertableTable, columns tree.NameList, options string) (DataLoader, error) {
	if err := rejectArrowCopyGMSInTransaction(ctx); err != nil {
		return nil, err
	}
	binding, err := capturePostgresCopyBindingWithHandler(ctx, handler, nil)
	if err != nil {
		return nil, err
	}
	loader, err := newArrowDataLoaderWithBinding(ctx, handler, schema, table, columns, options, binding)
	if err != nil {
		binding.releaseLease()
		return nil, err
	}
	if arrowLoader, ok := loader.(*ArrowDataLoader); ok {
		arrowLoader.ownsLease = true
	}
	return loader, nil
}

func newArrowDataLoaderWithBinding(
	ctx *sql.Context, handler *DuckHandler, schema string, table sql.InsertableTable,
	columns tree.NameList, options string, binding postgresCopyBinding,
) (DataLoader, error) {
	if err := rejectArrowCopyBinding(ctx, binding); err != nil {
		return nil, err
	}
	ownedBinding := false
	if !binding.snapshotCaptured {
		var err error
		binding, err = capturePostgresCopyBindingWithHandler(ctx, handler, nil)
		if err != nil {
			return nil, err
		}
		ownedBinding = true
	}
	if err := rejectArrowCopyBinding(ctx, binding); err != nil {
		if ownedBinding {
			binding.releaseLease()
		}
		return nil, err
	}
	cleanup := true
	defer func() {
		if cleanup && ownedBinding {
			binding.releaseLease()
		}
	}()
	target, err := resolvePostgresCopyTarget(ctx, handler, schema, table, binding.execer, binding.ownerConn)
	if err != nil {
		return nil, err
	}

	// Create the FIFO pipe
	duckBuilder := handler.e.Analyzer.ExecBuilder.PriorityBuilder.(*backend.DuckBuilder)
	pipePath, err := duckBuilder.CreatePipe(ctx, "pg-from-arrow")
	if err != nil {
		return nil, err
	}
	arrowName := "__sys_copy_from_arrow_" + strconv.Itoa(int(ctx.ID())) + "__"

	// Create a child without mutating the parent context into its own ancestor.
	ctx, cancel := newCopyContext(ctx)

	loader := &ArrowDataLoader{
		PipeDataLoader: PipeDataLoader{
			ctx:              ctx,
			cancel:           cancel,
			execer:           binding.execer,
			ownerConn:        binding.ownerConn,
			ownerTx:          binding.ownerTx,
			snapshotCaptured: binding.snapshotCaptured,
			lease:            binding.lease,
			schema:           schema,
			table:            table,
			target:           target,
			columns:          columns,
			pipePath:         pipePath,
			rowCount:         make(chan int64, 1),
			done:             make(chan struct{}),
			logger:           ctx.GetLogger(),
		},
		arrowName: arrowName,
		options:   options,
	}
	loader.read = func() {
		loader.executeInsert(loader.buildSQL(), pipePath)
	}

	cleanup = false
	return loader, nil
}

// buildSQL builds the DuckDB INSERT statement.
func (loader *ArrowDataLoader) buildSQL() string {
	var b strings.Builder
	b.Grow(256)

	b.WriteString("INSERT INTO ")
	if loader.target != "" {
		b.WriteString(loader.target)
	} else if loader.schema != "" {
		b.WriteString(loader.schema)
		b.WriteString(".")
		b.WriteString(loader.table.Name())
	} else {
		b.WriteString(loader.table.Name())
	}

	if len(loader.columns) > 0 {
		b.WriteString(" (")
		b.WriteString(loader.columns.String())
		b.WriteString(")")
	}

	b.WriteString(" FROM ")
	b.WriteString(loader.arrowName)

	return b.String()
}

func (loader *ArrowDataLoader) executeInsert(sql string, pipePath string) {
	defer func() {
		if loader.rowCount != nil {
			close(loader.rowCount)
		}
	}()

	if err := loader.validateBinding(loader.ctx); err != nil {
		loader.err.Store(&err)
		if loader.blocked.Load() {
			loader.unblockWriter()
		}
		return
	}

	// The constructor and protocol handler reject active transactions. Repeat
	// the check at execution time to cover a transaction that was bound between
	// COPY setup and the first CopyData message.
	if err := rejectArrowCopyBinding(loader.ctx, postgresCopyBinding{
		execer:           loader.execer,
		ownerConn:        loader.ownerConn,
		ownerTx:          loader.ownerTx,
		snapshotCaptured: loader.snapshotCaptured,
		lease:            loader.lease,
	}); err != nil {
		loader.err.Store(&err)
		if loader.blocked.Load() {
			loader.unblockWriter()
		}
		return
	}

	// Open the pipe for reading.
	loader.logger.Debugf("Opening pipe for reading: %s", pipePath)
	pipe, err := os.OpenFile(pipePath, os.O_RDONLY, os.ModeNamedPipe)
	if err != nil {
		loader.err.Store(&err)
		// Open the pipe once to unblock a writer that may still be waiting in
		// OpenFile. Use O_NONBLOCK: when the path disappeared or the reader
		// failed before FIFO creation, a second blocking open would leak the
		// loader goroutine forever.
		if unblock, openErr := os.OpenFile(pipePath, os.O_RDONLY|syscall.O_NONBLOCK, os.ModeNamedPipe); openErr == nil {
			loader.errPipe.Store(unblock)
		}
		return
	}
	defer pipe.Close()

	// Create an Arrow IPC reader from the pipe.
	loader.logger.Debugf("Creating Arrow IPC reader from pipe: %s", pipePath)
	arrowReader, err := ipc.NewReader(pipe)
	if err != nil {
		loader.err.Store(&err)
		return
	}
	defer arrowReader.Release()

	conn := loader.ownerConn
	if conn == nil {
		err := errors.New("COPY connection owner is unavailable")
		loader.err.Store(&err)
		return
	}
	if loader.execer == nil {
		loader.execer = conn
	}
	if conn == nil {
		err := errors.New("COPY connection is unavailable")
		loader.err.Store(&err)
		return
	}
	if err := loader.validateBinding(loader.ctx); err != nil {
		loader.err.Store(&err)
		return
	}

	// Register the Arrow IPC reader to DuckDB.
	loader.logger.Debugf("Registering Arrow IPC reader into DuckDB: %s", loader.arrowName)
	var release func()
	if err := conn.Raw(func(driverConn any) error {
		conn := driverConn.(*duckdb.Conn)
		arrow, err := duckdb.NewArrowFromConn(conn)
		if err != nil {
			return err
		}

		release, err = arrow.RegisterView(arrowReader, loader.arrowName)
		return err
	}); err != nil {
		loader.err.Store(&err)
		return
	}
	defer release()

	// Execute the INSERT statement.
	// This will block until the reader has finished reading the data.
	if err := loader.validateBinding(loader.ctx); err != nil {
		loader.err.Store(&err)
		return
	}
	loader.logger.Debugln("Executing SQL:", sql)
	result, err := loader.execer.ExecContext(loader.ctx, sql)
	if err != nil {
		loader.err.Store(&err)
		return
	}

	rows, err := result.RowsAffected()
	if err != nil {
		loader.err.Store(&err)
		return
	}

	loader.logger.Debugf("Inserted %d rows", rows)
	loader.rowCount <- rows
}
