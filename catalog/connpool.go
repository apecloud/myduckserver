// Copyright 2024-2025 ApeCloud, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package catalog

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/sirupsen/logrus"
)

type ConnectionPool struct {
	*stdsql.DB
	connector      *duckdb.Connector
	defaultCatalog string
	// lifecycleMu protects pool generation changes (Close/Reset). Individual
	// session state is serialized by sessionLocks so a blocked acquisition for
	// one logical session cannot prevent another session from releasing its
	// connection.
	lifecycleMu lifecycleGate
	// dbPtr is an atomic shutdown signal. Close/Reset use it to close the
	// current database before waiting on lifecycleMu, which releases pending
	// database/sql connection requests that may already hold lifecycleMu.RLock.
	dbPtr                 atomic.Pointer[stdsql.DB]
	sessionLocks          sync.Map // map[uint32]*sync.Mutex
	conns                 sync.Map // concurrent-safe map[uint32]*stdsql.Conn
	txns                  sync.Map // concurrent-safe map[uint32]*stdsql.Tx
	txnConns              sync.Map // concurrent-safe map[uint32]*stdsql.Conn
	closedConns           sync.Map // connection IDs that completed their lifecycle
	externalSessions      sync.Map // map[*ExternalSession]struct{}
	externalGeneration    uint64   // protected by lifecycleMu
	externalPanicMu       sync.Mutex
	externalPanicErr      error
	initializerMu         sync.RWMutex
	connectionInitializer func(context.Context, *stdsql.Conn) error
	transactionHooksMu    sync.RWMutex
	transactionAdmission  func() func(*stdsql.Tx)
	// transactionAdmissionWithError is the provider-aware admission surface.
	// The legacy transactionAdmission hook cannot report a poisoned generation
	// because its nil/empty completion callback historically meant only that a
	// driver BeginTx failed. Keep both fields so standalone pools and older test
	// doubles retain their original contract while production providers fail
	// closed before opening a new physical transaction.
	transactionAdmissionWithError func() (func(*stdsql.Tx), error)
	// transactionAdmissionGuard is an optional provider-owned check used to
	// reject new physical transactions after a storage generation has become
	// unusable. It is kept separate from transactionAdmission because the latter
	// predates error-returning admission and a nil completion callback historically
	// meant only that the attempted BeginTx failed.
	transactionAdmissionGuard func() error
	// connectionAdmissionGuard rejects ordinary connection/snapshot work while
	// the owning provider storage generation is transitioning or poisoned. It is
	// context-aware so provider-owned rollback cleanup and recovery can reuse an
	// explicitly reserved connection without opening the gate to frontend work.
	connectionAdmissionGuard   func(context.Context) error
	transactionFinished        func(*stdsql.Tx)
	transactionCompletionMu    sync.Mutex
	transactionCompletions     map[*stdsql.Tx]*transactionCompletion
	transactionCompletionOrder []*stdsql.Tx
	activeTransactions         map[*stdsql.Tx]struct{}
	registerMySQLUDFsOnce      sync.Once
	registerMySQLUDFsErr       error
}

var (
	ErrExternalSessionClosed              = errors.New("external session is closed")
	ErrExternalSessionStale               = errors.New("external session belongs to a stale pool generation")
	ErrExternalSessionGenerationPoisoned  = errors.New("external session driver panic poisoned the storage generation")
	ErrExternalSessionTransactionMismatch = errors.New(
		"external session transaction does not match the active transaction",
	)
)

// ExternalSession is an opaque, pool-owned connection identity for protocol
// surfaces whose handles do not share the uint32 namespace used by MySQL and
// PostgreSQL sessions. Driver access is exposed only through ExecutionLease so
// pool generation teardown can wait for admitted work before retiring the exact
// connection.
type ExternalSession struct {
	pool       *ConnectionPool
	generation uint64

	opMu sync.Mutex

	conn *stdsql.Conn
	tx   *stdsql.Tx

	cleanupHook    func()
	cleanupStarted bool
	closed         bool
	stale          bool
	teardownDone   chan struct{}
	teardownErr    error
}

type externalSessionTeardown struct {
	hook          func()
	conn          *stdsql.Conn
	tx            *stdsql.Tx
	quarantineErr error
}

type externalSessionPanicError struct {
	operation string
	value     any
}

func (e *externalSessionPanicError) Error() string {
	if e == nil {
		return "external session driver operation panicked"
	}
	return fmt.Sprintf("external session %s panicked: %v", e.operation, e.value)
}

// maxTransactionCompletionTombstones bounds the strong references retained by
// the late-registration bridge. Only completed entries are evicted; active
// transactions are never dropped from the admission map.
const maxTransactionCompletionTombstones = 1024

// transactionCompletionTombstoneTTL keeps a completed transaction addressable
// long enough for a protocol finalizer and its callback registration to cross
// threads, while ensuring an idle pool does not retain every *sql.Tx forever.
const transactionCompletionTombstoneTTL = 5 * time.Minute

// transactionCompletion is the pool-side counterpart to backend's physical
// transaction callback registry. It lets generation teardown notify a session
// even when the pool finalizes a transaction directly, outside Session's
// adapter methods. Completed entries remain as tombstones so late registration
// receives the terminal outcome immediately.
type transactionCompletion struct {
	mu               sync.Mutex
	callbacks        []func(bool)
	completed        bool
	dispatchComplete bool
	success          bool
	tombstone        bool
	completedAt      time.Time
}

// transactionCompletionDispatch carries terminal callbacks out of the pool's
// lifecycle critical section. Pool finalizers update their identity maps while
// holding a release/session lock; invoking a callback there lets the callback
// re-enter the same pool and deadlock (and can also invert the provider barrier
// lock order). The dispatch is deliberately idempotent because a helper may
// discover the same terminal transaction through more than one cleanup path.
type transactionCompletionDispatch struct {
	once       sync.Once
	callbacks  []func(bool)
	completion *transactionCompletion
	pool       *ConnectionPool
	finished   func(*stdsql.Tx)
	tx         *stdsql.Tx
	success    bool
}

func (d *transactionCompletionDispatch) run() {
	if d == nil {
		return
	}
	panicValue, panicked := d.runCollectPanic()
	if panicked {
		panic(panicValue)
	}
}

// runCollectPanic always executes every terminal hook in this dispatch. A
// session callback is outside the pool's control and may panic; skipping the
// provider finished hook in that case can strand an active transaction in a
// generation transition forever. Preserve the first panic for the caller, but
// finish the required bookkeeping before it is rethrown.
func (d *transactionCompletionDispatch) runCollectPanic() (panicValue any, panicked bool) {
	if d == nil {
		return nil, false
	}
	d.once.Do(func() {
		runCallbacks := func(callbacks []func(bool), success bool) {
			for _, callback := range callbacks {
				if callback == nil {
					continue
				}
				value, didPanic := callTransactionCompletionHook(func() {
					callback(success)
				})
				if didPanic && !panicked {
					panicValue, panicked = value, true
				}
			}
		}

		runCallbacks(d.callbacks, d.success)
		for d.completion != nil {
			callbacks, success, complete := d.completion.takeCallbacksForDispatch()
			runCallbacks(callbacks, success)
			if complete {
				break
			}
		}
		if d.finished != nil {
			value, didPanic := callTransactionCompletionHook(func() {
				d.finished(d.tx)
			})
			if didPanic && !panicked {
				panicValue, panicked = value, true
			}
		}
		if d.pool != nil && d.completion != nil {
			d.pool.recordTransactionCompletion(d.tx, d.completion)
		}
	})
	return panicValue, panicked
}

func callTransactionCompletionHook(hook func()) (panicValue any, panicked bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			panicValue, panicked = recovered, true
		}
	}()
	hook()
	return nil, false
}

type transactionCompletionBatch struct {
	dispatches []*transactionCompletionDispatch
	after      []func()
}

func (b *transactionCompletionBatch) add(dispatch *transactionCompletionDispatch) {
	if b == nil || dispatch == nil {
		return
	}
	b.dispatches = append(b.dispatches, dispatch)
}

func (b *transactionCompletionBatch) addAfter(callback func()) {
	if b == nil || callback == nil {
		return
	}
	b.after = append(b.after, callback)
}

func (b *transactionCompletionBatch) run() {
	if b == nil {
		return
	}
	dispatches := b.dispatches
	b.dispatches = nil
	after := b.after
	b.after = nil
	var firstPanic any
	var panicked bool
	for _, dispatch := range dispatches {
		panicValue, dispatchPanicked := dispatch.runCollectPanic()
		if dispatchPanicked && !panicked {
			firstPanic, panicked = panicValue, true
		}
	}
	for _, callback := range after {
		if callback == nil {
			continue
		}
		panicValue, callbackPanicked := callTransactionCompletionHook(callback)
		if callbackPanicked && !panicked {
			firstPanic, panicked = panicValue, true
		}
	}
	if panicked {
		panic(firstPanic)
	}
}

func (c *transactionCompletion) register(callback func(bool)) bool {
	if c == nil || callback == nil {
		return false
	}
	c.mu.Lock()
	if !c.completed || !c.dispatchComplete {
		c.callbacks = append(c.callbacks, callback)
		c.mu.Unlock()
		return true
	}
	success := c.success
	c.mu.Unlock()
	callback(success)
	return true
}

// completeDeferred records the terminal outcome without making callbacks
// inline-dispatchable. The owning public finalizer still holds pool locks at
// this point; its deferred dispatch marks dispatchComplete only after those
// locks have been released.
func (c *transactionCompletion) completeDeferred(success bool) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.completed {
		return false
	}
	c.completed = true
	c.success = success
	c.completedAt = time.Now()
	return true
}

// takeCallbacksForDispatch drains callbacks registered before or during the
// lock-free dispatch. Returning complete=true is the linearization point after
// which a genuinely late registration may invoke its callback inline.
func (c *transactionCompletion) takeCallbacksForDispatch() (callbacks []func(bool), success bool, complete bool) {
	if c == nil {
		return nil, false, true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	success = c.success
	if len(c.callbacks) > 0 {
		callbacks = append([]func(bool){}, c.callbacks...)
		c.callbacks = nil
		return callbacks, success, false
	}
	c.dispatchComplete = true
	return nil, success, true
}

func (c *transactionCompletion) markTombstone() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.completed || !c.dispatchComplete || c.tombstone {
		return false
	}
	if c.completedAt.IsZero() {
		c.completedAt = time.Now()
	}
	c.tombstone = true
	return true
}

func (c *transactionCompletion) isCompleted() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	completed := c.completed
	c.mu.Unlock()
	return completed
}

// completionSnapshot reports a pruneable completion only after the owning
// dispatch has drained callbacks and the provider finished hook, then recorded
// the tombstone. dispatchComplete alone is not sufficient: that bit becomes
// visible just before the finished hook runs so genuinely late callbacks may
// receive the terminal result without joining the completed batch.
func (c *transactionCompletion) completionSnapshot() (bool, time.Time) {
	if c == nil {
		return false, time.Time{}
	}
	c.mu.Lock()
	completed, completedAt := c.tombstone, c.completedAt
	c.mu.Unlock()
	return completed, completedAt
}

// lifecycleGate coordinates ordinary pool users with generation teardown.
//
// A plain sync.RWMutex is tempting here, but it has an important shutdown
// starvation property: once Close queues for the write lock, new readers are
// blocked even when one of those readers is a release/finalizer that could
// free the resource keeping an existing reader blocked. The gate explicitly
// distinguishes those release operations. Close first marks the gate as
// shutting down, then waits for both classes of in-flight operations; release
// operations may still enter while shutdown is pending, but not after the
// exclusive teardown section starts.
type lifecycleGate struct {
	mu           sync.Mutex
	cond         *sync.Cond
	readers      int
	releasers    int
	writer       bool
	shuttingDown bool
}

func (g *lifecycleGate) initLocked() {
	if g.cond == nil {
		g.cond = sync.NewCond(&g.mu)
	}
}

// RLock enters an ordinary pool operation. New ordinary readers are rejected
// once shutdown has been announced so teardown cannot starve behind traffic.
func (g *lifecycleGate) RLock() {
	g.mu.Lock()
	g.initLocked()
	for g.shuttingDown || g.writer {
		g.cond.Wait()
	}
	g.readers++
	g.mu.Unlock()
}

func (g *lifecycleGate) RUnlock() {
	g.mu.Lock()
	if g.readers > 0 {
		g.readers--
	}
	if g.cond != nil {
		g.cond.Broadcast()
	}
	g.mu.Unlock()
}

// ReleaseLock enters a transaction/connection finalizer. Finalizers are
// allowed to proceed while a writer is waiting, which lets them release a
// checked-out database/sql resource needed by an existing reader. They still
// wait if the exclusive teardown section has already started.
func (g *lifecycleGate) ReleaseLock() {
	g.mu.Lock()
	g.initLocked()
	for g.writer {
		g.cond.Wait()
	}
	g.releasers++
	g.mu.Unlock()
}

func (g *lifecycleGate) ReleaseUnlock() {
	g.mu.Lock()
	if g.releasers > 0 {
		g.releasers--
	}
	if g.cond != nil {
		g.cond.Broadcast()
	}
	g.mu.Unlock()
}

// BeginShutdown prevents new ordinary readers while allowing release
// operations to drain. It is separate from Lock because callers must be able
// to close database/sql.DB before waiting for a blocked acquisition to return.
func (g *lifecycleGate) BeginShutdown() {
	g.mu.Lock()
	g.initLocked()
	for g.shuttingDown || g.writer {
		g.cond.Wait()
	}
	g.shuttingDown = true
	g.mu.Unlock()
}

// LockAfterShutdown waits for all operations admitted before shutdown and then
// enters the exclusive generation teardown section. BeginShutdown must have
// been called by the caller first.
func (g *lifecycleGate) LockAfterShutdown() {
	g.mu.Lock()
	g.initLocked()
	if !g.shuttingDown {
		g.shuttingDown = true
	}
	for g.writer || g.readers > 0 || g.releasers > 0 {
		g.cond.Wait()
	}
	g.writer = true
	g.mu.Unlock()
}

// Lock retains the conventional API for any future exclusive callers.
func (g *lifecycleGate) Lock() {
	g.BeginShutdown()
	g.LockAfterShutdown()
}

func (g *lifecycleGate) Unlock() {
	g.mu.Lock()
	g.writer = false
	g.shuttingDown = false
	if g.cond != nil {
		g.cond.Broadcast()
	}
	g.mu.Unlock()
}

func NewConnectionPool(connector *duckdb.Connector, db *stdsql.DB, defaultCatalog string) *ConnectionPool {
	p := &ConnectionPool{
		DB:                     db,
		connector:              connector,
		defaultCatalog:         defaultCatalog,
		transactionCompletions: make(map[*stdsql.Tx]*transactionCompletion),
		activeTransactions:     make(map[*stdsql.Tx]struct{}),
	}
	p.dbPtr.Store(db)
	return p
}

// closeDatabaseForShutdown prevents a blocked database/sql acquisition from
// holding the pool's read lock forever. DB.Close is concurrency-safe and
// rejects pending connection requests; the lifecycle-locked closeLocked call
// below still performs identity-safe transaction/connection cleanup.
func (p *ConnectionPool) closeDatabaseForShutdown() error {
	p.lifecycleMu.BeginShutdown()
	db := p.dbPtr.Load()
	if db == nil {
		return nil
	}
	return db.Close()
}

func (p *ConnectionPool) sessionLock(id uint32) *sync.Mutex {
	for {
		entry, _ := p.sessionLocks.LoadOrStore(id, &sync.Mutex{})
		if mu, ok := entry.(*sync.Mutex); ok && mu != nil {
			return mu
		}
		// A malformed entry must not panic every lifecycle operation. Replace it
		// atomically so concurrent callers still converge on one session lock.
		replacement := &sync.Mutex{}
		if p.sessionLocks.CompareAndSwap(id, entry, replacement) {
			return replacement
		}
	}
}

func (p *ConnectionPool) lockSession(id uint32) func() {
	mu := p.sessionLock(id)
	mu.Lock()
	return mu.Unlock
}

func (p *ConnectionPool) Connector() *duckdb.Connector {
	return p.connector
}

// SetConnectionInitializer installs a hook that runs whenever a logical
// session acquires a connection outside an active session transaction. The
// hook receives the acquisition context, including its query-origin
// classification. Running it for both new and reused logical connections
// prevents a connection that was previously used by one origin from carrying
// session settings into another origin; GetTxn runs it before BeginTx and then
// keeps transaction-scoped state stable until that transaction closes.
func (p *ConnectionPool) SetConnectionInitializer(initializer func(context.Context, *stdsql.Conn) error) {
	p.initializerMu.Lock()
	p.connectionInitializer = initializer
	p.initializerMu.Unlock()
}

func (p *ConnectionPool) initializeConnection(ctx context.Context, conn *stdsql.Conn) error {
	p.initializerMu.RLock()
	initializer := p.connectionInitializer
	p.initializerMu.RUnlock()
	if initializer == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return initializer(ctx, conn)
}

// OpenExternalSession checks out one exact physical connection and registers
// an opaque identity with the pool generation that owns it.
func (p *ConnectionPool) OpenExternalSession(ctx context.Context) (*ExternalSession, error) {
	if p == nil {
		return nil, fmt.Errorf("connection pool is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := p.connectionAdmissionError(ctx); err != nil {
		return nil, err
	}

	p.lifecycleMu.RLock()
	defer p.lifecycleMu.RUnlock()
	if err := p.connectionAdmissionError(ctx); err != nil {
		return nil, err
	}

	db := p.dbPtr.Load()
	if db == nil {
		return nil, fmt.Errorf("connection pool database is unavailable")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if err := p.initializeExternalConnection(ctx, conn); err != nil {
		return nil, errors.Join(err, callExternalSessionDriver(func() error {
			return p.closePhysicalConnLocked(conn)
		}))
	}

	session := &ExternalSession{
		pool:       p,
		generation: p.externalGeneration,
		conn:       conn,
	}
	p.externalSessions.Store(session, struct{}{})
	return session, nil
}

// externalStateErrorLocked validates a handle while the caller holds both the
// pool lifecycle lock and opMu. A stale generation is reported before an
// explicit close so Reset remains distinguishable even if Close is later called
// on the old handle.
func (s *ExternalSession) externalStateErrorLocked() error {
	if s == nil || s.pool == nil {
		return ErrExternalSessionClosed
	}
	if s.stale || s.generation != s.pool.externalGeneration {
		return ErrExternalSessionStale
	}
	if s.closed || s.conn == nil {
		return ErrExternalSessionClosed
	}
	return nil
}

// ExecutionLease returns the exact connection and transaction owned by this
// handle. The release function is idempotent and must be held through all
// synchronous driver work and result consumption.
func (s *ExternalSession) ExecutionLease(
	ctx context.Context,
) (*stdsql.Conn, *stdsql.Tx, func(), error) {
	if s == nil || s.pool == nil {
		return nil, nil, func() {}, ErrExternalSessionClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	p := s.pool
	var completions transactionCompletionBatch
	defer completions.run()
	if err := p.connectionAdmissionError(ctx); err != nil {
		return nil, nil, func() {}, err
	}

	p.lifecycleMu.RLock()
	s.opMu.Lock()
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			s.opMu.Unlock()
			p.lifecycleMu.RUnlock()
		})
	}
	if err := p.connectionAdmissionError(ctx); err != nil {
		release()
		return nil, nil, func() {}, err
	}
	if err := s.externalStateErrorLocked(); err != nil {
		release()
		return nil, nil, func() {}, err
	}
	if s.tx == nil {
		if err := p.initializeExternalConnection(ctx, s.conn); err != nil {
			teardown, owner := s.beginTeardownLocked(false)
			if owner {
				err = s.finishTeardownLocked(teardown, err, &completions)
			} else {
				err = errors.Join(err, s.waitForTeardownLocked())
			}
			release()
			return nil, nil, func() {}, err
		}
	}
	return s.conn, s.tx, release, nil
}

// BeginTx opens a transaction on the handle's exact connection. Provider
// transaction admission is reserved without holding a lifecycle reader so a
// generation transition can make progress while admission waits.
func (s *ExternalSession) BeginTx(ctx context.Context, opts *stdsql.TxOptions) (*stdsql.Tx, error) {
	if s == nil || s.pool == nil {
		return nil, ErrExternalSessionClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	p := s.pool
	var completions transactionCompletionBatch
	defer completions.run()
	if err := p.connectionAdmissionError(ctx); err != nil {
		return nil, err
	}

	// Preserve the existing-transaction fast path without reserving another
	// provider transaction slot.
	p.lifecycleMu.RLock()
	s.opMu.Lock()
	if err := p.connectionAdmissionError(ctx); err != nil {
		s.opMu.Unlock()
		p.lifecycleMu.RUnlock()
		return nil, err
	}
	if err := s.externalStateErrorLocked(); err != nil {
		s.opMu.Unlock()
		p.lifecycleMu.RUnlock()
		return nil, err
	}
	if s.tx != nil {
		tx := s.tx
		s.opMu.Unlock()
		p.lifecycleMu.RUnlock()
		return tx, nil
	}
	s.opMu.Unlock()
	p.lifecycleMu.RUnlock()

	if err := p.transactionAdmissionError(); err != nil {
		return nil, err
	}
	completeAdmission, err := p.beginTransactionAdmissionWithError()
	if err != nil {
		return nil, err
	}
	admissionPending := true
	finishAdmission := func(tx *stdsql.Tx) {
		if !admissionPending {
			return
		}
		admissionPending = false
		completeAdmission(tx)
	}

	p.lifecycleMu.RLock()
	defer p.lifecycleMu.RUnlock()
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if err := p.connectionAdmissionError(ctx); err != nil {
		finishAdmission(nil)
		return nil, err
	}
	if err := p.transactionAdmissionError(); err != nil {
		finishAdmission(nil)
		return nil, err
	}
	if err := s.externalStateErrorLocked(); err != nil {
		finishAdmission(nil)
		return nil, err
	}
	if s.tx != nil {
		finishAdmission(nil)
		return s.tx, nil
	}
	if err := p.initializeExternalConnection(ctx, s.conn); err != nil {
		finishAdmission(nil)
		teardown, owner := s.beginTeardownLocked(false)
		if owner {
			teardownErr := s.finishTeardownLocked(teardown, err, &completions)
			if teardownErr != nil {
				err = teardownErr
			}
		} else {
			err = errors.Join(err, s.waitForTeardownLocked())
		}
		return nil, err
	}
	tx, err := callExternalSessionBeginTx(s.conn, context.WithoutCancel(ctx), opts)
	if err != nil {
		if externalSessionDriverPanicked(err) {
			p.recordExternalSessionPanic(err)
		}
		finishAdmission(nil)
		if externalSessionDriverPanicked(err) {
			teardown, owner := s.beginTeardownLocked(false)
			if owner {
				teardown.quarantineErr = err
				err = s.finishTeardownLocked(teardown, err, &completions)
			} else {
				err = errors.Join(err, s.waitForTeardownLocked())
			}
		}
		return nil, err
	}
	s.tx = tx
	p.markTransactionActive(tx)
	finishAdmission(tx)
	return tx, nil
}

func callExternalSessionCleanupHook(hook func()) (err error) {
	if hook == nil {
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("external session cleanup hook panicked: %v", recovered)
		}
	}()
	hook()
	return nil
}

func callExternalSessionDriver(fn func() error) (err error) {
	if fn == nil {
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("external session driver operation panicked: %v", recovered)
		}
	}()
	return fn()
}

func callExternalSessionTransactionDriver(operation string, fn func() error) (err error) {
	if fn == nil {
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &externalSessionPanicError{operation: operation, value: recovered}
		}
	}()
	return fn()
}

func externalSessionDriverPanicked(err error) bool {
	var panicErr *externalSessionPanicError
	return errors.As(err, &panicErr)
}

func callExternalSessionBeginTx(
	conn *stdsql.Conn,
	ctx context.Context,
	opts *stdsql.TxOptions,
) (tx *stdsql.Tx, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &externalSessionPanicError{operation: "begin transaction", value: recovered}
			tx = nil
		}
	}()
	if conn == nil {
		return nil, fmt.Errorf("external session connection is unavailable")
	}
	return conn.BeginTx(ctx, opts)
}

func (p *ConnectionPool) initializeExternalConnection(ctx context.Context, conn *stdsql.Conn) error {
	if err := callExternalSessionDriver(func() error {
		return p.initializeConnection(ctx, conn)
	}); err != nil {
		return err
	}
	return callExternalSessionDriver(func() error {
		return p.registerMySQLUDFs(conn)
	})
}

func (s *ExternalSession) beginTeardownLocked(stale bool) (*externalSessionTeardown, bool) {
	if stale {
		s.stale = true
	} else {
		s.closed = true
	}
	if s.pool != nil {
		s.pool.externalSessions.Delete(s)
	}
	if s.teardownDone != nil {
		return nil, false
	}
	s.teardownDone = make(chan struct{})
	s.cleanupStarted = true
	teardown := &externalSessionTeardown{
		hook: s.cleanupHook,
		conn: s.conn,
		tx:   s.tx,
	}
	s.cleanupHook = nil
	s.conn = nil
	s.tx = nil
	return teardown, true
}

// runCleanupOutsideOperationLock invokes the installed hook without opMu while
// retaining lifecycle admission. The caller holds opMu on entry and again on
// return. SetCleanupHook serializes late registration on opMu, so a hook that
// wins the race after invalidation is invoked inline instead of being lost.
func (s *ExternalSession) runCleanupOutsideOperationLock(teardown *externalSessionTeardown) error {
	if teardown == nil || teardown.hook == nil {
		return nil
	}
	s.opMu.Unlock()
	err := callExternalSessionCleanupHook(teardown.hook)
	s.opMu.Lock()
	return err
}

func (s *ExternalSession) closeResourcesLocked(
	teardown *externalSessionTeardown,
	completions *transactionCompletionBatch,
) error {
	if s == nil || s.pool == nil || teardown == nil {
		return nil
	}
	var lastErr error
	quarantineErr := teardown.quarantineErr
	if teardown.tx != nil {
		tx := teardown.tx
		rollbackErr := callExternalSessionTransactionDriver("rollback transaction", tx.Rollback)
		if rollbackErr != nil && !transactionInactiveError(rollbackErr) {
			lastErr = errors.Join(lastErr, rollbackErr)
		}
		if externalSessionDriverPanicked(rollbackErr) {
			quarantineErr = errors.Join(quarantineErr, rollbackErr)
			s.pool.recordExternalSessionPanic(rollbackErr)
		}
		completions.add(s.pool.finishTransactionWithOutcomeDeferred(tx, false))
	}
	if teardown.conn != nil {
		if quarantineErr != nil {
			lastErr = errors.Join(
				lastErr,
				s.pool.quarantineExternalConnectionAfterPanic(teardown.conn, quarantineErr),
			)
		} else {
			lastErr = errors.Join(lastErr, callExternalSessionDriver(func() error {
				return s.pool.closePhysicalConnLocked(teardown.conn)
			}))
		}
	}
	return lastErr
}

// quarantineExternalConnectionAfterPanic discards the physical driver
// connection whose BeginTx, Commit, or Rollback call panicked. Those
// database/sql paths transfer a Conn read lock to the transaction without a
// panic-side release; ordinary Conn.Raw/Close retirement then blocks forever
// trying to acquire the corresponding write lock. Closing driver.Conn inside
// the Raw callback avoids that write lock. The callback must always return nil:
// returning ErrBadConn (or propagating a panic) would make database/sql enter
// the same blocked retirement path after the callback.
//
// The database/sql wrapper remains permanently checked out and detached, so it
// is never returned to the reusable pool. The generation is poisoned and must
// be rebuilt by process restart rather than Reset.
func (p *ConnectionPool) quarantineExternalConnectionAfterPanic(
	conn *stdsql.Conn,
	panicErr error,
) error {
	if p == nil || conn == nil {
		return nil
	}
	p.recordExternalSessionPanic(panicErr)
	var physicalCloseErr error
	rawErr := callExternalSessionDriver(func() error {
		return conn.Raw(func(raw any) error {
			physical, ok := raw.(driver.Conn)
			if !ok || physical == nil {
				physicalCloseErr = fmt.Errorf(
					"external session driver connection has unexpected type %T",
					raw,
				)
				return nil
			}
			physicalCloseErr = callExternalSessionDriver(physical.Close)
			return nil
		})
	})
	err := errors.Join(rawErr, physicalCloseErr)
	if err != nil && !errors.Is(err, stdsql.ErrConnDone) {
		logrus.WithError(err).Warn("Failed to quarantine external session connection")
	}
	return err
}

func (p *ConnectionPool) recordExternalSessionPanic(err error) {
	if p == nil || err == nil {
		return
	}
	p.externalPanicMu.Lock()
	if p.externalPanicErr == nil {
		p.externalPanicErr = errors.Join(ErrExternalSessionGenerationPoisoned, err)
	}
	p.externalPanicMu.Unlock()
}

func (p *ConnectionPool) externalSessionPanicError() error {
	if p == nil {
		return nil
	}
	p.externalPanicMu.Lock()
	err := p.externalPanicErr
	p.externalPanicMu.Unlock()
	return err
}

func (s *ExternalSession) completeTeardownLocked(err error) {
	if s == nil || s.teardownDone == nil {
		return
	}
	s.teardownErr = err
	close(s.teardownDone)
}

func (s *ExternalSession) completeTeardown(err error) {
	if s == nil {
		return
	}
	s.opMu.Lock()
	s.completeTeardownLocked(err)
	s.opMu.Unlock()
}

func (s *ExternalSession) waitForTeardownLocked() error {
	done := s.teardownDone
	s.opMu.Unlock()
	<-done
	s.opMu.Lock()
	return s.teardownErr
}

func (s *ExternalSession) finishTeardownLocked(
	teardown *externalSessionTeardown,
	baseErr error,
	completions *transactionCompletionBatch,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.Join(err, fmt.Errorf("external session teardown panicked: %v", recovered))
		}
		if completions == nil {
			s.completeTeardownLocked(err)
		} else {
			teardownErr := err
			completions.addAfter(func() { s.completeTeardown(teardownErr) })
		}
	}()
	err = errors.Join(baseErr, s.runCleanupOutsideOperationLock(teardown))
	err = errors.Join(err, s.closeResourcesLocked(teardown, completions))
	return err
}

// SetCleanupHook installs the protocol-owned prepared-state cleanup for this
// handle. Only one hook may be installed. If teardown won the race, the hook is
// invoked inline so protocol state is never left behind; the returned state
// error still tells the caller that the handle can no longer be used.
//
// Cleanup hooks must not re-enter this ExternalSession. Both the normal
// teardown path and an already-invalid late registration invoke hooks outside
// opMu; a late registration waits for the teardown owner first so hooks cannot
// overlap or race connection retirement.
func (s *ExternalSession) SetCleanupHook(hook func()) error {
	if hook == nil {
		return nil
	}
	if s == nil || s.pool == nil {
		return errors.Join(ErrExternalSessionClosed, callExternalSessionCleanupHook(hook))
	}

	p := s.pool
	// Registration is release-side work: while shutdown is waiting, admitting it
	// lets the writer observe the hook and run it before connection retirement.
	// Once the writer is active, ReleaseLock waits and this method instead takes
	// the already-stale inline-cleanup path below.
	p.lifecycleMu.ReleaseLock()
	s.opMu.Lock()
	var stateErr error
	if s.stale || s.generation != p.externalGeneration {
		stateErr = ErrExternalSessionStale
	} else if s.closed || s.cleanupStarted {
		stateErr = ErrExternalSessionClosed
	} else if s.cleanupHook != nil {
		s.opMu.Unlock()
		p.lifecycleMu.ReleaseUnlock()
		return fmt.Errorf("external session cleanup hook is already set")
	} else {
		s.cleanupHook = hook
		s.opMu.Unlock()
		p.lifecycleMu.ReleaseUnlock()
		return nil
	}
	done := s.teardownDone
	s.opMu.Unlock()
	p.lifecycleMu.ReleaseUnlock()
	if done != nil {
		<-done
	}
	return errors.Join(stateErr, callExternalSessionCleanupHook(hook))
}

// Commit commits only the exact active transaction supplied by the caller.
// A stale or mismatched pointer is rejected before any driver operation.
func (s *ExternalSession) Commit(expected *stdsql.Tx) error {
	if s == nil || s.pool == nil {
		return ErrExternalSessionClosed
	}
	p := s.pool
	var completions transactionCompletionBatch
	defer completions.run()
	p.lifecycleMu.ReleaseLock()
	defer p.lifecycleMu.ReleaseUnlock()
	s.opMu.Lock()
	defer s.opMu.Unlock()

	if err := s.externalStateErrorLocked(); err != nil {
		return err
	}
	if expected == nil || s.tx == nil || s.tx != expected {
		return ErrExternalSessionTransactionMismatch
	}
	err := callExternalSessionTransactionDriver("commit transaction", expected.Commit)
	if externalSessionDriverPanicked(err) {
		p.recordExternalSessionPanic(err)
	}
	s.tx = nil
	completions.add(p.finishTransactionWithOutcomeDeferred(expected, err == nil))
	if err == nil {
		return nil
	}

	teardown, owner := s.beginTeardownLocked(false)
	if !owner {
		return errors.Join(err, s.waitForTeardownLocked())
	}
	if externalSessionDriverPanicked(err) {
		teardown.quarantineErr = err
	}
	return s.finishTeardownLocked(teardown, err, &completions)
}

// Rollback rolls back only the exact active transaction supplied by the
// caller. cleanup runs on the same physical connection after the driver proves
// the transaction inactive and before another operation can enter the handle.
// The caller must reserve any provider rollback-maintenance barrier first.
func (s *ExternalSession) Rollback(
	expected *stdsql.Tx,
	cleanup func(*stdsql.Conn) error,
) error {
	if s == nil || s.pool == nil {
		return ErrExternalSessionClosed
	}
	p := s.pool
	var completions transactionCompletionBatch
	defer completions.run()
	p.lifecycleMu.ReleaseLock()
	defer p.lifecycleMu.ReleaseUnlock()
	s.opMu.Lock()
	defer s.opMu.Unlock()

	if err := s.externalStateErrorLocked(); err != nil {
		return err
	}
	if expected == nil || s.tx == nil || s.tx != expected {
		return ErrExternalSessionTransactionMismatch
	}
	err := callExternalSessionTransactionDriver("rollback transaction", expected.Rollback)
	if externalSessionDriverPanicked(err) {
		p.recordExternalSessionPanic(err)
	}
	inactive := transactionInactiveError(err)
	s.tx = nil
	completions.add(p.finishTransactionWithOutcomeDeferred(expected, false))
	if !inactive {
		teardown, owner := s.beginTeardownLocked(false)
		if !owner {
			return errors.Join(err, s.waitForTeardownLocked())
		}
		if externalSessionDriverPanicked(err) {
			teardown.quarantineErr = err
		}
		return s.finishTeardownLocked(teardown, err, &completions)
	}
	if cleanup != nil {
		err = errors.Join(err, callExternalSessionDriver(func() error { return cleanup(s.conn) }))
	}
	return err
}

// Close releases an external handle. It is idempotent and uses release-side
// lifecycle admission so callers can close handles while pool shutdown waits
// for older operations to drain.
func (s *ExternalSession) Close() error {
	if s == nil || s.pool == nil {
		return nil
	}
	p := s.pool
	var completions transactionCompletionBatch
	defer completions.run()
	p.lifecycleMu.ReleaseLock()
	defer p.lifecycleMu.ReleaseUnlock()
	s.opMu.Lock()
	defer s.opMu.Unlock()

	if s.teardownDone != nil {
		return s.waitForTeardownLocked()
	}
	teardown, _ := s.beginTeardownLocked(false)
	return s.finishTeardownLocked(teardown, nil, &completions)
}

// SetTransactionLifecycleHooks installs optional hooks for providers that need
// to coordinate transaction admission with shared catalog maintenance. The
// admission hook is called only when a new transaction is about to be opened;
// it returns a completion function that must be called with the newly opened
// transaction, or nil when opening failed. The finished hook is called whenever
// a transaction mapping is removed by any pool lifecycle path.
//
// Hooks are deliberately optional so standalone test pools and callers that do
// not own a provider retain the original pool behavior.
func (p *ConnectionPool) SetTransactionLifecycleHooks(
	admission func() func(*stdsql.Tx),
	finished func(*stdsql.Tx),
) {
	p.transactionHooksMu.Lock()
	p.transactionAdmission = admission
	p.transactionAdmissionWithError = nil
	p.transactionFinished = finished
	p.transactionHooksMu.Unlock()
}

// SetTransactionLifecycleHooksWithError installs the provider-aware
// transaction admission hook. A non-nil error prevents the pool from calling
// BeginTx; the returned completion callback is invoked only for an admission
// that actually reserved a transaction slot. The legacy setter above remains
// available for test pools and downstream embedders.
func (p *ConnectionPool) SetTransactionLifecycleHooksWithError(
	admission func() (func(*stdsql.Tx), error),
	finished func(*stdsql.Tx),
) {
	if p == nil {
		return
	}
	p.transactionHooksMu.Lock()
	p.transactionAdmission = nil
	p.transactionAdmissionWithError = admission
	p.transactionFinished = finished
	p.transactionHooksMu.Unlock()
}

// SetTransactionAdmissionGuard installs an optional provider-owned guard for
// new physical transaction admission. Existing transactions still use the
// ordinary fast path so they can be finalized or repaired after a generation
// failure; only attempts to open a new transaction are rejected.
func (p *ConnectionPool) SetTransactionAdmissionGuard(guard func() error) {
	if p == nil {
		return
	}
	p.transactionHooksMu.Lock()
	p.transactionAdmissionGuard = guard
	p.transactionHooksMu.Unlock()
}

// SetConnectionAdmissionGuard installs the provider-owned guard used by
// ordinary connection, execution-snapshot, and metadata entry points. The
// guard runs once before lifecycle admission and once after the pool/session
// locks are held. The second check closes the gap where shutdown completed and
// the lifecycle gate reopened while the provider transition was still active.
func (p *ConnectionPool) SetConnectionAdmissionGuard(guard func(context.Context) error) {
	if p == nil {
		return
	}
	p.transactionHooksMu.Lock()
	p.connectionAdmissionGuard = guard
	p.transactionHooksMu.Unlock()
}

func (p *ConnectionPool) transactionAdmissionError() error {
	if p == nil {
		return nil
	}
	if err := p.externalSessionPanicError(); err != nil {
		return err
	}
	p.transactionHooksMu.RLock()
	guard := p.transactionAdmissionGuard
	p.transactionHooksMu.RUnlock()
	if guard == nil {
		return nil
	}
	return guard()
}

func (p *ConnectionPool) connectionAdmissionError(ctx context.Context) error {
	if p == nil {
		return nil
	}
	if err := p.externalSessionPanicError(); err != nil {
		return err
	}
	p.transactionHooksMu.RLock()
	guard := p.connectionAdmissionGuard
	p.transactionHooksMu.RUnlock()
	if guard == nil {
		return nil
	}
	return guard(ctx)
}

func (p *ConnectionPool) beginTransactionAdmission() func(*stdsql.Tx) {
	completion, _ := p.beginTransactionAdmissionWithError()
	return completion
}

func (p *ConnectionPool) beginTransactionAdmissionWithError() (func(*stdsql.Tx), error) {
	p.transactionHooksMu.RLock()
	admissionWithError := p.transactionAdmissionWithError
	admission := p.transactionAdmission
	p.transactionHooksMu.RUnlock()
	if admissionWithError != nil {
		completion, err := admissionWithError()
		if err != nil {
			return func(*stdsql.Tx) {}, err
		}
		if completion == nil {
			return func(*stdsql.Tx) {}, nil
		}
		return completion, nil
	}
	if admission == nil {
		return func(*stdsql.Tx) {}, nil
	}
	completion := admission()
	if completion == nil {
		return func(*stdsql.Tx) {}, nil
	}
	return completion, nil
}

func (p *ConnectionPool) markTransactionActive(tx *stdsql.Tx) {
	if p == nil || tx == nil {
		return
	}
	p.transactionCompletionMu.Lock()
	if p.activeTransactions == nil {
		p.activeTransactions = make(map[*stdsql.Tx]struct{})
	}
	p.activeTransactions[tx] = struct{}{}
	p.transactionCompletionMu.Unlock()
}

func (p *ConnectionPool) transactionIsMapped(tx *stdsql.Tx) bool {
	if p == nil || tx == nil {
		return false
	}
	if _, active := p.activeTransactions[tx]; active {
		return true
	}
	found := false
	p.txns.Range(func(_, value any) bool {
		if mapped, ok := value.(*stdsql.Tx); ok && mapped == tx {
			found = true
			return false
		}
		return true
	})
	return found
}

// pruneTransactionCompletionsLocked removes only completed tombstones that
// have exceeded the retention window or the FIFO bound. Callers hold
// transactionCompletionMu. Pending callbacks and active transactions are never
// removed by this method.
func (p *ConnectionPool) pruneTransactionCompletionsLocked(now time.Time) {
	if p == nil {
		return
	}
	if len(p.transactionCompletions) == 0 {
		p.transactionCompletionOrder = nil
		return
	}
	for tx, state := range p.transactionCompletions {
		completed, completedAt := state.completionSnapshot()
		if completed && !completedAt.IsZero() && now.Sub(completedAt) >= transactionCompletionTombstoneTTL {
			delete(p.transactionCompletions, tx)
		}
	}

	// Rebuild the order from live map entries, removing stale/duplicate keys
	// left behind by an earlier bounded prune. Completed entries created by a
	// concurrent finalizer are appended below if they have not reached the FIFO.
	order := make([]*stdsql.Tx, 0, len(p.transactionCompletionOrder))
	seen := make(map[*stdsql.Tx]struct{}, len(p.transactionCompletionOrder))
	for _, tx := range p.transactionCompletionOrder {
		if _, duplicate := seen[tx]; duplicate {
			continue
		}
		state, present := p.transactionCompletions[tx]
		if !present {
			continue
		}
		completed, _ := state.completionSnapshot()
		if !completed {
			// This should not normally occur because entries enter the FIFO only
			// after completion, but retaining a pending map entry is safer than
			// deleting it if a future caller violates that assumption.
			continue
		}
		seen[tx] = struct{}{}
		order = append(order, tx)
	}
	for tx, state := range p.transactionCompletions {
		completed, completedAt := state.completionSnapshot()
		if !completed {
			continue
		}
		if !completedAt.IsZero() && now.Sub(completedAt) >= transactionCompletionTombstoneTTL {
			delete(p.transactionCompletions, tx)
			continue
		}
		if _, present := seen[tx]; !present {
			seen[tx] = struct{}{}
			order = append(order, tx)
		}
	}
	for len(order) > maxTransactionCompletionTombstones {
		oldest := order[0]
		order = order[1:]
		if state := p.transactionCompletions[oldest]; state != nil {
			completed, _ := state.completionSnapshot()
			if completed {
				delete(p.transactionCompletions, oldest)
			}
		}
	}
	p.transactionCompletionOrder = order
}

// recordTransactionCompletion keeps a bounded FIFO of completed states. The
// map lock is acquired before the state lock so it has the same lock ordering
// as registration/pruning and cannot deadlock with a concurrent late register.
func (p *ConnectionPool) recordTransactionCompletion(tx *stdsql.Tx, state *transactionCompletion) {
	if p == nil || tx == nil || state == nil {
		return
	}
	p.transactionCompletionMu.Lock()
	defer p.transactionCompletionMu.Unlock()
	if p.transactionCompletions == nil {
		return
	}
	if current := p.transactionCompletions[tx]; current != state {
		return
	}
	if !state.markTombstone() {
		return
	}
	p.transactionCompletionOrder = append(p.transactionCompletionOrder, tx)
	p.pruneTransactionCompletionsLocked(time.Now())
}

func (p *ConnectionPool) clearCompletedTransactionCompletions() {
	if p == nil {
		return
	}
	p.transactionCompletionMu.Lock()
	for tx, state := range p.transactionCompletions {
		if completed, _ := state.completionSnapshot(); completed {
			delete(p.transactionCompletions, tx)
		}
	}
	p.transactionCompletionOrder = nil
	p.transactionCompletionMu.Unlock()
}

// finishTransactionWithOutcomeDeferred records the terminal outcome and
// returns a dispatch token. The caller must run the token after releasing any
// pool lifecycle and session locks. Provider teardown can still own its outer
// generation locks while the pool drains this token, so callbacks are limited
// to terminal notification and resource release; they must not re-enter
// provider DDL, cleanup, Close, or Restart.
func (p *ConnectionPool) finishTransactionWithOutcomeDeferred(tx *stdsql.Tx, success bool) *transactionCompletionDispatch {
	if tx == nil {
		return nil
	}
	p.transactionCompletionMu.Lock()
	p.pruneTransactionCompletionsLocked(time.Now())
	if p.transactionCompletions == nil {
		p.transactionCompletions = make(map[*stdsql.Tx]*transactionCompletion)
	}
	if p.activeTransactions != nil {
		delete(p.activeTransactions, tx)
	}
	completion := p.transactionCompletions[tx]
	if completion == nil {
		completion = &transactionCompletion{}
		p.transactionCompletions[tx] = completion
	}
	p.transactionCompletionMu.Unlock()
	ownsCompletionDispatch := completion.completeDeferred(success)
	if !ownsCompletionDispatch {
		return nil
	}

	p.transactionHooksMu.RLock()
	finished := p.transactionFinished
	p.transactionHooksMu.RUnlock()
	return &transactionCompletionDispatch{
		completion: completion,
		pool:       p,
		finished:   finished,
		tx:         tx,
		success:    success,
	}
}

// RegisterTransactionCompletion attaches a callback to a raw transaction's
// terminal lifecycle. It is intentionally small and optional; backend.Session
// uses it to bridge provider/pool teardown into its protocol callback registry.
//
// A recent terminal outcome is delivered inline before this method returns.
// Ordinary finalizers also deliver callbacks synchronously, but only after
// releasing the pool lifecycle and session locks, so callbacks may re-enter
// pool release/read APIs. Callbacks must remain terminal notification/resource
// release hooks and must not re-enter provider DDL, cleanup, Close, or Restart;
// a provider generation transition may still own those outer locks.
func (p *ConnectionPool) RegisterTransactionCompletion(tx *stdsql.Tx, callback func(bool)) bool {
	if p == nil || tx == nil || callback == nil {
		return false
	}
	p.transactionCompletionMu.Lock()
	// Prune completed tombstones before admitting a new registration. If the
	// transaction is no longer active and its tombstone has aged out, fail closed
	// and let the caller release its resource through the fallback path.
	p.pruneTransactionCompletionsLocked(time.Now())
	if p.transactionCompletions == nil {
		p.transactionCompletions = make(map[*stdsql.Tx]*transactionCompletion)
	}
	completion := p.transactionCompletions[tx]
	if completion == nil {
		if !p.transactionIsMapped(tx) {
			p.transactionCompletionMu.Unlock()
			callback(false)
			return false
		}
		completion = &transactionCompletion{}
		p.transactionCompletions[tx] = completion
	}
	p.transactionCompletionMu.Unlock()
	return completion.register(callback)
}

// CurrentSchema retrieves the current schema of the connection.
// Returns an empty string if the connection is not established
// or the schema cannot be retrieved.
func (p *ConnectionPool) CurrentSchema(id uint32) string {
	if err := p.connectionAdmissionError(context.Background()); err != nil {
		return ""
	}
	p.lifecycleMu.RLock()
	defer p.lifecycleMu.RUnlock()
	unlock := p.lockSession(id)
	defer unlock()
	if err := p.connectionAdmissionError(context.Background()); err != nil {
		return ""
	}
	return p.currentSchemaLocked(id)
}

func (p *ConnectionPool) currentSchemaLocked(id uint32) string {
	if tx, active, err := p.transactionEntryLocked(id); active {
		if err != nil {
			return ""
		}
		// An active transaction is usable only with the owner captured at
		// BeginTx. Do not fall back to the generic session connection when that
		// owner mapping is absent or malformed.
		if _, err := p.activeTxnOwnerLocked(id); err != nil {
			return ""
		}
		var schema string
		if err := tx.QueryRowContext(context.Background(), "SELECT CURRENT_SCHEMA").Scan(&schema); err != nil {
			logrus.WithError(err).Error("Failed to get current schema")
			return ""
		}
		return schema
	}
	conn, present, err := p.connectionEntryLocked(id)
	if !present || err != nil {
		return ""
	}
	var schema string
	if err := conn.QueryRowContext(context.Background(), "SELECT CURRENT_SCHEMA").Scan(&schema); err != nil {
		logrus.WithError(err).Error("Failed to get current schema")
		return ""
	}
	return schema
}

// CurrentCatalog retrieves the current catalog of the connection. Before the
// first connection, it returns the owning provider's catalog so GMS can resolve
// fully qualified names. Closed or broken connections still return empty.
func (p *ConnectionPool) CurrentCatalog(id uint32) string {
	if err := p.connectionAdmissionError(context.Background()); err != nil {
		return ""
	}
	p.lifecycleMu.RLock()
	defer p.lifecycleMu.RUnlock()
	unlock := p.lockSession(id)
	defer unlock()
	if err := p.connectionAdmissionError(context.Background()); err != nil {
		return ""
	}
	return p.currentCatalogLocked(id)
}

func (p *ConnectionPool) currentCatalogLocked(id uint32) string {
	if tx, active, err := p.transactionEntryLocked(id); active {
		if err != nil {
			return ""
		}
		// See currentSchemaLocked: never use p.conns as an owner substitute for
		// an active transaction.
		if _, err := p.activeTxnOwnerLocked(id); err != nil {
			return ""
		}
		var catalog string
		if err := tx.QueryRowContext(context.Background(), "SELECT CURRENT_CATALOG").Scan(&catalog); err != nil {
			logrus.WithError(err).Error("Failed to get current catalog")
			return ""
		}
		return catalog
	}
	conn, present, err := p.connectionEntryLocked(id)
	if !present {
		if _, closed := p.closedConns.Load(id); closed {
			return ""
		}
		return p.defaultCatalog
	}
	if err != nil {
		return ""
	}
	var catalog string
	if err := conn.QueryRowContext(context.Background(), "SELECT CURRENT_CATALOG").Scan(&catalog); err != nil {
		logrus.WithError(err).Error("Failed to get current catalog")
		return ""
	}
	return catalog
}

func (p *ConnectionPool) GetConn(ctx context.Context, id uint32) (*stdsql.Conn, error) {
	conn, _, err := p.GetConnWithBinding(ctx, id)
	return conn, err
}

// GetConnWithBinding acquires a catalog connection and returns the transaction
// binding observed in the same lifecycle/session critical section. Callers that
// need to remember an identity for a later close must use this method instead
// of doing a second GetTxnBinding lookup after GetConn returns.
func (p *ConnectionPool) GetConnWithBinding(ctx context.Context, id uint32) (*stdsql.Conn, *stdsql.Tx, error) {
	return p.getConnWithBinding(ctx, id, "")
}

func (p *ConnectionPool) getConnWithBinding(ctx context.Context, id uint32, schemaName string) (*stdsql.Conn, *stdsql.Tx, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := p.connectionAdmissionError(ctx); err != nil {
		return nil, nil, err
	}
	p.lifecycleMu.RLock()
	defer p.lifecycleMu.RUnlock()
	unlock := p.lockSession(id)
	defer unlock()
	if err := p.connectionAdmissionError(ctx); err != nil {
		return nil, nil, err
	}
	var (
		conn *stdsql.Conn
		err  error
	)
	if schemaName == "" {
		conn, err = p.getConnLocked(ctx, id)
	} else {
		conn, err = p.getConnForSchemaLocked(ctx, id, schemaName)
	}
	if err != nil {
		return nil, nil, err
	}
	boundConn, tx := p.txnBindingLocked(id)
	if tx != nil && boundConn != conn {
		// The helper should never return a transaction with a different owner. A
		// mismatch indicates lifecycle corruption; do not let a caller remember a
		// pair that was not selected atomically.
		return nil, nil, fmt.Errorf("transaction execution binding changed during connection acquisition")
	}
	if tx == nil && boundConn != conn {
		// A detached owner marker makes an idle connection unsafe to associate with
		// this session. Preserve the connection result for normal callers only when
		// no owner marker is present; identity-aware callers receive a nil binding.
		if _, ownerPresent := p.txnConns.Load(id); ownerPresent {
			boundConn = nil
		}
	}
	return conn, tx, nil
}

func (p *ConnectionPool) getConnLocked(ctx context.Context, id uint32) (*stdsql.Conn, error) {
	// An active transaction is bound to the physical connection captured at
	// BeginTx. Never consult the generic session map for this path: after a
	// reconnect or a partially failed lifecycle operation it may point at a
	// different connection. A missing/invalid owner is fail-closed rather than
	// guessed from that generic entry.
	if owner, err := p.activeTxnOwnerLocked(id); err != nil {
		return nil, err
	} else if owner != nil {
		return owner, nil
	}
	var conn *stdsql.Conn
	entry, present, err := p.connectionEntryLocked(id)
	if err != nil {
		return nil, err
	}
	if !present {
		if p.DB == nil {
			return nil, fmt.Errorf("connection pool database is unavailable")
		}
		c, err := p.DB.Conn(ctx)
		if err != nil {
			return nil, err
		}
		if _, transactionActive := p.txns.Load(id); !transactionActive {
			if err := p.initializeConnection(ctx, c); err != nil {
				_ = c.Close()
				return nil, err
			}
		}
		if err := p.registerMySQLUDFs(c); err != nil {
			_ = c.Close()
			return nil, err
		}
		p.closedConns.Delete(id)
		p.conns.Store(id, c)
		conn = c
	} else {
		conn = entry
		// A session transaction owns this connection's transaction-scoped
		// state. Re-running the initializer here would execute LOAD/CREATE
		// SECRET inside that transaction and could alter or roll back with user
		// work. GetTxn initializes before BeginTx; keep the state stable until
		// the transaction is closed.
		if _, transactionActive := p.txns.Load(id); !transactionActive {
			if err := p.initializeConnection(ctx, conn); err != nil {
				// Do not leave a failed or partially initialized connection available
				// to a later request. CompareAndDelete avoids removing a replacement
				// installed by a concurrent recovery path.
				p.conns.CompareAndDelete(id, conn)
				p.closedConns.Store(id, struct{}{})
				_ = conn.Close()
				return nil, err
			}
		}
	}
	return conn, nil
}

// activeTxnOwnerLocked returns the physical owner for an active transaction.
// A nil owner with a nil error means that no transaction is active; callers
// must hold the per-session lifecycle lock while using this helper.
func (p *ConnectionPool) activeTxnOwnerLocked(id uint32) (*stdsql.Conn, error) {
	_, active, err := p.transactionEntryLocked(id)
	if !active {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	owner, present, err := p.transactionOwnerEntryLocked(id)
	if !present {
		return nil, fmt.Errorf("active transaction has no physical owner")
	}
	if err != nil {
		return nil, err
	}
	return owner, nil
}

// connectionEntryLocked and transactionEntryLocked distinguish a missing map
// entry from a malformed one. The latter is a lifecycle corruption and must be
// surfaced to callers that can return an error instead of being guessed away.
func (p *ConnectionPool) connectionEntryLocked(id uint32) (*stdsql.Conn, bool, error) {
	entry, ok := p.conns.Load(id)
	if !ok {
		return nil, false, nil
	}
	conn, valid := entry.(*stdsql.Conn)
	if !valid || conn == nil {
		return nil, true, fmt.Errorf("connection mapping is invalid")
	}
	return conn, true, nil
}

func (p *ConnectionPool) transactionEntryLocked(id uint32) (*stdsql.Tx, bool, error) {
	entry, ok := p.txns.Load(id)
	if !ok {
		return nil, false, nil
	}
	tx, valid := entry.(*stdsql.Tx)
	if !valid || tx == nil {
		return nil, true, fmt.Errorf("active transaction mapping is invalid")
	}
	return tx, true, nil
}

func (p *ConnectionPool) transactionOwnerEntryLocked(id uint32) (*stdsql.Conn, bool, error) {
	entry, ok := p.txnConns.Load(id)
	if !ok || entry == nil {
		return nil, false, nil
	}
	conn, valid := entry.(*stdsql.Conn)
	if !valid || conn == nil {
		return nil, true, fmt.Errorf("active transaction has invalid physical owner")
	}
	return conn, true, nil
}

// DuckDB stores registered scalar UDFs in the database catalog shared by all connections.
func (p *ConnectionPool) registerMySQLUDFs(conn *stdsql.Conn) error {
	p.registerMySQLUDFsOnce.Do(func() {
		if err := registerMySQLRand(conn); err != nil {
			p.registerMySQLUDFsErr = fmt.Errorf("register mysql_rand: %w", err)
			return
		}
		if err := registerMySQLRandomBytes(conn); err != nil {
			p.registerMySQLUDFsErr = fmt.Errorf("register mysql_random_bytes: %w", err)
			return
		}
		if err := registerMySQLStringToVector(conn); err != nil {
			p.registerMySQLUDFsErr = fmt.Errorf("register string_to_vector: %w", err)
		}
	})
	return p.registerMySQLUDFsErr
}

func (p *ConnectionPool) GetConnForSchema(ctx context.Context, id uint32, schemaName string) (*stdsql.Conn, error) {
	conn, _, err := p.GetConnForSchemaWithBinding(ctx, id, schemaName)
	return conn, err
}

// GetConnForSchemaWithBinding is the schema-selecting counterpart to
// GetConnWithBinding. The connection, schema selection, and transaction
// identity are observed under one lifecycle/session lock.
func (p *ConnectionPool) GetConnForSchemaWithBinding(ctx context.Context, id uint32, schemaName string) (*stdsql.Conn, *stdsql.Tx, error) {
	return p.getConnWithBinding(ctx, id, schemaName)
}

// GetExecutionSnapshotLease acquires the connection and observes the
// transaction binding while holding the lifecycle lock. The returned release
// function keeps that lifecycle reader held until the caller has finished all
// driver work and consumed any result iterator. This prevents Close, Reset,
// and provider Restart from retiring the selected physical generation in the
// gap between snapshot acquisition and execution.
//
// The release function is idempotent. On an acquisition error the lease is
// released before returning and the returned callback is a no-op, so callers
// do not need a special error cleanup branch.
func (p *ConnectionPool) GetExecutionSnapshotLease(
	ctx context.Context,
	id uint32,
	schemaName string,
	catalogOnly bool,
) (*stdsql.Conn, *stdsql.Tx, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := p.connectionAdmissionError(ctx); err != nil {
		return nil, nil, func() {}, err
	}
	p.lifecycleMu.RLock()
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			p.lifecycleMu.RUnlock()
		})
	}
	unlock := p.lockSession(id)
	defer unlock()
	if err := p.connectionAdmissionError(ctx); err != nil {
		release()
		return nil, nil, func() {}, err
	}

	if tx, active, txErr := p.transactionEntryLocked(id); active {
		if txErr != nil {
			release()
			return nil, nil, func() {}, txErr
		}
		conn, connOK, connErr := p.transactionOwnerEntryLocked(id)
		if connErr != nil {
			release()
			return nil, tx, func() {}, connErr
		}
		if !connOK {
			release()
			return nil, tx, func() {}, fmt.Errorf("transaction execution snapshot has no physical owner")
		}
		return conn, tx, release, nil
	}
	var (
		conn *stdsql.Conn
		err  error
	)
	if catalogOnly {
		conn, err = p.getConnLocked(ctx, id)
	} else {
		conn, err = p.getConnForSchemaLocked(ctx, id, schemaName)
	}
	if err != nil {
		release()
		return nil, nil, func() {}, err
	}
	return conn, nil, release, nil
}

// GetExecutionSnapshot acquires the connection and observes the transaction
// binding while holding the lifecycle lock. It retains the historical
// snapshot-only contract by releasing the lifecycle lease before returning;
// callers that execute or consume results after this point should use
// GetExecutionSnapshotLease through the adapter helper instead.
func (p *ConnectionPool) GetExecutionSnapshot(ctx context.Context, id uint32, schemaName string, catalogOnly bool) (*stdsql.Conn, *stdsql.Tx, error) {
	conn, tx, release, err := p.GetExecutionSnapshotLease(ctx, id, schemaName, catalogOnly)
	if release != nil {
		release()
	}
	return conn, tx, err
}

func (p *ConnectionPool) CloseConn(id uint32) error {
	var completions transactionCompletionBatch
	defer completions.run()
	p.lifecycleMu.ReleaseLock()
	defer p.lifecycleMu.ReleaseUnlock()
	unlock := p.lockSession(id)
	defer unlock()
	return p.closeConnLockedWithBatch(id, nil, &completions)
}

func (p *ConnectionPool) GetTxn(ctx context.Context, id uint32, schemaName string, options *stdsql.TxOptions) (*stdsql.Tx, error) {
	_, tx, err := p.GetTxnWithBinding(ctx, id, schemaName, options)
	return tx, err
}

// GetTxnWithBinding opens (or observes) a transaction and returns its exact
// physical owner from the same lifecycle/session critical section. Session
// adapters use this to remember a close identity without a second, racy pool
// lookup.
func (p *ConnectionPool) GetTxnWithBinding(ctx context.Context, id uint32, schemaName string, options *stdsql.TxOptions) (*stdsql.Conn, *stdsql.Tx, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// New-transaction admission may wait for a normal provider generation
	// transition. Do that wait
	// before entering lifecycleMu.RLock; otherwise a caller that is blocked on
	// cleanupActive would itself keep pool teardown from acquiring its writer.
	// Existing transactions remain available to the dedicated release/finalizer
	// accessors while cleanup is waiting. This execution-facing method checks
	// connection admission before and after its lifecycle/session snapshot so an
	// active mapping cannot be used to start more work in the retiring generation.
	for {
		if _, alreadyActive := p.txns.Load(id); alreadyActive {
			if err := p.connectionAdmissionError(ctx); err != nil {
				return nil, nil, err
			}
			p.lifecycleMu.RLock()
			unlock := p.lockSession(id)
			tx, active, err := p.transactionEntryLocked(id)
			if active {
				if err != nil {
					unlock()
					p.lifecycleMu.RUnlock()
					return nil, nil, err
				}
				if err := p.connectionAdmissionError(ctx); err != nil {
					unlock()
					p.lifecycleMu.RUnlock()
					return nil, nil, err
				}
				owner, err := p.activeTxnOwnerLocked(id)
				if err != nil {
					unlock()
					p.lifecycleMu.RUnlock()
					return nil, nil, err
				}
				unlock()
				p.lifecycleMu.RUnlock()
				return owner, tx, nil
			}
			// The observed transaction closed between the lock-free probe and
			// the lifecycle snapshot. Retry so a new transaction gets a fresh
			// admission reservation outside lifecycleMu.
			unlock()
			p.lifecycleMu.RUnlock()
			continue
		}

		if err := p.transactionAdmissionError(); err != nil {
			return nil, nil, err
		}
		completeAdmission, admissionErr := p.beginTransactionAdmissionWithError()
		if admissionErr != nil {
			return nil, nil, admissionErr
		}
		p.lifecycleMu.RLock()
		unlock := p.lockSession(id)
		// A generation can become poisoned while the admission hook waits for a
		// cleanup barrier. Recheck after the hook so its historical nil-completion
		// convention cannot accidentally allow a new BeginTx into a retired
		// generation.
		if err := p.transactionAdmissionError(); err != nil {
			completeAdmission(nil)
			unlock()
			p.lifecycleMu.RUnlock()
			return nil, nil, err
		}

		if tx, active, err := p.transactionEntryLocked(id); active {
			var ownerErr error
			var owner *stdsql.Conn
			if err == nil {
				owner, ownerErr = p.activeTxnOwnerLocked(id)
			}
			completeAdmission(nil)
			unlock()
			p.lifecycleMu.RUnlock()
			if err != nil {
				return nil, nil, err
			}
			if ownerErr != nil {
				return nil, nil, ownerErr
			}
			return owner, tx, nil
		}

		conn, err := p.getConnForSchemaLocked(ctx, id, schemaName)
		if err != nil {
			completeAdmission(nil)
			unlock()
			p.lifecycleMu.RUnlock()
			return nil, nil, err
		}
		// A session transaction can span multiple protocol requests (for example,
		// after SET autocommit=0), so a request-scoped cancellation must not end it.
		tx, err := conn.BeginTx(context.WithoutCancel(ctx), options)
		if err != nil {
			completeAdmission(nil)
			unlock()
			p.lifecycleMu.RUnlock()
			return nil, nil, err
		}
		// Keep the transaction and its physical owner as one lifecycle binding while
		// holding the pool mutex. Readers can therefore never observe a transaction
		// without the connection it owns.
		p.txns.Store(id, tx)
		p.txnConns.Store(id, conn)
		p.markTransactionActive(tx)
		completeAdmission(tx)
		unlock()
		p.lifecycleMu.RUnlock()
		return conn, tx, nil
	}
}

func (p *ConnectionPool) TryGetTxn(id uint32) *stdsql.Tx {
	p.lifecycleMu.RLock()
	defer p.lifecycleMu.RUnlock()
	unlock := p.lockSession(id)
	defer unlock()
	return p.tryGetTxnLocked(id)
}

// TryGetTxnForRelease is the release-side transaction snapshot. Unlike the
// ordinary lookup, it remains admissible while pool shutdown is waiting for
// readers to drain, so a close/finalizer path can still discover the binding it
// needs to release. The returned pointer is only a snapshot; callers must use
// an identity-safe finalizer for the subsequent driver operation.
func (p *ConnectionPool) TryGetTxnForRelease(id uint32) *stdsql.Tx {
	p.lifecycleMu.ReleaseLock()
	defer p.lifecycleMu.ReleaseUnlock()
	unlock := p.lockSession(id)
	defer unlock()
	return p.tryGetTxnLocked(id)
}

func (p *ConnectionPool) tryGetTxnLocked(id uint32) *stdsql.Tx {
	tx, _, _ := p.transactionEntryLocked(id)
	return tx
}

func (p *ConnectionPool) CloseTxn(id uint32) {
	_ = p.CloseTxnAndGet(id)
}

// CloseTxnAndGet removes the transaction currently bound to id and returns the
// exact mapping that was removed.  Capturing, identity removal, and completion
// notification happen under one release/session lifecycle section; callers do
// not need to perform a racy lookup followed by an unconditional CloseTxn.
// A nil result means that no valid transaction mapping was removed.
func (p *ConnectionPool) CloseTxnAndGet(id uint32) *stdsql.Tx {
	var completions transactionCompletionBatch
	defer completions.run()
	p.lifecycleMu.ReleaseLock()
	defer p.lifecycleMu.ReleaseUnlock()
	unlock := p.lockSession(id)
	defer unlock()
	tx := p.closeTxnLocked(id, nil)
	completions.add(p.finishTransactionWithOutcomeDeferred(tx, false))
	return tx
}

// GetTxnBinding returns the physical connection and transaction currently
// associated with a logical session. The connection is returned even when no
// transaction is active, which lets statement callers use one atomic snapshot
// instead of acquiring a connection and then racing a transaction opener.
func (p *ConnectionPool) GetTxnBinding(id uint32) (*stdsql.Conn, *stdsql.Tx) {
	p.lifecycleMu.RLock()
	defer p.lifecycleMu.RUnlock()
	unlock := p.lockSession(id)
	defer unlock()
	return p.txnBindingLocked(id)
}

// GetTxnBindingForRelease captures a transaction binding from a release-side
// lifecycle section. It is intended for close/finalizer paths that may run
// after shutdown has been announced; ordinary execution must continue using
// GetTxnBinding or GetExecutionSnapshot so new work remains excluded during
// teardown.
func (p *ConnectionPool) GetTxnBindingForRelease(id uint32) (*stdsql.Conn, *stdsql.Tx) {
	p.lifecycleMu.ReleaseLock()
	defer p.lifecycleMu.ReleaseUnlock()
	unlock := p.lockSession(id)
	defer unlock()
	return p.txnBindingLocked(id)
}

// GetTxnBindingForReleaseStatus is the error-preserving form used by lifecycle
// callback bridges. A malformed active transaction mapping must remain
// distinguishable from an absent mapping; otherwise a callback could release a
// lease while the physical transaction is still live but uninspectable.
func (p *ConnectionPool) GetTxnBindingForReleaseStatus(id uint32) (*stdsql.Conn, *stdsql.Tx, bool, error) {
	p.lifecycleMu.ReleaseLock()
	defer p.lifecycleMu.ReleaseUnlock()
	unlock := p.lockSession(id)
	defer unlock()
	tx, active, txErr := p.transactionEntryLocked(id)
	if active {
		if txErr != nil {
			return nil, nil, true, txErr
		}
		conn, present, ownerErr := p.transactionOwnerEntryLocked(id)
		if ownerErr != nil {
			return nil, tx, true, ownerErr
		}
		if !present {
			return nil, tx, true, fmt.Errorf("active transaction has no physical owner")
		}
		return conn, tx, true, nil
	}
	conn, present, connErr := p.connectionEntryLocked(id)
	if connErr != nil {
		return nil, nil, false, connErr
	}
	if !present {
		// A transaction owner marker without a transaction is a lifecycle
		// corruption, not an idle session. Treat it as active so callback bridges
		// cannot release leases or close a checked-out owner before recovery has
		// resolved the marker.
		if _, ownerPresent := p.txnConns.Load(id); ownerPresent {
			_, _, ownerErr := p.transactionOwnerEntryLocked(id)
			if ownerErr != nil {
				return nil, nil, true, ownerErr
			}
			return nil, nil, true, fmt.Errorf("transaction owner mapping exists without active transaction")
		}
		return nil, nil, false, nil
	}
	return conn, nil, false, nil
}

func (p *ConnectionPool) txnBindingLocked(id uint32) (*stdsql.Conn, *stdsql.Tx) {
	tx, active, txErr := p.transactionEntryLocked(id)
	if active {
		if txErr != nil {
			// An invalid active mapping must not be mistaken for an idle session
			// and then fall back to p.conns.
			return nil, nil
		}
		// An active transaction is only usable with the exact owner captured at
		// BeginTx. Never substitute the session's generic connection mapping:
		// after a reset/replacement that entry may belong to a different
		// transaction lifecycle.
		if conn, present, err := p.transactionOwnerEntryLocked(id); present && err == nil {
			return conn, tx
		}
		return nil, tx
	}
	// A detached owner marker is not a usable idle connection. Returning nil
	// prevents release callers from falling back to p.conns and retiring a
	// physical handle whose transaction identity is still unknown.
	if _, ownerPresent := p.txnConns.Load(id); ownerPresent {
		return nil, nil
	}
	if conn, present, err := p.connectionEntryLocked(id); present && err == nil {
		return conn, nil
	}
	return nil, nil
}

func (p *ConnectionPool) closeTxnLocked(id uint32, expected *stdsql.Tx) *stdsql.Tx {
	tx, present, err := p.transactionEntryLocked(id)
	if !present {
		return nil
	}
	if err != nil {
		// An unconditional close is allowed to discard a poisoned mapping so it
		// cannot block future work. Identity-aware cleanup leaves malformed state
		// untouched because it cannot prove that the caller owns it.
		if expected == nil {
			p.txns.Delete(id)
			p.txnConns.Delete(id)
		}
		return nil
	}
	if expected != nil && tx != expected {
		return nil
	}
	p.txns.Delete(id)
	p.txnConns.Delete(id)
	return tx
}

// CloseTxnIf removes a transaction mapping only if it still refers to tx.
// Older deferred cleanup must not delete a newer transaction for the same
// logical session ID.
func (p *ConnectionPool) CloseTxnIf(id uint32, tx *stdsql.Tx) {
	if tx == nil {
		return
	}
	var completions transactionCompletionBatch
	defer completions.run()
	p.lifecycleMu.ReleaseLock()
	defer p.lifecycleMu.ReleaseUnlock()
	unlock := p.lockSession(id)
	defer unlock()
	closed := p.closeTxnLocked(id, tx)
	completions.add(p.finishTransactionWithOutcomeDeferred(closed, false))
}

// CommitTxn commits the transaction only while it is still the transaction
// bound to id. The driver operation, identity check, and bad-connection
// eviction are serialized under lifecycleMu, so a delayed commit cannot
// remove or close a replacement binding.
func (p *ConnectionPool) CommitTxn(id uint32, expected *stdsql.Tx) error {
	_, err := p.CommitTxnWithOutcome(id, expected)
	return err
}

// CommitTxnWithOutcome is the identity-aware commit surface used by the
// production Session adapter. matched is false when the expected transaction
// is no longer the current binding (for example, it was already rolled back or
// replaced). The legacy CommitTxn wrapper intentionally preserves its historic
// no-op behavior for that stale case, while callers that need to report the
// distinction can turn matched=false into a binding error without guessing.
func (p *ConnectionPool) CommitTxnWithOutcome(id uint32, expected *stdsql.Tx) (matched bool, err error) {
	var completions transactionCompletionBatch
	defer completions.run()
	p.lifecycleMu.ReleaseLock()
	defer p.lifecycleMu.ReleaseUnlock()
	unlock := p.lockSession(id)
	defer unlock()

	tx, conn, matched, bindingErr := p.txnBindingForFinalizeLocked(id, expected)
	if bindingErr != nil {
		return false, bindingErr
	}
	if !matched {
		// Another owner already finalized this transaction. Treat a delayed
		// callback as a no-op rather than touching the replacement transaction.
		return false, nil
	}
	err = tx.Commit()
	closed := p.closeTxnLocked(id, tx)
	completions.add(p.finishTransactionWithOutcomeDeferred(closed, err == nil))
	if err != nil && conn != nil {
		// A failed commit leaves the driver state unknown. Evict this exact
		// physical owner before it can be reused. The generic session mapping may
		// already point at a replacement connection; closeExactOwnerLocked keeps
		// that replacement untouched while still retiring the captured owner.
		err = errors.Join(err, p.closeExactOwnerLockedWithBatch(id, conn, &completions))
	}
	return true, err
}

// RollbackTxn rolls back the transaction only while it is still the binding
// owned by id. It returns the physical owner and whether the driver proved the
// transaction inactive. Unexpected rollback errors invalidate and evict that
// owner; a stale callback returns no owner so it cannot clean a replacement.
func (p *ConnectionPool) RollbackTxn(id uint32, expected *stdsql.Tx) (*stdsql.Conn, bool, error) {
	var conn *stdsql.Conn
	var inactive bool
	var err error
	var completions transactionCompletionBatch
	defer completions.run()
	p.lifecycleMu.ReleaseLock()
	defer p.lifecycleMu.ReleaseUnlock()
	unlock := p.lockSession(id)
	defer unlock()

	tx, owner, matched, bindingErr := p.txnBindingForFinalizeLocked(id, expected)
	if bindingErr != nil {
		return nil, false, bindingErr
	}
	if !matched {
		return nil, false, nil
	}
	conn = owner
	err = tx.Rollback()
	inactive = transactionInactiveError(err)
	closed := p.closeTxnLocked(id, tx)
	completions.add(p.finishTransactionWithOutcomeDeferred(closed, false))
	if !inactive && conn != nil {
		// The physical connection is not trustworthy after an unexpected
		// rollback result. Retire the exact owner even when the generic session
		// mapping was replaced while this transaction was active.
		err = errors.Join(err, p.closeExactOwnerLockedWithBatch(id, conn, &completions))
	}
	return conn, inactive, err
}

// RollbackTxnWithCleanup performs rollback, unbinds the transaction, and runs
// cleanup before releasing the session lock. A replacement transaction for the
// same logical session therefore cannot slip between rollback and maintenance.
// The callback is invoked only for the still-current transaction and only when
// the driver proves that it is inactive.
func (p *ConnectionPool) RollbackTxnWithCleanup(
	id uint32,
	expected *stdsql.Tx,
	cleanup func(*stdsql.Conn) error,
) (bool, error) {
	var completions transactionCompletionBatch
	defer completions.run()
	p.lifecycleMu.ReleaseLock()
	defer p.lifecycleMu.ReleaseUnlock()
	unlock := p.lockSession(id)
	defer unlock()

	tx, conn, matched, bindingErr := p.txnBindingForFinalizeLocked(id, expected)
	if bindingErr != nil {
		return false, bindingErr
	}
	if !matched {
		return false, nil
	}
	err := tx.Rollback()
	inactive := transactionInactiveError(err)
	closed := p.closeTxnLocked(id, tx)
	completions.add(p.finishTransactionWithOutcomeDeferred(closed, false))
	if !inactive && conn != nil {
		// The physical connection is not trustworthy after an unexpected
		// rollback result. Retire the exact owner even when the generic session
		// mapping was replaced while this transaction was active.
		err = errors.Join(err, p.closeExactOwnerLockedWithBatch(id, conn, &completions))
		return inactive, err
	}
	if inactive && conn != nil && cleanup != nil {
		err = errors.Join(err, cleanup(conn))
	}
	return inactive, err
}

func (p *ConnectionPool) txnBindingForFinalizeLocked(id uint32, expected *stdsql.Tx) (*stdsql.Tx, *stdsql.Conn, bool, error) {
	tx, present, err := p.transactionEntryLocked(id)
	if !present {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, err
	}
	if expected != nil && tx != expected {
		return nil, nil, false, nil
	}
	conn, present, ownerErr := p.transactionOwnerEntryLocked(id)
	if ownerErr != nil {
		return nil, nil, false, ownerErr
	}
	if !present {
		return nil, nil, false, fmt.Errorf("active transaction has no physical owner")
	}
	return tx, conn, true, nil
}

func (p *ConnectionPool) CloseConnIf(id uint32, conn *stdsql.Conn) error {
	if conn == nil {
		return nil
	}
	var completions transactionCompletionBatch
	defer completions.run()
	p.lifecycleMu.ReleaseLock()
	defer p.lifecycleMu.ReleaseUnlock()
	unlock := p.lockSession(id)
	defer unlock()
	current, present, err := p.connectionEntryLocked(id)
	if err != nil {
		return err
	}
	if !present || current != conn {
		return nil
	}
	return p.closeConnLockedWithBatch(id, conn, &completions)
}

// CloseConnIfBinding closes a connection only while both the generic session
// mapping and the transaction binding still match the caller's snapshot. The
// session lock keeps a replacement transaction from being admitted between the
// identity check and closeConnLocked, which may itself roll back an active tx.
func (p *ConnectionPool) CloseConnIfBinding(id uint32, conn *stdsql.Conn, expectedTx *stdsql.Tx) error {
	if conn == nil {
		return nil
	}
	var completions transactionCompletionBatch
	defer completions.run()
	p.lifecycleMu.ReleaseLock()
	defer p.lifecycleMu.ReleaseUnlock()
	unlock := p.lockSession(id)
	defer unlock()
	if expectedTx != nil {
		// For a transaction-bound close, the transaction-owner map is the
		// authoritative identity. Do not require the generic p.conns entry to
		// still point at conn: a reconnect may have replaced that entry while the
		// old transaction remains active, and the old owner can still be retired
		// safely in isolation.
		currentTx, active, txErr := p.transactionEntryLocked(id)
		if txErr != nil {
			return txErr
		}
		if !active || currentTx != expectedTx {
			return nil
		}
		owner, ownerOK, ownerErr := p.transactionOwnerEntryLocked(id)
		if ownerErr != nil {
			return ownerErr
		}
		if !ownerOK {
			return fmt.Errorf("active transaction has no physical owner")
		}
		if owner != conn {
			return nil
		}
		return p.closeExactBindingLockedWithBatch(id, conn, expectedTx, &completions)
	}

	current, present, err := p.connectionEntryLocked(id)
	if err != nil {
		return err
	}
	if !present || current != conn {
		return nil
	}
	_, active, txErr := p.transactionEntryLocked(id)
	if txErr != nil {
		return txErr
	}
	if active {
		return nil
	}
	if _, ownerMapped := p.txnConns.Load(id); ownerMapped {
		return nil
	}
	return p.closeConnLockedWithBatch(id, conn, &completions)
}

func transactionInactiveError(err error) bool {
	if err == nil {
		return true
	}
	return errors.Is(err, stdsql.ErrTxDone) ||
		strings.Contains(strings.ToLower(err.Error()), "no transaction is active")
}

func (p *ConnectionPool) getConnForSchemaLocked(ctx context.Context, id uint32, schemaName string) (*stdsql.Conn, error) {
	conn, err := p.getConnLocked(ctx, id)
	if err != nil {
		return nil, err
	}
	// GetTxn performs schema selection before BeginTx. Never issue session-state
	// SQL through *sql.Conn while an active *sql.Tx owns this connection.
	if _, active, txErr := p.transactionEntryLocked(id); active {
		if txErr != nil {
			return nil, txErr
		}
		return conn, nil
	}
	if schemaName == "" {
		return conn, nil
	}
	schemaCtx := context.WithoutCancel(ctx)
	var currentSchema string
	if err := conn.QueryRowContext(schemaCtx, "SELECT CURRENT_SCHEMA").Scan(&currentSchema); err != nil {
		logrus.WithError(err).Error("Failed to get current schema")
		return nil, err
	}
	if currentSchema == schemaName {
		return conn, nil
	}
	if _, err := conn.ExecContext(schemaCtx, "USE "+FullSchemaName(p.currentCatalogLocked(id), schemaName)); err != nil {
		if IsDuckDBSetSchemaNotFoundError(err) {
			return nil, sql.ErrDatabaseNotFound.New(schemaName)
		}
		logrus.WithField("schema", schemaName).WithError(err).Error("Failed to switch schema")
		return nil, err
	}
	return conn, nil
}

// closeExternalSessionsLocked invalidates every handle in the retiring pool
// generation. The lifecycle writer guarantees all admitted execution leases
// have drained; opMu additionally serializes teardown with hook registration.
// Cleanup and driver failures are accumulated so one broken handle cannot keep
// later sessions from being rolled back and closed.
func (p *ConnectionPool) closeExternalSessionsLocked(completions *transactionCompletionBatch) error {
	var sessions []*ExternalSession
	p.externalSessions.Range(func(key, _ any) bool {
		sessions = append(sessions, key.(*ExternalSession))
		return true
	})

	var lastErr error
	for _, session := range sessions {
		session.opMu.Lock()
		teardown, owner := session.beginTeardownLocked(true)
		if !owner {
			lastErr = errors.Join(lastErr, session.waitForTeardownLocked())
			session.opMu.Unlock()
			continue
		}
		teardownErr := session.finishTeardownLocked(teardown, nil, completions)
		session.opMu.Unlock()
		lastErr = errors.Join(lastErr, teardownErr)
	}
	return lastErr
}

func (p *ConnectionPool) Close() error {
	var completions transactionCompletionBatch
	defer p.clearCompletedTransactionCompletions()
	defer completions.run()
	preCloseErr := p.closeDatabaseForShutdown()
	p.lifecycleMu.LockAfterShutdown()
	defer p.lifecycleMu.Unlock()
	err := errors.Join(
		preCloseErr,
		p.closeExternalSessionsLocked(&completions),
		p.closeLockedWithBatch(&completions),
		p.externalSessionPanicError(),
	)
	return err
}

// closePhysicalConnLocked retires exactly the supplied database/sql
// connection. The caller must hold the pool lifecycle/session locks and must
// have already dealt with any transaction that owns the connection. Marking
// the driver handle bad prevents database/sql from returning a connection with
// unknown state to the shared pool.
func (p *ConnectionPool) closePhysicalConnLocked(conn *stdsql.Conn) error {
	if conn == nil {
		return nil
	}
	var lastErr error
	if err := conn.Raw(func(any) error { return driver.ErrBadConn }); err != nil &&
		!errors.Is(err, driver.ErrBadConn) && !errors.Is(err, stdsql.ErrConnDone) {
		logrus.WithError(err).Warn("Failed to close connection during Raw function call")
		lastErr = errors.Join(lastErr, err)
	}
	if err := conn.Close(); err != nil && !errors.Is(err, stdsql.ErrConnDone) {
		logrus.WithError(err).Warn("Failed to close connection")
		lastErr = errors.Join(lastErr, err)
	}
	return lastErr
}

// closeExactOwnerLocked retires a captured transaction owner even when the
// generic session connection mapping has since been replaced. The replacement
// mapping is deliberately left intact; only the exact owner is bad/closed.
// This helper is used after a failed commit/rollback, when the transaction
// mapping has already been removed and the old owner must not leak.
func (p *ConnectionPool) closeExactOwnerLockedWithBatch(
	id uint32,
	owner *stdsql.Conn,
	completions *transactionCompletionBatch,
) error {
	if owner == nil {
		return nil
	}
	current, present, mappingErr := p.connectionEntryLocked(id)
	if mappingErr != nil {
		// A malformed generic entry cannot identify a replacement safely. Retire
		// only the captured owner, preserve the malformed entry for diagnostics,
		// and report both lifecycle failures to the caller.
		return errors.Join(mappingErr, p.closePhysicalConnLocked(owner))
	}
	if present && current == owner {
		return p.closeConnLockedWithBatch(id, owner, completions)
	}

	// The transaction map was removed by the caller, but CompareAndDelete also
	// clears a stale owner marker without touching a newer owner installed by a
	// recovery path.
	p.txnConns.CompareAndDelete(id, owner)
	if !present {
		p.closedConns.Store(id, struct{}{})
	}
	return p.closePhysicalConnLocked(owner)
}

// closeExactBindingLocked rolls back and retires an exact transaction/owner
// pair. It intentionally does not require p.conns[id] to point at owner: a
// reconnect may have installed a replacement generic connection while the
// original transaction is still active. In that case only owner is closed and
// the replacement mapping remains available to its owner.
func (p *ConnectionPool) closeExactBindingLockedWithBatch(
	id uint32,
	owner *stdsql.Conn,
	expectedTx *stdsql.Tx,
	completions *transactionCompletionBatch,
) error {
	if owner == nil || expectedTx == nil {
		return nil
	}
	tx, active, txErr := p.transactionEntryLocked(id)
	if txErr != nil {
		return txErr
	}
	if !active || tx != expectedTx {
		return nil
	}
	boundOwner, ownerPresent, ownerErr := p.transactionOwnerEntryLocked(id)
	if ownerErr != nil {
		return ownerErr
	}
	if !ownerPresent {
		return fmt.Errorf("active transaction has no physical owner")
	}
	if boundOwner != owner {
		return nil
	}

	rollbackErr := tx.Rollback()
	closed := p.closeTxnLocked(id, tx)
	completions.add(p.finishTransactionWithOutcomeDeferred(closed, false))
	closeErr := p.closePhysicalConnLocked(owner)
	p.txnConns.CompareAndDelete(id, owner)

	// Remove the generic map only when it still points at this exact owner. A
	// replacement connection must remain untouched; if no generic mapping
	// remains, remember the completed lifecycle so CurrentCatalog does not
	// incorrectly fall back to the default catalog.
	current, present, mappingErr := p.connectionEntryLocked(id)
	if mappingErr != nil {
		return errors.Join(rollbackErr, closeErr, mappingErr)
	}
	if present && current == owner {
		p.conns.Delete(id)
		p.closedConns.Store(id, struct{}{})
	} else if !present {
		p.closedConns.Store(id, struct{}{})
	}
	return errors.Join(rollbackErr, closeErr)
}

// closeDetachedSessionStateLocked retires transaction state for an
// unconditional session teardown when the generic p.conns entry is already
// absent (or malformed). The transaction-owner map is authoritative for the
// physical handle; dropping the logical maps without closing that handle
// leaks a checked-out database/sql connection and can leave an active DuckDB
// transaction behind.
//
// The caller must hold the lifecycle and session locks. Identity-sensitive
// callers do not use this helper because they cannot prove ownership of a
// detached mapping.
func (p *ConnectionPool) closeDetachedSessionStateLockedWithBatch(
	id uint32,
	completions *transactionCompletionBatch,
) error {
	var lastErr error
	// A malformed transaction marker may still hide a live *sql.Tx. Its
	// transaction-held connection close lock makes Raw/Close potentially block
	// forever, so an unconditional teardown must report the corruption and leave
	// the physical owner untouched for an operator-driven recovery path.
	if _, present, txErr := p.transactionEntryLocked(id); present && txErr != nil {
		return txErr
	}

	// Capture the owner before closeTxnLocked removes the owner mapping. Even a
	// malformed transaction marker may have a valid owner that still needs to be
	// retired during an unconditional teardown.
	owner, ownerPresent, ownerErr := p.transactionOwnerEntryLocked(id)
	if ownerErr != nil {
		lastErr = errors.Join(lastErr, ownerErr)
	}

	tx, txPresent, txErr := p.transactionEntryLocked(id)
	if txErr != nil {
		lastErr = errors.Join(lastErr, txErr)
	} else if txPresent {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !transactionInactiveError(rollbackErr) {
			logrus.WithError(rollbackErr).Warn("Failed to rollback transaction")
			lastErr = errors.Join(lastErr, rollbackErr)
		}
	}

	// An unconditional close is allowed to discard a poisoned transaction
	// marker. closeTxnLocked also removes txnConns for a valid transaction.
	closed := p.closeTxnLocked(id, nil)
	completions.add(p.finishTransactionWithOutcomeDeferred(closed, false))

	if ownerPresent && ownerErr == nil && owner != nil {
		lastErr = errors.Join(lastErr, p.closePhysicalConnLocked(owner))
		p.txnConns.CompareAndDelete(id, owner)
	} else if ownerPresent {
		// Do not leave an invalid owner marker to block a future lifecycle. There
		// is no type-safe physical handle to close in this branch, so preserve the
		// validation error while discarding only the map entry.
		p.txnConns.Delete(id)
	}

	return lastErr
}

func (p *ConnectionPool) closeConnLockedWithBatch(
	id uint32,
	expected *stdsql.Conn,
	completions *transactionCompletionBatch,
) error {
	// Never retire a physical connection while the transaction marker for this
	// session is malformed. The marker may conceal a live *sql.Tx whose
	// database/sql close lock would make Raw/Close wait indefinitely.
	if _, present, txErr := p.transactionEntryLocked(id); present && txErr != nil {
		return txErr
	}
	conn, present, connErr := p.connectionEntryLocked(id)
	if !present {
		if expected == nil {
			lastErr := p.closeDetachedSessionStateLockedWithBatch(id, completions)
			p.closedConns.Store(id, struct{}{})
			return lastErr
		}
		return nil
	}
	if connErr != nil {
		if expected != nil {
			return connErr
		}
		// An unconditional close still has to retire any transaction owner even
		// when the generic entry itself is poisoned. Preserve the mapping error
		// for diagnostics, but do not strand the detached physical state.
		lastErr := errors.Join(connErr, p.closeDetachedSessionStateLockedWithBatch(id, completions))
		p.closedConns.Store(id, struct{}{})
		return lastErr
	}
	if expected != nil && conn != expected {
		return nil
	}
	var lastErr error
	if tx, txOK, txErr := p.transactionEntryLocked(id); txOK {
		if txErr != nil {
			// The connection is explicitly being retired, so remove the poisoned
			// transaction marker but never attempt a rollback through an unknown
			// value. The owner cannot be closed safely while an untyped live
			// database/sql transaction may still hold its close mutex; closeLocked's
			// error report preserves this lifecycle corruption for the caller.
			lastErr = txErr
			p.txns.Delete(id)
			p.txnConns.Delete(id)
		} else {
			owner, ownerPresent, ownerErr := p.transactionOwnerEntryLocked(id)
			if ownerErr != nil {
				if expected != nil {
					// An invalid owner is not evidence that the generic connection is
					// safe to retire. Leave the active transaction untouched and fail
					// closed so callers cannot accidentally close a replacement.
					return ownerErr
				}
				// CloseConn is unconditional. Roll back the transaction through its
				// own handle, discard the malformed owner marker, and continue to
				// retire the generic connection while returning the corruption error.
				lastErr = ownerErr
				if err := tx.Rollback(); err != nil && !transactionInactiveError(err) {
					lastErr = errors.Join(lastErr, err)
				}
				closed := p.closeTxnLocked(id, tx)
				completions.add(p.finishTransactionWithOutcomeDeferred(closed, false))
			} else if !ownerPresent {
				if expected != nil {
					return fmt.Errorf("active transaction has no physical owner")
				}
				// The transaction object is still usable for rollback even though
				// its owner identity was lost. Unconditional teardown must not leave
				// that transaction open behind the generic connection.
				if err := tx.Rollback(); err != nil && !transactionInactiveError(err) {
					lastErr = err
				}
				closed := p.closeTxnLocked(id, tx)
				completions.add(p.finishTransactionWithOutcomeDeferred(closed, false))
			} else if owner == conn || expected == nil {
				if err := tx.Rollback(); err != nil &&
					!errors.Is(err, stdsql.ErrTxDone) &&
					!strings.Contains(strings.ToLower(err.Error()), "no transaction is active") {
					logrus.WithError(err).Warn("Failed to rollback transaction")
					lastErr = err
				}
				closed := p.closeTxnLocked(id, tx)
				completions.add(p.finishTransactionWithOutcomeDeferred(closed, false))
				if owner != conn {
					// CloseConn(id) is unconditional and must retire both the active
					// transaction owner and a stale/replacement generic connection.
					lastErr = errors.Join(lastErr, p.closePhysicalConnLocked(owner))
				}
			}
			// When expected != nil and owner != conn, the active transaction is
			// deliberately left mapped. The generic connection below is still the
			// caller-supplied identity and can be retired independently.
		}
	} else {
		if expected == nil {
			// A stale owner marker without a transaction is also a checked-out
			// physical handle. Sweep it before closing the generic mapping, while
			// avoiding a second close when both entries point at the same object.
			owner, ownerPresent, ownerErr := p.transactionOwnerEntryLocked(id)
			if ownerErr != nil {
				lastErr = errors.Join(lastErr, ownerErr)
				p.txnConns.Delete(id)
			} else if ownerPresent {
				p.txnConns.Delete(id)
				if owner != nil && owner != conn {
					lastErr = errors.Join(lastErr, p.closePhysicalConnLocked(owner))
				}
			}
		}
		closed := p.closeTxnLocked(id, nil)
		completions.add(p.finishTransactionWithOutcomeDeferred(closed, false))
	}
	lastErr = errors.Join(lastErr, p.closePhysicalConnLocked(conn))
	p.conns.Delete(id)
	p.closedConns.Store(id, struct{}{})
	return lastErr
}

func (p *ConnectionPool) closeLockedWithBatch(completions *transactionCompletionBatch) error {
	type transactionEntry struct {
		id uint32
		tx *stdsql.Tx
	}

	var (
		txns              []transactionEntry
		conns             = make(map[*stdsql.Conn]struct{})
		protectedConns    = make(map[*stdsql.Conn]struct{})
		validConnByID     = make(map[uint32]*stdsql.Conn)
		validOwnerByID    = make(map[uint32]*stdsql.Conn)
		validTxnByID      = make(map[uint32]*stdsql.Tx)
		malformedTxnIDs   = make(map[uint32]struct{})
		malformedConnIDs  = make(map[uint32]struct{})
		malformedOwnerIDs = make(map[uint32]struct{})
		lastErr           error
	)

	// Inspect every transaction entry first. A malformed value cannot be
	// interpreted as a live *sql.Tx, but it must not prevent unrelated valid
	// sessions from being rolled back and closed. Keep its ID marked so any
	// physical owner associated with that session remains protected below.
	p.txns.Range(func(key, value any) bool {
		id, keyOK := key.(uint32)
		if !keyOK {
			lastErr = errors.Join(lastErr, fmt.Errorf("active transaction mapping key is invalid"))
			return true
		}
		tx, ok := value.(*stdsql.Tx)
		if !ok || tx == nil {
			lastErr = errors.Join(lastErr, fmt.Errorf("active transaction mapping is invalid for session %d", id))
			malformedTxnIDs[id] = struct{}{}
			return true
		}
		validTxnByID[id] = tx
		txns = append(txns, transactionEntry{id: id, tx: tx})
		return true
	})

	// Snapshot valid transaction owners. Owners attached to malformed transaction
	// IDs are deliberately protected: closing a database/sql connection while an
	// untyped transaction may still hold its internal close lock can hang the
	// entire teardown. Invalid owner entries remain available for diagnostics.
	p.txnConns.Range(func(key, value any) bool {
		id, keyOK := key.(uint32)
		if !keyOK {
			lastErr = errors.Join(lastErr, fmt.Errorf("transaction owner mapping key is invalid"))
			return true
		}
		conn, ok := value.(*stdsql.Conn)
		if !ok || conn == nil {
			lastErr = errors.Join(lastErr, fmt.Errorf("active transaction owner mapping is invalid for session %d", id))
			malformedOwnerIDs[id] = struct{}{}
			return true
		}
		validOwnerByID[id] = conn
		if _, malformed := malformedTxnIDs[id]; malformed {
			protectedConns[conn] = struct{}{}
		} else {
			conns[conn] = struct{}{}
		}
		return true
	})

	// Generic connections are also captured before any close. Preserve entries
	// whose session has a malformed transaction/owner mapping; all other valid
	// connections can be retired independently of transaction processing.
	p.conns.Range(func(key, value any) bool {
		id, keyOK := key.(uint32)
		if !keyOK {
			lastErr = errors.Join(lastErr, fmt.Errorf("connection mapping key is invalid"))
			return true
		}
		conn, ok := value.(*stdsql.Conn)
		if !ok || conn == nil {
			lastErr = errors.Join(lastErr, fmt.Errorf("connection mapping is invalid for session %d", id))
			malformedConnIDs[id] = struct{}{}
			return true
		}
		validConnByID[id] = conn
		if _, malformed := malformedTxnIDs[id]; malformed {
			protectedConns[conn] = struct{}{}
		} else if _, malformed := malformedOwnerIDs[id]; malformed {
			protectedConns[conn] = struct{}{}
		} else {
			conns[conn] = struct{}{}
		}
		return true
	})

	// Roll back each distinct transaction once, then remove its identity
	// binding. The owner was captured above, so it remains in the close set even
	// though closeTxnLocked deletes txnConns.
	seenTxns := make(map[*stdsql.Tx]struct{}, len(txns))
	for _, entry := range txns {
		if _, seen := seenTxns[entry.tx]; seen {
			continue
		}
		seenTxns[entry.tx] = struct{}{}
		if err := entry.tx.Rollback(); err != nil && !transactionInactiveError(err) {
			logrus.WithError(err).Warn("Failed to rollback transaction")
			lastErr = errors.Join(lastErr, err)
		}
		closed := p.closeTxnLocked(entry.id, entry.tx)
		completions.add(p.finishTransactionWithOutcomeDeferred(closed, false))
	}

	// closePhysicalConnLocked is identity-safe and marks each handle bad before
	// returning it to database/sql. The set deduplicates a normal owner/generic
	// pair and also covers owners whose generic map entry was already removed.
	for conn := range conns {
		if _, protected := protectedConns[conn]; protected {
			continue
		}
		lastErr = errors.Join(lastErr, p.closePhysicalConnLocked(conn))
	}

	// Remove only entries that were validated and processed. Malformed entries
	// stay in place so an operator/recovery path can identify and repair them;
	// using Clear here would erase the evidence and could strand a live owner.
	for id, tx := range validTxnByID {
		p.txns.CompareAndDelete(id, tx)
	}
	for id, conn := range validOwnerByID {
		if _, malformed := malformedTxnIDs[id]; malformed {
			continue
		}
		p.txnConns.CompareAndDelete(id, conn)
	}
	for id, conn := range validConnByID {
		if _, malformed := malformedTxnIDs[id]; malformed {
			continue
		}
		if _, malformed := malformedOwnerIDs[id]; malformed {
			continue
		}
		p.conns.CompareAndDelete(id, conn)
	}
	// A connection-less session lock has no lifecycle state after a successful
	// sweep. Keep locks for malformed IDs so a later repair can synchronize with
	// the preserved mapping; clearing unrelated locks avoids retaining a whole
	// generation after shutdown.
	p.sessionLocks.Range(func(key, value any) bool {
		id, ok := key.(uint32)
		if !ok {
			return true
		}
		if _, bad := malformedTxnIDs[id]; bad {
			return true
		}
		if _, bad := malformedConnIDs[id]; bad {
			return true
		}
		if _, bad := malformedOwnerIDs[id]; bad {
			return true
		}
		p.sessionLocks.CompareAndDelete(id, value)
		return true
	})
	if p.DB == nil {
		return lastErr
	}
	return errors.Join(lastErr, p.DB.Close())
}

func (p *ConnectionPool) Reset(connector *duckdb.Connector, db *stdsql.DB) error {
	var completions transactionCompletionBatch
	defer p.clearCompletedTransactionCompletions()
	defer completions.run()
	preCloseErr := p.closeDatabaseForShutdown()
	p.lifecycleMu.LockAfterShutdown()
	defer p.lifecycleMu.Unlock()
	err := errors.Join(
		preCloseErr,
		p.closeExternalSessionsLocked(&completions),
		p.closeLockedWithBatch(&completions),
		p.externalSessionPanicError(),
	)
	if err != nil {
		return fmt.Errorf("failed to close connection pool: %w", err)
	}
	p.transactionCompletionMu.Lock()
	p.activeTransactions = make(map[*stdsql.Tx]struct{})
	p.transactionCompletionMu.Unlock()

	p.conns.Clear()
	p.txns.Clear()
	p.txnConns.Clear()
	p.closedConns.Clear()
	p.sessionLocks.Clear()
	p.externalGeneration++
	p.DB = db
	p.dbPtr.Store(db)
	p.connector = connector
	p.registerMySQLUDFsOnce = sync.Once{}
	p.registerMySQLUDFsErr = nil

	return nil
}
