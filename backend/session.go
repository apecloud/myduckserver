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
package backend

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	adapter "github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/vitess/go/mysql"
)

type Session struct {
	*memory.Session
	db               *catalog.DatabaseProvider
	queryRowLimit    uint64
	mysqlImplicitDDL atomic.Bool
	// physicalBinding is the last connection/transaction identity acquired by
	// this Session object.  SessionManager can invoke SessionEnd on an old
	// session after a connection ID has been reused; retaining the identity lets
	// lifecycle cleanup fail closed instead of rolling back the replacement.
	bindingMu   sync.Mutex
	boundConn   *stdsql.Conn
	boundTx     *stdsql.Tx
	bindingSeen bool
	// sessionEndMu serializes lifecycle callbacks from SessionManager. GMS may
	// call SessionEnd while replacing a connection session and can call it again
	// during disconnect cleanup; only one callback may consume the current
	// transaction/implicit marker at a time.
	sessionEndMu sync.Mutex
	implicitMu   sync.Mutex
	implicit     *implicitTransactionCleanup
	// postPhysical retains callbacks for protocol paths (notably PostgreSQL)
	// that own a database/sql transaction without creating a GMS Transaction.
	// Entries stay addressable after completion so a registration racing the
	// finalizer can be invoked immediately with the already-known outcome.
	postPhysicalMu    sync.Mutex
	postPhysical      map[*stdsql.Tx]*postTransactionCleanup
	postPhysicalOrder []*stdsql.Tx
	// procedureScopes holds markers detached by GMS's stored-procedure
	// transaction swap. The newest procedure scope is always at the end; this
	// lets nested CALLs restore their exact outer transaction without allowing
	// an inner marker to overwrite it.
	procedureScopes []*procedureTransactionScope
	cleanupErr      error
}

// maxPostPhysicalTombstones bounds the raw-transaction callback registry. A
// short recent window preserves late registration while preventing a long-lived
// PostgreSQL session from retaining every completed *sql.Tx forever.
const maxPostPhysicalTombstones = 1024

// postPhysicalTombstoneTTL retains a completed raw transaction long enough for
// a late protocol finalizer to observe its outcome, without keeping every
// database/sql transaction alive for the lifetime of a session.
const postPhysicalTombstoneTTL = 5 * time.Minute

// ImplicitTransactionCleanupRegistrar is implemented by sessions that can
// retain a callback for the GMS autocommit transaction. GMS clears an
// implicitly-created transaction with SetTransaction(nil), which has no error
// return and no context argument; the callback gives the session a chance to
// close the statement iterator and release its operation/connection leases
// before it performs the compensating rollback.
type ImplicitTransactionCleanupRegistrar interface {
	RegisterImplicitTransactionCleanup(*sql.Context, sql.Transaction, func() error) func(success bool)
}

// PostTransactionCleanupRegistrar is implemented by sessions that can retain
// a callback until a logical GMS transaction has physically and logically
// finished. The callback receives true only for a successful commit; rollback
// and every failed finalization receive false.
type PostTransactionCleanupRegistrar interface {
	RegisterPostTransactionCleanup(*sql.Context, sql.Transaction, func(success bool)) bool
}

// PhysicalPostTransactionCleanupRegistrar is the protocol-facing counterpart
// for a raw database/sql transaction which is finalized outside GMS's
// TransactionSession callbacks.
type PhysicalPostTransactionCleanupRegistrar interface {
	RegisterPostPhysicalTransactionCleanup(*sql.Context, *stdsql.Tx, func(success bool)) bool
}

// postTransactionCleanup is a small one-shot callback registry. Registration
// and completion synchronize on the same mutex, so a callback can never be
// stranded between a finalizer's callback snapshot and its completion bit.
type postTransactionCleanup struct {
	mu                   sync.Mutex
	callbacks            []func(bool)
	completed            bool
	success              bool
	tombstone            bool
	completedAt          time.Time
	poolBridgeRegistered bool
}

func completedPostTransactionCleanup(success bool) *postTransactionCleanup {
	return &postTransactionCleanup{completed: true, success: success, completedAt: time.Now()}
}

func (c *postTransactionCleanup) register(callback func(bool)) bool {
	if c == nil || callback == nil {
		return false
	}
	c.mu.Lock()
	if !c.completed {
		c.callbacks = append(c.callbacks, callback)
		c.mu.Unlock()
		return true
	}
	success := c.success
	c.mu.Unlock()
	// Completion already happened. Invoke outside the lock so callbacks may
	// safely register/release other lifecycle state.
	callback(success)
	return true
}

func (c *postTransactionCleanup) complete(success bool) {
	panicValue, panicked := c.completeCollectPanic(success)
	if panicked {
		panic(panicValue)
	}
}

// completeCollectPanic drains every callback even when one of them panics. A
// physical pool completion reaches Session through one bridge callback, so the
// pool cannot see or recover the remaining session callbacks individually.
// Preserve the first panic for the caller after all local resources are
// released.
func (c *postTransactionCleanup) completeCollectPanic(success bool) (panicValue any, panicked bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	if c.completed {
		c.mu.Unlock()
		return nil, false
	}
	c.completed = true
	c.success = success
	c.completedAt = time.Now()
	callbacks := append([]func(bool){}, c.callbacks...)
	c.callbacks = nil
	c.mu.Unlock()
	for _, callback := range callbacks {
		if callback == nil {
			continue
		}
		value, didPanic := callPostTransactionCleanupCallback(func() {
			callback(success)
		})
		if didPanic && !panicked {
			panicValue, panicked = value, true
		}
	}
	return panicValue, panicked
}

func callPostTransactionCleanupCallback(callback func()) (panicValue any, panicked bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			panicValue, panicked = recovered, true
		}
	}()
	callback()
	return nil, false
}

func (c *postTransactionCleanup) markTombstone() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.completed || c.tombstone {
		return false
	}
	if c.completedAt.IsZero() {
		c.completedAt = time.Now()
	}
	c.tombstone = true
	return true
}

func (c *postTransactionCleanup) isCompleted() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	completed := c.completed
	c.mu.Unlock()
	return completed
}

func (c *postTransactionCleanup) completionSnapshot() (bool, time.Time) {
	if c == nil {
		return false, time.Time{}
	}
	c.mu.Lock()
	completed, completedAt := c.tombstone, c.completedAt
	c.mu.Unlock()
	return completed, completedAt
}

func (sess *Session) recordPostPhysicalCompletion(tx *stdsql.Tx, state *postTransactionCleanup) {
	if sess == nil || tx == nil || state == nil {
		return
	}
	sess.postPhysicalMu.Lock()
	defer sess.postPhysicalMu.Unlock()
	if sess.postPhysical == nil || sess.postPhysical[tx] != state {
		return
	}
	// Keep the registry's lock order consistent (session map, then state). A
	// concurrent registration may inspect the state while holding the map lock;
	// taking state first here would create an avoidable cycle.
	if !state.markTombstone() {
		return
	}
	sess.postPhysicalOrder = append(sess.postPhysicalOrder, tx)
	sess.prunePostPhysicalCompletionsLocked(time.Now())
}

// prunePostPhysicalCompletionsLocked removes only completed raw transaction
// tombstones that have aged out or exceeded the FIFO bound. Pending callback
// states remain in the map so a provider teardown cannot silently strand a
// lease. The caller holds postPhysicalMu.
func (sess *Session) prunePostPhysicalCompletionsLocked(now time.Time) {
	if sess == nil {
		return
	}
	if len(sess.postPhysical) == 0 {
		sess.postPhysicalOrder = nil
		return
	}
	for tx, state := range sess.postPhysical {
		completed, completedAt := state.completionSnapshot()
		if completed && !completedAt.IsZero() && now.Sub(completedAt) >= postPhysicalTombstoneTTL {
			delete(sess.postPhysical, tx)
		}
	}
	order := make([]*stdsql.Tx, 0, len(sess.postPhysicalOrder))
	seen := make(map[*stdsql.Tx]struct{}, len(sess.postPhysicalOrder))
	for _, tx := range sess.postPhysicalOrder {
		if _, duplicate := seen[tx]; duplicate {
			continue
		}
		state, present := sess.postPhysical[tx]
		if !present {
			continue
		}
		completed, _ := state.completionSnapshot()
		if !completed {
			// Never remove a pending state merely because it appeared in a stale
			// order slice.
			continue
		}
		seen[tx] = struct{}{}
		order = append(order, tx)
	}
	for tx, state := range sess.postPhysical {
		completed, completedAt := state.completionSnapshot()
		if !completed {
			continue
		}
		if !completedAt.IsZero() && now.Sub(completedAt) >= postPhysicalTombstoneTTL {
			delete(sess.postPhysical, tx)
			continue
		}
		if _, present := seen[tx]; !present {
			seen[tx] = struct{}{}
			order = append(order, tx)
		}
	}
	for len(order) > maxPostPhysicalTombstones {
		oldest := order[0]
		order = order[1:]
		if state := sess.postPhysical[oldest]; state != nil {
			completed, _ := state.completionSnapshot()
			if completed {
				delete(sess.postPhysical, oldest)
			}
		}
	}
	sess.postPhysicalOrder = order
}

type implicitTransactionCleanup struct {
	tx  *Transaction
	ctx *sql.Context

	mu             sync.Mutex
	callbacks      []func() error
	cleanupStarted bool
	cleanupOnce    sync.Once
	cleanupErr     error
	suspended      bool
	procedureOwned bool
}

type procedureTransactionScope struct {
	suspended *implicitTransactionCleanup
}

func (c *implicitTransactionCleanup) add(callback func() error) bool {
	if c == nil || callback == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cleanupStarted {
		return false
	}
	c.callbacks = append(c.callbacks, callback)
	return true
}

func (c *implicitTransactionCleanup) run() error {
	if c == nil {
		return nil
	}
	c.cleanupOnce.Do(func() {
		c.mu.Lock()
		c.cleanupStarted = true
		callbacks := append([]func() error(nil), c.callbacks...)
		c.mu.Unlock()

		var cleanupErr error
		for _, callback := range callbacks {
			if callback != nil {
				cleanupErr = errors.Join(cleanupErr, callback())
			}
		}
		c.mu.Lock()
		c.cleanupErr = cleanupErr
		c.mu.Unlock()
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cleanupErr
}

type SessionOption func(*Session)

// WithQueryRowLimit limits the number of rows returned by queries in a session.
// A limit of zero leaves query results unlimited.
func WithQueryRowLimit(limit uint64) SessionOption {
	return func(sess *Session) {
		sess.queryRowLimit = limit
	}
}

func NewSession(base *memory.Session, provider *catalog.DatabaseProvider, opts ...SessionOption) *Session {
	sess := &Session{Session: base, db: provider}
	for _, opt := range opts {
		opt(sess)
	}
	return sess
}

// Provider returns the database provider for the session.
func (sess *Session) Provider() *catalog.DatabaseProvider {
	return sess.db
}

func (sess *Session) rememberBinding(conn *stdsql.Conn, tx *stdsql.Tx) {
	if sess == nil || (conn == nil && tx == nil) {
		return
	}
	sess.bindingMu.Lock()
	sess.boundConn = conn
	sess.boundTx = tx
	sess.bindingSeen = true
	sess.bindingMu.Unlock()
}

func (sess *Session) trackedBinding() (*stdsql.Conn, *stdsql.Tx, bool) {
	if sess == nil {
		return nil, nil, false
	}
	sess.bindingMu.Lock()
	conn, tx, seen := sess.boundConn, sess.boundTx, sess.bindingSeen
	sess.bindingMu.Unlock()
	return conn, tx, seen
}

func (sess *Session) clearTrackedBinding(conn *stdsql.Conn, tx *stdsql.Tx) {
	if sess == nil {
		return
	}
	sess.bindingMu.Lock()
	// A nil value is an actual part of the binding, not a wildcard. Callers
	// that only want to clear a transaction use clearTrackedTransaction below.
	// Treating nil as a wildcard let an idle snapshot erase a newer transaction
	// tracked on the same physical connection.
	if sess.bindingSeen && sess.boundConn == conn && sess.boundTx == tx {
		sess.boundConn = nil
		sess.boundTx = nil
		sess.bindingSeen = false
	}
	sess.bindingMu.Unlock()
}

// clearTrackedTransaction retires only the transaction half of a remembered
// binding. The physical connection remains tracked until its own identity-safe
// close; this prevents a later stale callback from falling back to an ID-only
// pool lookup.
func (sess *Session) clearTrackedTransaction(tx *stdsql.Tx) {
	if sess == nil || tx == nil {
		return
	}
	sess.bindingMu.Lock()
	if sess.bindingSeen && sess.boundTx == tx {
		sess.boundTx = nil
		if sess.boundConn == nil {
			sess.bindingSeen = false
		}
	}
	sess.bindingMu.Unlock()
}

// clearTrackedTransactionIfChanged retires a remembered transaction only after
// the pool proves that the exact transaction is no longer active. In
// particular, a malformed owner mapping must keep the identity available for a
// later recovery/teardown path.
func (sess *Session) clearTrackedTransactionIfChanged(tx *stdsql.Tx) {
	if sess == nil || tx == nil || sess.db == nil || sess.db.Pool() == nil {
		return
	}
	_, trackedTx, seen := sess.trackedBinding()
	if !seen || trackedTx != tx {
		return
	}
	_, current, active, err := sess.db.Pool().GetTxnBindingForReleaseStatus(sess.ID())
	if err != nil || (active && current == tx) {
		return
	}
	sess.clearTrackedTransaction(tx)
}

// clearTrackedBindingIfChanged drops an old remembered pair only after the
// pool confirms that the pair is no longer current. A malformed active mapping
// is deliberately retained so a later recovery path still has its identity.
func (sess *Session) clearTrackedBindingIfChanged(conn *stdsql.Conn, tx *stdsql.Tx) {
	if sess == nil || sess.db == nil || sess.db.Pool() == nil {
		return
	}
	trackedConn, trackedTx, seen := sess.trackedBinding()
	if !seen || trackedConn != conn || trackedTx != tx {
		return
	}
	currentConn, current, _, err := sess.db.Pool().GetTxnBindingForReleaseStatus(sess.ID())
	if err != nil {
		return
	}
	if currentConn == conn && current == tx {
		return
	}
	sess.clearTrackedBinding(conn, tx)
}

// RegisterImplicitTransactionCleanup attaches a resource-release callback to
// the currently tracked GMS implicit transaction. The returned handle is a
// success-path disarmer: false is intentionally a no-op (the callback carries
// terminal failure state), while true disarms the marker after a successful
// commit. Resource callbacks are run by CommitTransaction or the rollback
// path, never by the success handle itself.
//
// The callback is intentionally allowed to return an error, but the handle has
// no error result because it is normally called from iterator/finalizer paths
// that cannot return one. Such errors are retained by the session and exposed
// through TakeAutocommitCleanupError. SetTransaction(nil) still performs the
// rollback when a callback fails.
func (sess *Session) RegisterImplicitTransactionCleanup(
	ctx *sql.Context,
	tx sql.Transaction,
	callback func() error,
) func(success bool) {
	noOp := func(bool) {}
	if sess == nil || callback == nil {
		return noOp
	}
	transaction, ok := tx.(*Transaction)
	if !ok || transaction == nil {
		return noOp
	}

	// Check the marker and append the callback while holding implicitMu. The
	// previous split check/add sequence allowed SetTransaction(nil) or a newer
	// armImplicitState call to detach the marker between those operations,
	// leaving the callback on an unreachable state and leaking its leases.
	sess.implicitMu.Lock()
	state := sess.implicit
	if state == nil || state.tx != transaction || !state.add(callback) {
		sess.implicitMu.Unlock()
		// The marker may have been detached/finalized concurrently. The callback
		// still owns a resource, so run it immediately instead of silently
		// leaking that resource. Preserve callback failures for the protocol
		// layer, which cannot receive an error from this registration API.
		if callbackErr := callback(); callbackErr != nil {
			sess.recordAutocommitCleanupError(callbackErr)
		}
		return noOp
	}
	sess.implicitMu.Unlock()

	return func(success bool) {
		if !success {
			// Failure is reported by the registered callback. The wrapper has
			// already closed its child and released its leases before reaching
			// this handle, so there is no work to do on the false path.
			return
		}
		// The success handle is called only after Session.CommitTransaction has
		// completed. Do not run callbacks here: doing so before the physical
		// commit would make a later commit failure look like a clean statement.
		sess.removeImplicitState(state)
	}
}

// RegisterPostTransactionCleanup retains a callback until the supplied logical
// transaction has completed. Unlike the pre-finalization implicit hook, this
// callback is deliberately not run by iterator close: an operation lease must
// remain admitted while commit/rollback is still deciding the transaction's
// outcome. If registration races a completed transaction, the callback is
// invoked immediately with the recorded outcome.
func (sess *Session) RegisterPostTransactionCleanup(
	_ *sql.Context,
	tx sql.Transaction,
	callback func(success bool),
) bool {
	if sess == nil || callback == nil {
		return false
	}
	transaction, ok := tx.(*Transaction)
	if !ok || transaction == nil {
		return false
	}
	return transaction.registerPostCleanup(callback)
}

// RegisterPostPhysicalTransactionCleanup retains a callback for a raw
// database/sql transaction finalized directly by a protocol adapter. This is
// used by PostgreSQL's extended/COPY paths, whose transaction controls bypass
// GMS's logical TransactionSession methods.
func (sess *Session) RegisterPostPhysicalTransactionCleanup(
	_ *sql.Context,
	tx *stdsql.Tx,
	callback func(success bool),
) bool {
	if sess == nil || tx == nil || callback == nil {
		return false
	}
	sess.postPhysicalMu.Lock()
	sess.prunePostPhysicalCompletionsLocked(time.Now())
	if sess.postPhysical == nil {
		sess.postPhysical = make(map[*stdsql.Tx]*postTransactionCleanup)
	}
	state := sess.postPhysical[tx]
	statePresent := state != nil
	if state == nil {
		state = &postTransactionCleanup{}
		sess.postPhysical[tx] = state
	}
	state.mu.Lock()
	completed := state.completed
	bridgeNeeded := !state.poolBridgeRegistered && !completed
	if bridgeNeeded {
		state.poolBridgeRegistered = true
	}
	state.mu.Unlock()
	sess.postPhysicalMu.Unlock()
	pool := (*catalog.ConnectionPool)(nil)
	if sess.db != nil {
		pool = sess.db.Pool()
	}
	if bridgeNeeded && pool == nil {
		// Without a pool (or another physical finalizer) there is no trustworthy
		// proof that a newly-created raw transaction is still active. Fail closed
		// instead of installing a callback that could wait forever. Existing
		// completed tombstones take the path above and still support late delivery.
		if !statePresent {
			sess.postPhysicalMu.Lock()
			if sess.postPhysical[tx] == state {
				delete(sess.postPhysical, tx)
			}
			sess.postPhysicalMu.Unlock()
		}
		state.complete(false)
		return false
	}
	// The pool may finalize this raw transaction directly during provider
	// Close/Reset. Bridge that terminal event into the session registry as well
	// as the normal Session adapter finalizers; the local state remains the
	// single once-guard for user callbacks.
	if bridgeNeeded {
		if pool.RegisterTransactionCompletion(tx, func(success bool) {
			sess.completePhysicalPostCleanup(tx, success)
		}) {
			// The pool has an active binding or a retained recent tombstone.
			// Continue with local registration below.
		} else {
			// An evicted/inactive physical transaction cannot safely accept a new
			// pending callback. Resolve callbacks already retained for this state and
			// return false so COPY/operation callers take their fallback release path.
			sess.completePhysicalPostCleanup(tx, false)
			return false
		}
	}
	return state.register(callback)
}

func (transaction *Transaction) registerPostCleanup(callback func(bool)) bool {
	if transaction == nil || callback == nil {
		return false
	}
	transaction.postMu.Lock()
	if transaction.post == nil {
		transaction.post = &postTransactionCleanup{}
	}
	state := transaction.post
	transaction.postMu.Unlock()
	return state.register(callback)
}

func (transaction *Transaction) completePostCleanup(success bool) {
	if transaction == nil {
		return
	}
	transaction.postMu.Lock()
	state := transaction.post
	if state == nil {
		// Keep a completion tombstone so a callback registered after the
		// finalizer still observes the terminal outcome instead of being queued
		// on an unreachable transaction.
		state = completedPostTransactionCleanup(success)
		transaction.post = state
	}
	transaction.postMu.Unlock()
	state.complete(success)
}

func (sess *Session) completePhysicalPostCleanup(tx *stdsql.Tx, success bool) {
	if sess == nil || tx == nil {
		return
	}
	sess.postPhysicalMu.Lock()
	sess.prunePostPhysicalCompletionsLocked(time.Now())
	if sess.postPhysical == nil {
		sess.postPhysical = make(map[*stdsql.Tx]*postTransactionCleanup)
	}
	state := sess.postPhysical[tx]
	if state == nil {
		// Pool/connection teardown can complete a raw transaction before the
		// protocol callback has registered. Retain the outcome under the same
		// registry lock so late registration is immediately resolved.
		state = completedPostTransactionCleanup(success)
		sess.postPhysical[tx] = state
	}
	sess.postPhysicalMu.Unlock()
	panicValue, panicked := state.completeCollectPanic(success)
	sess.recordPostPhysicalCompletion(tx, state)
	if panicked {
		panic(panicValue)
	}
}

// completePhysicalPostCleanupIfUnbound resolves a callback after an
// identity-safe teardown call. A close operation may remove the transaction
// mapping and still return a physical close error; that is nevertheless a
// terminal lifecycle event for the callback. Conversely, retain the callback
// when the exact transaction is still mapped, because its driver state remains
// live/unknown and must not be released early.
func (sess *Session) completePhysicalPostCleanupIfUnbound(tx *stdsql.Tx) {
	if sess == nil || tx == nil || sess.db == nil {
		return
	}
	_, current, active, err := sess.db.Pool().GetTxnBindingForReleaseStatus(sess.ID())
	if err != nil || (active && current == tx) {
		// A malformed owner/transaction mapping is still active even though the
		// pool cannot return a typed pointer. Keep the callback armed until a real
		// teardown path resolves that exact physical state.
		return
	}
	sess.completePhysicalPostCleanup(tx, false)
}

// TakeAutocommitCleanupError returns and clears errors retained by an implicit
// rollback/lease cleanup path. SetTransaction cannot return an error, so GMS
// or a protocol adapter may consume this value after the original statement
// error has been recorded.
func (sess *Session) TakeAutocommitCleanupError() error {
	if sess == nil {
		return nil
	}
	sess.implicitMu.Lock()
	defer sess.implicitMu.Unlock()
	err := sess.cleanupErr
	sess.cleanupErr = nil
	return err
}

// AutocommitCleanupError observes the retained cleanup error without clearing
// it. It is useful to include the error in a surrounding protocol response
// before a later owner consumes it.
func (sess *Session) AutocommitCleanupError() error {
	if sess == nil {
		return nil
	}
	sess.implicitMu.Lock()
	defer sess.implicitMu.Unlock()
	return sess.cleanupErr
}

// ConsumeAutocommitCleanupError is an explicit alias for callers that prefer
// the verb used by other session error queues.
func (sess *Session) ConsumeAutocommitCleanupError() error {
	return sess.TakeAutocommitCleanupError()
}

func (sess *Session) recordAutocommitCleanupError(err error) {
	if sess == nil || err == nil {
		return
	}
	sess.implicitMu.Lock()
	sess.cleanupErr = errors.Join(sess.cleanupErr, err)
	sess.implicitMu.Unlock()
	if sess.Session != nil {
		sess.GetLogger().WithError(err).Error("implicit autocommit cleanup failed")
	}
}

// beginProcedureTransactionScope brackets the synchronous GMS buildCall
// transaction swap. GMS clears the session transaction before running a stored
// procedure and restores it in a defer; SetTransaction has no context
// argument, so the executor supplies this small, session-local scope marker.
// The returned release is idempotent because a builder may abandon a plan
// after constructing an iterator. A missing restore is treated as a real clear
// at scope end, so a failed procedure cannot strand a marker or its leases.
func (sess *Session) beginProcedureTransactionScope() func() {
	if sess == nil {
		return func() {}
	}
	scope := &procedureTransactionScope{}
	sess.implicitMu.Lock()
	sess.procedureScopes = append(sess.procedureScopes, scope)
	sess.implicitMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			sess.implicitMu.Lock()
			idx := -1
			for i := len(sess.procedureScopes) - 1; i >= 0; i-- {
				if sess.procedureScopes[i] == scope {
					idx = i
					break
				}
			}
			if idx < 0 {
				sess.implicitMu.Unlock()
				return
			}
			// Scopes are expected to unwind LIFO. If an unusual builder path
			// releases an outer scope first, leave inner scopes intact and only
			// remove this exact record.
			state := scope.suspended
			scope.suspended = nil
			copy(sess.procedureScopes[idx:], sess.procedureScopes[idx+1:])
			sess.procedureScopes[len(sess.procedureScopes)-1] = nil
			sess.procedureScopes = sess.procedureScopes[:len(sess.procedureScopes)-1]
			if state != nil {
				state.suspended = false
			}
			sess.implicitMu.Unlock()
			if state != nil {
				sess.resolveImplicitState(state)
			}
		})
	}
}

func (sess *Session) removeImplicitState(state *implicitTransactionCleanup) {
	if sess == nil || state == nil {
		return
	}
	sess.implicitMu.Lock()
	if sess.implicit == state {
		sess.implicit = nil
		state.suspended = false
	}
	for _, scope := range sess.procedureScopes {
		if scope == nil || scope.suspended != state {
			continue
		}
		scope.suspended = nil
		state.suspended = false
	}
	sess.implicitMu.Unlock()
}

func (sess *Session) armImplicitState(ctx *sql.Context, tx *Transaction) {
	if sess == nil || tx == nil || ctx == nil {
		return
	}
	state := &implicitTransactionCleanup{tx: tx, ctx: ctx}
	sess.implicitMu.Lock()
	old := sess.implicit
	state.procedureOwned = len(sess.procedureScopes) > 0
	sess.implicit = state
	sess.implicitMu.Unlock()
	if old != nil && old != state {
		// A new transaction should never inherit an old iterator lease. Do not
		// rollback the stale logical transaction here (it may be a temporary GMS
		// transaction swap), but release resources that are no longer usable.
		if err := old.run(); err != nil {
			sess.recordAutocommitCleanupError(err)
		}
	}
}

func (sess *Session) takeImplicitForCurrent() (*implicitTransactionCleanup, sql.Transaction, bool) {
	if sess == nil {
		return nil, nil, false
	}
	sess.implicitMu.Lock()
	defer sess.implicitMu.Unlock()
	state := sess.implicit
	if state == nil {
		return nil, nil, false
	}
	current := sql.Transaction(nil)
	if sess.Session != nil {
		current = sess.Session.GetTransaction()
	}
	matched := current == state.tx
	// A mismatch can be a stored-procedure/event temporary swap or a newer
	// transaction that replaced the tracked one. Leave the marker intact and
	// never clear the replacement from this stale nil setter.
	if matched {
		sess.implicit = nil
	}
	return state, current, matched
}

// suspendImplicitForProcedure detaches the current implicit marker while a
// GMS stored procedure owns the session transaction slot. The marker remains
// available for the deferred SetTransaction(oldTx) restore.
func (sess *Session) suspendImplicitForProcedure() bool {
	if sess == nil || sess.Session == nil {
		return false
	}
	sess.implicitMu.Lock()
	state := sess.implicit
	current := sess.Session.GetTransaction()
	if len(sess.procedureScopes) == 0 || state == nil || current != state.tx {
		sess.implicitMu.Unlock()
		return false
	}
	scope := sess.procedureScopes[len(sess.procedureScopes)-1]
	if scope == nil || scope.suspended != nil {
		// This scope already detached its outer transaction. Any nil clear now
		// belongs to a child statement and must retain the ordinary rollback
		// semantics.
		sess.implicitMu.Unlock()
		return false
	}
	sess.implicit = nil
	state.suspended = true
	scope.suspended = state
	sess.implicitMu.Unlock()

	// Keep the base session's transaction slot in the state GMS expects while
	// the procedure's first child statement starts its own transaction.
	sess.Session.SetTransaction(nil)
	return true
}

// resolveImplicitState performs the real clear for a marker that was
// speculatively detached. It restores the exact logical transaction while the
// callback runs, then uses the normal rollback path, and finally clears only
// that identity from the base session.
func (sess *Session) resolveImplicitState(state *implicitTransactionCleanup) {
	if sess == nil || state == nil || sess.Session == nil {
		return
	}
	sess.Session.SetTransaction(state.tx)
	cleanupErr := state.run()
	rollbackErr := sess.Rollback(state.ctx, state.tx)
	if err := errors.Join(cleanupErr, rollbackErr); err != nil {
		sess.recordAutocommitCleanupError(err)
	}
	current := sess.Session.GetTransaction()
	if current == state.tx || current == nil {
		sess.Session.SetTransaction(nil)
	}
}

// restoreSuspendedImplicit recognizes the exact transaction object captured
// by GMS before its temporary nil assignment. Any newer detached markers are
// resolved first; they belong to an inner procedure that unwound without
// restoring its own transaction.
func (sess *Session) restoreSuspendedImplicit(tx *Transaction) bool {
	if sess == nil || tx == nil || sess.Session == nil {
		return false
	}
	sess.implicitMu.Lock()
	if len(sess.procedureScopes) == 0 {
		sess.implicitMu.Unlock()
		return false
	}
	scope := sess.procedureScopes[len(sess.procedureScopes)-1]
	if scope == nil || scope.suspended == nil || scope.suspended.tx != tx {
		sess.implicitMu.Unlock()
		return false
	}
	current := sess.Session.GetTransaction()
	if current != nil && current != tx {
		// A procedure child may have left a transaction active. Resolve it only
		// when its marker was created inside a procedure scope; an unrelated
		// replacement must remain untouched.
		child := sess.implicit
		if child == nil || child.tx != current || !child.procedureOwned {
			sess.implicitMu.Unlock()
			return false
		}
		sess.implicitMu.Unlock()
		sess.resolveImplicitState(child)
		sess.implicitMu.Lock()
		if len(sess.procedureScopes) == 0 || sess.procedureScopes[len(sess.procedureScopes)-1] != scope ||
			scope.suspended == nil || scope.suspended.tx != tx {
			sess.implicitMu.Unlock()
			return false
		}
		current = sess.Session.GetTransaction()
		if current != nil && current != tx {
			sess.implicitMu.Unlock()
			return false
		}
	}
	target := scope.suspended
	scope.suspended = nil
	target.suspended = false
	sess.implicit = target
	sess.implicitMu.Unlock()
	sess.Session.SetTransaction(tx)
	return true
}

func (sess *Session) peekImplicit(tx *Transaction) *implicitTransactionCleanup {
	if sess == nil || tx == nil {
		return nil
	}
	sess.implicitMu.Lock()
	defer sess.implicitMu.Unlock()
	if sess.implicit == nil || sess.implicit.tx != tx {
		return nil
	}
	return sess.implicit
}

// HasImplicitTransactionCleanup reports whether tx is currently backed by the
// frontend implicit-autocommit marker. Iterator wrappers use this distinction
// before registering a pre-finalization callback: explicit transactions and
// MySQL's DDL transaction wrapper intentionally have no such marker, and a
// failed registration must not close their live child iterator immediately.
func (sess *Session) HasImplicitTransactionCleanup(tx sql.Transaction) bool {
	transaction, ok := tx.(*Transaction)
	if !ok || sess == nil || sess.Session == nil {
		return false
	}
	sess.implicitMu.Lock()
	defer sess.implicitMu.Unlock()
	return sess.implicit != nil && sess.implicit.tx == transaction && sess.Session.GetTransaction() == transaction
}

func (sess *Session) disarmImplicitTransaction(tx *Transaction, runCleanup bool) error {
	state := sess.peekImplicit(tx)
	if state == nil {
		return nil
	}
	sess.removeImplicitState(state)
	if !runCleanup {
		return nil
	}
	err := state.run()
	if err != nil {
		sess.recordAutocommitCleanupError(err)
	}
	return err
}

// RegisterImplicitTransactionCleanupForContext lets wrappers avoid a direct
// type assertion while keeping the concrete implementation in this package.
func RegisterImplicitTransactionCleanupForContext(
	ctx *sql.Context,
	tx sql.Transaction,
	callback func() error,
) func(success bool) {
	if ctx == nil || ctx.Session == nil {
		return func(bool) {}
	}
	if registrar, ok := ctx.Session.(ImplicitTransactionCleanupRegistrar); ok {
		return registrar.RegisterImplicitTransactionCleanup(ctx, tx, callback)
	}
	return func(bool) {}
}

// BeginDuckLakeOperation reserves provider-wide admission for a top-level
// frontend statement. Keeping this small forwarding method on the session
// lets the execution builder use the session lifecycle without reaching into
// provider internals or changing the existing ConnectionHolder contract.
func (sess *Session) BeginDuckLakeOperation(ctx context.Context) func() {
	if sess == nil || sess.db == nil {
		return func() {}
	}
	return sess.db.BeginDuckLakeOperation(ctx)
}

// BeginDuckLakeOperationWithError is the production admission surface used by
// execution builders that can return an error. Keeping the error-bearing form
// here prevents a poisoned provider generation from being represented as the
// legacy no-op release closure.
func (sess *Session) BeginDuckLakeOperationWithError(ctx context.Context) (func(), error) {
	if sess == nil || sess.db == nil {
		return func() {}, nil
	}
	return sess.db.BeginDuckLakeOperationWithError(ctx)
}

// QueryRowLimit returns the maximum number of rows a query may return. Zero
// means unlimited.
func (sess *Session) QueryRowLimit() uint64 {
	return sess.queryRowLimit
}

func (sess *Session) SetSessionVariable(ctx *sql.Context, name string, value interface{}) error {
	if err := sess.Session.SetSessionVariable(ctx, name, value); err != nil {
		return err
	}
	if !strings.EqualFold(name, sql.AutoCommitSessionVar) || ctx.GetIgnoreAutoCommit() {
		return nil
	}
	autocommit, err := plan.IsSessionAutocommit(ctx)
	if err != nil || autocommit {
		return err
	}

	// SET autocommit=0 begins while the old value is still 1. GMS therefore
	// creates an autocommit transaction without a DuckDB transaction for the
	// SET statement. Clear only that empty wrapper so the next statement starts
	// the real session transaction required by the new value.
	transaction, ok := ctx.GetTransaction().(*Transaction)
	if !ok || transaction.tx != nil {
		return nil
	}
	// The SET itself is the boundary that changes autocommit semantics. It is
	// not a failed user operation, so discard the temporary GMS marker and its
	// resource callback without invoking the DuckLake orphan sweep here.
	cleanupErr := sess.disarmImplicitTransaction(transaction, true)
	rollbackErr := sess.Session.Rollback(ctx, &transaction.Transaction)
	ctx.SetTransaction(nil)
	return errors.Join(cleanupErr, rollbackErr)
}

func (sess *Session) CurrentSchemaOfUnderlyingConn() string {
	return sess.db.Pool().CurrentSchema(sess.ID())
}

// NewSessionBuilder returns a session builder for the given database provider.
func NewSessionBuilder(provider *catalog.DatabaseProvider, opts ...SessionOption) func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
	return func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
		host := ""
		user := ""
		mysqlConnectionUser, ok := conn.UserData.(sql.MysqlConnectionUser)
		if ok {
			host = mysqlConnectionUser.Host
			user = mysqlConnectionUser.User
		}

		client := sql.Client{Address: host, User: user, Capabilities: conn.Capabilities}
		baseSession := sql.NewBaseSessionWithClientServer(addr, client, conn.ConnectionID)
		memSession := memory.NewSession(baseSession, provider)

		schema := provider.Pool().CurrentSchema(conn.ConnectionID)
		if schema != "" {
			logrus.Traceln("SessionBuilder: new session: current schema:", schema)
			memSession.SetCurrentDatabase(schema)
		}

		return NewSession(memSession, provider, opts...), nil
	}
}

var _ sql.TransactionSession = (*Session)(nil)
var _ sql.PersistableSession = (*Session)(nil)
var _ sql.LifecycleAwareSession = (*Session)(nil)
var _ adapter.ConnectionHolder = (*Session)(nil)
var _ adapter.ExecutionSnapshotHolder = (*Session)(nil)
var _ adapter.ExecutionSnapshotLeaseHolder = (*Session)(nil)
var _ adapter.ReleaseTransactionBindingHolder = (*Session)(nil)
var _ adapter.ReleaseTransactionHolder = (*Session)(nil)
var _ adapter.TransactionFinalizer = (*Session)(nil)
var _ adapter.RollbackCleanupFinalizer = (*Session)(nil)
var _ PostTransactionCleanupRegistrar = (*Session)(nil)
var _ PhysicalPostTransactionCleanupRegistrar = (*Session)(nil)

type Transaction struct {
	memory.Transaction
	tx     *stdsql.Tx
	postMu sync.Mutex
	post   *postTransactionCleanup
}

// CommandBegin and CommandEnd opt the session into GMS lifecycle callbacks.
// MyDuck does not need per-command bookkeeping, but implementing the optional
// interface is important because SessionEnd is the only callback GMS gives an
// integrator when a MySQL session is reset or disconnected.
func (sess *Session) CommandBegin() error { return nil }

func (sess *Session) CommandEnd() {}

// SessionEnd rolls back the exact logical transaction still owned by this
// session. GMS invokes this callback from SessionManager while replacing or
// removing a connection; the request context that started the transaction may
// already be canceled, so use a fresh, non-cancelable frontend context. The
// embedded setter is used only after rollback and only when the identity is
// unchanged, preserving a replacement transaction installed by a concurrent
// lifecycle path.
func (sess *Session) SessionEnd() {
	if sess == nil || sess.Session == nil {
		return
	}
	sess.sessionEndMu.Lock()
	defer sess.sessionEndMu.Unlock()

	transaction, ok := sess.Session.GetTransaction().(*Transaction)
	ctx := sql.NewContext(
		mycontext.WithFrontendQuery(context.Background()),
		sql.WithSession(sess),
	)
	if ok && transaction != nil {
		if err := sess.Rollback(ctx, transaction); err != nil {
			sess.recordAutocommitCleanupError(err)
		}

		// Rollback intentionally does not clear GMS's logical transaction slot: the
		// normal rowexec finalizer does that through SetTransaction(nil). Do the same
		// here without re-entering the custom setter, and never clear a replacement.
		if current := sess.Session.GetTransaction(); current == transaction {
			sess.Session.SetTransaction(nil)
		}
		return
	}

	// PostgreSQL transaction controls bypass GMS's TransactionSession slot. A
	// reset/disconnect can therefore leave only the raw pool binding behind. Use
	// the exact pair captured when this Session acquired it; an ID-only lookup is
	// unsafe because SessionManager may already have installed a replacement.
	trackedConn, trackedTx, seen := sess.trackedBinding()
	if !seen || trackedTx == nil || sess.db == nil || sess.db.Pool() == nil {
		return
	}
	currentConn, currentTx := sess.db.Pool().GetTxnBindingForRelease(sess.ID())
	if currentTx == trackedTx && currentConn == trackedConn {
		var cleanup func(*stdsql.Conn) error
		var releaseCleanup func()
		var reservationErr error
		if sess.db.DuckLakeObjectStorageEnabled() {
			releaseCleanup, reservationErr = sess.db.BeginDuckLakeRollbackCleanupForOwnerWithError(trackedTx, trackedTx)
			if reservationErr == nil {
				defer releaseCleanup()
				cleanup = func(conn *stdsql.Conn) error {
					cleanupBase := context.WithoutCancel(ctx)
					cleanupBase = mycontext.WithQueryOrigin(cleanupBase, mycontext.MaintenanceQueryOrigin)
					cleanupBase = catalog.WithRollbackCleanupOwner(cleanupBase, conn)
					cleanupBase = catalog.WithDuckLakeCleanupLease(cleanupBase)
					return sess.db.CleanupDuckLakeOrphansOnConn(ctx.WithContext(cleanupBase), conn)
				}
			}
		}
		_, finalizerErr := sess.RollbackTxnWithCleanup(trackedTx, cleanup)
		if err := errors.Join(reservationErr, finalizerErr); err != nil {
			sess.recordAutocommitCleanupError(err)
		}
		sess.clearTrackedBindingIfChanged(trackedConn, trackedTx)
		return
	}

	// The mapping disappeared or was replaced before SessionEnd. Roll back the
	// captured raw transaction itself so database/sql can return its owner, but
	// never close the connection pointer: it may now be serving the replacement.
	if err := trackedTx.Rollback(); err != nil && !adapter.IsTransactionInactiveError(err) {
		sess.recordAutocommitCleanupError(err)
	}
	// The pool normally reports this terminal event when it removes the old
	// mapping. A stale SessionEnd can nevertheless arrive after an external
	// owner removed the mapping without going through the pool finalizer. The raw
	// rollback above is terminal for database/sql, so resolve this exact session
	// callback even in that path; the registry is one-shot and tolerates the
	// normal pool callback racing with it.
	sess.completePhysicalPostCleanup(trackedTx, false)
	sess.clearTrackedBindingIfChanged(trackedConn, trackedTx)
}

var _ sql.Transaction = (*Transaction)(nil)

// StartTransaction implements sql.TransactionSession.
func (sess *Session) StartTransaction(ctx *sql.Context, tCharacteristic sql.TransactionCharacteristic) (sql.Transaction, error) {
	sess.GetLogger().Trace("StartTransaction")
	base, err := sess.Session.StartTransaction(ctx, tCharacteristic)
	if err != nil {
		return nil, err
	}

	startUnderlyingTx := !sess.mysqlImplicitDDL.Load()
	if startUnderlyingTx && !ctx.GetIgnoreAutoCommit() {
		autocommit, err := plan.IsSessionAutocommit(ctx)
		if err != nil {
			return nil, err
		}
		if autocommit {
			// Don't start a DuckDB transcation if it is in autocommit mode
			startUnderlyingTx = false
		}
	}

	var tx *stdsql.Tx
	if startUnderlyingTx {
		sess.GetLogger().Trace("StartDuckTransaction")
		tx, err = sess.GetTxn(ctx, &stdsql.TxOptions{ReadOnly: tCharacteristic == sql.ReadOnly})
		if err != nil {
			return nil, err
		}
	}
	transaction := &Transaction{Transaction: *base.(*memory.Transaction), tx: tx}
	// GMS creates a logical transaction for ordinary autocommit statements even
	// though no physical DuckDB transaction is opened. Only frontend statements
	// receive the compensating rollback hook; explicit controls, DDL wrappers,
	// replication, maintenance, and recovery contexts must retain their own
	// lifecycle semantics.
	if tx == nil && !sess.mysqlImplicitDDL.Load() && !ctx.GetIgnoreAutoCommit() &&
		mycontext.QueryOrigin(ctx) == mycontext.FrontendQueryOrigin {
		sess.armImplicitState(ctx, transaction)
	}
	return transaction, nil
}

// SetTransaction overrides the embedded BaseSession setter so GMS's
// clearAutocommitOnError path becomes a real rollback boundary. The setter has
// no error return, therefore callback/rollback failures are retained for the
// protocol layer to consume through TakeAutocommitCleanupError.
func (sess *Session) SetTransaction(tx sql.Transaction) {
	if sess == nil || sess.Session == nil {
		return
	}
	if tx != nil {
		// GMS restores the transaction captured before a stored-procedure swap
		// through this setter. Recognize only the exact suspended identity; all
		// other assignments retain ordinary replacement semantics.
		if transaction, ok := tx.(*Transaction); ok && sess.restoreSuspendedImplicit(transaction) {
			return
		}
		sess.Session.SetTransaction(tx)
		return
	}

	// The nil assignment at the start of GMS buildCall is a temporary detach,
	// not a statement boundary. The executor brackets that synchronous call so
	// this path can suspend the marker without invoking rollback/cleanup.
	if sess.suspendImplicitForProcedure() {
		return
	}

	state, current, matched := sess.takeImplicitForCurrent()
	if state == nil {
		// There is no implicit marker to resolve, so preserve the ordinary GMS
		// setter behavior.
		sess.Session.SetTransaction(nil)
		return
	}
	if !matched {
		// A different transaction may be a stored-procedure temporary scope or a
		// replacement installed by a newer request. This nil setter cannot prove
		// that the tracked transaction is the one being cleared, so leave the
		// marker and replacement untouched. When the underlying value is already
		// nil, forwarding the nil assignment is harmless and keeps BaseSession's
		// state canonical.
		if current == nil {
			sess.Session.SetTransaction(nil)
		}
		return
	}

	// Keep the logical transaction visible to the rollback implementation until
	// its provider/session cleanup has completed. This preserves the saved
	// frontend context and prevents a replacement from racing the cleanup path.
	cleanupErr := state.run()
	rollbackErr := sess.Rollback(state.ctx, state.tx)
	if err := errors.Join(cleanupErr, rollbackErr); err != nil {
		sess.recordAutocommitCleanupError(err)
	}

	// A concurrent lifecycle path may have installed a replacement transaction
	// while cleanup ran. Clear only the exact transaction captured above; never
	// evict a newer binding from this stale setter.
	current = sess.Session.GetTransaction()
	if current == state.tx || current == nil {
		sess.Session.SetTransaction(nil)
	}
}

// CommitTransaction implements sql.TransactionSession.
func (sess *Session) CommitTransaction(ctx *sql.Context, tx sql.Transaction) error {
	sess.GetLogger().Trace("CommitTransaction")
	transaction, ok := tx.(*Transaction)
	if !ok || transaction == nil {
		return fmt.Errorf("cannot commit transaction of type %T", tx)
	}
	// Release any iterator/operation callback before committing the logical
	// transaction. If resource release fails, leave the marker armed so GMS's
	// subsequent SetTransaction(nil) can roll the transaction back instead of
	// reporting a successful commit with live resources.
	state := sess.peekImplicit(transaction)
	if state != nil {
		if err := state.run(); err != nil {
			sess.recordAutocommitCleanupError(err)
			return err
		}
	}
	if transaction.tx != nil {
		sess.GetLogger().Trace("CommitDuckTransaction")
		if err := adapter.FinalizeCommit(ctx, transaction.tx); err != nil {
			return err
		}
	}
	if err := sess.Session.CommitTransaction(ctx, &transaction.Transaction); err != nil {
		// Physical commit may already have completed, but the logical GMS
		// transaction did not. Release post callbacks as a failed overall
		// finalization rather than leaving operation admission stranded.
		transaction.completePostCleanup(false)
		return err
	}
	// Both physical and logical commit have completed. Only now may operation
	// leases registered against this logical transaction be released.
	transaction.completePostCleanup(true)
	// A successful logical commit must not be mistaken for a later failed
	// autocommit clear. Remove only the exact marker; a replacement transaction
	// installed concurrently remains untouched.
	sess.removeImplicitState(sess.peekImplicit(transaction))
	return nil
}

// Rollback implements sql.TransactionSession.
func (sess *Session) Rollback(ctx *sql.Context, tx sql.Transaction) error {
	sess.GetLogger().Trace("Rollback")
	transaction := tx.(*Transaction)
	var implicitCleanupErr error
	if state := sess.peekImplicit(transaction); state != nil {
		// Explicit rollback paths (including protocol ROLLBACK) also need to
		// release a retained iterator/operation lease before touching provider
		// cleanup. The marker is removed first so the later SetTransaction(nil)
		// is idempotent and cannot rollback twice.
		sess.removeImplicitState(state)
		implicitCleanupErr = state.run()
	}
	var rollbackErr error
	underlyingInactive := transaction.tx == nil
	var rollbackConn *stdsql.Conn
	var cleanupErr error
	cleanupEligible := ctx != nil &&
		mycontext.QueryOrigin(ctx) == mycontext.FrontendQueryOrigin &&
		sess.db != nil && sess.db.DuckLakeObjectStorageEnabled()
	// Reserve the provider cleanup barrier before entering the pool/session
	// finalizer. The finalizer holds a release-side lifecycle lock; acquiring
	// the barrier from inside its callback can otherwise deadlock against an
	// operation that still needs that lock to finish. The reservation also
	// covers GMS-only (no physical *sql.Tx) rollback scopes.
	var releaseCleanup func()
	var reservationErr error
	if cleanupEligible {
		releaseCleanup, reservationErr = sess.db.BeginDuckLakeRollbackCleanupForOwnerWithError(transaction.tx, transaction)
		if reservationErr == nil {
			defer releaseCleanup()
		} else {
			// A poisoned generation must not prevent the physical/logical rollback;
			// skip maintenance and return the admission error alongside rollback
			// results.
			releaseCleanup = nil
		}
	}
	cleanup := func(conn *stdsql.Conn) error {
		if !cleanupEligible || reservationErr != nil || conn == nil {
			return nil
		}
		cleanupBase := context.WithoutCancel(ctx)
		cleanupBase = mycontext.WithQueryOrigin(cleanupBase, mycontext.MaintenanceQueryOrigin)
		cleanupBase = catalog.WithRollbackCleanupOwner(cleanupBase, conn)
		cleanupBase = catalog.WithDuckLakeCleanupLease(cleanupBase)
		cleanupSQLCtx := ctx.WithContext(cleanupBase)
		return sess.db.CleanupDuckLakeOrphansOnConn(cleanupSQLCtx, conn)
	}
	if transaction.tx != nil {
		sess.GetLogger().Trace("RollbackDuckTransaction")
		underlyingInactive, rollbackErr = adapter.FinalizeRollbackWithCleanup(ctx, transaction.tx, cleanup)
	}

	// For the GMS-only transaction case there is no physical transaction in the
	// pool to hold the session lifecycle lock. Select and run post-rollback
	// maintenance while the logical transaction is still owned by this session;
	// clearing it first would let a replacement transaction or connection-close
	// path race the cleanup operation.
	if transaction.tx == nil && underlyingInactive && cleanupEligible && reservationErr == nil {
		cleanupBase := context.WithoutCancel(ctx)
		cleanupBase = mycontext.WithQueryOrigin(cleanupBase, mycontext.MaintenanceQueryOrigin)
		cleanupBase = catalog.WithDuckLakeCleanupLease(cleanupBase)
		cleanupSQLCtx := ctx.WithContext(cleanupBase)
		rollbackConn, cleanupErr = sess.GetConn(cleanupSQLCtx)
		if cleanupErr == nil && reservationErr == nil {
			cleanupErr = sess.db.CleanupDuckLakeOrphansOnConn(cleanupSQLCtx, rollbackConn)
		}
	}
	sessionErr := sess.Session.Rollback(ctx, &transaction.Transaction)
	// Rollback has reached its terminal point even when one component reports an
	// error. A post-finalization callback must not keep the provider barrier held
	// forever after the physical owner has been retired or proven inactive.
	transaction.completePostCleanup(false)
	return errors.Join(implicitCleanupErr, reservationErr, rollbackErr, sessionErr, cleanupErr)
}

// PersistGlobal implements sql.PersistableSession.
func (sess *Session) PersistGlobal(ctx *sql.Context, sysVarName string, value interface{}) error {
	if _, _, ok := sql.SystemVariables.GetGlobal(sysVarName); !ok {
		return sql.ErrUnknownSystemVariable.New(sysVarName)
	}
	sess.GetLogger().Tracef("Persisting global variable %s = %v", sysVarName, value)
	_, err := sess.ExecContext(
		ctx,
		catalog.InternalTables.PersistentVariable.UpsertStmt(),
		sysVarName, value, fmt.Sprintf("%T", value),
	)
	return err
}

// RemovePersistedGlobal implements sql.PersistableSession.
func (sess *Session) RemovePersistedGlobal(sysVarName string) error {
	_, err := sess.ExecContext(
		context.Background(),
		catalog.InternalTables.PersistentVariable.DeleteStmt(),
		sysVarName,
	)
	return err
}

// RemoveAllPersistedGlobals implements sql.PersistableSession.
func (sess *Session) RemoveAllPersistedGlobals() error {
	_, err := sess.ExecContext(context.Background(), "DELETE FROM "+catalog.InternalTables.PersistentVariable.Name)
	return err
}

// GetPersistedValue implements sql.PersistableSession.
func (sess *Session) GetPersistedValue(k string) (interface{}, error) {
	var value, vtype string
	err := sess.QueryRow(
		context.Background(),
		catalog.InternalTables.PersistentVariable.SelectStmt(),
		k,
	).Scan(&value, &vtype)
	sess.GetLogger().Tracef("Getting persisted global variable %s = %s [%s]", k, value, vtype)
	switch {
	case err == stdsql.ErrNoRows:
		return nil, nil
	case err != nil:
		return nil, err
	default:
		switch vtype {
		case "string":
			return value, nil
		case "int":
			return strconv.Atoi(value)
		case "bool":
			return value == "true", nil
		default:
			return nil, fmt.Errorf("unknown variable type %s", vtype)
		}
	}
}

// GetConn implements adapter.ConnectionHolder.
func (sess *Session) GetConn(ctx context.Context) (*stdsql.Conn, error) {
	if sess == nil || sess.db == nil || sess.db.Pool() == nil {
		return nil, fmt.Errorf("database provider is unavailable")
	}
	conn, tx, err := sess.db.Pool().GetConnForSchemaWithBinding(
		ctx, sess.ID(), sess.GetCurrentDatabase(),
	)
	if err == nil && conn != nil {
		// The pool selected the connection and transaction under one
		// lifecycle/session critical section. Keep that exact pair; a second
		// binding lookup here could observe a replacement transaction.
		sess.rememberBinding(conn, tx)
	}
	return conn, err
}

// GetCatalogConn implements adapter.ConnectionHolder.
func (sess *Session) GetCatalogConn(ctx context.Context) (*stdsql.Conn, error) {
	if sess == nil || sess.db == nil || sess.db.Pool() == nil {
		return nil, fmt.Errorf("database provider is unavailable")
	}
	conn, tx, err := sess.db.Pool().GetConnWithBinding(ctx, sess.ID())
	if err == nil && conn != nil {
		sess.rememberBinding(conn, tx)
	}
	return conn, err
}

// GetTxn implements adapter.ConnectionHolder.
func (sess *Session) GetTxn(ctx context.Context, options *stdsql.TxOptions) (*stdsql.Tx, error) {
	if sess == nil || sess.db == nil || sess.db.Pool() == nil {
		return nil, fmt.Errorf("database provider is unavailable")
	}
	conn, tx, err := sess.db.Pool().GetTxnWithBinding(
		ctx, sess.ID(), sess.GetCurrentDatabase(), options,
	)
	if err == nil && conn != nil && tx != nil {
		sess.rememberBinding(conn, tx)
	}
	return tx, err
}

// GetCatalogTxn implements adapter.ConnectionHolder.
func (sess *Session) GetCatalogTxn(ctx context.Context, options *stdsql.TxOptions) (*stdsql.Tx, error) {
	if sess == nil || sess.db == nil || sess.db.Pool() == nil {
		return nil, fmt.Errorf("database provider is unavailable")
	}
	conn, tx, err := sess.db.Pool().GetTxnWithBinding(ctx, sess.ID(), "", options)
	if err == nil && conn != nil && tx != nil {
		sess.rememberBinding(conn, tx)
	}
	return tx, err
}

// TryGetTxn implements adapter.ConnectionHolder.
func (sess *Session) TryGetTxn() *stdsql.Tx {
	if sess == nil || sess.db == nil || sess.db.Pool() == nil {
		return nil
	}
	return sess.db.Pool().TryGetTxn(sess.ID())
}

// GetTxnBinding implements adapter.TransactionBindingHolder. The pool returns
// the physical connection and transaction from one lifecycle snapshot.
func (sess *Session) GetTxnBinding() (*stdsql.Conn, *stdsql.Tx) {
	if sess == nil || sess.db == nil || sess.db.Pool() == nil {
		return nil, nil
	}
	return sess.db.Pool().GetTxnBinding(sess.ID())
}

// GetTxnBindingForRelease implements adapter.ReleaseTransactionBindingHolder.
// Close/finalizer paths may still capture this snapshot after pool shutdown
// has been announced; the pool admits it through the release lifecycle gate.
func (sess *Session) GetTxnBindingForRelease() (*stdsql.Conn, *stdsql.Tx) {
	if sess == nil || sess.db == nil || sess.db.Pool() == nil {
		return nil, nil
	}
	return sess.db.Pool().GetTxnBindingForRelease(sess.ID())
}

// TryGetTxnForRelease implements adapter.ReleaseTransactionHolder.
func (sess *Session) TryGetTxnForRelease() *stdsql.Tx {
	if sess == nil || sess.db == nil || sess.db.Pool() == nil {
		return nil
	}
	return sess.db.Pool().TryGetTxnForRelease(sess.ID())
}

// GetExecutionSnapshot implements adapter.ExecutionSnapshotHolder. The pool
// selects the physical owner and transaction under one lifecycle lock; catalog
// snapshots deliberately avoid changing the current schema.
func (sess *Session) GetExecutionSnapshot(ctx context.Context, catalogOnly bool) (*stdsql.Conn, *stdsql.Tx, error) {
	if sess == nil || sess.db == nil || sess.db.Pool() == nil {
		return nil, nil, fmt.Errorf("database provider is unavailable")
	}
	return sess.db.Pool().GetExecutionSnapshot(ctx, sess.ID(), sess.GetCurrentDatabase(), catalogOnly)
}

// GetExecutionSnapshotLease implements adapter.ExecutionSnapshotLeaseHolder.
// The returned release callback keeps the selected pool generation alive until
// the caller has finished consuming its iterator.
func (sess *Session) GetExecutionSnapshotLease(
	ctx context.Context,
	catalogOnly bool,
) (*stdsql.Conn, *stdsql.Tx, func(), error) {
	if sess == nil || sess.db == nil || sess.db.Pool() == nil {
		return nil, nil, func() {}, fmt.Errorf("database provider is unavailable")
	}
	return sess.db.Pool().GetExecutionSnapshotLease(ctx, sess.ID(), sess.GetCurrentDatabase(), catalogOnly)
}

// GetCurrentCatalog implements adapter.ConnectionHolder.
func (sess *Session) GetCurrentCatalog() string {
	return sess.db.Pool().CurrentCatalog(sess.ID())
}

// GetCurrentSchema implements adapter.ConnectionHolder.
func (sess *Session) GetCurrentSchema() string {
	return sess.db.Pool().CurrentSchema(sess.ID())
}

// CloseTxn implements adapter.ConnectionHolder.
func (sess *Session) CloseTxn() {
	if sess == nil || sess.db == nil || sess.db.Pool() == nil {
		return
	}
	// Prefer the exact transaction this Session observed. An unconditional
	// session-ID close can retire a replacement transaction after a reset has
	// reused the numeric ID. Only sessions that never observed a binding use the
	// no identity means this Session cannot prove ownership, so it must not
	// touch a transaction that may belong to a replacement session.
	_, trackedTx, seen := sess.trackedBinding()
	if seen {
		if trackedTx == nil {
			return
		}
		sess.db.Pool().CloseTxnIf(sess.ID(), trackedTx)
		sess.completePhysicalPostCleanupIfUnbound(trackedTx)
		sess.clearTrackedTransactionIfChanged(trackedTx)
		return
	}
	return
}

// CloseTxnIf implements adapter.IdentityTransactionCloser.
func (sess *Session) CloseTxnIf(tx *stdsql.Tx) {
	if sess == nil || sess.db == nil || sess.db.Pool() == nil || tx == nil {
		return
	}
	sess.db.Pool().CloseTxnIf(sess.ID(), tx)
	// Identity-safe stale cleanup can be the only finalization path for a raw
	// physical transaction (for example, when PostgreSQL discovers an ownerless
	// or replaced binding). Complete any callbacks retained for that exact
	// transaction so deferred COPY/operation leases cannot remain stranded.
	sess.completePhysicalPostCleanupIfUnbound(tx)
	sess.clearTrackedTransactionIfChanged(tx)
}

// CommitTxn implements adapter.TransactionFinalizer. The pool performs the
// driver operation and identity check while holding its lifecycle lock, so a
// delayed GMS callback cannot finalize or evict a replacement transaction.
func (sess *Session) CommitTxn(tx *stdsql.Tx) error {
	matched, err := sess.db.Pool().CommitTxnWithOutcome(sess.ID(), tx)
	if !matched {
		if err != nil {
			// A malformed/missing physical owner leaves the transaction mapping
			// active and therefore is not a terminal callback boundary. Keep the
			// post-cleanup registration armed until a real finalizer or teardown
			// resolves that mapping.
			return err
		}
		// The expected transaction was already finalized or replaced. Surface
		// that stale identity instead of reporting a successful commit, and
		// complete only this old transaction's callbacks with failure.
		sess.completePhysicalPostCleanupIfUnbound(tx)
		sess.clearTrackedTransactionIfChanged(tx)
		return adapter.ErrTransactionBindingChanged
	}
	sess.completePhysicalPostCleanup(tx, err == nil)
	sess.clearTrackedTransactionIfChanged(tx)
	return err
}

// RollbackTxn implements adapter.TransactionFinalizer. The returned connection
// is the exact physical owner of tx and may be reused for post-rollback
// maintenance only when inactive is true.
func (sess *Session) RollbackTxn(tx *stdsql.Tx) (*stdsql.Conn, bool, error) {
	conn, inactive, err := sess.db.Pool().RollbackTxn(sess.ID(), tx)
	// A binding error leaves the exact transaction mapped and potentially live;
	// only complete the callback after the pool proves that mapping is gone (or a
	// replacement is current). This keeps an operation lease from being released
	// while a malformed owner is still active.
	sess.completePhysicalPostCleanupIfUnbound(tx)
	sess.clearTrackedTransactionIfChanged(tx)
	return conn, inactive, err
}

// RollbackTxnWithCleanup keeps the session lifecycle lock held while the
// caller performs same-connection post-rollback maintenance.
func (sess *Session) RollbackTxnWithCleanup(tx *stdsql.Tx, cleanup func(*stdsql.Conn) error) (bool, error) {
	inactive, err := sess.db.Pool().RollbackTxnWithCleanup(sess.ID(), tx, cleanup)
	sess.completePhysicalPostCleanupIfUnbound(tx)
	sess.clearTrackedTransactionIfChanged(tx)
	return inactive, err
}

// CloseConn implements adapter.ConnectionHolder.
func (sess *Session) CloseConn() {
	if sess == nil || sess.db == nil || sess.db.Pool() == nil {
		return
	}
	// Prefer the identity this Session acquired. If no acquisition was observed,
	// take one release-side snapshot and close only that exact connection; never
	// issue an unconditional ID-based close that could retire a replacement.
	conn, tx, seen := sess.trackedBinding()
	if !seen || conn == nil {
		return
	}
	_ = sess.db.Pool().CloseConnIfBinding(sess.ID(), conn, tx)
	sess.clearTrackedBindingIfChanged(conn, tx)
}

// CloseConnIf implements adapter.IdentityConnectionCloser.
func (sess *Session) CloseConnIf(conn *stdsql.Conn) error {
	if sess == nil || sess.db == nil || sess.db.Pool() == nil {
		return nil
	}
	currentConn, tx := sess.db.Pool().GetTxnBindingForRelease(sess.ID())
	if currentConn != conn {
		return nil
	}
	err := sess.db.Pool().CloseConnIfBinding(sess.ID(), conn, tx)
	sess.completePhysicalPostCleanupIfUnbound(tx)
	sess.clearTrackedBindingIfChanged(conn, tx)
	return err
}

// CloseConnIfBinding implements adapter.IdentityConnectionBindingCloser.
// Pool teardown verifies the exact physical connection and transaction pair
// while holding the session lifecycle lock.
func (sess *Session) CloseConnIfBinding(conn *stdsql.Conn, tx *stdsql.Tx) error {
	if sess == nil || sess.db == nil || sess.db.Pool() == nil {
		return nil
	}
	err := sess.db.Pool().CloseConnIfBinding(sess.ID(), conn, tx)
	sess.completePhysicalPostCleanupIfUnbound(tx)
	sess.clearTrackedBindingIfChanged(conn, tx)
	return err
}

func (sess *Session) ExecContext(ctx context.Context, query string, args ...any) (stdsql.Result, error) {
	// Persistable-session and other database/sql callers may supply either a GMS
	// context or a plain context. In both cases retain the selected pool generation
	// through the driver call; a transaction pointer observed before Restart is an
	// identity, not permission to execute after teardown begins.
	conn, tx, release, err := sess.GetExecutionSnapshotLease(ctx, true)
	if err != nil {
		return nil, err
	}
	defer release()
	if tx != nil {
		return tx.ExecContext(ctx, query, args...)
	}
	return conn.ExecContext(ctx, query, args...)
}

func (sess *Session) QueryRow(ctx context.Context, query string, args ...any) *adapter.LeasedRow {
	conn, tx, release, err := sess.GetExecutionSnapshotLease(ctx, true)
	if err != nil {
		return adapter.NewLeasedRowError(err)
	}
	if tx != nil {
		return adapter.NewLeasedRow(tx.QueryRowContext(ctx, query, args...), release)
	}
	return adapter.NewLeasedRow(conn.QueryRowContext(ctx, query, args...), release)
}
