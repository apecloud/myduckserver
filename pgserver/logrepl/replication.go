// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logrepl

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/binlog"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/delta"
	"github.com/apecloud/myduckserver/pgtypes"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sirupsen/logrus"
)

const outputPlugin = "pgoutput"

type rcvMsg struct {
	msg pgproto3.BackendMessage
	err error
}

type LogicalReplicator struct {
	primaryDns string

	running         bool
	messageReceived bool
	stop            chan struct{}
	mu              *sync.Mutex

	logger *logrus.Entry
}

// NewLogicalReplicator creates a new logical replicator instance which connects to the primary and replication
// databases using the connection strings provided. The connection to the replica is established immediately, and the
// connection to the primary is established when StartReplication is called.
func NewLogicalReplicator(primaryDns string) (*LogicalReplicator, error) {
	return &LogicalReplicator{
		primaryDns: primaryDns,
		mu:         &sync.Mutex{},
		logger: logrus.WithFields(logrus.Fields{
			"component": "replicator",
			"protocol":  "pg",
		}),
	}, nil
}

// PrimaryDns returns the DNS for the primary database. Not suitable for RPCs used in replication e.g.
// StartReplication. See ReplicationDns.
func (r *LogicalReplicator) PrimaryDns() string {
	return r.primaryDns
}

// ReplicationDns returns the DNS for the primary database with the replication query parameter appended. Not suitable
// for normal query RPCs.
func (r *LogicalReplicator) ReplicationDns() string {
	if strings.Contains(r.primaryDns, "?") {
		return fmt.Sprintf("%s&replication=database", r.primaryDns)
	}
	return fmt.Sprintf("%s?replication=database", r.primaryDns)
}

// CaughtUp returns true if the replication slot is caught up to the primary, and false otherwise. This only works if
// there is only a single replication slot on the primary, so it's only suitable for testing. This method uses a
// threshold value to determine if the primary considers us caught up. This corresponds to the maximum number of bytes
// that the primary is ahead of the replica's last flush position. This rarely is zero when caught up, since the
// primary often sends additional WAL records after the last WAL location that was flushed to the replica. These
// additional WAL locations cannot be recorded as flushed since they don't result in writes to the replica, and could
// result in the primary not sending us necessary records after a shutdown and restart.
func (r *LogicalReplicator) CaughtUp(threshold int) (bool, error) {
	r.mu.Lock()
	if !r.messageReceived {
		r.mu.Unlock()
		// We can't query the replication state until after receiving our first message
		return false, nil
	}
	r.mu.Unlock()

	r.logger.Debugf("Checking replication lag with threshold %d\n", threshold)
	conn, err := pgx.Connect(context.Background(), r.PrimaryDns())
	if err != nil {
		return false, err
	}
	defer conn.Close(context.Background())

	result, err := conn.Query(context.Background(), "SELECT pg_wal_lsn_diff(write_lsn, sent_lsn) AS replication_lag FROM pg_stat_replication")
	if err != nil {
		return false, err
	}

	defer result.Close()

	for result.Next() {
		rows, err := result.Values()
		if err != nil {
			return false, err
		}

		row := rows[0]
		lag, ok := row.(pgtype.Numeric)
		if ok && lag.Valid {
			r.logger.Debugf("Current replication lag: %v", row)
			return int(math.Abs(float64(lag.Int.Int64()))) < threshold, nil
		} else {
			r.logger.Debugf("Replication lag unknown: %v", row)
		}
	}

	if result.Err() != nil {
		return false, result.Err()
	}

	// If we didn't get any rows, that usually means that replication has stopped and we're caught up
	return true, nil
}

// maxConsecutiveFailures is the maximum number of consecutive RPC errors that can occur before we stop
// the replication thread
const maxConsecutiveFailures = 10

var errShutdownRequested = errors.New("shutdown requested")

type replicationState struct {
	replicaCtx *sql.Context
	slotName   string

	// lastWrittenLSN is the LSN of the commit record of the last transaction that was successfully replicated to the
	// database.
	lastWrittenLSN pglogrepl.LSN

	// lastReceivedLSN is the last WAL position we have received from the server, which we send back to the server via
	// SendStandbyStatusUpdate after every message we get.
	lastReceivedLSN pglogrepl.LSN

	// currentTransactionLSN is the LSN of the current transaction we are processing.
	// This becomes the lastCommitLSN when we get a CommitMessage.
	currentTransactionLSN pglogrepl.LSN

	// lastCommitLSN is the LSN of the last commit message we received.
	// This becomes the lastWrittenLSN when we commit the transaction to the database.
	lastCommitLSN pglogrepl.LSN

	// inStream tracks the state of the replication stream. When we receive a StreamStartMessage, we set inStream to
	// true, and then back to false when we receive a StreamStopMessage.
	inStream bool

	// We selectively ignore messages that are from before our last flush, which can be resent by postgres in certain
	// crash scenarios. Postgres sends messages in batches based on changes in a transaction, beginning with a Begin
	// message that records the last WAL position of the transaction. The individual INSERT, UPDATE, DELETE messages are
	// sent, each tagged with the WAL position of that tuple write. This WAL position can be before the last flush LSN
	// in some cases. Whether we ignore them or not has nothing to do with the WAL position of any individual write, but
	// the final LSN of the transaction, as recorded in the Begin message. So for every Begin, we decide whether to
	// process or ignore all messages until a corresponding Commit message.
	processMessages bool

	typeMap   *pgtype.Map
	relations map[uint32]*pglogrepl.RelationMessageV2
	schemas   map[uint32]sql.Schema
	keys      map[uint32][]uint16 // relationID -> slice of key column indices
	deltas    *delta.DeltaController

	deltaBufSize    atomic.Uint64 // size of the delta buffer in bytes
	lastCommitTime  time.Time     // time of last commit
	ongoingBatchTxn atomic.Bool   // true if we're in a batched transaction
	dirtyTxn        atomic.Bool   // true if we have uncommitted changes
	dirtyStream     atomic.Bool   // true if the binlog stream does not end with a commit
	inTxnStmtID     atomic.Uint64 // statement ID within transaction
}

// StartReplication starts the replication process for the given slot name. This function blocks until replication is
// stopped via the Stop method, or an error occurs.
func (r *LogicalReplicator) StartReplication(sqlCtx *sql.Context, slotName string) error {
	standbyMessageTimeout := 10 * time.Second
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	lastWrittenLsn, err := r.readWALPosition(sqlCtx, slotName)
	if err != nil {
		return err
	}

	state := &replicationState{
		replicaCtx:     sqlCtx,
		slotName:       slotName,
		lastCommitLSN:  lastWrittenLsn,
		lastWrittenLSN: lastWrittenLsn,
		typeMap:        pgtype.NewMap(),
		relations:      map[uint32]*pglogrepl.RelationMessageV2{},
		schemas:        map[uint32]sql.Schema{},
		keys:           map[uint32][]uint16{},
		deltas:         delta.NewController(),
	}

	// Switch to the `public` schema.
	if _, err := adapter.Exec(sqlCtx, "USE public"); err != nil {
		return err
	}
	sqlCtx.SetCurrentDatabase("public")

	var primaryConn *pgconn.PgConn
	defer func() {
		if primaryConn != nil {
			_ = primaryConn.Close(context.Background())
		}
		// We always shut down here and only here, so we do the cleanup on thread exit in exactly one place
		r.shutdown(sqlCtx, state)
	}()

	connErrCnt := 0
	handleErrWithRetry := func(err error, incrementErrorCount bool) error {
		if err != nil {
			if incrementErrorCount {
				connErrCnt++
			}
			if connErrCnt < maxConsecutiveFailures {
				r.logger.Debugf("Error: %v. Retrying", err)
				if primaryConn != nil {
					_ = primaryConn.Close(context.Background())
				}
				primaryConn = nil
				return nil
			}
		} else {
			connErrCnt = 0
		}

		return err
	}

	sendStandbyStatusUpdate := func(state *replicationState) error {
		// The StatusUpdate message wants us to respond with the current position in the WAL + 1:
		// https://www.postgresql.org/docs/current/protocol-replication.html
		err := pglogrepl.SendStandbyStatusUpdate(context.Background(), primaryConn, pglogrepl.StandbyStatusUpdate{
			WALWritePosition: state.lastReceivedLSN + 1,
			WALFlushPosition: state.lastWrittenLSN + 1,
			WALApplyPosition: state.lastWrittenLSN + 1,
		})
		if err != nil {
			return handleErrWithRetry(err, false)
		}

		r.logger.Debugf("Sent Standby status message with WALWritePosition = %s, WALApplyPosition = %s\n", state.lastReceivedLSN+1, state.lastWrittenLSN+1)
		nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		return nil
	}

	r.logger.Debugf("Starting replicator: primaryDsn=%s, slotName=%s", r.PrimaryDns(), slotName)
	r.mu.Lock()
	r.running = true
	r.messageReceived = false
	r.stop = make(chan struct{})
	r.mu.Unlock()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		err := func() error {
			// Shutdown if requested
			select {
			case <-r.stop:
				return errShutdownRequested
			default:
				// continue below
			}

			if primaryConn == nil {
				var err error
				primaryConn, err = r.beginReplication(slotName, state.lastWrittenLSN)
				if err != nil {
					// unlike other error cases, back off a little here, since we're likely to just get the same error again
					// on initial replication establishment
					time.Sleep(3 * time.Second)
					return handleErrWithRetry(err, true)
				}
			}

			if time.Now().After(nextStandbyMessageDeadline) && state.lastReceivedLSN > 0 {
				err := sendStandbyStatusUpdate(state)
				if err != nil {
					return err
				}
				if primaryConn == nil {
					// if we've lost the connection, we'll re-establish it on the next pass through the loop
					return nil
				}
			}

			ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
			receiveMsgChan := make(chan rcvMsg)
			go func() {
				rawMsg, err := primaryConn.ReceiveMessage(ctx)
				receiveMsgChan <- rcvMsg{msg: rawMsg, err: err}
			}()

			var msgAndErr rcvMsg
			select {
			case <-r.stop:
				cancel()
				return errShutdownRequested
			case <-ctx.Done():
				cancel()
				return nil
			case msgAndErr = <-receiveMsgChan:
				cancel()
			case <-ticker.C:
				cancel()
				return r.commitOngoingTxnIfClean(state, delta.TimeTickFlushReason)
			}

			if msgAndErr.err != nil {
				if pgconn.Timeout(msgAndErr.err) {
					return nil
				} else {
					return handleErrWithRetry(msgAndErr.err, true)
				}
			}

			r.mu.Lock()
			r.messageReceived = true
			r.mu.Unlock()

			rawMsg := msgAndErr.msg
			if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				return fmt.Errorf("received Postgres WAL error: %+v", errMsg)
			}

			msg, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				r.logger.Debugf("Received unexpected message: %T\n", rawMsg)
				return nil
			}

			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %w", err)
				}

				r.logger.Traceln("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)
				state.lastReceivedLSN = pkm.ServerWALEnd

				if pkm.ReplyRequested {
					// Send our reply the next time through the loop
					nextStandbyMessageDeadline = time.Time{}
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return err
				}

				_, err = r.processMessage(xld, state)
				if err != nil {
					// TODO: do we need more than one handler, one for each connection?
					return handleErrWithRetry(err, true)
				}

				return sendStandbyStatusUpdate(state)
			default:
				r.logger.Debugf("Received unexpected message: %T\n", rawMsg)
			}

			return nil
		}()

		if err != nil {
			if errors.Is(err, errShutdownRequested) {
				return nil
			}
			r.logger.Errorln("Error during replication:", err)
			return err
		}
	}
}

func (r *LogicalReplicator) shutdown(ctx *sql.Context, state *replicationState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logger.Info("shutting down replicator")

	r.commitOngoingTxnIfClean(state, delta.OnCloseFlushReason)

	// Rollback any open transaction
	_, err := adapter.ExecCatalog(ctx, "ROLLBACK")
	if err != nil && !strings.Contains(err.Error(), "no transaction is active") {
		r.logger.Debugf("Failed to roll back transaction: %v", err)
	}

	r.running = false
	close(r.stop)
}

// Running returns whether replication is currently running
func (r *LogicalReplicator) Running() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.running
}

// Stop stops the replication process and blocks until clean shutdown occurs.
func (r *LogicalReplicator) Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	r.logger.Info("stopping replication...")
	r.stop <- struct{}{}
	// wait for the channel to be closed, acknowledging that the replicator has stopped
	<-r.stop
}

// beginReplication starts a new replication connection to the primary server and returns it. The LSN provided is the
// last one we have confirmed that we flushed to disk.
func (r *LogicalReplicator) beginReplication(slotName string, lastFlushLsn pglogrepl.LSN) (*pgconn.PgConn, error) {
	r.logger.Debugf("Connecting to primary for replication: %s", r.ReplicationDns())
	conn, err := pgconn.Connect(context.Background(), r.ReplicationDns())
	if err != nil {
		return nil, err
	}

	// streaming of large transactions is available since PG 14 (protocol version 2)
	// we also need to set 'streaming' to 'true'
	pluginArguments := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", slotName),
		"messages 'true'",
		"streaming 'true'",
	}

	// The LSN is the position in the WAL where we want to start replication, but it can only be used to skip entries,
	// not rewind to previous entries that we've already confirmed to the primary that we flushed. We still pass an LSN
	// for the edge case where we have flushed an entry to disk, but crashed before the primary received confirmation.
	// In that edge case, we want to "skip" entries (from the primary's perspective) that we have already flushed to disk.
	r.logger.Debugf("Starting logical replication on slot %s at WAL location %s", slotName, lastFlushLsn+1)
	err = pglogrepl.StartReplication(context.Background(), conn, slotName, lastFlushLsn+1, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArguments,
	})
	if err != nil {
		return nil, err
	}
	r.logger.Infoln("Logical replication started on slot", slotName)

	return conn, nil
}

// DropPublication drops the publication with the given name if it exists. Mostly useful for testing.
func DropPublication(primaryDns, slotName string) error {
	conn, err := pgconn.Connect(context.Background(), primaryDns)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	result := conn.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION IF EXISTS %s;", slotName))
	_, err = result.ReadAll()
	return err
}

// CreatePublication creates a publication with the given name if it does not already exist. Mostly useful for testing.
// Customers should run the CREATE PUBLICATION command on their primary server manually, specifying whichever tables
// they want to replicate.
func CreatePublication(primaryDns, slotName string) error {
	conn, err := pgconn.Connect(context.Background(), primaryDns)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	result := conn.Exec(context.Background(), fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES;", slotName))
	_, err = result.ReadAll()
	return err
}

// DropReplicationSlot drops the replication slot with the given name. Any error from the slot not existing is ignored.
func (r *LogicalReplicator) DropReplicationSlot(slotName string) error {
	conn, err := pgconn.Connect(context.Background(), r.ReplicationDns())
	if err != nil {
		return err
	}

	_ = pglogrepl.DropReplicationSlot(context.Background(), conn, slotName, pglogrepl.DropReplicationSlotOptions{})
	return nil
}

// CreateReplicationSlotIfNecessary creates the replication slot named if it doesn't already exist.
func (r *LogicalReplicator) CreateReplicationSlotIfNecessary(slotName string) error {
	conn, err := pgx.Connect(context.Background(), r.PrimaryDns())
	if err != nil {
		return err
	}

	rows, err := conn.Query(context.Background(), "select * from pg_replication_slots where slot_name = $1", slotName)
	if err != nil {
		return err
	}

	slotExists := false
	defer rows.Close()
	for rows.Next() {
		_, err := rows.Values()
		if err != nil {
			return err
		}
		slotExists = true
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	// We need a different connection to create the replication slot
	conn, err = pgx.Connect(context.Background(), r.ReplicationDns())
	if err != nil {
		return err
	}

	if !slotExists {
		_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn.PgConn(), slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{})
		if err != nil {
			pgErr, ok := err.(*pgconn.PgError)
			if ok && pgErr.Code == "42710" {
				// replication slot already exists, we can ignore this error
			} else {
				return err
			}
		}

		r.logger.Infoln("Created replication slot:", slotName)
	}

	return nil
}

// processMessage processes a logical replication message as appropriate. A couple important aspects:
//  1. Relation messages describe tables being replicated and are used to build a type map for decoding tuples
//  2. INSERT/UPDATE/DELETE messages describe changes to rows that must be applied to the replica.
//     These describe a row in the form of a tuple, and are used to construct a query to apply the change to the replica.
//
// Returns a boolean true if the message was a commit that should be acknowledged, and an error if one occurred.
func (r *LogicalReplicator) processMessage(
	xld pglogrepl.XLogData,
	state *replicationState,
) (bool, error) {
	walData := xld.WALData
	logicalMsg, err := pglogrepl.ParseV2(walData, state.inStream)
	if err != nil {
		return false, err
	}

	r.logger.Debugf("XLogData (%T) => WALStart %s ServerWALEnd %s ServerTime %s", logicalMsg, xld.WALStart, xld.ServerWALEnd, xld.ServerTime)

	// Update the last received LSN
	if xld.ServerWALEnd > state.lastReceivedLSN {
		state.lastReceivedLSN = xld.ServerWALEnd
	}

	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		// When a Relation message is received, commit any buffered ongoing batch transactions.
		err := r.commitOngoingTxn(state, delta.DDLStmtFlushReason)
		if err != nil {
			return false, err
		}

		state.relations[logicalMsg.RelationID] = logicalMsg

		schema := make(sql.Schema, len(logicalMsg.Columns))
		var keys []uint16
		for i, col := range logicalMsg.Columns {
			pgType, err := pgtypes.NewPostgresType(col.DataType)
			if err != nil {
				return false, err
			}
			schema[i] = &sql.Column{
				Name:       col.Name,
				Type:       pgType,
				PrimaryKey: col.Flags == 1,
			}
			if col.Flags == 1 {
				keys = append(keys, uint16(i))
			}
		}
		state.schemas[logicalMsg.RelationID] = schema
		state.keys[logicalMsg.RelationID] = keys

	case *pglogrepl.BeginMessage:
		// Indicates the beginning of a group of changes in a transaction.
		// This is only sent for committed transactions. We won't get any events from rolled back transactions.

		if state.lastWrittenLSN > logicalMsg.FinalLSN {
			r.logger.Debugf("Received stale message, ignoring. Last written LSN: %s Message LSN: %s", state.lastWrittenLSN, logicalMsg.FinalLSN)
			state.processMessages = false
			return false, nil
		}

		state.processMessages = true
		state.currentTransactionLSN = logicalMsg.FinalLSN

		// Start a new transaction or extend existing batch
		extend, reason := r.mayExtendBatchTxn(state)
		if !extend {
			err := r.commitOngoingTxn(state, reason)
			if err != nil {
				return false, err
			}
			_, err = adapter.GetCatalogTxn(state.replicaCtx, nil)
			if err != nil {
				return false, err
			}
			state.ongoingBatchTxn.Store(true)
		}

	case *pglogrepl.CommitMessage:
		r.logger.Debugf("CommitMessage: %v", logicalMsg)

		state.lastCommitLSN = logicalMsg.CommitLSN

		extend, reason := r.mayExtendBatchTxn(state)
		if !extend {
			err = r.commitOngoingTxn(state, reason)
			if err != nil {
				return false, err
			}
		}
		state.dirtyStream.Store(false)
		state.inTxnStmtID.Store(0)

		state.processMessages = false

		return true, nil
	case *pglogrepl.InsertMessageV2:
		if !state.processMessages {
			r.logger.Debugf("Received stale message, ignoring. Last written LSN: %s Message LSN: %s", state.lastWrittenLSN, xld.ServerWALEnd)
			return false, nil
		}

		err = r.append(state, logicalMsg.RelationID, logicalMsg.Tuple.Columns, binlog.InsertRowEvent, false)
		if err != nil {
			return false, err
		}

		state.dirtyTxn.Store(true)
		state.dirtyStream.Store(true)
		state.inTxnStmtID.Add(1)

	case *pglogrepl.UpdateMessageV2:
		if !state.processMessages {
			r.logger.Debugf("Received stale message, ignoring. Last written LSN: %s Message LSN: %s", state.lastWrittenLSN, xld.ServerWALEnd)
			return false, nil
		}

		// Delete the old tuple
		switch logicalMsg.OldTupleType {
		case pglogrepl.UpdateMessageTupleTypeKey:
			err = r.append(state, logicalMsg.RelationID, logicalMsg.OldTuple.Columns, binlog.DeleteRowEvent, true)
		case pglogrepl.UpdateMessageTupleTypeOld:
			err = r.append(state, logicalMsg.RelationID, logicalMsg.OldTuple.Columns, binlog.DeleteRowEvent, false)
		default:
			// No old tuple provided; it means the key columns are unchanged
		}
		if err != nil {
			return false, err
		}

		// Insert the new tuple
		err = r.append(state, logicalMsg.RelationID, logicalMsg.NewTuple.Columns, binlog.InsertRowEvent, false)
		if err != nil {
			return false, err
		}

		state.dirtyTxn.Store(true)
		state.dirtyStream.Store(true)
		state.inTxnStmtID.Add(1)

	case *pglogrepl.DeleteMessageV2:
		if !state.processMessages {
			r.logger.Debugf("Received stale message, ignoring. Last written LSN: %s Message LSN: %s", state.lastWrittenLSN, xld.ServerWALEnd)
			return false, nil
			// Determine which columns to use based on OldTupleType
		}

		switch logicalMsg.OldTupleType {
		case pglogrepl.UpdateMessageTupleTypeKey:
			err = r.append(state, logicalMsg.RelationID, logicalMsg.OldTuple.Columns, binlog.DeleteRowEvent, true)
		case pglogrepl.UpdateMessageTupleTypeOld:
			err = r.append(state, logicalMsg.RelationID, logicalMsg.OldTuple.Columns, binlog.DeleteRowEvent, false)
		default:
			// No old tuple provided; cannot perform delete
			err = fmt.Errorf("DeleteMessage without OldTuple")
		}

		if err != nil {
			return false, err
		}

		state.dirtyTxn.Store(true)
		state.dirtyStream.Store(true)
		state.inTxnStmtID.Add(1)

	case *pglogrepl.TruncateMessageV2:
		r.logger.Debugf("truncate for xid %d\n", logicalMsg.Xid)
	case *pglogrepl.TypeMessageV2:
		r.logger.Debugf("typeMessage for xid %d\n", logicalMsg.Xid)
	case *pglogrepl.OriginMessage:
		r.logger.Debugf("originMessage for xid %s\n", logicalMsg.Name)
	case *pglogrepl.LogicalDecodingMessageV2:
		r.logger.Debugf("Logical decoding message: %q, %q, %d", logicalMsg.Prefix, logicalMsg.Content, logicalMsg.Xid)
	case *pglogrepl.StreamStartMessageV2:
		state.inStream = true
		r.logger.Debugf("Stream start message: xid %d, first segment? %d", logicalMsg.Xid, logicalMsg.FirstSegment)
	case *pglogrepl.StreamStopMessageV2:
		state.inStream = false
		r.logger.Debugf("Stream stop message")
	case *pglogrepl.StreamCommitMessageV2:
		r.logger.Debugf("Stream commit message: xid %d", logicalMsg.Xid)
	case *pglogrepl.StreamAbortMessageV2:
		r.logger.Debugf("Stream abort message: xid %d", logicalMsg.Xid)
	default:
		r.logger.Debugf("Unknown message type in pgoutput stream: %T", logicalMsg)
	}

	return false, nil
}

// readWALPosition reads the recorded WAL position from the WAL position table
func (r *LogicalReplicator) readWALPosition(ctx *sql.Context, slotName string) (pglogrepl.LSN, error) {
	var lsn string
	if err := adapter.QueryRow(ctx, catalog.InternalTables.PgReplicationLSN.SelectStmt(), slotName).Scan(&lsn); err != nil {
		if errors.Is(err, stdsql.ErrNoRows) {
			// if the LSN doesn't exist, consider this a cold start and return 0
			return pglogrepl.LSN(0), nil
		}
		return 0, err
	}

	return pglogrepl.ParseLSN(lsn)
}

// writeWALPosition writes the recorded WAL position to the WAL position table
func (r *LogicalReplicator) writeWALPosition(ctx *sql.Context, slotName string, lsn pglogrepl.LSN) error {
	_, err := adapter.ExecCatalogInTxn(ctx, catalog.InternalTables.PgReplicationLSN.UpsertStmt(), slotName, lsn.String())
	return err
}

// whereClause returns a WHERE clause string with the contents of the builder if it's non-empty, or the empty
// string otherwise
func whereClause(str strings.Builder) string {
	if str.Len() > 0 {
		return " WHERE " + str.String()
	}
	return ""
}

// decodeTextColumnData decodes the given data using the given data type OID and returns the result as a golang value
func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

// encodeColumnData encodes the given data using the given data type OID and returns the result as a string to be
// used in an INSERT or other DML query.
func encodeColumnData(mi *pgtype.Map, data interface{}, dataType uint32) (string, error) {
	var value string
	if dt, ok := mi.TypeForOID(dataType); ok {
		e := dt.Codec.PlanEncode(mi, dataType, pgtype.TextFormatCode, data)
		if e != nil {
			encoded, err := e.Encode(data, nil)
			if err != nil {
				return "", err
			}
			value = string(encoded)
		} else {
			// no encoder for this type, use the string representation
			value = fmt.Sprintf("%v", data)
		}
	} else {
		value = fmt.Sprintf("%v", data)
	}

	// Some types need additional quoting after encoding
	switch data := data.(type) {
	case string, time.Time, pgtype.Time, bool:
		return fmt.Sprintf("'%s'", value), nil
	case [16]byte:
		// TODO: should we actually register an encoder for this type?
		bytes, err := mi.Encode(pgtype.UUIDOID, pgtype.TextFormatCode, data, nil)
		if err != nil {
			return "", err
		}
		return `'` + string(bytes) + `'`, nil
	default:
		return value, nil
	}
}

// mayExtendBatchTxn checks if we should extend the current batch transaction
func (r *LogicalReplicator) mayExtendBatchTxn(state *replicationState) (bool, delta.FlushReason) {
	extend, reason := false, delta.UnknownFlushReason
	if state.ongoingBatchTxn.Load() {
		extend = true
		switch {
		case time.Since(state.lastCommitTime) >= 200*time.Millisecond:
			extend, reason = false, delta.TimeTickFlushReason
		case state.deltaBufSize.Load() >= (128 << 20): // 128MB
			extend, reason = false, delta.MemoryLimitFlushReason
		}
	}
	return extend, reason
}

func (r *LogicalReplicator) commitOngoingTxnIfClean(state *replicationState, reason delta.FlushReason) error {
	if state.dirtyTxn.Load() && !state.dirtyStream.Load() {
		return r.commitOngoingTxn(state, reason)
	}
	return nil
}

// commitOngoingTxn commits the current transaction
func (r *LogicalReplicator) commitOngoingTxn(state *replicationState, flushReason delta.FlushReason) error {
	tx := adapter.TryGetTxn(state.replicaCtx)
	if tx == nil {
		return nil
	}

	defer tx.Rollback()
	defer adapter.CloseTxn(state.replicaCtx)

	// Flush the delta buffer if too large
	err := r.flushDeltaBuffer(state, tx, flushReason)
	if err != nil {
		return err
	}

	r.logger.Debugf("Writing LSN %s\n", state.lastCommitLSN)
	if err = r.writeWALPosition(state.replicaCtx, state.slotName, state.lastCommitLSN); err != nil {
		return err
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return err
	}

	// Reset transaction state
	state.ongoingBatchTxn.Store(false)
	state.dirtyTxn.Store(false)
	state.dirtyStream.Store(false)
	state.inTxnStmtID.Store(0)
	state.lastCommitTime = time.Now()

	state.lastWrittenLSN = state.lastCommitLSN

	return nil
}

// flushDeltaBuffer flushes the accumulated changes in the delta buffer
func (r *LogicalReplicator) flushDeltaBuffer(state *replicationState, tx *stdsql.Tx, reason delta.FlushReason) error {
	defer state.deltaBufSize.Store(0)

	_, err := state.deltas.Flush(state.replicaCtx, tx, reason)
	return err
}

func (r *LogicalReplicator) append(state *replicationState, relationID uint32, tuple []*pglogrepl.TupleDataColumn, eventType binlog.RowEventType, onlyKeys bool) error {
	rel, ok := state.relations[relationID]
	if !ok {
		return fmt.Errorf("unknown relation ID %d", relationID)
	}
	appender, err := state.deltas.GetDeltaAppender(rel.Namespace, rel.RelationName, state.schemas[relationID])
	if err != nil {
		return err
	}

	fields := appender.Fields()
	actions := appender.Action()
	txnTags := appender.TxnTag()
	txnServers := appender.TxnServer()
	txnGroups := appender.TxnGroup()
	txnSeqNumbers := appender.TxnSeqNumber()
	txnStmtOrdinals := appender.TxnStmtOrdinal()

	actions.Append(int8(eventType))
	txnTags.AppendNull()
	txnServers.Append([]byte(""))
	txnGroups.AppendNull()
	txnSeqNumbers.Append(uint64(state.currentTransactionLSN))
	txnStmtOrdinals.Append(state.inTxnStmtID.Load())

	size := 0
	idx := 0

	for i, metadata := range rel.Columns {
		builder := fields[i]
		var col *pglogrepl.TupleDataColumn
		if onlyKeys {
			if metadata.Flags != 1 { // not a key column
				builder.AppendNull()
				continue
			}
			col = tuple[idx]
			idx++
		} else {
			col = tuple[i]
		}
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			builder.AppendNull()
		case pglogrepl.TupleDataTypeText, pglogrepl.TupleDataTypeBinary:
			length, err := decodeToArrow(state.typeMap, metadata, col.Data, tupleDataFormat(col.DataType), builder)
			if err != nil {
				return err
			}
			size += length
		default:
			return fmt.Errorf("unsupported replication data format %d", col.DataType)
		}
	}

	state.deltaBufSize.Add(uint64(size))
	return nil
}

func tupleDataFormat(dataType uint8) int16 {
	switch dataType {
	case pglogrepl.TupleDataTypeBinary:
		return pgtype.BinaryFormatCode
	default:
		return pgtype.TextFormatCode
	}
}
