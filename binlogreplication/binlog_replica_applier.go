// Copyright 2023 Dolthub, Inc.
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

package binlogreplication

import (
	"context"
	stdsql "database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/binlog"
	"github.com/apecloud/myduckserver/charset"
	"github.com/apecloud/myduckserver/delta"
	"github.com/apecloud/myduckserver/mysqlutil"
	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	vquery "github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/sirupsen/logrus"
)

// positionStore is a singleton instance for loading/saving binlog position state to disk for durable storage.
var positionStore = &binlogPositionStore{}

const (
	ERNetReadError      = 1158
	ERFatalReplicaError = 13117
)

type tableIdentifier struct {
	dbName, tableName string
}

type queryFlushRequest struct {
	response chan error
}

type sourceCapabilities struct {
	gtidMode            bool
	flavorName          string
	binlogDumpGTIDFlags uint16
}

type queryExecutor interface {
	ExecuteFetch(query string, maxrows int, wantfields bool) (*sqltypes.Result, error)
}

// binlogReplicaApplier represents the process that applies updates from a binlog connection.
//
// This type is NOT used concurrently – there is currently only one single applier process running to process binlog
// events, so the state in this type is NOT protected with a mutex.
type binlogReplicaApplier struct {
	format                *mysql.BinlogFormat
	tableMapsById         map[uint64]*mysql.TableMap
	tablesByName          map[tableIdentifier]sql.Table
	stopReplicationChan   chan struct{}
	currentGtid           mysql.GTID
	replicationSourceUuid string
	currentPosition       mysql.Position // successfully executed GTIDs
	filters               *filterConfiguration
	running               atomic.Bool
	engine                *gms.Engine
	lifecycleMutex        sync.Mutex
	queryFlushRequests    chan queryFlushRequest
	runDone               chan struct{}

	tableWriterProvider TableWriterProvider
	previousGtid        mysql.GTID
	pendingPosition     mysql.Position
	ongoingBatchTxn     atomic.Bool   // true if we're in a batched transaction, i.e., a series of binlog-format=ROW primary transactions
	dirtyTxn            atomic.Bool   // true if we're in a transaction that is opened and/or has uncommited changes
	dirtyStream         atomic.Bool   // true if the binlog stream does not end with a commit event
	inTxnStmtID         atomic.Uint64 // auto-incrementing ID for statements within a transaction
	deltaBufSize        atomic.Uint64 // size of the delta buffer
	lastCommitTime      time.Time     // time of the last commit
}

func newBinlogReplicaApplier(filters *filterConfiguration) *binlogReplicaApplier {
	return &binlogReplicaApplier{
		tableMapsById:       make(map[uint64]*mysql.TableMap),
		tablesByName:        make(map[tableIdentifier]sql.Table),
		stopReplicationChan: make(chan struct{}),
		filters:             filters,
	}
}

// Row Flags – https://mariadb.com/kb/en/rows_event_v1v2-rows_compressed_event_v1/

// rowFlag_endOfStatement indicates that a row event with this flag set is the last event in a statement.
const rowFlag_endOfStatement = 0x0001
const rowFlag_noForeignKeyChecks = 0x0002
const rowFlag_noUniqueKeyChecks = 0x0004
const rowFlag_noCheckConstraints = 0x0010

// rowFlag_rowsAreComplete indicates that rows in this event are complete, and contain values for all columns of the table.
const rowFlag_rowsAreComplete = 0x0008

// Go spawns a new goroutine to run the applier's binlog event handler.
func (a *binlogReplicaApplier) Go(ctx *sql.Context) {
	a.lifecycleMutex.Lock()
	if a.running.Load() {
		a.lifecycleMutex.Unlock()
		return
	}
	queryFlushRequests := make(chan queryFlushRequest)
	runDone := make(chan struct{})
	a.queryFlushRequests = queryFlushRequests
	a.runDone = runDone
	a.running.Store(true)
	a.lifecycleMutex.Unlock()

	go func() {
		err := a.replicaBinlogEventHandler(ctx, queryFlushRequests)
		if cleanupErr := a.discardOngoingTxn(ctx); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}

		a.lifecycleMutex.Lock()
		if a.runDone == runDone {
			a.running.Store(false)
			a.queryFlushRequests = nil
			a.runDone = nil
			close(runDone)
		}
		a.lifecycleMutex.Unlock()

		if err != nil {
			ctx.GetLogger().Errorf("unexpected error of type %T: '%v'", err, err.Error())
			MyBinlogReplicaController.setSqlError(mysql.ERUnknownError, err.Error())
		}
	}()
}

// IsRunning returns true if this binlog applier is running and has not been stopped, otherwise returns false.
func (a *binlogReplicaApplier) IsRunning() bool {
	return a.running.Load()
}

// RequestQueryFlush asks the active applier run to make completed replication
// work visible. The applier owns both the delta buffer and replication position,
// so a foreground query must never flush either one directly.
func (a *binlogReplicaApplier) RequestQueryFlush(ctx context.Context) error {
	a.lifecycleMutex.Lock()
	if !a.running.Load() || a.queryFlushRequests == nil || a.runDone == nil {
		a.lifecycleMutex.Unlock()
		return nil
	}
	requests := a.queryFlushRequests
	runDone := a.runDone
	a.lifecycleMutex.Unlock()

	request := queryFlushRequest{response: make(chan error, 1)}
	select {
	case requests <- request:
	case <-runDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-request.response:
		return err
	case <-runDone:
		// If the applier handled the request immediately before exiting, preserve
		// its result instead of racing the run-done notification.
		select {
		case err := <-request.response:
			return err
		default:
			return nil
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

// detectSourceCapabilities connects to the MySQL server and detects the
// replication capabilities needed for the binlog stream.
func detectSourceCapabilities(ctx *sql.Context, params mysql.ConnParams) (sourceCapabilities, error) {
	capabilities := sourceCapabilities{flavorName: filePosFlavorID}
	conn, err := mysql.Connect(ctx, &params)
	if err != nil {
		return capabilities, err
	}
	defer conn.Close()
	return detectSourceCapabilitiesFromQueries(conn)
}

func detectSourceCapabilitiesFromQueries(conn queryExecutor) (sourceCapabilities, error) {
	capabilities := sourceCapabilities{flavorName: filePosFlavorID}

	qr, err := conn.ExecuteFetch("SELECT VERSION()", 1, true)
	if err != nil {
		return capabilities, fmt.Errorf("failed to check MySQL version: %w", err)
	}
	if len(qr.Rows) == 0 {
		return capabilities, errors.New("no rows returned when checking MySQL version")
	}
	version := string(qr.Rows[0][0].Raw())

	mariaDB := strings.Contains(version, "MariaDB")
	if mariaDB {
		qr, err = conn.ExecuteFetch("SELECT @@GLOBAL.GTID_STRICT_MODE", 1, true)
		if err != nil {
			return capabilities, fmt.Errorf("failed to check GTID_STRICT_MODE: %w", err)
		}
	} else {
		capabilities.binlogDumpGTIDFlags = detectBinlogDumpGTIDFlags(conn)
		qr, err = conn.ExecuteFetch("SELECT @@GLOBAL.GTID_MODE", 1, true)
		if err != nil {
			return capabilities, fmt.Errorf("failed to check GTID_MODE: %w", err)
		}
	}
	if len(qr.Rows) == 0 {
		return capabilities, errors.New("no rows returned when checking GTID mode")
	}

	gtidModeValue := qr.Rows[0][0].ToString()
	gtidMode := strings.EqualFold(gtidModeValue, "ON") || gtidModeValue == "1"
	capabilities.gtidMode = gtidMode

	if !gtidMode {
		capabilities.flavorName = filePosFlavorID
	} else if mariaDB {
		capabilities.flavorName = mariadbFlavorID
	} else {
		capabilities.flavorName = mysql56FlavorID
	}

	return capabilities, nil
}

// detectBinlogDumpGTIDFlags probes optional source-specific behavior. A source
// that rejects either identity query is treated as standard MySQL.
func detectBinlogDumpGTIDFlags(conn queryExecutor) uint16 {
	qr, err := conn.ExecuteFetch("SELECT @@VERSION_COMMENT", 1, true)
	if err != nil || qr == nil || len(qr.Rows) == 0 || string(qr.Rows[0][0].Raw()) != "Dolt" {
		return 0
	}

	qr, err = conn.ExecuteFetch("SELECT DOLT_VERSION()", 1, true)
	if err != nil || qr == nil || len(qr.Rows) == 0 || string(qr.Rows[0][0].Raw()) != "2.3.1" {
		return 0
	}

	// Dolt 2.3.1 only decodes the SID block when this flag is set.
	return mysql.BinlogThroughGTID
}

// connectAndStartReplicationEventStream connects to the configured MySQL replication source, including pausing
// and retrying if errors are encountered.
func (a *binlogReplicaApplier) connectAndStartReplicationEventStream(ctx *sql.Context) (*mysql.Conn, error) {
	var maxConnectionAttempts uint64
	var connectRetryDelay uint32
	MyBinlogReplicaController.updateStatus(func(status *binlogreplication.ReplicaStatus) {
		status.ReplicaIoRunning = binlogreplication.ReplicaIoConnecting
		status.ReplicaSqlRunning = binlogreplication.ReplicaSqlRunning
		maxConnectionAttempts = status.SourceRetryCount
		connectRetryDelay = status.ConnectRetry
	})

	var (
		conn         *mysql.Conn
		err          error
		capabilities sourceCapabilities
	)
	for connectionAttempts := uint64(0); ; connectionAttempts++ {
		replicaSourceInfo, err := loadReplicationConfiguration(ctx, a.engine.Analyzer.Catalog.MySQLDb)

		if replicaSourceInfo == nil {
			err = ErrServerNotConfiguredAsReplica
			MyBinlogReplicaController.setIoError(ERFatalReplicaError, err.Error())
			return nil, err
		} else if replicaSourceInfo.Uuid != "" {
			a.replicationSourceUuid = replicaSourceInfo.Uuid
		}

		if replicaSourceInfo.Host == "" {
			MyBinlogReplicaController.setIoError(ERFatalReplicaError, ErrEmptyHostname.Error())
			return nil, ErrEmptyHostname
		} else if replicaSourceInfo.User == "" {
			MyBinlogReplicaController.setIoError(ERFatalReplicaError, ErrEmptyUsername.Error())
			return nil, ErrEmptyUsername
		}

		connParams := mysql.ConnParams{
			Host:             replicaSourceInfo.Host,
			Port:             int(replicaSourceInfo.Port),
			Uname:            replicaSourceInfo.User,
			Pass:             replicaSourceInfo.Password,
			ConnectTimeoutMs: 4_000,
		}

		capabilities, err = detectSourceCapabilities(ctx, connParams)
		if err != nil && connectionAttempts >= maxConnectionAttempts {
			return nil, err
		}
		connParams.Flavor = capabilities.flavorName

		conn, err = mysql.Connect(ctx, &connParams)
		if err != nil {
			logrus.Warnf("failed connection attempt to source (%s): %s",
				replicaSourceInfo.Host, err.Error())

			if connectionAttempts >= maxConnectionAttempts {
				ctx.GetLogger().Errorf("Exceeded max connection attempts (%d) to source (%s)",
					maxConnectionAttempts, replicaSourceInfo.Host)
				return nil, err
			}
			// If there was an error connecting (and we haven't used up all our retry attempts), listen for a
			// STOP REPLICA signal or for the retry delay timer to fire. We need to use select here so that we don't
			// block on our retry backoff and ignore the STOP REPLICA signal for a long time.
			select {
			case <-a.stopReplicationChan:
				ctx.GetLogger().Debugf("Received stop replication signal while trying to connect")
				return nil, ErrReplicationStopped
			case <-time.After(time.Duration(connectRetryDelay) * time.Second):
				// Nothing to do here if our timer completes; just fall through
			}
		} else {
			break
		}
	}

	// Request binlog events to start
	// TODO: This should also have retry logic
	err = a.startReplicationEventStream(ctx, conn, capabilities)
	if err != nil {
		return nil, err
	}

	MyBinlogReplicaController.updateStatus(func(status *binlogreplication.ReplicaStatus) {
		status.ReplicaIoRunning = binlogreplication.ReplicaIoRunning
	})

	return conn, nil
}

func (a *binlogReplicaApplier) loadGtidPosition(ctx *sql.Context, positionStore *binlogPositionStore, flavorName string) (mysql.Position, error) {
	position, err := positionStore.Load(flavorName, ctx, a.engine)
	if err != nil {
		return mysql.Position{}, err
	}

	if position.IsZero() {
		// If the positionStore doesn't have a record of executed GTIDs, check to see if the gtid_purged system
		// variable is set. If it holds a GTIDSet, then we use that as our starting position. As part of loading
		// a mysqldump onto a replica, gtid_purged will be set to indicate where to start replication.
		_, value, ok := sql.SystemVariables.GetGlobal("gtid_purged")
		gtidPurged, isString := value.(string)
		if ok && value != nil && isString {
			// Starting in MySQL 8.0, when setting the GTID_PURGED sys variable, if the new value starts with '+', then
			// the specified GTID Set value is added to the current GTID Set value to get a new GTID Set that contains
			// all the previous GTIDs, plus the new ones from the current assignment. Dolt doesn't support this
			// special behavior for appending to GTID Sets yet, so in this case the GTID_PURGED sys var will end up
			// with a "+" prefix. For now, just ignore the "+" prefix if we see it.
			// https://dev.mysql.com/doc/refman/8.0/en/replication-options-gtids.html#sysvar_gtid_purged
			if strings.HasPrefix(gtidPurged, "+") {
				ctx.GetLogger().Warnf("Ignoring unsupported '+' prefix on @@GTID_PURGED value")
				gtidPurged = gtidPurged[1:]
			}

			purged, err := mysql.ParsePosition(flavorName, gtidPurged)
			if err != nil {
				return mysql.Position{}, err
			}
			position = purged
		}
	}

	if position.IsZero() {
		// If we still don't have any record of executed GTIDs, we create a GTIDSet with just one transaction ID
		// for the 0000 server ID. There doesn't seem to be a cleaner way of saying "start at the very beginning".
		//
		// Also... "starting position" is a bit of a misnomer – it's actually the processed GTIDs, which
		// indicate the NEXT GTID where replication should start, but it's not as direct as specifying
		// a starting position, like the Vitess function signature seems to suggest.
		gtid := mysql.Mysql56GTID{
			Sequence: 1,
		}
		position = mysql.Position{GTIDSet: gtid.GTIDSet()}
	}

	return position, nil
}

// another method like "initializedGtidPosition" to get the current log file based position
func (a *binlogReplicaApplier) loadLogFilePosition(ctx *sql.Context, positionStore *binlogPositionStore, flavorName string) (mysql.Position, error) {
	position, err := positionStore.Load(flavorName, ctx, a.engine)
	if err != nil {
		return mysql.Position{}, err
	}

	if position.IsZero() {
		position, err = newFilePosition("", 0)
		if err != nil {
			return mysql.Position{}, err
		}
	}

	return position, nil
}

// startReplicationEventStream sends a request over |conn|, the connection to the MySQL source server, to begin
// sending binlog events.
func (a *binlogReplicaApplier) startReplicationEventStream(ctx *sql.Context, conn *mysql.Conn, capabilities sourceCapabilities) error {
	// A disconnected stream may have applied keyless changes directly into the
	// applier transaction or buffered indexed changes in delta. Neither may
	// survive reconnection without its matching commit event and position.
	if err := a.discardOngoingTxn(ctx); err != nil {
		return fmt.Errorf("unable to discard incomplete replication transaction: %w", err)
	}

	serverId, err := loadReplicaServerId()
	if err != nil {
		return err
	}

	var position mysql.Position
	if capabilities.gtidMode {
		position, err = a.loadGtidPosition(ctx, positionStore, capabilities.flavorName)
		if err != nil {
			return err
		}
		if err := sql.SystemVariables.AssignValues(map[string]interface{}{"gtid_executed": position.GTIDSet.String()}); err != nil {
			ctx.GetLogger().Errorf("unable to set @@GLOBAL.gtid_executed: %s", err.Error())
		}
	} else {
		position, err = a.loadLogFilePosition(ctx, positionStore, capabilities.flavorName)
		if err != nil {
			return err
		}
	}

	a.currentPosition = position
	a.pendingPosition = position

	// Clear out the format description in case we're reconnecting, so that we don't use the old format description
	// to interpret any event messages before we receive the new format description from the new stream.
	a.format = nil

	// Clear out the transactional states for the new stream.
	a.previousGtid = nil
	a.currentGtid = nil
	a.lastCommitTime = time.Now()

	// If the source server has binlog checksums enabled (@@global.binlog_checksum), then the replica MUST
	// set @master_binlog_checksum to handshake with the server to acknowledge that it knows that checksums
	// are in use. Without this step, the server will just send back error messages saying that the replica
	// does not support the binlog checksum algorithm in use on the primary.
	// For more details, see: https://dev.mysql.com/worklog/task/?id=2540
	_, err = conn.ExecuteFetch("set @master_binlog_checksum=@@global.binlog_checksum;", 0, false)
	if err != nil {
		return err
	}

	binlogFile := ""
	if file, _, ok := parseFilePosition(position); ok {
		binlogFile = file
	}

	ctx.GetLogger().WithFields(logrus.Fields{
		"serverId":   serverId,
		"binlogFile": binlogFile,
		"position":   position.String(),
	}).Trace("Sending binlog dump command to source")

	return sendBinlogDumpCommand(conn, serverId, binlogFile, position, capabilities.binlogDumpGTIDFlags)
}

type mysql56BinlogDumpRequest struct {
	serverID   uint32
	binlogFile string
	binlogPos  uint64
	flags      uint16
	sidBlock   []byte
}

func sendBinlogDumpCommand(conn *mysql.Conn, serverID uint32, binlogFile string, position mysql.Position, flags uint16) error {
	if !useWriteComBinlogDumpGTID(position, flags) {
		return conn.SendBinlogDumpCommand(serverID, position)
	}

	request, ok := newMySQL56BinlogDumpRequest(serverID, binlogFile, position, flags)
	if !ok {
		return conn.SendBinlogDumpCommand(serverID, position)
	}

	return conn.WriteComBinlogDumpGTID(
		request.serverID,
		request.binlogFile,
		request.binlogPos,
		request.flags,
		request.sidBlock,
	)
}

// useWriteComBinlogDumpGTID is true only for a MySQL56 GTID set after Dolt 2.3.1
// identity confirmed BinlogThroughGTID. Every other source keeps main's two-arg dump.
func useWriteComBinlogDumpGTID(position mysql.Position, flags uint16) bool {
	if flags != mysql.BinlogThroughGTID {
		return false
	}
	_, ok := position.GTIDSet.(mysql.Mysql56GTIDSet)
	return ok
}

func newMySQL56BinlogDumpRequest(serverID uint32, binlogFile string, position mysql.Position, flags uint16) (mysql56BinlogDumpRequest, bool) {
	gtidSet, ok := position.GTIDSet.(mysql.Mysql56GTIDSet)
	if !ok {
		return mysql56BinlogDumpRequest{}, false
	}

	return mysql56BinlogDumpRequest{
		serverID:   serverID,
		binlogFile: binlogFile,
		binlogPos:  4,
		flags:      flags,
		sidBlock:   gtidSet.SIDBlock(),
	}, true
}

// replicaBinlogEventHandler runs a loop, processing binlog events until the applier's stop replication channel
// receives a signal to stop.
func (a *binlogReplicaApplier) replicaBinlogEventHandler(ctx *sql.Context, queryFlushRequests <-chan queryFlushRequest) error {
	engine := a.engine

	var conn *mysql.Conn
	var eventProducer *binlogEventProducer

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	// Process binlog events
	for {
		if conn == nil {
			ctx.GetLogger().Debug("no binlog connection to source, attempting to establish one")
			if eventProducer != nil {
				eventProducer.Stop()
			}

			var err error
			if conn, err = a.connectAndStartReplicationEventStream(ctx); err == ErrReplicationStopped {
				return nil
			} else if err != nil {
				return err
			}
			eventProducer = newBinlogEventProducer(conn)
			eventProducer.Go(ctx)
		}

		select {
		case event := <-eventProducer.EventChan():
			err := a.processBinlogEvent(ctx, engine, event)
			if err != nil {
				ctx.GetLogger().Errorf("unexpected error of type %T: '%v'", err, err.Error())
				MyBinlogReplicaController.setSqlError(mysql.ERUnknownError, err.Error())
			}

		case err := <-eventProducer.ErrorChan():
			if sqlError, isSqlError := err.(*mysql.SQLError); isSqlError {
				badConnection := sqlError.Message == io.EOF.Error() ||
					strings.HasPrefix(sqlError.Message, io.ErrUnexpectedEOF.Error())
				if badConnection {
					MyBinlogReplicaController.updateStatus(func(status *binlogreplication.ReplicaStatus) {
						status.LastIoError = sqlError.Message
						status.LastIoErrNumber = ERNetReadError
						currentTime := time.Now()
						status.LastIoErrorTimestamp = &currentTime
					})
					eventProducer.Stop()
					eventProducer = nil
					conn.Close()
					conn = nil
				}
			} else {
				// otherwise, log the error if it's something we don't expect and continue
				recordReplicationError(ctx, err)
			}

		case <-ticker.C:
			if a.ongoingBatchTxn.Load() && !a.dirtyStream.Load() {
				// We should commit the transaction to flush the changes to the database
				// if we're in a batched transaction and haven't seen any changes for a while.
				if err := a.extendOrCommitBatchTxn(ctx, engine); err != nil {
					recordReplicationError(ctx, err)
				}
			}

		case request := <-queryFlushRequests:
			var err error
			if a.ongoingBatchTxn.Load() && !a.dirtyStream.Load() {
				err = a.commitOngoingTxn(ctx, engine, NormalCommit, delta.QueryFlushReason)
				if err != nil {
					recordReplicationError(ctx, err)
				}
			}
			request.response <- err

		case <-a.stopReplicationChan:
			ctx.GetLogger().Trace("received stop replication signal")
			eventProducer.Stop()
			if a.ongoingBatchTxn.Load() && !a.dirtyStream.Load() {
				if err := a.commitOngoingTxn(ctx, engine, NormalCommit, delta.OnCloseFlushReason); err != nil {
					recordReplicationError(ctx, err)
				}
			}
			return nil
		}
	}
}

func recordReplicationError(ctx *sql.Context, err error) {
	ctx.GetLogger().Errorf("unexpected error of type %T: '%v'", err, err.Error())
	MyBinlogReplicaController.setSqlError(mysql.ERUnknownError, err.Error())
}

func (a *binlogReplicaApplier) discardOngoingTxn(ctx *sql.Context) error {
	var rollbackErr error
	if txn := ctx.GetTransaction(); txn != nil {
		if session, ok := ctx.Session.(sql.TransactionSession); ok {
			rollbackErr = session.Rollback(ctx, txn)
		} else {
			rollbackErr = fmt.Errorf("replication session does not support transactions")
		}
		ctx.SetTransaction(nil)
		ctx.SetIgnoreAutoCommit(false)
	}

	// Most replication transactions are owned by the GMS transaction above.
	// Keep a fallback for direct writers that opened only the DuckDB transaction.
	if txn := adapter.TryGetTxn(ctx); txn != nil {
		if err := txn.Rollback(); err != nil && err != stdsql.ErrTxDone {
			rollbackErr = errors.Join(rollbackErr, err)
		}
		adapter.CloseTxn(ctx)
	}

	a.tableWriterProvider.DiscardDeltaBuffer(ctx)
	a.pendingPosition = a.currentPosition
	a.ongoingBatchTxn.Store(false)
	a.dirtyTxn.Store(false)
	a.dirtyStream.Store(false)
	a.inTxnStmtID.Store(0)
	a.deltaBufSize.Store(0)
	return rollbackErr
}

// processBinlogEvent processes a single binlog event message and returns an error if there were any problems
// processing it.
func (a *binlogReplicaApplier) processBinlogEvent(ctx *sql.Context, engine *gms.Engine, event mysql.BinlogEvent) error {
	var err error

	// File-position replication emits synthetic GTID events whose Bytes method
	// panics. Only real wire events expose the concrete ServerID method.
	if serverID, ok := binlogEventServerID(event); ok {
		// TODO(fan): detect server ID changes and reset the replication
		MyBinlogReplicaController.setSourceServerID(serverID)
	}

	// We don't support checksum validation, so we MUST strip off any checksum bytes if present, otherwise it gets
	// interpreted as part of the payload and corrupts the data. Future checksum sizes, are not guaranteed to be the
	// same size, so we can't strip the checksum until we've seen a valid Format binlog event that definitively
	// tells us if checksums are enabled and what algorithm they use. We can NOT strip the checksum off of
	// FormatDescription events, because FormatDescription always includes a CRC32 checksum, and Vitess depends on
	// those bytes always being present when we parse the event into a FormatDescription type.
	if a.format != nil && !event.IsFormatDescription() {
		var err error
		event, _, err = event.StripChecksum(*a.format)
		if err != nil {
			msg := fmt.Sprintf("unable to strip checksum from binlog event: '%v'", err.Error())
			ctx.GetLogger().Error(msg)
			MyBinlogReplicaController.setSqlError(mysql.ERUnknownError, msg)
		}
	}

	// ------------------- NOTE -----------------------
	// Since this function is called in a hot loop,
	// we invoke the logging API conditionally
	// to avoid unnecessary memory allocation
	// made by logrus and interface boxing.
	// ------------------------------------------------
	logger := ctx.GetLogger()
	isTraceLevelEnabled := logger.Logger.IsLevelEnabled(logrus.TraceLevel)

	switch {
	case event.IsRand():
		// A RAND_EVENT contains two seed values that set the rand_seed1 and rand_seed2 system variables that are
		// used to compute the random number. For more details, see: https://mariadb.com/kb/en/rand_event/
		// Note: it is written only before a QUERY_EVENT and is NOT used with row-based logging.
		if isTraceLevelEnabled {
			logger.Trace("Received binlog event: Rand")
		}

	case event.IsXID():
		// An XID event is generated for a COMMIT of a transaction that modifies one or more tables of an
		// XA-capable storage engine. For more details, see: https://mariadb.com/kb/en/xid_event/
		if isTraceLevelEnabled {
			logger.Trace("Received binlog event: XID")
		}
		return a.extendOrCommitBatchTxn(ctx, engine)

	case event.IsQuery():
		// A Query event represents a statement executed on the source server that should be executed on the
		// replica. Used for all statements with statement-based replication, DDL statements with row-based replication
		// as well as COMMITs for non-transactional engines such as MyISAM.
		// For more details, see: https://mariadb.com/kb/en/query_event/
		query, err := event.Query(*a.format)
		if err != nil {
			return err
		}

		flags, mode := parseQueryEventVars(*a.format, event)

		if isTraceLevelEnabled {
			logger.WithFields(logrus.Fields{
				"database": query.Database,
				"charset":  query.Charset,
				"query":    query.SQL,
				"flags":    fmt.Sprintf("0x%x", flags),
				"sql_mode": fmt.Sprintf("0x%x", mode),
			}).Trace("Received binlog event: Query")
		}

		var msg string
		if flags&mysql.QFlagOptionAutoIsNull > 0 {
			msg = "Setting sql_auto_is_null ON"
			ctx.SetSessionVariable(ctx, "sql_auto_is_null", 1)
		} else {
			msg = "Setting sql_auto_is_null OFF"
			ctx.SetSessionVariable(ctx, "sql_auto_is_null", 0)
		}
		if isTraceLevelEnabled {
			logger.Trace(msg)
		}

		if flags&mysql.QFlagOptionNotAutocommit > 0 {
			msg = "Setting autocommit=0"
			ctx.SetSessionVariable(ctx, "autocommit", 0)
		} else {
			msg = "Setting autocommit=1"
			ctx.SetSessionVariable(ctx, "autocommit", 1)
		}
		if isTraceLevelEnabled {
			logger.Trace(msg)
		}

		if flags&mysql.QFlagOptionNoForeignKeyChecks > 0 {
			msg = "Setting foreign_key_checks=0"
			ctx.SetSessionVariable(ctx, "foreign_key_checks", 0)
		} else {
			msg = "Setting foreign_key_checks=1"
			ctx.SetSessionVariable(ctx, "foreign_key_checks", 1)
		}
		if isTraceLevelEnabled {
			logger.Trace(msg)
		}

		// NOTE: unique_checks is not currently honored by Dolt
		if flags&mysql.QFlagOptionRelaxedUniqueChecks > 0 {
			msg = "Setting unique_checks=0"
			ctx.SetSessionVariable(ctx, "unique_checks", 0)
		} else {
			msg = "Setting unique_checks=1"
			ctx.SetSessionVariable(ctx, "unique_checks", 1)
		}
		if isTraceLevelEnabled {
			logger.Trace(msg)
		}

		if err := a.executeQueryWithEngine(ctx, engine, query); err != nil {
			ctx.GetLogger().WithFields(logrus.Fields{
				"error": err.Error(),
				"query": query.SQL,
			}).Error("Applying query failed")
			msg := fmt.Sprintf("Applying query failed: %v", err.Error())
			MyBinlogReplicaController.setSqlError(mysql.ERUnknownError, msg)
		}
		a.inTxnStmtID.Add(1)

	case event.IsRotate():
		// When a binary log file exceeds the configured size limit, a ROTATE_EVENT is written at the end of the file,
		// pointing to the next file in the sequence. ROTATE_EVENT is generated locally and written to the binary log
		// on the source server and it's also written when a FLUSH LOGS statement occurs on the source server.
		// For more details, see: https://mariadb.com/kb/en/rotate_event/
		if isTraceLevelEnabled {
			logger.Trace("Received binlog event: Rotate")
		}
		// https://dev.mysql.com/doc/refman/8.4/en/binary-log.html
		// > ... a transaction is written to the file in one piece, never split between files.
		if a.currentGtid != nil {
			return a.extendOrCommitBatchTxn(ctx, engine)
		}

	case event.IsFormatDescription():
		// This is a descriptor event that is written to the beginning of a binary log file, at position 4 (after
		// the 4 magic number bytes). For more details, see: https://mariadb.com/kb/en/format_description_event/
		format, err := event.Format()
		if err != nil {
			return err
		}
		a.format = &format

		if isTraceLevelEnabled {
			logger.WithFields(logrus.Fields{
				"format":        a.format,
				"formatVersion": a.format.FormatVersion,
				"serverVersion": a.format.ServerVersion,
				"checksum":      a.format.ChecksumAlgorithm,
			}).Trace("Received binlog event: FormatDescription")
		}

	case event.IsPreviousGTIDs():
		// Logged in every binlog to record the current replication state. Consists of the last GTID seen for each
		// replication domain. For more details, see: https://mariadb.com/kb/en/gtid_list_event/
		position, err := event.PreviousGTIDs(*a.format)
		if err != nil {
			return err
		}

		if isTraceLevelEnabled {
			logger.WithFields(logrus.Fields{
				"previousGtids": position.GTIDSet.String(),
			}).Trace("Received binlog event: PreviousGTIDs")
		}

	case event.IsGTID():
		// For global transaction ID, used to start a new transaction event group, instead of the old BEGIN query event,
		// and also to mark stand-alone (ddl). For more details, see: https://mariadb.com/kb/en/gtid_event/
		gtid, isBegin, err := event.GTID(*a.format)
		if err != nil {
			return err
		}

		if isTraceLevelEnabled {
			logger.WithFields(logrus.Fields{
				"gtid":    gtid,
				"isBegin": isBegin,
			}).Trace("Received binlog event: GTID")
		}

		// BEGIN will commit the previous transaction implicitly, so we need to set the previous GTID to the current GTID.
		a.previousGtid = a.currentGtid
		a.currentGtid = gtid

		if isBegin {
			if err := a.executeQueryWithEngine(ctx, engine, mysql.Query{SQL: "BEGIN"}); err != nil {
				return err
			}
		}

		// if the source's UUID hasn't been set yet, set it and persist it
		if a.replicationSourceUuid == "" {
			uuid := fmt.Sprintf("%v", gtid.SourceServer())
			err = persistSourceUuid(ctx, uuid, a.engine.Analyzer.Catalog.MySQLDb)
			if err != nil {
				return err
			}
			a.replicationSourceUuid = uuid
		}

	case event.IsTableMap():
		// Used for row-based binary logging beginning (binlog_format=ROW or MIXED). This event precedes each row
		// operation event and maps a table definition to a number, where the table definition consists of database
		// and table names. For more details, see: https://mariadb.com/kb/en/table_map_event/
		// Note: TableMap events are sent before each row event, so there is no need to persist them between restarts.
		tableId := event.TableID(*a.format)
		tableMap, err := event.TableMap(*a.format)
		if err != nil {
			return err
		}

		if isTraceLevelEnabled {
			logger.WithFields(logrus.Fields{
				"id":        tableId,
				"tableName": tableMap.Name,
				"database":  tableMap.Database,
				"flags":     convertToHexString(tableMap.Flags),
				"metadata":  tableMap.Metadata,
				"types":     tableMap.Types,
			}).Trace("Received binlog event: TableMap")
		}

		if tableId == 0xFFFFFF {
			// Table ID 0xFFFFFF is a special value that indicates table maps can be freed.
			ctx.GetLogger().Trace("binlog protocol message: table ID '0xFFFFFF'; clearing table maps")
			a.tableMapsById = make(map[uint64]*mysql.TableMap)
		} else {
			flags := tableMap.Flags
			if flags&rowFlag_endOfStatement == rowFlag_endOfStatement {
				// nothing to be done for end of statement; just clear the flag
				flags = flags &^ rowFlag_endOfStatement
			}
			if flags&rowFlag_noForeignKeyChecks == rowFlag_noForeignKeyChecks {
				flags = flags &^ rowFlag_noForeignKeyChecks
			}
			if flags != 0 {
				msg := fmt.Sprintf("unsupported binlog protocol message: TableMap event with unsupported flags '%x'", flags)
				ctx.GetLogger().Error(msg)
				MyBinlogReplicaController.setSqlError(mysql.ERUnknownError, msg)
			}
			a.tableMapsById[tableId] = tableMap
		}

	case event.IsDeleteRows(), event.IsWriteRows(), event.IsUpdateRows():
		// A ROWS_EVENT is written for row based replication if data is inserted, deleted or updated.
		// For more details, see: https://mariadb.com/kb/en/rows_event_v1v2-rows_compressed_event_v1/
		err = a.processRowEvent(ctx, event, engine)
		if err != nil {
			return err
		}
		a.dirtyTxn.Store(true)
		a.dirtyStream.Store(true)
		a.inTxnStmtID.Add(1)

	default:
		// https://mariadb.com/kb/en/2-binlog-event-header/
		// MySQL include/mysql/binlog_event.h: byte 4 is Log_event_type.
		bytes := event.Bytes()
		if len(bytes) < 5 {
			return fmt.Errorf("received unknown event: %v", event)
		}
		switch bytes[4] {
		case binlogEventHeartbeat:
			// Type 27 is a Heartbeat event. This event does not appear in the binary log. It's only sent over the
			// network by a primary to a replica to let it know that the primary is still alive, and is only sent
			// when the primary has no binlog events to send to replica servers.
			// For more details, see: https://mariadb.com/kb/en/heartbeat_log_event/
			ctx.GetLogger().Trace("Received binlog event: Heartbeat")
		case binlogEventStop:
			ctx.GetLogger().Trace("Received binlog event: Stop")
		case binlogEventIgnorable, binlogEventRowsQuery:
			// ROWS_QUERY is the original SQL text when binlog_rows_query_log_events=ON.
			// It is informational only; the following WRITE/UPDATE/DELETE_ROWS events apply the change.
			// Treating it as unknown aborted replication (#359).
			ctx.GetLogger().Trace("Received binlog event: Ignorable/Rows_query")
		default:
			return fmt.Errorf("received unknown event: %v", event)
		}
	}

	return nil
}

type CommitKind int

const (
	NormalCommit CommitKind = iota
	ImplicitCommitBeforeStmt
	ImplicitCommitAfterStmt
)

func (a *binlogReplicaApplier) commitOngoingTxn(ctx *sql.Context, engine *gms.Engine, kind CommitKind, reason delta.FlushReason) error {
	// Flush the delta buffer if it's grown too large
	// TODO(fan): Make the threshold configurable
	if err := a.flushDeltaBuffer(ctx, reason); err != nil {
		return err
	}

	// Record the last GTID processed.
	// If the commit is caused implicitly (by, e.g., a BEGIN statment or a DDL statement),
	// then we don't want to update the saved position to include the current GTID.
	if kind != ImplicitCommitBeforeStmt {
		a.pendingPosition = mysql.AppendGTID(a.pendingPosition, a.currentGtid)
	}
	if err := positionStore.Save(ctx, engine, a.pendingPosition); err != nil {
		return fmt.Errorf("unable to store GTID executed metadata to disk: %s", err.Error())
	}

	// --- Commit the transaction --- //

	// Commit the transaction started on this session
	if kind != ImplicitCommitAfterStmt || !getAutocommit(ctx) {
		subctx := sql.NewContext(ctx, sql.WithSession(ctx.Session)).WithQuery("COMMIT")
		if err := a.execute(subctx, engine, "COMMIT"); err != nil {
			return err
		}
	}
	// The session manager does not start an actual transaction in autocommit=1 mode,
	// but there may be a transaction in progress if we have started it manually.
	if tx := adapter.TryGetTxn(ctx); tx != nil {
		if err := tx.Commit(); err != nil && err != stdsql.ErrTxDone {
			return err
		}
		adapter.CloseTxn(ctx)
	}

	// --- Update the in-memory states --- //

	// Reset the transaction-related flags
	a.ongoingBatchTxn.Store(false)
	a.dirtyTxn.Store(false)
	a.dirtyStream.Store(false)
	a.inTxnStmtID.Store(0)

	// Record the time of the last commit
	a.lastCommitTime = time.Now()

	// Synchronize the current position with the pending position
	a.currentPosition = a.pendingPosition

	// Expose the last GTID executed as a system variable
	err := sql.SystemVariables.AssignValues(map[string]interface{}{"gtid_executed": a.currentPosition.GTIDSet.String()})
	if err != nil {
		ctx.GetLogger().Errorf("unable to set @@GLOBAL.gtid_executed: %s", err.Error())
	}

	return nil
}

func (a *binlogReplicaApplier) mayExtendBatchTxn() (bool, delta.FlushReason) {
	extend, reason := false, delta.UnknownFlushReason
	if a.ongoingBatchTxn.Load() {
		extend = true
		switch {
		case time.Since(a.lastCommitTime) >= 200*time.Millisecond: // commit the batched txn every 200ms
			extend, reason = false, delta.TimeTickFlushReason
		case a.deltaBufSize.Load() >= (128 << 20): // commit the batched txn if the delta buffer is too large (>= 128MB)
			extend, reason = false, delta.MemoryLimitFlushReason
		}
	}
	return extend, reason
}

func (a *binlogReplicaApplier) extendOrCommitBatchTxn(ctx *sql.Context, engine *gms.Engine) error {
	// If we're in a batched transaction, then we don't want to commit yet.
	extend, reason := a.mayExtendBatchTxn()
	if extend {
		a.pendingPosition = mysql.AppendGTID(a.pendingPosition, a.currentGtid)
		a.dirtyStream.Store(false)
		return nil
	}

	return a.commitOngoingTxn(ctx, engine, NormalCommit, reason)
}

// executeQueryWithEngine executes a query against the engine and returns an error if the query failed.
func (a *binlogReplicaApplier) executeQueryWithEngine(ctx *sql.Context, engine *gms.Engine, query mysql.Query) error {
	// TODO(fan): Here we skip the operations on `mysql.time_zone*` tables, which are not supported by go-mysql-server yet.
	if query.Database == "mysql" && strings.HasPrefix(query.SQL, "TRUNCATE TABLE time_zone") {
		return a.commitOngoingTxn(ctx, engine, ImplicitCommitAfterStmt, delta.DMLStmtFlushReason)
	}

	// Create a sub-context when running queries against the engine, so that we get an accurate query start time.
	subctx := sql.NewContext(ctx, sql.WithSession(ctx.Session)).WithQuery(query.SQL)
	subctx.SetCurrentDatabase(query.Database)
	if subctx.GetCurrentDatabase() == "" {
		ctx.GetLogger().WithField("query", query).Warn("No current database selected")
	}

	// Analyze the query first to check if it's a DDL or DML statement,
	// and flush the changelog if necessary.
	var (
		implicitCommit bool
		flushChangelog bool
		flushReason    delta.FlushReason
	)
	node, err := engine.PrepareQuery(subctx, query.SQL)
	if err != nil {
		return err
	}

	if log := ctx.GetLogger(); log.Logger.IsLevelEnabled(logrus.TraceLevel) {
		log.WithFields(logrus.Fields{
			"query":           query.SQL,
			"db":              subctx.GetCurrentDatabase(),
			"ongoingBatchTxn": a.ongoingBatchTxn.Load(),
			"dirtyTxn":        a.dirtyTxn.Load(),
			"dirtyStream":     a.dirtyStream.Load(),
		}).Tracef("Executing %T query", node)
	}

	switch node.(type) {
	case *plan.StartTransaction:
		var extendable bool
		extendable, flushReason = a.mayExtendBatchTxn()
		if extendable {
			// If we're in an extended batched transaction,
			// then we don't want to start a new transaction yet.
			a.pendingPosition = mysql.AppendGTID(a.pendingPosition, a.previousGtid)
			a.dirtyStream.Store(true)
			return nil
		}
		implicitCommit, flushChangelog = true, true

	case *plan.Commit:
		return a.extendOrCommitBatchTxn(subctx, engine)

	case *plan.InsertInto, *plan.Update, *plan.DeleteFrom, *plan.LoadData:
		implicitCommit, flushChangelog, flushReason = false, true, delta.DMLStmtFlushReason

	default:
		if mysqlutil.CauseImplicitCommitBefore(node) {
			implicitCommit, flushChangelog, flushReason = true, true, delta.UnknownFlushReason
		}
		if mysqlutil.CauseSchemaChange(node) {
			flushReason = delta.DDLStmtFlushReason
			a.tablesByName = make(map[tableIdentifier]sql.Table)
		}
	}

	if flushChangelog {
		// Flush the buffered changes
		if err := a.flushDeltaBuffer(subctx, flushReason); err != nil {
			return err
		}
	}

	if implicitCommit && a.dirtyTxn.Load() {
		// Commit the previous transaction before executing the query.
		if err := a.commitOngoingTxn(subctx, engine, ImplicitCommitBeforeStmt, flushReason); err != nil {
			return err
		}
	}

	if err := a.execute(subctx, engine, query.SQL); err != nil {
		return err
	}

	a.dirtyTxn.Store(true)
	a.dirtyStream.Store(true)

	switch node.(type) {
	case *plan.StartTransaction:
		a.ongoingBatchTxn.Store(true)
	default:
		a.ongoingBatchTxn.Store(false)
	}

	if mysqlutil.CauseImplicitCommitAfter(node) {
		// Commit the transaction after executing the query
		return a.commitOngoingTxn(subctx, engine, ImplicitCommitAfterStmt, flushReason)
	}

	return nil
}

func (a *binlogReplicaApplier) execute(ctx *sql.Context, engine *gms.Engine, query string) error {
	_, iter, _, err := engine.Query(ctx, query)
	if err != nil {
		// Log any errors, except for commits with "nothing to commit"
		if err.Error() != "nothing to commit" {
			return err
		}
		return nil
	}
	for {
		if _, err := iter.Next(ctx); err != nil {
			if err == io.EOF {
				return nil
			}
			ctx.GetLogger().Errorf("ERROR reading query results: %v ", err.Error())
			return err
		}
	}
}

// processRowEvent processes a WriteRows, DeleteRows, or UpdateRows binlog event and returns an error if any problems
// were encountered.
func (a *binlogReplicaApplier) processRowEvent(ctx *sql.Context, event mysql.BinlogEvent, engine *gms.Engine) error {
	var eventName string
	switch {
	case event.IsDeleteRows():
		eventName = "DeleteRows"
	case event.IsWriteRows():
		eventName = "WriteRows"
	case event.IsUpdateRows():
		eventName = "UpdateRows"
	default:
		return fmt.Errorf("unsupported event type: %v", event)
	}
	if log := ctx.GetLogger(); log.Logger.IsLevelEnabled(logrus.TraceLevel) {
		log.Tracef("Received binlog event: %s", eventName)
	}

	tableId := event.TableID(*a.format)
	tableMap, ok := a.tableMapsById[tableId]
	if !ok {
		return fmt.Errorf("unable to find replication metadata for table ID: %d", tableId)
	}

	// Skip processing of MySQL system tables
	if tableMap.Database == "mysql" {
		return nil
	}

	if a.filters.isTableFilteredOut(ctx, tableMap) {
		return nil
	}

	rows, err := event.Rows(*a.format, tableMap)
	if err != nil {
		return err
	}

	if log := ctx.GetLogger(); log.Logger.IsLevelEnabled(logrus.TraceLevel) {
		log.WithFields(logrus.Fields{
			"flags": fmt.Sprintf("%x", rows.Flags),
		}).Tracef("Processing rows from %s event", eventName)
	}

	foreignKeyChecksDisabled, flags := classifyRowEventFlags(rows.Flags)
	if flags != 0 {
		msg := fmt.Sprintf("unsupported binlog protocol message: row event with unsupported flags '%x'", flags)
		ctx.GetLogger().Error(msg)
		MyBinlogReplicaController.setSqlError(mysql.ERUnknownError, msg)
	}
	pkSchema, tableName, err := a.getTableSchema(ctx, engine, tableMap.Name, tableMap.Database)
	if err != nil {
		return err
	}
	schema := pkSchema.Schema

	fieldCount := len(schema)
	if len(tableMap.Types) != fieldCount {
		return fmt.Errorf("schema mismatch: expected %d fields, got %d from binlog", fieldCount, len(tableMap.Types))
	}

	var eventType binlog.RowEventType
	var isRowFormat bool // all columns are present
	switch {
	case event.IsDeleteRows():
		eventType = binlog.DeleteRowEvent
		isRowFormat = rows.IdentifyColumns.BitCount() == fieldCount
	case event.IsUpdateRows():
		eventType = binlog.UpdateRowEvent
		isRowFormat = rows.IdentifyColumns.BitCount() == fieldCount && rows.DataColumns.BitCount() == fieldCount
	case event.IsWriteRows():
		eventType = binlog.InsertRowEvent
		isRowFormat = rows.DataColumns.BitCount() == fieldCount
	}

	if log := ctx.GetLogger(); log.Logger.IsLevelEnabled(logrus.TraceLevel) {
		log.Tracef(" - %s Rows (db: %s, table: %s, IsRowFormat: %v, RowCount: %v)", eventType, tableMap.Database, tableName, isRowFormat, len(rows.Rows))
	}

	if isRowFormat && len(pkSchema.PkOrdinals) > 0 {
		// --binlog-format=ROW & --binlog-row-image=full
		return a.appendRowFormatChanges(ctx, tableMap, tableName, schema, eventType, &rows)
	} else {
		a.ongoingBatchTxn.Store(false)
		return a.writeChanges(ctx, engine, tableMap, tableName, pkSchema, eventType, &rows, foreignKeyChecksDisabled)
	}
}

func classifyRowEventFlags(flags uint16) (foreignKeyChecksDisabled bool, unsupported uint16) {
	foreignKeyChecksDisabled = flags&rowFlag_noForeignKeyChecks != 0
	unsupported = flags &^ (rowFlag_endOfStatement | rowFlag_noForeignKeyChecks)
	return foreignKeyChecksDisabled, unsupported
}

func (a *binlogReplicaApplier) writeChanges(
	ctx *sql.Context, engine *gms.Engine,
	tableMap *mysql.TableMap, tableName string, pkSchema sql.PrimaryKeySchema,
	event binlog.RowEventType, rows *mysql.Rows,
	foreignKeyChecksDisabled bool,
) error {
	identityRows := make([]sql.Row, 0, len(rows.Rows))
	dataRows := make([]sql.Row, 0, len(rows.Rows))
	for _, row := range rows.Rows {
		var identityRow, dataRow sql.Row
		var err error

		if len(row.Identify) > 0 {
			identityRow, err = parseRow(ctx, engine, tableMap, pkSchema.Schema, rows.IdentifyColumns, row.NullIdentifyColumns, row.Identify)
			if err != nil {
				return err
			}
			// `sql.FormatRow` could be used to format the row content in a more readable way for debugging purposes.
			// But DO NOT use it in production, as it can be expensive.
			ctx.GetLogger().Tracef("     - Identity: %#v ", identityRow)
		}
		identityRows = append(identityRows, identityRow)

		if len(row.Data) > 0 {
			dataRow, err = parseRow(ctx, engine, tableMap, pkSchema.Schema, rows.DataColumns, row.NullColumns, row.Data)
			if err != nil {
				return err
			}
			ctx.GetLogger().Tracef("     - Data: %#v ", dataRow)
		}
		dataRows = append(dataRows, dataRow)
	}

	txn, err := adapter.GetTxn(ctx, nil)
	if err != nil {
		return err
	}
	tableWriter, err := a.tableWriterProvider.GetTableWriter(
		ctx,
		txn,
		tableMap.Database, tableName,
		pkSchema,
		len(tableMap.Types), len(rows.Rows),
		rows.IdentifyColumns, rows.DataColumns,
		event,
		foreignKeyChecksDisabled,
	)
	if err != nil {
		return err
	}

	switch event {
	case binlog.DeleteRowEvent:
		err = tableWriter.Delete(ctx, identityRows)
	case binlog.InsertRowEvent:
		err = tableWriter.Insert(ctx, dataRows)
	case binlog.UpdateRowEvent:
		err = tableWriter.Update(ctx, identityRows, dataRows)
	}
	if err != nil {
		return err
	}

	if log := ctx.GetLogger(); log.Logger.IsLevelEnabled(logrus.TraceLevel) {
		log.WithFields(logrus.Fields{
			"db":    tableMap.Database,
			"table": tableName,
			"event": event,
			"rows":  len(rows.Rows),
		}).Trace("processRowEvent")
	}

	return nil
}

func (a *binlogReplicaApplier) appendRowFormatChanges(
	ctx *sql.Context,
	tableMap *mysql.TableMap, tableName string, schema sql.Schema,
	event binlog.RowEventType, rows *mysql.Rows,
) error {
	appender, err := a.tableWriterProvider.GetDeltaAppender(ctx, tableMap.Database, tableName, schema)
	if err != nil {
		return err
	}

	var (
		fields          = appender.Fields()
		actions         = appender.Action()
		txnTags         = appender.TxnTag()
		txnServers      = appender.TxnServer()
		txnGroups       = appender.TxnGroup()
		txnSeqNumbers   = appender.TxnSeqNumber()
		TxnStmtOrdinals = appender.TxnStmtOrdinal()

		txnTag         []byte
		txnServer      []byte
		txnGroup       []byte
		txnSeq         uint64
		txnStmtOrdinal = a.inTxnStmtID.Load()
	)

	switch gtid := a.currentGtid.(type) {
	case mysql.Mysql56GTID:
		// TODO(fan): Add support for GTID tags in MySQL >=8.4
		txnServer = gtid.Server[:]
		txnSeq = uint64(gtid.Sequence)
	case mysql.MariadbGTID:
		var domain, server [4]byte
		binary.BigEndian.PutUint32(domain[:], gtid.Domain)
		binary.BigEndian.PutUint32(server[:], gtid.Server)
		txnTag = domain[:]
		txnServer = server[:]
		txnSeq = gtid.Sequence
	default:
		if file, pos, ok := parseFileGTID(a.currentGtid); ok {
			txnGroup = []byte(file)
			txnSeq = pos
		}
	}

	// The following code is a bit repetitive, but we want to avoid the overhead of function calls for each row.

	// Delete the before image
	switch event {
	case binlog.DeleteRowEvent, binlog.UpdateRowEvent:
		for _, row := range rows.Rows {
			actions.Append(int8(binlog.DeleteRowEvent))
			txnTags.Append(txnTag)
			txnServers.Append(txnServer)
			txnGroups.Append(txnGroup)
			txnSeqNumbers.Append(txnSeq)
			TxnStmtOrdinals.Append(txnStmtOrdinal)

			pos := 0
			for i := range schema {
				builder := fields[i]

				if row.NullIdentifyColumns.Bit(i) {
					builder.AppendNull()
					continue
				}

				length, err := binlog.CellValue(row.Identify, pos, tableMap.Types[i], tableMap.Metadata[i], schema[i], builder)
				if err != nil {
					return err
				}
				pos += length
			}
			a.deltaBufSize.Add(uint64(pos))
		}
		appender.UpdateActionStats(binlog.DeleteRowEvent, len(rows.Rows))
	}

	// Insert the after image
	switch event {
	case binlog.InsertRowEvent, binlog.UpdateRowEvent:
		for _, row := range rows.Rows {
			actions.Append(int8(binlog.InsertRowEvent))
			txnTags.Append(txnTag)
			txnServers.Append(txnServer)
			txnGroups.Append(txnGroup)
			txnSeqNumbers.Append(txnSeq)
			TxnStmtOrdinals.Append(txnStmtOrdinal)

			pos := 0
			for i := range schema {
				builder := appender.Field(i)

				if row.NullColumns.Bit(i) {
					builder.AppendNull()
					continue
				}

				length, err := binlog.CellValue(row.Data, pos, tableMap.Types[i], tableMap.Metadata[i], schema[i], builder)
				if err != nil {
					return err
				}
				pos += length
			}
			a.deltaBufSize.Add(uint64(pos))
		}
		appender.UpdateActionStats(binlog.InsertRowEvent, len(rows.Rows))
	}

	appender.ObserveEvents(event, len(rows.Rows))
	return nil
}

func (a *binlogReplicaApplier) flushDeltaBuffer(ctx *sql.Context, reason delta.FlushReason) error {
	conn, err := adapter.GetCatalogConn(ctx)
	if err != nil {
		return err
	}
	tx, err := adapter.GetCatalogTxn(ctx, nil)
	if err != nil {
		return err
	}

	defer a.deltaBufSize.Store(0)

	if err = a.tableWriterProvider.FlushDeltaBuffer(ctx, conn, tx, reason); err != nil {
		ctx.GetLogger().Errorf("Failed to flush changelog: %v", err.Error())
		MyBinlogReplicaController.setSqlError(mysql.ERUnknownError, err.Error())
	}
	return err
}

//
// Helper functions
//

// parseQueryEventVars parses the status variables block of a Query event and returns the flags and SQL mode.
// Copied from: Vitess's vitess/go/mysql/binlog_event_common.go
// See also: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_replication_binlog_event.html#sect_protocol_replication_event_query_03
func parseQueryEventVars(format mysql.BinlogFormat, event mysql.BinlogEvent) (flags uint32, mode uint64) {
	data := event.Bytes()[format.HeaderLength:]
	const varPos = 4 + 4 + 1 + 2 + 2
	varsLen := int(binary.LittleEndian.Uint16(data[4+4+1+2 : 4+4+1+2+2]))
	vars := data[varPos : varPos+varsLen]

varsLoop:
	for pos := 0; pos < len(vars); {
		code := vars[pos]
		pos++

		switch code {
		case mysql.QFlags2Code:
			flags = binary.LittleEndian.Uint32(vars[pos : pos+4])
			pos += 4
		case mysql.QSQLModeCode:
			mode = binary.LittleEndian.Uint64(vars[pos : pos+8])
		case mysql.QAutoIncrement:
			pos += 4
		case mysql.QCatalog:
			pos += 1 + int(vars[pos]) + 1
		case mysql.QCatalogNZCode:
			pos += 1 + int(vars[pos])
		case mysql.QCharsetCode:
			pos += 6
		default:
			break varsLoop
		}
	}

	return
}

// getTableSchema returns a sql.Schema for the case-insensitive |tableName| in the database named
// |databaseName|, along with the exact, case-sensitive table name.
func (a *binlogReplicaApplier) getTableSchema(ctx *sql.Context, engine *gms.Engine, tableName, databaseName string) (sql.PrimaryKeySchema, string, error) {
	key := tableIdentifier{dbName: strings.ToLower(databaseName), tableName: strings.ToLower(tableName)}
	table, found := a.tablesByName[key]

	if !found {
		database, err := engine.Analyzer.Catalog.Database(ctx, databaseName)
		if err != nil {
			return sql.PrimaryKeySchema{}, "", err
		}
		var ok bool
		table, ok, err = database.GetTableInsensitive(ctx, tableName)
		if err != nil {
			return sql.PrimaryKeySchema{}, "", err
		}
		if !ok {
			return sql.PrimaryKeySchema{}, "", fmt.Errorf("unable to find table %q", tableName)
		}

		a.tablesByName[key] = table
	}

	if pkTable, ok := table.(sql.PrimaryKeyTable); ok {
		return pkTable.PrimaryKeySchema(ctx), table.Name(), nil
	}

	return sql.NewPrimaryKeySchema(table.Schema(ctx)), table.Name(), nil
}

// parseRow parses the binary row data from a MySQL binlog event and converts it into a go-mysql-server Row using the
// |schema| information provided. |columnsPresentBitmap| indicates which column values are present in |data| and
// |nullValuesBitmap| indicates which columns have null values and are NOT present in |data|.
func parseRow(ctx *sql.Context, engine *gms.Engine, tableMap *mysql.TableMap, schema sql.Schema, columnsPresentBitmap, nullValuesBitmap mysql.Bitmap, data []byte) (sql.Row, error) {
	var parsedRow sql.Row
	pos := 0

	for i, typ := range tableMap.Types {
		column := schema[i]

		if !columnsPresentBitmap.Bit(i) {
			parsedRow = append(parsedRow, nil)
			continue
		}

		var value sqltypes.Value
		var err error
		if nullValuesBitmap.Bit(i) {
			value, err = sqltypes.NewValue(vquery.Type_NULL_TYPE, nil)
			if err != nil {
				return nil, err
			}
		} else {
			var length int
			value, length, err = mysql.CellValue(data, pos, typ, tableMap.Metadata[i], vquery.Type(column.Type.Type()))
			if err != nil {
				return nil, err
			}
			pos += length
		}

		convertedValue, err := convertSqlTypesValue(ctx, engine, value, column)
		if err != nil {
			return nil, err
		}
		parsedRow = append(parsedRow, convertedValue)
	}

	return parsedRow, nil
}

// convertSqlTypesValues converts a sqltypes.Value instance (from vitess) into a sql.Type value (for go-mysql-server).
func convertSqlTypesValue(ctx *sql.Context, engine *gms.Engine, value sqltypes.Value, column *sql.Column) (interface{}, error) {
	if value.IsNull() {
		return nil, nil
	}

	var convertedValue interface{}
	var err error
	switch {
	case types.IsTextOnly(column.Type):
		// For text-based types, try to convert the value to a UTF-8 string.
		t, ok := column.Type.(sql.StringType)
		if ok {
			convertedValue, err = charset.Decode(t.CharacterSet(), value.ToString())
		}
		if !ok || errors.Is(err, charset.ErrUnsupported) {
			// Unsupported character set, return the raw bytes as a string (Go's string can hold arbitrary bytes).
			// TODO(fan): When written to DuckDB, the string is interpreted as UTF-8, which may cause issues.
			convertedValue, _, err = column.Type.Convert(ctx, value.ToString())
		}

	case types.IsEnum(column.Type), types.IsSet(column.Type):
		var atoi int
		atoi, err = strconv.Atoi(value.ToString())
		if err != nil {
			return nil, err
		}
		convertedValue, _, err = column.Type.Convert(ctx, atoi)
		if err != nil {
			return nil, err
		}

		// NOTE(fan): We expect the enum/set value to be a string instead of an integer to be written into DuckDB.
		switch t := column.Type.(type) {
		case sql.EnumType:
			convertedValue, _ = t.At(int(convertedValue.(uint16)))
		case sql.SetType:
			convertedValue, err = t.BitsToString(convertedValue.(uint64))
		}

	case types.IsDecimal(column.Type):
		// Decimal values need to have any leading/trailing whitespace trimmed off
		// TODO: Consider moving this into DecimalType_.Convert; if DecimalType_.Convert handled trimming
		//       leading/trailing whitespace, this special case for Decimal types wouldn't be needed.
		convertedValue, _, err = column.Type.Convert(ctx, strings.TrimSpace(value.ToString()))
	case types.IsTimespan(column.Type):
		convertedValue, _, err = column.Type.Convert(ctx, value.ToString())
		if err != nil {
			return nil, err
		}
		convertedValue = convertedValue.(types.Timespan).String()
	default:
		convertedValue, _, err = column.Type.Convert(ctx, value.ToString())

		// logrus.WithField("column", column.Name).WithField("type", column.Type).Infof(
		// 	"Converting value[%s %v %s] to %v %T",
		// 	value.Type(), value.Raw(), value.ToString(), convertedValue, convertedValue,
		// )
	}
	if err != nil {
		return nil, fmt.Errorf("unable to convert value %q, for column of type %T: %v", value.ToString(), column.Type, err.Error())
	}

	return convertedValue, nil
}

func getAllUserDatabaseNames(ctx *sql.Context, engine *gms.Engine) []string {
	allDatabases := engine.Analyzer.Catalog.AllDatabases(ctx)
	userDatabaseNames := make([]string, 0, len(allDatabases))
	for _, database := range allDatabases {
		switch database.Name() {
		case "information_schema", "mysql":
		default:
			userDatabaseNames = append(userDatabaseNames, database.Name())
		}
	}
	return userDatabaseNames
}

// loadReplicaServerId loads the @@GLOBAL.server_id system variable needed to register the replica with the source,
// and returns an error specific to replication configuration if the variable is not set to a valid value.
func loadReplicaServerId() (uint32, error) {
	serverIdVar, value, ok := sql.SystemVariables.GetGlobal("server_id")
	if !ok {
		return 0, fmt.Errorf("no server_id global system variable set")
	}

	// Persisted values stored in .dolt/config.json can cause string values to be stored in
	// system variables, so attempt to convert the value if we can't directly cast it to a uint32.
	serverId, ok := value.(uint32)
	if !ok {
		var err error
		value, _, err = serverIdVar.GetType().Convert(context.Background(), value)
		if err != nil {
			return 0, err
		}
	}

	serverId, ok = value.(uint32)
	if !ok || serverId == 0 {
		return 0, fmt.Errorf("invalid server ID configured for @@GLOBAL.server_id (%v); "+
			"must be an integer greater than zero and less than 4,294,967,296", serverId)
	}

	return serverId, nil
}

//
// Generic util functions...
//

// convertToHexString returns a lower-case hex string representation of the specified uint16 value |v|.
func convertToHexString(v uint16) string {
	return fmt.Sprintf("%x", v)
}

func getAutocommit(ctx *sql.Context) bool {
	autocommit := true
	autoCommitSessionVar, err := ctx.GetSessionVariable(ctx, sql.AutoCommitSessionVar)
	if err == nil {
		autocommit, err = sql.ConvertToBool(ctx, autoCommitSessionVar)
	}
	if err != nil {
		ctx.GetLogger().Warn("Unable to get @@autocommit session variable; assuming autocommit is enabled:", err)
		return true
	}
	return autocommit
}
