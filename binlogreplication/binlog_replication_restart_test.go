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
	"strings"
	"testing"
	"time"

	"github.com/apecloud/myduckserver/testutil"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

type restartTableSnapshot struct {
	Count any
	Max   any
	Rows  []int
}

func snapshotRestartTable(t *testing.T, database *sqlx.DB) restartTableSnapshot {
	t.Helper()

	rows, err := database.Queryx("SELECT COUNT(pk) AS count, MAX(pk) AS max FROM db01.t")
	require.NoError(t, err)
	summary := convertMapScanResultToStrings(readNextRow(t, rows))
	require.NoError(t, rows.Close())

	var allRows []int
	require.NoError(t, database.Select(&allRows, "SELECT pk FROM db01.t ORDER BY pk"))
	return restartTableSnapshot{
		Count: summary["count"],
		Max:   summary["max"],
		Rows:  allRows,
	}
}

func requireRestartConfigurationEqual(t *testing.T, before, after map[string]interface{}) {
	t.Helper()

	for _, field := range []string{
		"Source_Host",
		"Source_Port",
		"Source_User",
		"Source_UUID",
		"Connect_Retry",
		"Source_Retry_Count",
		"Auto_Position",
	} {
		require.Equal(t, before[field], after[field], field)
	}
}

func restartPositionFlavor(position string, gtidEnabled bool) string {
	if separator := strings.IndexByte(position, '/'); separator > 0 {
		return position[:separator]
	}
	if gtidEnabled {
		return "MySQL56"
	}
	return "FilePos"
}

// TestBinlogReplicationServerRestart tests that a replica can be configured and started, then the
// server process can be restarted and replica can be restarted without problems.
func TestBinlogReplicationServerRestart(t *testing.T) {
	defer teardown(t)
	startSqlServersWithSystemVars(t, duckReplicaSystemVars)
	startReplicationAndCreateTestDb(t, mySqlPort)

	primaryDatabase.MustExec("create table t (pk int auto_increment primary key)")
	primaryDatabase.MustExec("insert into t values (DEFAULT), (DEFAULT), (DEFAULT)")
	waitForReplicaToCatchUp(t)

	// Freeze the replica before adding more source rows, so the restart boundary
	// has deterministic before and after snapshots.
	// This test covers the explicit STOP -> process restart -> START path. Leave
	// automatic resume to TestAutoRestartReplica so the stored position cannot
	// race ahead while it is compared across the restart boundary.
	replicaDatabase.MustExec("STOP REPLICA")
	statusBeforeRestart := showReplicaStatus(t)
	require.Equal(t, "No", statusBeforeRestart["Replica_IO_Running"])
	require.Equal(t, "No", statusBeforeRestart["Replica_SQL_Running"])
	var positionBeforeRestart string
	require.NoError(t, replicaDatabase.Get(
		&positionBeforeRestart,
		"SELECT position FROM __sys__.binlog_position WHERE channel = ''",
	))
	require.NotEmpty(t, positionBeforeRestart)
	if getGtidEnabled() {
		require.Equal(t, "MySQL56", restartPositionFlavor(positionBeforeRestart, true))
		require.Equal(t, "1", statusBeforeRestart["Auto_Position"])
		require.Equal(t, "INVALID", statusBeforeRestart["Source_Log_File"])
		require.Equal(t, "0", statusBeforeRestart["Read_Source_Log_Pos"])
	} else {
		require.Equal(t, "FilePos", restartPositionFlavor(positionBeforeRestart, false))
		require.Equal(t, "1", statusBeforeRestart["Auto_Position"])
		require.NotEmpty(t, statusBeforeRestart["Source_Log_File"])
		require.NotEqual(t, "INVALID", statusBeforeRestart["Source_Log_File"])
		require.NotEqual(t, "0", statusBeforeRestart["Read_Source_Log_Pos"])
	}
	tableBeforeRestart := snapshotRestartTable(t, replicaDatabase)
	primaryDatabase.MustExec("insert into t values (DEFAULT), (DEFAULT)")
	testutil.StopDuckSqlServer(t, duckProcess)
	time.Sleep(1000 * time.Millisecond)

	var err error
	testEnv := testutil.NewTestEnv()
	setupTestEnv(testEnv)
	err = testutil.StartDuckSqlServer(t, testDir, nil, testEnv)
	require.NoError(t, err)
	loadEnvFromTestEnv(testEnv)

	// Check replication status on the replica and assert configuration persisted
	status := showReplicaStatus(t)
	// The default Connect_Retry interval is 60s; but some tests configure a faster connection retry interval
	require.True(t, status["Connect_Retry"] == "5" || status["Connect_Retry"] == "60")
	require.Equal(t, "86400", status["Source_Retry_Count"])
	require.Equal(t, "localhost", status["Source_Host"])
	require.NotEmpty(t, status["Source_Port"])
	require.NotEmpty(t, status["Source_User"])
	require.Equal(t, "No", status["Replica_IO_Running"])
	require.Equal(t, "No", status["Replica_SQL_Running"])
	requireRestartConfigurationEqual(t, statusBeforeRestart, status)
	require.Equal(t, statusBeforeRestart["Source_Log_File"], status["Source_Log_File"])
	require.Equal(t, statusBeforeRestart["Read_Source_Log_Pos"], status["Read_Source_Log_Pos"])
	if getGtidEnabled() {
		require.Equal(t, "INVALID", status["Source_Log_File"])
		require.Equal(t, "0", status["Read_Source_Log_Pos"])
	} else {
		require.NotEmpty(t, status["Source_Log_File"])
		require.NotEqual(t, "INVALID", status["Source_Log_File"])
		require.NotEqual(t, "0", status["Read_Source_Log_Pos"])
	}
	var positionAfterRestart string
	require.NoError(t, replicaDatabase.Get(
		&positionAfterRestart,
		"SELECT position FROM __sys__.binlog_position WHERE channel = ''",
	))
	require.NotEmpty(t, positionAfterRestart)
	require.Equal(t, positionBeforeRestart, positionAfterRestart)
	require.Equal(t, tableBeforeRestart, snapshotRestartTable(t, replicaDatabase))

	// Restart replication on replica
	// TODO: For now, we have to set server_id each time we start the service.
	//       Turn this into a persistent sys var
	replicaDatabase.MustExec("set @@global.server_id=123;")
	replicaDatabase.MustExec("START REPLICA")

	// Assert that all changes have replicated from the primary
	waitForReplicaToCatchUp(t)
	statusAfterStart := showReplicaStatus(t)
	require.Equal(t, "Yes", statusAfterStart["Replica_IO_Running"])
	require.Equal(t, "Yes", statusAfterStart["Replica_SQL_Running"])
	requireRestartConfigurationEqual(t, statusBeforeRestart, statusAfterStart)
	var positionAfterStart string
	require.NoError(t, replicaDatabase.Get(
		&positionAfterStart,
		"SELECT position FROM __sys__.binlog_position WHERE channel = ''",
	))
	require.NotEmpty(t, positionAfterStart)
	require.NotEqual(t, positionBeforeRestart, positionAfterStart)
	require.Equal(t, snapshotRestartTable(t, primaryDatabase), snapshotRestartTable(t, replicaDatabase))
}
