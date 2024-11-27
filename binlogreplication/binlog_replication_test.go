// Copyright 2022 Dolthub, Inc.
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
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apecloud/myduckserver/test"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
)

var mySqlContainer string
var mySqlPort, duckPort, duckPgPort int
var primaryDatabase, replicaDatabase *sqlx.DB
var duckProcess *os.Process
var duckLogFilePath, oldDuckLogFilePath string
var duckLogFile, mysqlLogFile *os.File
var testDir string
var originalWorkingDir string

const (
	duckSubdir = "duck"
)

// duckReplicaSystemVars are the common system variables that need
// to be set on a Dolt replica before replication is turned on.
var duckReplicaSystemVars = map[string]string{
	"server_id": "42",
}

func teardown(t *testing.T) {
	if mySqlContainer != "" {
		if t.Failed() {
			fmt.Println("\nMySQL server log:")
			if out, err := exec.Command("docker", "logs", mySqlContainer).Output(); err == nil {
				fmt.Print(string(out))
			} else {
				t.Log(err)
			}
		}
		stopMySqlServer(t)
	}
	if duckProcess != nil {
		test.StopDuckSqlServer(t, duckProcess)
	}
	if mysqlLogFile != nil {
		mysqlLogFile.Close()
	}
	if duckLogFile != nil {
		duckLogFile.Close()
	}

	// Output server logs on failure for easier debugging
	if t.Failed() {
		if oldDuckLogFilePath != "" {
			fmt.Printf("\nDolt server log from %s:\n", oldDuckLogFilePath)
			printFile(oldDuckLogFilePath)
		}

		fmt.Printf("\nDolt server log from %s:\n", duckLogFilePath)
		printFile(duckLogFilePath)
	} else {
		// clean up temp files on clean test runs
		defer os.RemoveAll(testDir)
	}

	if toxiClient != nil {
		proxies, _ := toxiClient.Proxies()
		for _, value := range proxies {
			value.Delete()
		}
	}
}

func setupTestEnv(testEnv *test.TestEnv) {
	testEnv.MySqlContainer = mySqlContainer
	testEnv.MySqlPort = mySqlPort
	testEnv.DuckPort = duckPort
	testEnv.DuckPgPort = duckPgPort
	testEnv.MyDuckServer = replicaDatabase
	testEnv.DuckProcess = duckProcess
	testEnv.DuckLogFilePath = duckLogFilePath
	testEnv.OldDuckLogFilePath = oldDuckLogFilePath
	testEnv.DuckLogFile = duckLogFile
	testEnv.MysqlLogFile = mysqlLogFile
	testEnv.TestDir = testDir
	testEnv.OriginalWorkingDir = originalWorkingDir
}

func loadEnvFromTestEnv(testEnv *test.TestEnv) {
	mySqlContainer = testEnv.MySqlContainer
	mySqlPort = testEnv.MySqlPort
	duckPort = testEnv.DuckPort
	duckPgPort = testEnv.DuckPgPort
	replicaDatabase = testEnv.MyDuckServer
	duckProcess = testEnv.DuckProcess
	duckLogFilePath = testEnv.DuckLogFilePath
	oldDuckLogFilePath = testEnv.OldDuckLogFilePath
	duckLogFile = testEnv.DuckLogFile
	mysqlLogFile = testEnv.MysqlLogFile
	testDir = testEnv.TestDir
	originalWorkingDir = testEnv.OriginalWorkingDir
}

// TestBinlogReplicationSanityCheck performs the simplest possible binlog replication test. It starts up
// a MySQL primary and a Dolt replica, and asserts that a CREATE TABLE statement properly replicates to the
// Dolt replica, along with simple insert, update, and delete statements.
func TestBinlogReplicationSanityCheck(t *testing.T) {
	defer teardown(t)
	startSqlServersWithSystemVars(t, duckReplicaSystemVars)
	startReplicationAndCreateTestDb(t, mySqlPort)

	// Create a table on the primary and verify on the replica
	primaryDatabase.MustExec("create table tableT (pk int primary key)")
	waitForReplicaToCatchUp(t)
	assertCreateTableStatement(t, replicaDatabase, "tableT",
		"CREATE TABLE tableT ( pk int NOT NULL, PRIMARY KEY (pk)) "+
			"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin")

	// Insert/Update/Delete on the primary
	primaryDatabase.MustExec("insert into tableT values(100), (200)")
	waitForReplicaToCatchUp(t)
	requireReplicaResults(t, "select * from db01.tableT order by pk", [][]any{{"100"}, {"200"}})
	primaryDatabase.MustExec("delete from tableT where pk = 100")
	waitForReplicaToCatchUp(t)
	requireReplicaResults(t, "select * from db01.tableT", [][]any{{"200"}})
	primaryDatabase.MustExec("update tableT set pk = 300")
	waitForReplicaToCatchUp(t)
	requireReplicaResults(t, "select * from db01.tableT", [][]any{{"300"}})
}

// TestAutoRestartReplica tests that a Dolt replica automatically starts up replication if
// replication was running when the replica was shut down.
func TestAutoRestartReplica(t *testing.T) {
	defer teardown(t)
	startSqlServersWithSystemVars(t, duckReplicaSystemVars)

	// Assert that replication is not running yet
	status := queryReplicaStatus(t)
	require.Equal(t, "0", status["Last_IO_Errno"])
	require.Equal(t, "", status["Last_IO_Error"])
	require.Equal(t, "0", status["Last_SQL_Errno"])
	require.Equal(t, "", status["Last_SQL_Error"])
	require.Equal(t, "No", status["Replica_IO_Running"])
	require.Equal(t, "No", status["Replica_SQL_Running"])

	// Start up replication and replicate some test data
	startReplicationAndCreateTestDb(t, mySqlPort)
	primaryDatabase.MustExec("create table db01.autoRestartTest(pk int primary key);")
	waitForReplicaToCatchUp(t)
	primaryDatabase.MustExec("insert into db01.autoRestartTest values (100);")
	waitForReplicaToCatchUp(t)
	requireReplicaResults(t, "select * from db01.autoRestartTest;", [][]any{{"100"}})

	// Test for the presence of the replica-running state file
	require.True(t, fileExists(filepath.Join(testDir, duckSubdir, ".replica", "replica-running")))

	// Restart the Dolt replica
	test.StopDuckSqlServer(t, duckProcess)
	var err error
	testEnv := test.NewTestEnv()
	setupTestEnv(testEnv)
	err = test.StartDuckSqlServer(t, testDir, nil, testEnv)
	require.NoError(t, err)
	loadEnvFromTestEnv(testEnv)

	// Assert that some test data replicates correctly
	primaryDatabase.MustExec("insert into db01.autoRestartTest values (200);")
	waitForReplicaToCatchUp(t)
	requireReplicaResults(t, "select * from db01.autoRestartTest;",
		[][]any{{"100"}, {"200"}})

	// SHOW REPLICA STATUS should show that replication is running, with no errors
	status = queryReplicaStatus(t)
	require.Equal(t, "0", status["Last_IO_Errno"])
	require.Equal(t, "", status["Last_IO_Error"])
	require.Equal(t, "0", status["Last_SQL_Errno"])
	require.Equal(t, "", status["Last_SQL_Error"])
	require.Equal(t, "Yes", status["Replica_IO_Running"])
	require.Equal(t, "Yes", status["Replica_SQL_Running"])
	require.Equal(t, "1", status["Source_Server_Id"])

	// Stop replication and assert the replica-running marker file is removed
	replicaDatabase.MustExec("stop replica")
	require.False(t, fileExists(filepath.Join(testDir, duckSubdir, ".replica", "replica-running")))

	// Restart the Dolt replica
	test.StopDuckSqlServer(t, duckProcess)
	setupTestEnv(testEnv)
	err = test.StartDuckSqlServer(t, testDir, nil, testEnv)
	require.NoError(t, err)
	loadEnvFromTestEnv(testEnv)

	// SHOW REPLICA STATUS should show that replication is NOT running, with no errors
	status = queryReplicaStatus(t)
	require.Equal(t, "0", status["Last_IO_Errno"])
	require.Equal(t, "", status["Last_IO_Error"])
	require.Equal(t, "0", status["Last_SQL_Errno"])
	require.Equal(t, "", status["Last_SQL_Error"])
	require.Equal(t, "No", status["Replica_IO_Running"])
	require.Equal(t, "No", status["Replica_SQL_Running"])
}

// TestBinlogSystemUserIsLocked tests that the binlog applier user is locked and cannot be used to connect to the server.
func TestBinlogSystemUserIsLocked(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)

	dsn := fmt.Sprintf("%s@tcp(127.0.0.1:%v)/", binlogApplierUser, duckPort)
	db, err := sqlx.Open("mysql", dsn)
	require.NoError(t, err)

	// Before starting replication, the system account does not exist
	err = db.Ping()
	require.Error(t, err)
	// require.ErrorContains(t, err, "User not found")

	// After starting replication, the system account is locked
	startReplicationAndCreateTestDb(t, mySqlPort)
	err = db.Ping()
	require.Error(t, err)
	require.ErrorContains(t, err, "Access denied for user")
}

// TestFlushLogs tests that binary logs can be flushed on the primary, which forces a new binlog file to be written,
// including sending new Rotate and FormatDescription events to the replica. This is a simple sanity tests that we can
// process the events without errors.
func TestFlushLogs(t *testing.T) {
	defer teardown(t)
	startSqlServersWithSystemVars(t, duckReplicaSystemVars)
	startReplicationAndCreateTestDb(t, mySqlPort)

	// Make changes on the primary and verify on the replica
	primaryDatabase.MustExec("create table t (pk int primary key)")
	waitForReplicaToCatchUp(t)
	expectedStatement := "CREATE TABLE t ( pk int NOT NULL, PRIMARY KEY (pk)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"
	assertCreateTableStatement(t, replicaDatabase, "t", expectedStatement)

	primaryDatabase.MustExec("flush binary logs;")
	gtidEnabled := getGtidEnabled()
	if gtidEnabled {
		// The 'FLUSH BINARY LOGS' statement will update the file position, but not the GTID.
		// Since the applier will not update the replication progress when it receives a
		// FormatDescription event, we should only check the replica status if GTID is enabled.
		waitForReplicaToCatchUp(t)
	}

	primaryDatabase.MustExec("insert into t values (1), (2), (3);")
	waitForReplicaToCatchUp(t)

	requireReplicaResults(t, "select * from db01.t order by pk;", [][]any{
		{"1"}, {"2"}, {"3"},
	})
}

// TestResetReplica tests that "RESET REPLICA" and "RESET REPLICA ALL" correctly clear out
// replication configuration and metadata.
func TestResetReplica(t *testing.T) {
	defer teardown(t)
	startSqlServersWithSystemVars(t, duckReplicaSystemVars)
	startReplicationAndCreateTestDb(t, mySqlPort)

	// RESET REPLICA returns an error if replication is running
	_, err := replicaDatabase.Queryx("RESET REPLICA")
	require.Error(t, err)
	require.ErrorContains(t, err, "unable to reset replica while replication is running")

	// Calling RESET REPLICA clears out any errors
	replicaDatabase.MustExec("STOP REPLICA;")
	rows, err := replicaDatabase.Queryx("RESET REPLICA;")
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	status := queryReplicaStatus(t)
	require.Equal(t, "0", status["Last_Errno"])
	require.Equal(t, "", status["Last_Error"])
	require.Equal(t, "0", status["Last_IO_Errno"])
	require.Equal(t, "", status["Last_IO_Error"])
	require.Equal(t, "", status["Last_IO_Error_Timestamp"])
	require.Equal(t, "0", status["Last_SQL_Errno"])
	require.Equal(t, "", status["Last_SQL_Error"])
	require.Equal(t, "", status["Last_SQL_Error_Timestamp"])

	// Calling RESET REPLICA ALL clears out all replica configuration
	rows, err = replicaDatabase.Queryx("RESET REPLICA ALL;")
	require.NoError(t, err)
	require.NoError(t, rows.Close())
	status = queryReplicaStatus(t)
	require.Equal(t, "", status["Source_Host"])
	require.Equal(t, "", status["Source_User"])
	require.Equal(t, "No", status["Replica_IO_Running"])
	require.Equal(t, "No", status["Replica_SQL_Running"])

	rows, err = replicaDatabase.Queryx("select * from mysql.slave_master_info;")
	require.NoError(t, err)
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())

	// Start replication again and verify that we can still query replica status
	startReplicationAndCreateTestDb(t, mySqlPort)
	replicaStatus := showReplicaStatus(t)
	require.Equal(t, "0", replicaStatus["Last_Errno"])
	require.Equal(t, "", replicaStatus["Last_Error"])
	require.True(t, replicaStatus["Replica_IO_Running"] == binlogreplication.ReplicaIoRunning ||
		replicaStatus["Replica_IO_Running"] == binlogreplication.ReplicaIoConnecting)
}

// TestStartReplicaErrors tests that the "START REPLICA" command returns appropriate responses
// for various error conditions.
func TestStartReplicaErrors(t *testing.T) {
	defer teardown(t)
	startSqlServersWithSystemVars(t, duckReplicaSystemVars)

	// START REPLICA returns an error when no replication source is configured
	_, err := replicaDatabase.Queryx("START REPLICA;")
	require.Error(t, err)
	require.ErrorContains(t, err, ErrServerNotConfiguredAsReplica.Error())

	// For an incomplete source configuration, throw an error as early as possible to make sure the user notices it.
	replicaDatabase.MustExec("CHANGE REPLICATION SOURCE TO SOURCE_PORT=1234, SOURCE_HOST='localhost';")
	rows, err := replicaDatabase.Queryx("START REPLICA;")
	require.Error(t, err)
	require.ErrorContains(t, err, "Invalid (empty) username")
	require.Nil(t, rows)

	// SOURCE_AUTO_POSITION cannot be disabled – we only support GTID positioning
	rows, err = replicaDatabase.Queryx("CHANGE REPLICATION SOURCE TO SOURCE_PORT=1234, " +
		"SOURCE_HOST='localhost', SOURCE_USER='replicator', SOURCE_AUTO_POSITION=0;")
	require.Error(t, err)
	require.ErrorContains(t, err, "Error 1105 (HY000): SOURCE_AUTO_POSITION cannot be disabled")
	require.Nil(t, rows)

	// START REPLICA logs a warning if replication is already running
	startReplicationAndCreateTestDb(t, mySqlPort)
	replicaDatabase.MustExec("START REPLICA;")
	assertWarning(t, replicaDatabase, 3083, "Replication thread(s) for channel '' are already running.")
}

// TestShowReplicaStatus tests various cases "SHOW REPLICA STATUS" that aren't covered by other tests.
func TestShowReplicaStatus(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)

	// Assert that very long hostnames are handled correctly
	longHostname := "really.really.really.really.long.host.name.012345678901234567890123456789012345678901234567890123456789.com"
	replicaDatabase.MustExec(fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s';", longHostname))
	status := showReplicaStatus(t)
	require.Equal(t, longHostname, status["Source_Host"])
}

// TestStopReplica tests that STOP REPLICA correctly stops the replication process, and that
// warnings are logged when STOP REPLICA is invoked when replication is not running.
func TestStopReplica(t *testing.T) {
	defer teardown(t)
	startSqlServersWithSystemVars(t, duckReplicaSystemVars)

	// STOP REPLICA logs a warning if replication is not running
	replicaDatabase.MustExec("STOP REPLICA;")
	assertWarning(t, replicaDatabase, 3084, "Replication thread(s) for channel '' are already stopped.")

	// Start replication with bad connection params
	replicaDatabase.MustExec("CHANGE REPLICATION SOURCE TO SOURCE_HOST='doesnotexist', SOURCE_PORT=111, SOURCE_USER='nobody';")
	replicaDatabase.MustExec("START REPLICA;")
	time.Sleep(200 * time.Millisecond)
	status := showReplicaStatus(t)
	require.Equal(t, "Connecting", status["Replica_IO_Running"])
	require.Equal(t, "Yes", status["Replica_SQL_Running"])

	// STOP REPLICA works when replication cannot establish a connection
	replicaDatabase.MustExec("STOP REPLICA;")
	status = showReplicaStatus(t)
	require.Equal(t, "No", status["Replica_IO_Running"])
	require.Equal(t, "No", status["Replica_SQL_Running"])

	// START REPLICA and verify status
	startReplicationAndCreateTestDb(t, mySqlPort)
	time.Sleep(100 * time.Millisecond)
	status = showReplicaStatus(t)
	require.True(t, status["Replica_IO_Running"] == "Connecting" || status["Replica_IO_Running"] == "Yes")
	require.Equal(t, "Yes", status["Replica_SQL_Running"])

	// STOP REPLICA stops replication when it is running and connected to the source
	replicaDatabase.MustExec("STOP REPLICA;")
	status = showReplicaStatus(t)
	require.Equal(t, "No", status["Replica_IO_Running"])
	require.Equal(t, "No", status["Replica_SQL_Running"])

	// STOP REPLICA logs a warning if replication is not running
	replicaDatabase.MustExec("STOP REPLICA;")
	assertWarning(t, replicaDatabase, 3084, "Replication thread(s) for channel '' are already stopped.")
}

// TestForeignKeyChecks tests that foreign key constraints replicate correctly when foreign key checks are
// enabled and disabled.
func TestForeignKeyChecks(t *testing.T) {
	t.SkipNow()

	defer teardown(t)
	startSqlServersWithSystemVars(t, duckReplicaSystemVars)
	startReplicationAndCreateTestDb(t, mySqlPort)

	// Test that we can execute statement-based replication that requires foreign_key_checks
	// being turned off (referenced table doesn't exist yet).
	primaryDatabase.MustExec("SET foreign_key_checks = 0;")
	primaryDatabase.MustExec("CREATE TABLE t1 (pk int primary key, color varchar(100), FOREIGN KEY (color) REFERENCES colors(name));")
	primaryDatabase.MustExec("CREATE TABLE colors (name varchar(100) primary key);")
	primaryDatabase.MustExec("SET foreign_key_checks = 1;")

	// Insert a record with foreign key checks enabled
	primaryDatabase.MustExec("START TRANSACTION;")
	primaryDatabase.MustExec("INSERT INTO colors VALUES ('green'), ('red'), ('blue');")
	primaryDatabase.MustExec("INSERT INTO t1 VALUES (1, 'red'), (2, 'green');")
	primaryDatabase.MustExec("COMMIT;")

	// Test the Insert path with foreign key checks turned off
	primaryDatabase.MustExec("START TRANSACTION;")
	primaryDatabase.MustExec("SET foreign_key_checks = 0;")
	primaryDatabase.MustExec("INSERT INTO t1 VALUES (3, 'not-a-color');")
	primaryDatabase.MustExec("COMMIT;")

	// Test the Update and Delete paths with foreign key checks turned off
	primaryDatabase.MustExec("START TRANSACTION;")
	primaryDatabase.MustExec("DELETE FROM colors WHERE name='red';")
	primaryDatabase.MustExec("UPDATE t1 SET color='still-not-a-color' WHERE pk=2;")
	primaryDatabase.MustExec("COMMIT;")

	// Verify the changes on the replica
	waitForReplicaToCatchUp(t)
	rows, err := replicaDatabase.Queryx("select * from db01.t1 order by pk;")
	require.NoError(t, err)
	row := convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "1", row["pk"])
	require.Equal(t, "red", row["color"])
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "2", row["pk"])
	require.Equal(t, "still-not-a-color", row["color"])
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "3", row["pk"])
	require.Equal(t, "not-a-color", row["color"])
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())

	rows, err = replicaDatabase.Queryx("select * from db01.colors order by name;")
	require.NoError(t, err)
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "blue", row["name"])
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "green", row["name"])
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())
}

// TestCharsetsAndCollations tests that we can successfully replicate data using various charsets and collations.
func TestCharsetsAndCollations(t *testing.T) {
	defer teardown(t)
	startSqlServersWithSystemVars(t, duckReplicaSystemVars)
	startReplicationAndCreateTestDb(t, mySqlPort)

	// Use non-default charset/collations to create data on the primary
	primaryDatabase.MustExec("CREATE TABLE t1 (pk int primary key, c1 varchar(255) COLLATE ascii_general_ci, c2 varchar(255) COLLATE utf16_general_ci);")
	primaryDatabase.MustExec("insert into t1 values (1, \"one\", \"one\");")

	// Verify on the replica
	waitForReplicaToCatchUp(t)
	rows, err := replicaDatabase.Queryx("show create table db01.t1;")
	require.NoError(t, err)
	row := convertMapScanResultToStrings(readNextRow(t, rows))
	require.Contains(t, row["Create Table"], "ascii_general_ci")
	require.Contains(t, row["Create Table"], "utf16_general_ci")
	require.NoError(t, rows.Close())

	rows, err = replicaDatabase.Queryx("select * from db01.t1;")
	require.NoError(t, err)
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "one", row["c1"])
	require.Equal(t, "\x00o\x00n\x00e", row["c2"])
	require.NoError(t, rows.Close())
}

//
// Test Helper Functions
//

// waitForReplicaToCatchUp waits (up to 30s) for the replica to catch up with the primary database. The
// lag is measured by checking that gtid_executed is the same on the primary and replica.
func waitForReplicaToCatchUp(t *testing.T) {
	timeLimit := 30 * time.Second

	endTime := time.Now().Add(timeLimit)
	for time.Now().Before(endTime) {
		replicaGtid := queryGtid(t, replicaDatabase)
		primaryGtid := queryGtid(t, primaryDatabase)

		if primaryGtid == replicaGtid {
			return
		} else {
			fmt.Printf("primary and replica not in sync yet... (primary: %s, replica: %s)\n", primaryGtid, replicaGtid)
			time.Sleep(250 * time.Millisecond)
		}
	}

	// Log some status of the replica, before failing the test
	outputShowReplicaStatus(t)
	t.Fatal("primary and replica did not synchronize within " + timeLimit.String())
}

// outputShowReplicaStatus prints out replica status information. This is useful for debugging
// replication failures in tests since status will show whether the replica is successfully connected,
// any recent errors, and what GTIDs have been executed.
func outputShowReplicaStatus(t *testing.T) {
	newRows, err := replicaDatabase.Queryx("show replica status;")
	require.NoError(t, err)
	allNewRows := readAllRowsIntoMaps(t, newRows)
	fmt.Printf("\n\nSHOW REPLICA STATUS: %v\n", allNewRows)
}

// waitForReplicaToReachGtid waits (up to 10s) for the replica's @@gtid_executed sys var to show that
// it has executed the |target| gtid transaction number.
func waitForReplicaToReachGtid(t *testing.T, target int) {
	timeLimit := 10 * time.Second
	endTime := time.Now().Add(timeLimit)
	for time.Now().Before(endTime) {
		time.Sleep(250 * time.Millisecond)
		replicaGtid := queryGtid(t, replicaDatabase)

		if replicaGtid != "" {
			components := strings.Split(replicaGtid, ":")
			require.Equal(t, 2, len(components))
			sourceGtid := components[1]
			if strings.Contains(sourceGtid, "-") {
				gtidRange := strings.Split(sourceGtid, "-")
				require.Equal(t, 2, len(gtidRange))
				sourceGtid = gtidRange[1]
			}

			i, err := strconv.Atoi(sourceGtid)
			require.NoError(t, err)
			if i >= target {
				return
			}
		}

		fmt.Printf("replica has not reached transaction %d yet; currently at: %s \n", target, replicaGtid)
	}

	t.Fatal("replica did not reach target GTID within " + timeLimit.String())
}

// assertWarning asserts that the specified |database| has a warning with |code| and |message|,
// otherwise it will fail the current test.
func assertWarning(t *testing.T, database *sqlx.DB, code int, message string) {
	rows, err := database.Queryx("SHOW WARNINGS;")
	require.NoError(t, err)
	warning := convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, strconv.Itoa(code), warning["Code"])
	require.Equal(t, message, warning["Message"])
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())
}

func queryGtid(t *testing.T, database *sqlx.DB) string {
	gtidEnabled := getGtidEnabled()
	isPrimary := database == primaryDatabase
	if gtidEnabled {
		rows, err := database.Queryx("SELECT @@global.gtid_executed as gtid_executed;")
		require.NoError(t, err)
		defer rows.Close()
		row := convertMapScanResultToStrings(readNextRow(t, rows))
		if row["gtid_executed"] == nil {
			t.Fatal("no value for @@GLOBAL.gtid_executed")
		}
		return row["gtid_executed"].(string)
	} else if isPrimary {
		sourceLogFile, sourceLogPos := getPrimaryLogPosition(t, gtidEnabled)
		return fmt.Sprintf("%s:%s", sourceLogFile, sourceLogPos)
	} else {
		sourceLogFile, sourceLogPos := getReplicaLogPosition(t)
		return fmt.Sprintf("%s:%s", sourceLogFile, sourceLogPos)
	}
}

func readNextRow(t *testing.T, rows *sqlx.Rows) map[string]interface{} {
	row := make(map[string]interface{})
	require.True(t, rows.Next())
	err := rows.MapScan(row)
	require.NoError(t, err)
	return row
}

// readAllRowsIntoMaps reads all data from |rows| and returns a slice of maps, where each key
// in the map is the field name, and each value is the string representation of the field value.
func readAllRowsIntoMaps(t *testing.T, rows *sqlx.Rows) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)
	for {
		row := make(map[string]interface{})
		if rows.Next() == false {
			return result
		}
		err := rows.MapScan(row)
		require.NoError(t, err)
		row = convertMapScanResultToStrings(row)
		result = append(result, row)
	}
}

// readAllRowsIntoSlices reads all data from |rows| and returns a slice of slices, with
// all values converted to strings.
func readAllRowsIntoSlices(t *testing.T, rows *sqlx.Rows) [][]any {
	result := make([][]any, 0)
	for {
		if rows.Next() == false {
			return result
		}
		row, err := rows.SliceScan()
		require.NoError(t, err)
		row = convertSliceScanResultToStrings(row)
		result = append(result, row)
	}
}

// startSqlServers starts a MySQL server and a Dolt sql-server for use in tests.
func startSqlServers(t *testing.T) {
	startSqlServersWithSystemVars(t, nil)
}

// startSqlServersWithSystemVars starts a MySQL server and a my-sql-server for use in tests. Before the
// my-sql-server is started, the specified |persistentSystemVars| are persisted in the  my-sql-server's
// local configuration. These are useful when you need to set system variables that must be available when the
// sql-server starts up, such as replication system variables.
func startSqlServersWithSystemVars(t *testing.T, persistentSystemVars map[string]string) {
	if runtime.GOOS == "windows" {
		t.Skip("Skipping binlog replication integ tests on Windows OS")
	} else if runtime.GOOS == "darwin" && os.Getenv("CI") == "true" {
		t.Skip("Skipping binlog replication integ tests in CI environment on Mac OS")
	}

	testDir = test.CreateTestDir(t)
	var err error

	// Start up primary and replica databases
	mySqlPort, mySqlContainer, err = startMySqlServer(testDir)
	require.NoError(t, err)
	testEnv := test.NewTestEnv()
	setupTestEnv(testEnv)
	err = test.StartDuckSqlServer(t, testDir, persistentSystemVars, testEnv)
	require.NoError(t, err)
	loadEnvFromTestEnv(testEnv)
}

// stopMySqlServer stops the running MySQL server. If any errors are encountered while stopping
// the MySQL server, this function will fail the current test.
func stopMySqlServer(t *testing.T) error {
	cmd := exec.Command("docker", "kill", mySqlContainer)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("unable to stop MySQL container: %v - %s", err, output)
	}
	return nil
}

func getPrimaryLogPosition(t *testing.T, gtidEnabled bool) (string, string) {
	rows, err := primaryDatabase.Queryx("SHOW BINARY LOG STATUS;")
	require.NoError(t, err)
	primaryStatus := convertMapScanResultToStrings(readNextRow(t, rows))
	sourceLogFile := primaryStatus["File"].(string)
	sourceLogPos := primaryStatus["Position"].(string)
	require.NoError(t, rows.Close())

	return sourceLogFile, sourceLogPos
}

func getReplicaLogPosition(t *testing.T) (string, string) {
	rows, err := replicaDatabase.Queryx("SHOW REPLICA STATUS;")
	require.NoError(t, err)
	replicaStatus := convertMapScanResultToStrings(readNextRow(t, rows))
	executedGtidSet := replicaStatus["Executed_Gtid_Set"].(string)
	require.NoError(t, rows.Close())

	// the executedGtidSet is like the format of "binlog.000002:6757", split it into file and pos
	parts := strings.Split(executedGtidSet, ":")
	require.Equal(t, 2, len(parts))
	sourceLogFile := parts[0]
	sourceLogPos := parts[1]
	return sourceLogFile, sourceLogPos
}

// startReplication configures the replication source on the replica and runs the START REPLICA statement.
func startReplication(t *testing.T, port int) {
	gtidEnabled := getGtidEnabled()

	// If GTID is not enabled, we should get the log position by "SHOW MASTER STATUS" first,
	// if it failed, try "SHOW BINARY LOG STATUS" instead. Then we extract the log position from
	// the result and use it as the source_log_pos when starting the replica.
	sourceLogFile := ""
	sourceLogPos := ""
	if !gtidEnabled {
		sourceLogFile, sourceLogPos = getPrimaryLogPosition(t, gtidEnabled)
	}

	cmdStr := fmt.Sprintf("CHANGE REPLICATION SOURCE TO "+
		"SOURCE_HOST='localhost', "+
		"SOURCE_USER='replicator', "+
		"SOURCE_PASSWORD='Zqr8_blrGm1!', "+
		"SOURCE_PORT=%v, "+
		"SOURCE_AUTO_POSITION=1, "+
		"SOURCE_CONNECT_RETRY=5", port)
	if !gtidEnabled {
		cmdStr += fmt.Sprintf(", SOURCE_LOG_FILE='%s', SOURCE_LOG_POS=%s", sourceLogFile, sourceLogPos)
	}
	replicaDatabase.MustExec(cmdStr)
	replicaDatabase.MustExec("start replica;")
}

// startReplicationAndCreateTestDb starts up replication on the replica, connecting to |port| on the primary,
// creates the test database, db01, on the primary, and ensures it gets replicated to the replica.
func startReplicationAndCreateTestDb(t *testing.T, port int) {
	startReplicationAndCreateTestDbWithDelay(t, port, 100*time.Millisecond)
}

// startReplicationAndCreateTestDbWithDelay starts up replication on the replica, connecting to |port| on the primary,
// pauses for |delay| before creating the test database, db01, on the primary, and ensures it
// gets replicated to the replica.
func startReplicationAndCreateTestDbWithDelay(t *testing.T, port int, delay time.Duration) {
	startReplication(t, port)
	time.Sleep(delay)

	// Look to see if the test database, db01, has been created yet. If not, create it and wait for it to
	// replicate to the replica. Note that when re-starting replication in certain tests, we can't rely on
	// the replica to contain all GTIDs (i.e. Dolt -> MySQL replication when restarting the replica, since
	// Dolt doesn't yet resend events that occurred while the replica wasn't connected).
	dbNames := mustListDatabases(t, primaryDatabase)
	if !slices.Contains(dbNames, "db01") {
		primaryDatabase.MustExec("create database db01;")
		waitForReplicaToCatchUp(t)
	}
	primaryDatabase.MustExec("use db01;")
	_, _ = replicaDatabase.Exec("use db01;")
}

func assertCreateTableStatement(t *testing.T, database *sqlx.DB, table string, expectedStatement string) {
	rows, err := database.Queryx("show create table db01." + table + ";")
	require.NoError(t, err)
	var actualTable, actualStatement string
	require.True(t, rows.Next())
	err = rows.Scan(&actualTable, &actualStatement)
	require.NoError(t, err)
	require.Equal(t, table, actualTable)
	require.NotNil(t, actualStatement)
	actualStatement = sanitizeCreateTableString(actualStatement)
	require.Equal(t, expectedStatement, actualStatement)
}

func sanitizeCreateTableString(statement string) string {
	statement = strings.ReplaceAll(statement, "`", "")
	statement = strings.ReplaceAll(statement, "\n", "")
	regex := regexp.MustCompile("\\s+")
	return regex.ReplaceAllString(statement, " ")
}

func getGtidEnabled() bool {
	gtidEnabled := strings.ToLower(os.Getenv("GTID_ENABLED"))
	if gtidEnabled == "" {
		return true
	}
	if gtidEnabled != "true" && gtidEnabled != "false" {
		panic(fmt.Sprintf("GTID_ENABLED environment variable must be 'true' or 'false', got: %s", gtidEnabled))
	}
	return gtidEnabled == "true"
}

// startMySqlServer configures a starts a fresh MySQL server instance in a Docker container
// and returns the port it is running on. If unable to start up the MySQL server, an error is returned.
func startMySqlServer(dir string) (int, string, error) {
	mySqlPort = test.FindFreePort()

	// Use a random name for the container to avoid conflicts
	mySqlContainer = "mysql-test-" + strconv.Itoa(rand.Int())

	gtidEnabled := getGtidEnabled()

	// Build the Docker command to start the MySQL container
	cmdArgs := []string{
		"run",
		"--rm",                                  // Remove the container when it stops
		"-d",                                    // Run in detached mode
		"-p", fmt.Sprintf("%d:3306", mySqlPort), // Map the container's port 3306 to the host's mySqlPort
		"-e", "MYSQL_ROOT_PASSWORD=password", // Set the root password
		"-v", fmt.Sprintf("%s:/var/lib/mysql", dir), // Mount a volume for data persistence
		"--name", mySqlContainer, // Give the container a name
		"mysql:latest", // Use the latest MySQL image
		"mysqld",
	}
	if gtidEnabled {
		cmdArgs = append(cmdArgs, "--gtid_mode=ON", "--enforce-gtid-consistency=ON")
	} else {
		cmdArgs = append(cmdArgs, "--gtid_mode=OFF", "--enforce-gtid-consistency=OFF")
	}

	// Build the Docker command to start the MySQL container
	cmd := exec.Command("docker", cmdArgs...)

	// Execute the Docker command
	output, err := cmd.CombinedOutput()
	if err != nil {
		return -1, "", fmt.Errorf("unable to start MySQL container: %v - %s", err, output)
	}

	// Wait for the MySQL server to be ready
	dsn := fmt.Sprintf("root:password@tcp(127.0.0.1:%v)/", mySqlPort)
	primaryDatabase = sqlx.MustOpen("mysql", dsn)

	err = test.WaitForSqlServerToStart(primaryDatabase)
	if err != nil {
		return -1, "", err
	}

	// Ensure the replication user exists with the right grants
	mustCreateReplicatorUser(primaryDatabase)

	fmt.Printf("MySQL server started in container %s on port %v \n", mySqlContainer, mySqlPort)

	return mySqlPort, mySqlContainer, nil
}

// directoryExists returns true if the specified |path| is to a directory that exists, otherwise,
// if the path doesn't exist or isn't a directory, false is returned.
func directoryExists(path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return info.IsDir()
}

// mustCreateReplicatorUser creates the replicator user on the specified |db| and grants them replication slave privs.
func mustCreateReplicatorUser(db *sqlx.DB) {
	db.MustExec("CREATE USER if not exists 'replicator'@'%' IDENTIFIED BY 'Zqr8_blrGm1!';")
	db.MustExec("GRANT REPLICATION SLAVE ON *.* TO 'replicator'@'%';")
}

// printFile opens the specified filepath |path| and outputs the contents of that file to stdout.
func printFile(path string) {
	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("Unable to open file: %s \n", err)
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		s, err := reader.ReadString(byte('\n'))
		if err != nil {
			if err == io.EOF {
				break
			} else {
				panic(err)
			}
		}
		fmt.Print(s)
	}
	fmt.Println()
}

// requireReplicaResults runs the specified |query| on the replica database and asserts that the results match
// |expectedResults|. Note that the actual results are converted to string values in almost all cases, due to
// limitations in the SQL library we use to query the replica database, so |expectedResults| should generally
// be expressed in strings.
func requireReplicaResults(t *testing.T, query string, expectedResults [][]any) {
	requireResults(t, replicaDatabase, query, expectedResults)
}

// requireReplicaResults runs the specified |query| on the primary database and asserts that the results match
// |expectedResults|. Note that the actual results are converted to string values in almost all cases, due to
// limitations in the SQL library we use to query the replica database, so |expectedResults| should generally
// be expressed in strings.
func requirePrimaryResults(t *testing.T, query string, expectedResults [][]any) {
	requireResults(t, primaryDatabase, query, expectedResults)
}

func requireResults(t *testing.T, db *sqlx.DB, query string, expectedResults [][]any) {
	rows, err := db.Queryx(query)
	require.NoError(t, err)
	allRows := readAllRowsIntoSlices(t, rows)
	require.Equal(t, len(expectedResults), len(allRows), "Expected %v, got %v", expectedResults, allRows)
	for i := range expectedResults {
		require.Equal(t, expectedResults[i], allRows[i], "Expected %v, got %v", expectedResults[i], allRows[i])
	}
	require.NoError(t, rows.Close())
}

// queryReplicaStatus returns the results of `SHOW REPLICA STATUS` as a map, for the replica
// database. If any errors are encountered, this function will fail the current test.
func queryReplicaStatus(t *testing.T) map[string]any {
	rows, err := replicaDatabase.Queryx("SHOW REPLICA STATUS;")
	require.NoError(t, err)
	status := convertMapScanResultToStrings(readNextRow(t, rows))
	require.NoError(t, rows.Close())
	return status
}

// mustListDatabases returns a string slice of the databases (i.e. schemas) available on the specified |db|. If
// any errors are encountered, this function will fail the current test.
func mustListDatabases(t *testing.T, db *sqlx.DB) []string {
	rows, err := db.Queryx("show databases;")
	require.NoError(t, err)
	allRows := readAllRowsIntoSlices(t, rows)
	dbNames := make([]string, len(allRows))
	for i, row := range allRows {
		dbNames[i] = row[0].(string)
	}
	return dbNames
}
