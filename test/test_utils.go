package test

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

const (
	duckSubdir = "duck"
)

// Create a struct that stores all these values:
type TestEnv struct {
	MySqlContainer                      string
	MySqlPort, DuckPort, DuckPgPort     int
	DuckProcess                         *os.Process
	DuckLogFilePath, OldDuckLogFilePath string
	DuckLogFile, MysqlLogFile           *os.File
	TestDir                             string
	OriginalWorkingDir                  string
	MyDuckServer                        *sqlx.DB
}

func NewTestEnv() *TestEnv {
	return &TestEnv{}
}

func CreateTestDir(t *testing.T) string {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("%s-%v", t.Name(), time.Now().Unix()))
	err := os.MkdirAll(testDir, 0777)
	require.NoError(t, err)
	cmd := exec.Command("chmod", "777", testDir)
	_, err = cmd.Output()
	require.NoError(t, err)
	fmt.Printf("temp dir: %v \n", testDir)
	return testDir
}

// StartDuckSqlServer starts a sql-server on a free port from the specified directory |dir|. If
// |peristentSystemVars| is populated, then those system variables will be set, persistently, for
// the database, before the sql-server is started.
func StartDuckSqlServer(t *testing.T, dir string, persistentSystemVars map[string]string, testEnv *TestEnv) error {
	dir = filepath.Join(dir, duckSubdir)
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		return err
	}

	// If we already assigned a port, re-use it. This is useful when testing restarting a primary, since
	// we want the primary to come back up on the same port, so the replica can reconnect.
	if testEnv.DuckPort < 1 {
		testEnv.DuckPort = FindFreePort()
	}
	if testEnv.DuckPgPort < 1 {
		testEnv.DuckPgPort = FindFreePort()
	}
	fmt.Printf("Starting MyDuck on port: %d, PgPort: %d, with data dir %s\n", testEnv.DuckPort, testEnv.DuckPgPort, dir)

	// take the CWD and move up four directories to find the go directory
	if testEnv.OriginalWorkingDir == "" {
		var err error
		testEnv.OriginalWorkingDir, err = os.Getwd()
		if err != nil {
			panic(err)
		}
	}
	goDirPath := filepath.Join(testEnv.OriginalWorkingDir, "..")
	err = os.Chdir(goDirPath)
	if err != nil {
		panic(err)
	}

	args := []string{"go", "run", ".",
		fmt.Sprintf("--port=%v", testEnv.DuckPort),
		fmt.Sprintf("--datadir=%s", dir),
		fmt.Sprintf("--pg-port=%v", testEnv.DuckPgPort),
		"--default-time-zone=UTC",
		"--loglevel=4", // 4: INFO, 5: DEBUG, 6: TRACE
	}

	// If we're running in CI, use a precompiled dolt binary instead of go run
	devBuildPath := initializeDevBuild(dir, goDirPath)
	if devBuildPath != "" {
		args[2] = devBuildPath
		args = args[2:]
	}
	cmd := exec.Command(args[0], args[1:]...)

	// Set a unique process group ID so that we can cleanly kill this process, as well as
	// any spawned child processes later. Mac/Unix can set the "Setpgid" field directly, but
	// on windows, this field isn't present, so we need to use reflection so that this code
	// can still compile for windows, even though we don't run it there.
	procAttr := &syscall.SysProcAttr{}
	ps := reflect.ValueOf(procAttr)
	s := ps.Elem()
	f := s.FieldByName("Setpgid")
	f.SetBool(true)
	cmd.SysProcAttr = procAttr

	// Some tests restart the Dolt sql-server, so if we have a current log file, save a reference
	// to it so we can print the results later if the test fails.
	if testEnv.DuckLogFilePath != "" {
		testEnv.OldDuckLogFilePath = testEnv.DuckLogFilePath
	}

	testEnv.DuckLogFilePath = filepath.Join(dir, fmt.Sprintf("dolt-%d.out.log", time.Now().Unix()))
	testEnv.DuckLogFile, err = os.Create(testEnv.DuckLogFilePath)
	if err != nil {
		return err
	}
	fmt.Printf("myduck logs at: %s \n", testEnv.DuckLogFilePath)
	cmd.Stdout = testEnv.DuckLogFile
	cmd.Stderr = testEnv.DuckLogFile

	defer func() {
		if r := recover(); r != nil {
			StopDuckSqlServer(t, cmd.Process)
		}
	}()
	err = cmd.Start()
	if err != nil {
		return fmt.Errorf("unable to execute command %v: %v", cmd.String(), err.Error())
	}

	fmt.Printf("MyDuck CMD: %s\n", cmd.String())

	dsn := fmt.Sprintf("%s@tcp(127.0.0.1:%v)/", "root", testEnv.DuckPort)
	testEnv.MyDuckServer = sqlx.MustOpen("mysql", dsn)

	err = WaitForSqlServerToStart(testEnv.MyDuckServer)
	if err != nil {
		return err
	}

	//mustCreateReplicatorUser(testEnv.MyDuckServer)
	fmt.Printf("MyDuck server started on port %v and pg port %v\n", testEnv.DuckPort, testEnv.DuckPgPort)

	testEnv.DuckProcess = cmd.Process
	return nil
}

// StopDuckSqlServer stops the running Dolt sql-server. If any errors are encountered while
// stopping the Dolt sql-server, this function will fail the current test.
func StopDuckSqlServer(t *testing.T, duckProcess *os.Process) {
	// Use the negative process ID so that we grab the entire process group.
	// This is necessary to kill all the processes the child spawns.
	// Note that we use os.FindProcess, instead of syscall.Kill, since syscall.Kill
	// is not available on windows.
	p, err := os.FindProcess(-duckProcess.Pid)
	require.NoError(t, err)

	err = p.Signal(syscall.SIGKILL)
	require.NoError(t, err)
	time.Sleep(250 * time.Millisecond)
}

// WaitForSqlServerToStart polls the specified database to wait for it to become available, pausing
// between retry attempts, and returning an error if it is not able to verify that the database is
// available.
func WaitForSqlServerToStart(database *sqlx.DB) error {
	fmt.Printf("Waiting for server to start...\n")
	for counter := 0; counter < 50; counter++ {
		if database.Ping() == nil {
			return nil
		}
		fmt.Printf("not up yet; waiting...\n")
		time.Sleep(500 * time.Millisecond)
	}

	return database.Ping()
}

var cachedDevBuildPath = ""

func initializeDevBuild(dir string, goDirPath string) string {
	if cachedDevBuildPath != "" {
		return cachedDevBuildPath
	}

	// If we're not in a CI environment, don't worry about building a dev build
	if os.Getenv("CI") != "true" {
		return ""
	}

	basedir := filepath.Dir(filepath.Dir(dir))
	fullpath := filepath.Join(basedir, fmt.Sprintf("dev-build-%d", os.Getpid()))

	_, err := os.Stat(fullpath)
	if err == nil {
		return fullpath
	}

	fmt.Printf("building dev build at: %s \n", fullpath)
	cmd := exec.Command("go", "build", "-o", fullpath)
	cmd.Dir = goDirPath

	output, err := cmd.CombinedOutput()
	if err != nil {
		panic("unable to build dolt for binlog integration tests: " + err.Error() + "\nFull output: " + string(output) + "\n")
	}
	cachedDevBuildPath = fullpath

	return cachedDevBuildPath
}

// FindFreePort returns an available port that can be used for a server. If any errors are
// encountered, this function will panic and fail the current test.
func FindFreePort() int {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(fmt.Sprintf("unable to find available TCP port: %v", err.Error()))
	}
	freePort := listener.Addr().(*net.TCPAddr).Port
	err = listener.Close()
	if err != nil {
		panic(fmt.Sprintf("unable to find available TCP port: %v", err.Error()))
	}

	if freePort < 0 {
		panic(fmt.Sprintf("unable to find available TCP port; found port %v", freePort))
	}

	return freePort
}
