//go:build unix

package docker_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const processWaitTimeout = 5 * time.Second

type entrypointFixture struct {
	cmd               *exec.Cmd
	stdout            *bytes.Buffer
	stderr            *bytes.Buffer
	homeDir           string
	serverPIDFile     string
	loggerPIDFile     string
	readinessPIDFile  string
	forwardedSignals  string
	entrypointPIDFile string
	logPipe           string
	pidPublishStarted string
	pidPublishRelease string
}

func TestDockerfileUsesExecFormEntrypoint(t *testing.T) {
	contents, err := os.ReadFile("Dockerfile")
	require.NoError(t, err)
	require.Contains(t, string(contents), `ENTRYPOINT ["/home/admin/entrypoint.sh"]`)
	require.NotContains(t, string(contents), "ENTRYPOINT /home/admin/entrypoint.sh")
}

func TestEntrypointKeepsFIFOOpenAcrossChildLaunches(t *testing.T) {
	contents, err := os.ReadFile("entrypoint.sh")
	require.NoError(t, err)
	launch := string(contents)
	launch = launch[strings.Index(launch, "run_server_in_background()"):]
	guardOpen := strings.Index(launch, `exec 9<> "${LOG_PIPE}"`)
	serverLaunch := strings.Index(launch, `myduckserver "${SERVER_OPTIONS[@]}" 9>&-`)
	loggerLaunch := strings.Index(launch, `tee -a "${LOG_PATH}/server.log" 9>&-`)
	logReady := strings.Index(launch, `[[ ! -s "${LOG_PATH}/server.log" ]]`)
	guardClose := strings.Index(launch, "close_log_pipe_guard")
	require.GreaterOrEqual(t, guardOpen, 0)
	require.Greater(t, serverLaunch, guardOpen)
	require.Greater(t, loggerLaunch, serverLaunch)
	require.Greater(t, logReady, loggerLaunch)
	require.Greater(t, guardClose, logReady)
}

func TestEntrypointPreservesLegacyInitBehavior(t *testing.T) {
	contents, err := os.ReadFile("entrypoint.sh")
	require.NoError(t, err)
	script := string(contents)
	require.Contains(t, script, `export INIT_SQLS_DIR="/docker-entrypoint-initdb.d"`)
	require.Contains(t, script, `--file="$file" || true`)
	require.Contains(t, script, `-f "$file" || true`)
}

func TestEntrypointGoServerHelper(t *testing.T) {
	if os.Getenv("FAKE_SERVER_IMPL") != "go" {
		return
	}

	shutdownSignals := make(chan os.Signal, 1)
	signal.Notify(shutdownSignals, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	require.NoError(t, os.WriteFile(os.Getenv("FAKE_SERVER_PID_FILE"), []byte(strconv.Itoa(os.Getpid())+"\n"), 0o644))
	fmt.Println("server-started")
	received := <-shutdownSignals
	name := map[os.Signal]string{
		os.Interrupt:    "INT",
		syscall.SIGTERM: "TERM",
		syscall.SIGQUIT: "QUIT",
	}[received]
	require.NotEmpty(t, name)
	require.NoError(t, os.WriteFile(os.Getenv("FAKE_SIGNAL_FILE"), []byte(name+"\n"), 0o644))
	fmt.Println("server-drained")
	time.Sleep(300 * time.Millisecond)
	os.Exit(0)
}

func TestEntrypointForwardsOneSignalAndReapsServerAndLogger(t *testing.T) {
	fixture := newEntrypointFixture(t, "ready")
	require.NoError(t, fixture.cmd.Start())
	t.Cleanup(func() { cleanupCommand(fixture.cmd) })

	serverPID := waitForPIDFile(t, fixture.serverPIDFile)
	loggerPID := waitForPIDFile(t, fixture.loggerPIDFile)
	entrypointPID := waitForPIDFile(t, fixture.entrypointPIDFile)
	require.Equal(t, serverPID, entrypointPID)
	require.NotEqual(t, serverPID, loggerPID)

	require.NoError(t, fixture.cmd.Process.Signal(syscall.SIGTERM))
	waitForFile(t, fixture.forwardedSignals)
	// A repeated stop request must not interrupt PID 1 while it is reaping.
	require.NoError(t, fixture.cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, waitForCommand(fixture.cmd))

	require.Equal(t, "TERM\n", readFile(t, fixture.forwardedSignals))
	require.Contains(t, fixture.stdout.String(), "server-drained")
	serverLog := readFile(t, filepath.Join(fixture.homeDir, "log", "server.log"))
	require.Contains(t, serverLog, "server-started")
	require.Contains(t, serverLog, "server-drained")
	require.NoFileExists(t, fixture.entrypointPIDFile)
	require.NoFileExists(t, fixture.logPipe)
	requireProcessGone(t, serverPID)
	requireProcessGone(t, loggerPID)
}

func TestEntrypointTermDuringReadinessReapsAllChildren(t *testing.T) {
	fixture := newEntrypointFixture(t, "block-readiness")
	require.NoError(t, fixture.cmd.Start())
	t.Cleanup(func() { cleanupCommand(fixture.cmd) })

	serverPID := waitForPIDFile(t, fixture.serverPIDFile)
	loggerPID := waitForPIDFile(t, fixture.loggerPIDFile)
	readinessPID := waitForPIDFile(t, fixture.readinessPIDFile)
	require.NoError(t, fixture.cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, waitForCommand(fixture.cmd))

	require.Equal(t, "TERM\n", readFile(t, fixture.forwardedSignals))
	require.NoFileExists(t, fixture.entrypointPIDFile)
	require.NoFileExists(t, fixture.logPipe)
	requireProcessGone(t, serverPID)
	requireProcessGone(t, loggerPID)
	requireProcessGone(t, readinessPID)
}

func TestEntrypointTermDuringResistantReadinessDrainsServerAndReapsSetup(t *testing.T) {
	fixture := newEntrypointFixture(t, "ignore-readiness-term")
	require.NoError(t, fixture.cmd.Start())
	t.Cleanup(func() { cleanupCommand(fixture.cmd) })

	serverPID := waitForPIDFile(t, fixture.serverPIDFile)
	loggerPID := waitForPIDFile(t, fixture.loggerPIDFile)
	readinessPID := waitForPIDFile(t, fixture.readinessPIDFile)
	require.NoError(t, fixture.cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, waitForCommand(fixture.cmd))

	require.Equal(t, "TERM\n", readFile(t, fixture.forwardedSignals))
	require.Contains(t, fixture.stdout.String(), "server-drained")
	requireProcessGone(t, serverPID)
	requireProcessGone(t, loggerPID)
	requireProcessGone(t, readinessPID)
}

func TestEntrypointTermBeforePIDPublicationReapsChildren(t *testing.T) {
	fixture := newEntrypointFixture(t, "ready", "FAKE_MV_MODE=block")
	require.NoError(t, fixture.cmd.Start())
	t.Cleanup(func() { cleanupCommand(fixture.cmd) })

	serverPID := waitForPIDFile(t, fixture.serverPIDFile)
	loggerPID := waitForPIDFile(t, fixture.loggerPIDFile)
	waitForFile(t, fixture.pidPublishStarted)
	require.NoFileExists(t, fixture.entrypointPIDFile)
	require.NoError(t, fixture.cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, os.WriteFile(fixture.pidPublishRelease, []byte("release\n"), 0o644))
	require.NoError(t, waitForCommand(fixture.cmd))

	require.Equal(t, "TERM\n", readFile(t, fixture.forwardedSignals))
	require.NoFileExists(t, fixture.entrypointPIDFile)
	require.NoFileExists(t, fixture.logPipe)
	requireProcessGone(t, serverPID)
	requireProcessGone(t, loggerPID)
}

func TestEntrypointPreservesEarlyServerFailureStatus(t *testing.T) {
	fixture := newEntrypointFixture(t, "not-ready", "FAKE_SERVER_MODE=exit-23")
	require.NoError(t, fixture.cmd.Start())
	t.Cleanup(func() { cleanupCommand(fixture.cmd) })
	serverPID := waitForPIDFile(t, fixture.serverPIDFile)
	loggerPID := waitForPIDFile(t, fixture.loggerPIDFile)

	err := waitForCommand(fixture.cmd)
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, 23, exitErr.ExitCode())
	require.Contains(t, fixture.stdout.String(), "died during startup")
	require.NoFileExists(t, fixture.entrypointPIDFile)
	require.NoFileExists(t, fixture.logPipe)
	requireProcessGone(t, serverPID)
	requireProcessGone(t, loggerPID)
}

func TestEntrypointForwardsInterruptAndQuitToGoServer(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		signal syscall.Signal
		want   string
	}{
		{name: "interrupt", signal: syscall.SIGINT, want: "INT\n"},
		{name: "quit", signal: syscall.SIGQUIT, want: "QUIT\n"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newEntrypointFixture(t, "ready", "FAKE_SERVER_IMPL=go")
			require.NoError(t, fixture.cmd.Start())
			t.Cleanup(func() { cleanupCommand(fixture.cmd) })
			serverPID := waitForPIDFile(t, fixture.serverPIDFile)
			loggerPID := waitForPIDFile(t, fixture.loggerPIDFile)

			require.NoError(t, fixture.cmd.Process.Signal(testCase.signal))
			require.NoError(t, waitForCommand(fixture.cmd))

			require.Equal(t, testCase.want, readFile(t, fixture.forwardedSignals))
			requireProcessGone(t, serverPID)
			requireProcessGone(t, loggerPID)
		})
	}
}

func newEntrypointFixture(t *testing.T, mysqlshMode string, extraEnv ...string) *entrypointFixture {
	t.Helper()
	homeDir := t.TempDir()
	fakeBin := filepath.Join(homeDir, "bin")
	require.NoError(t, os.MkdirAll(fakeBin, 0o755))

	serverPIDFile := filepath.Join(homeDir, "server.pid")
	loggerPIDFile := filepath.Join(homeDir, "logger.pid")
	readinessPIDFile := filepath.Join(homeDir, "readiness.pid")
	forwardedSignals := filepath.Join(homeDir, "signals.log")
	entrypointPIDFile := filepath.Join(homeDir, "log", "myduck.pid")
	logPipe := filepath.Join(homeDir, "log", "myduck.log.pipe")
	pidPublishStarted := filepath.Join(homeDir, "pid-publish.started")
	pidPublishRelease := filepath.Join(homeDir, "pid-publish.release")
	testBinary, err := filepath.Abs(os.Args[0])
	require.NoError(t, err)

	writeExecutable(t, filepath.Join(fakeBin, "myduckserver"), `#!/bin/bash
if [[ "$FAKE_SERVER_IMPL" == "go" ]]; then
  exec "$FAKE_TEST_BINARY" -test.run '^TestEntrypointGoServerHelper$'
fi
on_signal() {
  printf '%s\n' "$1" >> "$FAKE_SIGNAL_FILE"
  printf 'server-drained\n'
  sleep "${FAKE_SERVER_EXIT_DELAY:-0.1}"
  exit 0
}
printf '%s\n' "$$" > "$FAKE_SERVER_PID_FILE"
if [[ "$FAKE_SERVER_MODE" == "exit-23" ]]; then
  printf 'server-started\n'
  exit 23
fi
trap 'on_signal TERM' TERM
trap 'on_signal INT' INT
trap 'on_signal QUIT' QUIT
printf 'server-started\n'
while true; do sleep 0.05; done
`)

	writeExecutable(t, filepath.Join(fakeBin, "mysqlsh"), `#!/bin/bash
is_file=false
for arg in "$@"; do
  case "$arg" in
    --file=*) is_file=true ;;
  esac
done
if [[ "$FAKE_MYSQLSH_MODE" == "block-readiness" && "$is_file" == "false" ]]; then
  printf '%s\n' "$$" > "$FAKE_READINESS_PID_FILE"
  trap 'exit 0' TERM INT QUIT
  while true; do sleep 0.05; done
fi
if [[ "$FAKE_MYSQLSH_MODE" == "ignore-readiness-term" && "$is_file" == "false" ]]; then
  printf '%s\n' "$$" > "$FAKE_READINESS_PID_FILE"
  trap '' TERM INT QUIT
  while true; do sleep 0.05; done
fi
if [[ "$FAKE_MYSQLSH_MODE" == "not-ready" && "$is_file" == "false" ]]; then
  exit 1
fi
exit 0
`)

	writeExecutable(t, filepath.Join(fakeBin, "psql"), "#!/bin/bash\nexit 0\n")
	realTee, err := exec.LookPath("tee")
	require.NoError(t, err)
	writeExecutable(t, filepath.Join(fakeBin, "tee"), fmt.Sprintf(`#!/bin/bash
printf '%%s\n' "$$" > "$FAKE_LOGGER_PID_FILE"
exec %q "$@"
`, realTee))
	realMv, err := exec.LookPath("mv")
	require.NoError(t, err)
	writeExecutable(t, filepath.Join(fakeBin, "mv"), fmt.Sprintf(`#!/bin/bash
if [[ "$FAKE_MV_MODE" == "block" ]]; then
  printf 'started\n' > "$FAKE_PID_PUBLISH_STARTED"
  until [[ -f "$FAKE_PID_PUBLISH_RELEASE" ]]; do sleep 0.01; done
fi
exec %q "$@"
`, realMv))

	entrypointPath, err := filepath.Abs("entrypoint.sh")
	require.NoError(t, err)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd := exec.Command("/bin/bash", entrypointPath)
	cmd.Env = []string{
		"HOME=" + homeDir,
		"PATH=" + fakeBin + ":/usr/bin:/bin",
		"SETUP_MODE=SERVER",
		"FAKE_MYSQLSH_MODE=" + mysqlshMode,
		"FAKE_SERVER_PID_FILE=" + serverPIDFile,
		"FAKE_LOGGER_PID_FILE=" + loggerPIDFile,
		"FAKE_READINESS_PID_FILE=" + readinessPIDFile,
		"FAKE_SIGNAL_FILE=" + forwardedSignals,
		"FAKE_SERVER_EXIT_DELAY=0.3",
		"FAKE_PID_PUBLISH_STARTED=" + pidPublishStarted,
		"FAKE_PID_PUBLISH_RELEASE=" + pidPublishRelease,
		"FAKE_TEST_BINARY=" + testBinary,
	}
	cmd.Env = append(cmd.Env, extraEnv...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("entrypoint stdout:\n%s", stdout.String())
			t.Logf("entrypoint stderr:\n%s", stderr.String())
		}
	})

	return &entrypointFixture{
		cmd:               cmd,
		stdout:            stdout,
		stderr:            stderr,
		homeDir:           homeDir,
		serverPIDFile:     serverPIDFile,
		loggerPIDFile:     loggerPIDFile,
		readinessPIDFile:  readinessPIDFile,
		forwardedSignals:  forwardedSignals,
		entrypointPIDFile: entrypointPIDFile,
		logPipe:           logPipe,
		pidPublishStarted: pidPublishStarted,
		pidPublishRelease: pidPublishRelease,
	}
}

func writeExecutable(t *testing.T, path, contents string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o755))
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(processWaitTimeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.FailNow(t, "timed out waiting for file", path)
}

func waitForPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(processWaitTimeout)
	for time.Now().Before(deadline) {
		contents, err := os.ReadFile(path)
		if err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(contents)))
			if parseErr == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.FailNow(t, "timed out waiting for complete PID file", path)
	return 0
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(contents)
}

func waitForCommand(cmd *exec.Cmd) error {
	result := make(chan error, 1)
	go func() { result <- cmd.Wait() }()
	select {
	case err := <-result:
		return err
	case <-time.After(processWaitTimeout):
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-result
		return fmt.Errorf("command did not exit within %s", processWaitTimeout)
	}
}

func requireProcessGone(t *testing.T, pid int) {
	t.Helper()
	require.Error(t, syscall.Kill(pid, 0), "process %d is still alive", pid)
}

func cleanupCommand(cmd *exec.Cmd) {
	if cmd.Process == nil || cmd.ProcessState != nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	_ = cmd.Wait()
}
