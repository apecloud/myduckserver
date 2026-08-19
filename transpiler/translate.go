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

package transpiler

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"

	"gopkg.in/src-d/go-errors.v1"
)

const (
	cmdExit = "CMD:EXIT"
	cmdRun  = "CMD:RUN"

	resultOK  = "OK:"
	resultErr = "ERROR:"
)

var (
	errPythonProcessUnhealthy = errors.NewKind("sqlglot python process is unhealthy: %s")
)

type translateService struct {
	mu       *sync.Mutex
	pyCmd    *exec.Cmd
	pyStdin  io.Writer
	pyStdout io.Reader
	pyStderr *bytes.Buffer
}

var (
	translationSvcOnce sync.Once
	translationSvc     *translateService
)

func newTranslateService() (*translateService, error) {
	pythonPath, err := getPythonPath()
	if err != nil {
		return nil, fmt.Errorf("failed to get Python path: %v", err)
	}

	pythonScript := fmt.Sprintf(`
import sys
import sqlglot
from sqlglot import exp

CMD_EXIT = %q
CMD_RUN = %q
RESULT_OK = %q
RESULT_ERR = %q

def read_bytes(n: int):
    bytes = b''
    while n > 0:
        reads = sys.stdin.buffer.read(n)
        if len(reads) == 0:
            # The stdin has been closed, indicating that the parent process has exited.
            # We should exit the child process to prevent orphan Python processes.
            raise EOFError("EOF")
        bytes += reads
        n -= len(reads)
    return bytes

def read_string():
    data = read_bytes(4)
    length = int.from_bytes(data, byteorder='big')
    data = read_bytes(length)
    return data.decode('utf-8')

def write_string(s: str):
    data = s.encode('utf-8')
    # write the length of the string first
    sys.stdout.buffer.write(len(data).to_bytes(4, byteorder='big'))
    sys.stdout.buffer.write(data)
    sys.stdout.flush()

def rewrite_mysql_for_duckdb(node):
    # Nested MySQL XOR is left as the XOR keyword, which DuckDB rejects.
    # Expand to the equivalent boolean form before generating DuckDB SQL.
    if isinstance(node, exp.Xor):
        left, right = node.this, node.expression
        return exp.or_(
            exp.and_(exp.paren(left), exp.Not(this=exp.paren(right))),
            exp.and_(exp.Not(this=exp.paren(left)), exp.paren(right)),
        )
    # DuckDB has no AUTO_INCREMENT; map it to IDENTITY.
    if isinstance(node, exp.ColumnDef):
        constraints = list(node.args.get("constraints") or [])
        kept = []
        has_autoinc = False
        for c in constraints:
            if isinstance(getattr(c, "kind", None), exp.AutoIncrementColumnConstraint):
                has_autoinc = True
                continue
            kept.append(c)
        if has_autoinc:
            kept.append(exp.ColumnConstraint(
                kind=exp.GeneratedAsIdentityColumnConstraint(this=False)
            ))
            return exp.ColumnDef(
                this=node.this,
                kind=node.args.get("kind"),
                constraints=kept,
            )
    return node

def transpile_mysql_to_duckdb(sql: str) -> str:
    trees = sqlglot.parse(sql, read="mysql")
    if not trees or trees[0] is None:
        return ""
    # Keep the historical contract: only the first statement is returned.
    return trees[0].transform(rewrite_mysql_for_duckdb).sql(dialect="duckdb")

while True:
    inp = read_string()
    if inp == CMD_EXIT:
        break
    if inp.startswith(CMD_RUN):
        sql = inp[len(CMD_RUN):]
        try:
            result = transpile_mysql_to_duckdb(sql)
            write_string(RESULT_OK + result)
        except Exception as e:
            write_string(RESULT_ERR + str(e))
`, cmdExit, cmdRun, resultOK, resultErr)

	pyCmd := exec.Command(pythonPath, "-u", "-c", pythonScript)

	pyStdin, err := pyCmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdin pipe: %v", err)
	}

	pyStdout, err := pyCmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdout pipe: %v", err)
	}

	var stderrBuf bytes.Buffer
	pyCmd.Stderr = &stderrBuf

	err = pyCmd.Start()
	if err != nil {
		return nil, fmt.Errorf("failed to start Python process: %v", err)
	}

	svc := &translateService{
		mu:       &sync.Mutex{},
		pyCmd:    pyCmd,
		pyStdin:  pyStdin,
		pyStdout: bufio.NewReader(pyStdout),
		pyStderr: &stderrBuf,
	}

	// Test the translation service with a simple query
	testSQL := "SELECT 1"
	translatedSQL, err := svc.translate(testSQL)
	if err != nil {
		svc.cleanup()
		return nil, fmt.Errorf("failed to test translation service: %v", err)
	}
	if translatedSQL != "SELECT 1" {
		svc.cleanup()
		return nil, fmt.Errorf("unexpected translation result: %s", translatedSQL)
	}

	return svc, nil
}

func (svc *translateService) translate(sql string) (string, error) {
	svc.mu.Lock()
	defer svc.mu.Unlock()

	translatedSQL, err := translateInternalImpl(svc.pyStdin, svc.pyStdout, sql)
	if err != nil {
		if errors.Is(err, errPythonProcessUnhealthy) {
			panic(fmt.Errorf("%v\ncmd:\n%s\nstderr:\n%s", err, svc.pyCmd.String(), svc.pyStderr.String()))
		}
		return "", err
	}
	return translatedSQL, nil
}

func translateInternalImpl(pyStdin io.Writer, pyStdout io.Reader, sql string) (string, error) {
	err := sendString(pyStdin, cmdRun+sql)
	if err != nil {
		return "", errPythonProcessUnhealthy.New(err)
	}

	result, err := recvString(pyStdout)
	if err != nil {
		return "", errPythonProcessUnhealthy.New(err)
	}

	result = strings.TrimSpace(result)

	if strings.HasPrefix(result, resultErr) {
		return "", fmt.Errorf(result[len(resultErr):])
	} else if strings.HasPrefix(result, resultOK) {
		return strings.TrimSpace(result[len(resultOK):]), nil
	} else {
		return "", fmt.Errorf("unexpected result: %s", result)
	}
}

// the schema is 4 bytes length + data
func sendString(writer io.Writer, str string) error {
	data := []byte(str)
	length := len(data)
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(length))
	_, err := writer.Write(lengthBytes)
	if err != nil {
		return err
	}
	_, err = writer.Write(data)
	return err
}

func recvString(reader io.Reader) (string, error) {
	lengthBytes := make([]byte, 4)
	_, err := io.ReadFull(reader, lengthBytes)
	if err != nil {
		return "", err
	}
	length := binary.BigEndian.Uint32(lengthBytes)
	data := make([]byte, length)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (svc *translateService) cleanup() {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	sendString(svc.pyStdin, cmdExit)
	svc.pyCmd.Wait()
}

func TranslateWithSQLGlot(sql string) (string, error) {
	translationSvcOnce.Do(func() {
		svc, err := newTranslateService()
		if err != nil {
			panic(fmt.Errorf("failed to initialize translation service: %v", err))
		}
		translationSvc = svc
	})

	return translationSvc.translate(sql)
}

func getPythonPath() (string, error) {
	// Try to find python3 in the system PATH
	pythonPath, err := exec.LookPath("python3")
	if err == nil {
		return pythonPath, nil
	}

	// If python3 is not found, try to find python
	pythonPath, err = exec.LookPath("python")
	if err == nil {
		return pythonPath, nil
	}

	// If neither python3 nor python is found, return an error
	return "", fmt.Errorf("neither python3 nor python was found in PATH")
}
