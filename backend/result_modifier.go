package backend

import (
	"strconv"
	"strings"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
)

// ResultModifier transforms a Result.
type ResultModifier func(*sqltypes.Result) *sqltypes.Result

func setReplicaFilePosition(result *sqltypes.Result) *sqltypes.Result {
	if result == nil {
		return result
	}

	fileIndex := resultFieldIndex(result, "Source_Log_File")
	positionIndex := resultFieldIndex(result, "Read_Source_Log_Pos")
	executedIndex := resultFieldIndex(result, "Executed_Gtid_Set")
	if fileIndex < 0 || positionIndex < 0 || executedIndex < 0 {
		return result
	}

	for _, row := range result.Rows {
		if executedIndex >= len(row) || fileIndex >= len(row) || positionIndex >= len(row) {
			continue
		}
		executed := row[executedIndex].ToString()
		if executed == "" || isGTIDPosition(executed) {
			continue
		}
		file, position, ok := splitFilePosition(executed)
		if !ok {
			continue
		}
		row[fileIndex] = sqltypes.MakeTrusted(result.Fields[fileIndex].Type, []byte(file))
		row[positionIndex] = sqltypes.MakeTrusted(result.Fields[positionIndex].Type, []byte(strconv.FormatUint(position, 10)))
	}
	return result
}

func resultFieldIndex(result *sqltypes.Result, name string) int {
	for i, field := range result.Fields {
		if field != nil && strings.EqualFold(field.Name, name) {
			return i
		}
	}
	return -1
}

func isGTIDPosition(value string) bool {
	if _, err := mysql.ParsePosition("MySQL56", value); err == nil {
		return true
	}
	if _, err := mysql.ParsePosition("MariaDB", value); err == nil {
		return true
	}
	return false
}

func splitFilePosition(value string) (string, uint64, bool) {
	split := strings.LastIndexByte(value, ':')
	if split <= 0 || split == len(value)-1 {
		return "", 0, false
	}
	position, err := strconv.ParseUint(value[split+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return value[:split], position, true
}
