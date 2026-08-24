package binlogreplication

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/vitess/go/mysql"
)

const (
	filePosFlavorID = "FilePos"
	mysql56FlavorID = "MySQL56"
	mariadbFlavorID = "MariaDB"
)

func newFilePosition(file string, pos uint64) (mysql.Position, error) {
	return mysql.ParsePosition(filePosFlavorID, fmt.Sprintf("%s:%d", file, pos))
}

func parseFilePosition(position mysql.Position) (string, uint64, bool) {
	if position.IsZero() || position.GTIDSet.Flavor() != filePosFlavorID {
		return "", 0, false
	}
	return parseFilePositionString(position.String())
}

func parseFileGTID(gtid mysql.GTID) (string, uint64, bool) {
	if gtid == nil || gtid.Flavor() != filePosFlavorID {
		return "", 0, false
	}
	return parseFilePositionString(gtid.String())
}

func parseFilePositionString(value string) (string, uint64, bool) {
	split := strings.LastIndexByte(value, ':')
	if split < 0 {
		return "", 0, false
	}
	pos, err := strconv.ParseUint(value[split+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return value[:split], pos, true
}

func binlogEventServerID(event mysql.BinlogEvent) (uint32, bool) {
	withServerID, ok := event.(interface{ ServerID() uint32 })
	if !ok {
		return 0, false
	}
	return withServerID.ServerID(), true
}
