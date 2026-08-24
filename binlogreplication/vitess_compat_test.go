package binlogreplication

import (
	"encoding/hex"
	"testing"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"
)

func TestFilePositionCompatibility(t *testing.T) {
	position, err := newFilePosition("mysql-bin.000123", 456)
	require.NoError(t, err)

	file, pos, ok := parseFilePosition(position)
	require.True(t, ok)
	require.Equal(t, "mysql-bin.000123", file)
	require.Equal(t, uint64(456), pos)

	gtid, err := mysql.ParseGTID(filePosFlavorID, "mysql-bin.000123:456")
	require.NoError(t, err)
	file, pos, ok = parseFileGTID(gtid)
	require.True(t, ok)
	require.Equal(t, "mysql-bin.000123", file)
	require.Equal(t, uint64(456), pos)
}

func TestFilePositionCompatibilityRejectsOtherFlavors(t *testing.T) {
	position := mysql.Position{GTIDSet: mysql.Mysql56GTID{Sequence: 1}.GTIDSet()}
	_, _, ok := parseFilePosition(position)
	require.False(t, ok)
}

func TestFilePositionEncodingPreservesFlavor(t *testing.T) {
	position, err := newFilePosition("mysql-bin.000123", 456)
	require.NoError(t, err)

	encoded := mysql.EncodePosition(position)
	require.Equal(t, "FilePos/mysql-bin.000123:456", encoded)

	decoded, err := mysql.DecodePosition(encoded)
	require.NoError(t, err)
	require.True(t, decoded.Equal(position))
	require.True(t, hasEncodedPositionFlavor(encoded))
	require.False(t, hasEncodedPositionFlavor("directory/mysql-bin.000123:456"))
}

type eventWithoutServerID struct {
	mysql.BinlogEvent
}

type eventWithServerID struct {
	mysql.BinlogEvent
	serverID uint32
}

func (e eventWithServerID) ServerID() uint32 {
	return e.serverID
}

func TestBinlogEventServerIDCompatibility(t *testing.T) {
	serverID, ok := binlogEventServerID(eventWithServerID{serverID: 42})
	require.True(t, ok)
	require.Equal(t, uint32(42), serverID)

	serverID, ok = binlogEventServerID(eventWithoutServerID{})
	require.False(t, ok)
	require.Zero(t, serverID)
}

func TestMySQL267WriteRowsNoCheckConstraintsDecoding(t *testing.T) {
	decode := func(value string) []byte {
		decoded, err := hex.DecodeString(value)
		require.NoError(t, err)
		return decoded
	}

	formatBytes := decode("f6518c6a0f010000007b000000000000000000040032362e372e3000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000013000d0008000000000400040000006300041a08000000000000020000000a0a0a2a2a001234000a28000001f41f7b0b")
	tableMapBytes := decode("01528c6a1301000000340000001805000000005c00000000000100046462303100067461626c65740001030000010100dda4faec")
	rowsBytes := decode("01528c6a1e010000002d0000004505000000005c00000000001100020001ff006400000000c80000003dfe60ef")

	format, err := mysql.NewMysql56BinlogEvent(formatBytes).Format()
	require.NoError(t, err)
	require.Equal(t, uint16(4), format.FormatVersion)
	require.Equal(t, "26.7.0", format.ServerVersion)
	require.Equal(t, byte(19), format.HeaderLength)

	tableMapEvent, _, err := mysql.NewMysql56BinlogEvent(tableMapBytes).StripChecksum(format)
	require.NoError(t, err)
	tableMap, err := tableMapEvent.TableMap(format)
	require.NoError(t, err)
	require.Equal(t, uint64(92), tableMapEvent.TableID(format))
	require.Equal(t, "db01", tableMap.Database)
	require.Equal(t, "tablet", tableMap.Name)
	require.Equal(t, uint16(0x1), tableMap.Flags)

	rowsEvent, _, err := mysql.NewMysql56BinlogEvent(rowsBytes).StripChecksum(format)
	require.NoError(t, err)
	require.True(t, rowsEvent.IsWriteRows())
	require.False(t, rowsEvent.IsUpdateRows())
	require.False(t, rowsEvent.IsDeleteRows())
	require.Equal(t, uint64(92), rowsEvent.TableID(format))
	rows, err := rowsEvent.Rows(format, tableMap)
	require.NoError(t, err)
	require.Equal(t, uint16(0x11), rows.Flags)
	require.Len(t, rows.Rows, 2)
	require.Equal(t, []byte{100, 0, 0, 0}, rows.Rows[0].Data)
	require.Equal(t, []byte{200, 0, 0, 0}, rows.Rows[1].Data)

	foreignKeyChecksDisabled, unsupported := classifyRowEventFlags(rows.Flags)
	require.False(t, foreignKeyChecksDisabled)
	require.Equal(t, uint16(rowFlag_noCheckConstraints), unsupported)
}

func TestClassifyRowEventFlagsPreservesUnsupportedBits(t *testing.T) {
	foreignKeyChecksDisabled, unsupported := classifyRowEventFlags(
		rowFlag_endOfStatement | rowFlag_noForeignKeyChecks | 0x20,
	)
	require.True(t, foreignKeyChecksDisabled)
	require.Equal(t, uint16(0x20), unsupported)
}
