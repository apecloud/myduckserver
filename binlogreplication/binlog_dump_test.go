package binlogreplication

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
)

func TestDetectBinlogDumpGTIDFlags(t *testing.T) {
	tests := []struct {
		name    string
		results map[string]fetchResult
		want    uint16
	}{
		{
			name: "Dolt 2.3.1",
			results: map[string]fetchResult{
				"SELECT @@VERSION_COMMENT": {value: "Dolt"},
				"SELECT DOLT_VERSION()":    {value: "2.3.1"},
			},
			want: mysql.BinlogThroughGTID,
		},
		{
			name: "standard MySQL",
			results: map[string]fetchResult{
				"SELECT @@VERSION_COMMENT": {value: "MySQL Community Server - GPL"},
			},
		},
		{
			name: "version comment unavailable",
			results: map[string]fetchResult{
				"SELECT @@VERSION_COMMENT": {err: errors.New("unknown system variable")},
			},
		},
		{
			name: "Dolt version function unavailable",
			results: map[string]fetchResult{
				"SELECT @@VERSION_COMMENT": {value: "Dolt"},
				"SELECT DOLT_VERSION()":    {err: errors.New("function DOLT_VERSION does not exist")},
			},
		},
		{
			name: "different Dolt version",
			results: map[string]fetchResult{
				"SELECT @@VERSION_COMMENT": {value: "Dolt"},
				"SELECT DOLT_VERSION()":    {value: "2.3.2"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, detectBinlogDumpGTIDFlags(fakeQueryExecutor(tt.results)))
		})
	}
}

func TestDetectSourceCapabilitiesToleratesOptionalProbeFailures(t *testing.T) {
	tests := []struct {
		name        string
		probeResult fetchResult
		results     map[string]fetchResult
	}{
		{
			name:        "version comment unavailable",
			probeResult: fetchResult{err: errors.New("unknown system variable")},
		},
		{
			name:        "Dolt version function unavailable",
			probeResult: fetchResult{value: "Dolt"},
			results: map[string]fetchResult{
				"SELECT DOLT_VERSION()": {err: errors.New("function DOLT_VERSION does not exist")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := map[string]fetchResult{
				"SELECT VERSION()":          {value: "8.0.31"},
				"SELECT @@VERSION_COMMENT":  tt.probeResult,
				"SELECT @@GLOBAL.GTID_MODE": {value: "ON"},
			}
			for query, result := range tt.results {
				results[query] = result
			}

			capabilities, err := detectSourceCapabilitiesFromQueries(fakeQueryExecutor(results))
			require.NoError(t, err)
			require.True(t, capabilities.gtidMode)
			require.Equal(t, replication.Mysql56FlavorID, capabilities.flavorName)
			require.Zero(t, capabilities.binlogDumpGTIDFlags)
		})
	}
}

func TestMySQL56BinlogDumpRequest(t *testing.T) {
	position, err := replication.ParsePosition(
		replication.Mysql56FlavorID,
		"568333e9-2b06-4d33-9a6b-c2308c930c95:1-5,"+
			"a4f6b11d-5f66-4fca-9c4a-d871e1d09a52:1-3",
	)
	require.NoError(t, err)
	wantSIDBlock := position.GTIDSet.(replication.Mysql56GTIDSet).SIDBlock()

	request, ok := newMySQL56BinlogDumpRequest(42, "mysql-bin.000007", position, mysql.BinlogThroughGTID)
	require.True(t, ok)
	require.Equal(t, uint32(42), request.serverID)
	require.Equal(t, "mysql-bin.000007", request.binlogFile)
	require.Equal(t, uint64(4), request.binlogPos)
	require.Equal(t, uint16(mysql.BinlogThroughGTID), request.flags)
	require.Equal(t, wantSIDBlock, request.sidBlock)

	request, ok = newMySQL56BinlogDumpRequest(42, "mysql-bin.000007", position, 0)
	require.True(t, ok)
	require.Zero(t, request.flags)
	require.Equal(t, "mysql-bin.000007", request.binlogFile)
	require.Equal(t, uint64(4), request.binlogPos)
	require.Equal(t, wantSIDBlock, request.sidBlock)

	filePosition := replication.Position{GTIDSet: replication.FilePosGTID{File: "binlog.000001", Pos: 4}}
	_, ok = newMySQL56BinlogDumpRequest(42, "binlog.000001", filePosition, mysql.BinlogThroughGTID)
	require.False(t, ok)
}

type fetchResult struct {
	value string
	err   error
}

type fakeQueryExecutor map[string]fetchResult

func (f fakeQueryExecutor) ExecuteFetch(query string, _ int, _ bool) (*sqltypes.Result, error) {
	result, ok := f[query]
	if !ok {
		return nil, errors.New("unexpected query: " + query)
	}
	if result.err != nil {
		return nil, result.err
	}
	return &sqltypes.Result{Rows: [][]sqltypes.Value{{sqltypes.NewVarChar(result.value)}}}, nil
}
