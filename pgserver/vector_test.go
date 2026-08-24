package pgserver

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/apecloud/myduckserver/testutil"
	gmsql "github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestVectorStorageWireProtocols(t *testing.T) {
	originalWorkingDir, err := os.Getwd()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Chdir(originalWorkingDir))
	}()

	testDir := testutil.CreateTestDir(t)
	testEnv := testutil.NewTestEnv()
	require.NoError(t, testutil.StartDuckSqlServer(t, testDir, nil, testEnv))
	t.Cleanup(func() {
		require.NoError(t, testEnv.MyDuckServer.Close())
		testutil.StopDuckSqlServer(t, testEnv.DuckProcess)
	})

	db := testEnv.MyDuckServer
	_, err = db.Exec("CREATE DATABASE vector_wire")
	require.NoError(t, err)
	_, err = db.Exec("USE vector_wire")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE invalid_zero_dimension (v VECTOR(0))")
	require.ErrorContains(t, err, "VECTOR dimension must be between 1")
	_, err = db.Exec("SELECT STRING_TO_VECTOR('[]')")
	require.ErrorContains(t, err, "VECTOR must contain at least one element")
	var serverStillAlive int
	require.NoError(t, db.QueryRow("SELECT 1").Scan(&serverStillAlive))
	require.Equal(t, 1, serverStillAlive)
	_, err = db.Exec(`CREATE TABLE vectors (
		id INT PRIMARY KEY,
		v VECTOR(2),
		source JSON,
		generated_v VECTOR(2) NOT NULL GENERATED ALWAYS AS (STRING_TO_VECTOR(source)) STORED
	)`)
	require.NoError(t, err)

	var tableName, createStatement string
	require.NoError(t, db.QueryRow("SHOW CREATE TABLE vectors").Scan(&tableName, &createStatement))
	require.Equal(t, "vectors", tableName)
	lowerCreate := strings.ToLower(createStatement)
	require.Contains(t, lowerCreate, "`v` vector(2)")
	require.Contains(t, lowerCreate, "`generated_v` vector(2) not null generated always as (string_to_vector(`source`)) stored")

	var columnType, nullable, extra string
	require.NoError(t, db.QueryRow(`SELECT column_type, is_nullable, extra
		FROM information_schema.columns
		WHERE table_schema = 'vector_wire' AND table_name = 'vectors' AND column_name = 'generated_v'`).Scan(
		&columnType,
		&nullable,
		&extra,
	))
	require.Equal(t, "vector(2)", strings.ToLower(columnType))
	require.Equal(t, "NO", nullable)
	require.Equal(t, "STORED GENERATED", strings.ToUpper(extra))

	_, err = db.Exec(`INSERT INTO vectors (id, v, source) VALUES
		(1, STRING_TO_VECTOR('[1.25,-2.5]'), '[3,4]'),
		(2, NULL, '[5,6]')`)
	require.NoError(t, err)
	requireVectorRow(t, db.QueryRow("SELECT v, generated_v FROM vectors WHERE id = 1"), []float32{1.25, -2.5}, []float32{3, 4})

	var nullVector []byte
	var generatedVector []byte
	require.NoError(t, db.QueryRow("SELECT v, generated_v FROM vectors WHERE id = 2").Scan(&nullVector, &generatedVector))
	require.Nil(t, nullVector)
	requireVectorBytes(t, generatedVector, []float32{5, 6})

	_, err = db.Exec("INSERT INTO vectors (id, v, source) VALUES (3, STRING_TO_VECTOR('[7.5,8.5]'), '[9,10]')")
	require.NoError(t, err)

	stmt, err := db.Prepare("SELECT v, generated_v FROM vectors WHERE id = 3")
	require.NoError(t, err)
	requireVectorRow(t, stmt.QueryRow(), []float32{7.5, 8.5}, []float32{9, 10})
	require.NoError(t, stmt.Close())

	_, err = db.Exec("INSERT INTO vectors (id, v, source) VALUES (4, STRING_TO_VECTOR('[1]'), '[1,2]')")
	require.Error(t, err)
	_, err = db.Exec("INSERT INTO vectors (id, v, source) VALUES (5, NULL, '[1]')")
	require.ErrorContains(t, err, "VECTOR dimension mismatch")

	ctx := context.Background()
	pgConn, err := pgx.Connect(ctx, "postgresql://postgres@127.0.0.1:"+strconv.Itoa(testEnv.DuckPgPort)+"/postgres")
	require.NoError(t, err)
	defer pgConn.Close(ctx)
	requireVectorRow(t, pgConn.QueryRow(ctx, "SELECT v, generated_v FROM vector_wire.vectors WHERE id = 1"), []float32{1.25, -2.5}, []float32{3, 4})
	_, err = pgConn.Prepare(ctx, "vector_wire_prepared", "SELECT v, generated_v FROM vector_wire.vectors WHERE id = 3")
	require.NoError(t, err)
	requireVectorRow(t, pgConn.QueryRow(ctx, "vector_wire_prepared"), []float32{7.5, 8.5}, []float32{9, 10})
	_, err = pgConn.Exec(ctx, "SELECT STRING_TO_VECTOR('[]')")
	require.ErrorContains(t, err, "VECTOR must contain at least one element")
	require.NoError(t, pgConn.QueryRow(ctx, "SELECT 1").Scan(&serverStillAlive))
	require.Equal(t, 1, serverStillAlive)
}

func requireVectorRow(t *testing.T, row sqlRowScanner, first, second []float32) {
	t.Helper()
	var firstBytes, secondBytes []byte
	require.NoError(t, row.Scan(&firstBytes, &secondBytes))
	requireVectorBytes(t, firstBytes, first)
	requireVectorBytes(t, secondBytes, second)
}

func requireVectorBytes(t *testing.T, encoded []byte, expected []float32) {
	t.Helper()
	decoded, err := gmsql.DecodeVector(encoded)
	require.NoError(t, err)
	require.Equal(t, expected, decoded)
}
