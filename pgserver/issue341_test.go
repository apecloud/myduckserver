package pgserver

import (
	"context"
	"strconv"
	"testing"

	"github.com/apecloud/myduckserver/testutil"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestIssue341PgAuthRejectsWrongPassword checks
// https://github.com/apecloud/myduckserver/issues/341 :
// --superuser-password enables SCRAM; a wrong password must not connect.
func TestIssue341PgAuthRejectsWrongPassword(t *testing.T) {
	testDir := testutil.CreateTestDir(t)
	testEnv := testutil.NewTestEnv()
	testEnv.SuperuserPassword = "secret"
	testEnv.ExtraArgs = []string{"--superuser-password=secret"}
	err := testutil.StartDuckSqlServer(t, testDir, nil, testEnv)
	require.NoError(t, err)
	defer testutil.StopDuckSqlServer(t, testEnv.DuckProcess)

	port := strconv.Itoa(testEnv.DuckPgPort)
	_, err = pgx.Connect(context.Background(), "postgresql://postgres:wrong@127.0.0.1:"+port+"/postgres")
	require.Error(t, err, "wrong password must be rejected")

	db, err := pgx.Connect(context.Background(), "postgresql://postgres:secret@127.0.0.1:"+port+"/postgres")
	require.NoError(t, err, "correct password must connect")
	defer db.Close(context.Background())
	_, err = db.Exec(context.Background(), "SELECT 1")
	require.NoError(t, err)
}
