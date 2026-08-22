package catalog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDatabaseProviderRestartClosesConnectionsBeforeReopen(t *testing.T) {
	prov, err := NewDBProvider("", t.TempDir(), "myduck")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, prov.Close())
	})

	ctx := context.Background()
	conn, err := prov.Pool().GetConn(ctx, 1)
	require.NoError(t, err)
	var value int
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT 1").Scan(&value))
	require.Equal(t, 1, value)

	require.NoError(t, prov.Restart(true))
	conn, err = prov.Pool().GetConn(ctx, 1)
	require.NoError(t, err)
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT 2").Scan(&value))
	require.Equal(t, 2, value)

	require.NoError(t, prov.Restart(false))
	conn, err = prov.Pool().GetConn(ctx, 1)
	require.NoError(t, err)
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT 3").Scan(&value))
	require.Equal(t, 3, value)
}
