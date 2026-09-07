package catalog

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Database-file DDL must use the connection already owned by the caller. A
// max-open-connections value of one makes an accidental second acquisition
// deterministic: the operation would block while its owner is checked out.
func TestCreateCatalogOnConnUsesOwnedConnection(t *testing.T) {
	provider, err := NewDBProvider("", t.TempDir(), "myduck")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })

	provider.Storage().SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := provider.Storage().Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	require.NoError(t, provider.CreateCatalogOnConn(ctx, conn, "owned_conn_db", false))
	var currentCatalog string
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT current_catalog").Scan(&currentCatalog))
	require.Equal(t, "myduck", currentCatalog)
	_, err = os.Stat(filepath.Join(provider.DataDir(), "owned_conn_db.db"))
	require.NoError(t, err)

	require.NoError(t, provider.DropCatalogOnConn(ctx, conn, "owned_conn_db", false))
	_, err = os.Stat(filepath.Join(provider.DataDir(), "owned_conn_db.db"))
	require.ErrorIs(t, err, os.ErrNotExist)
}
