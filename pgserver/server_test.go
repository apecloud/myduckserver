package pgserver

import (
	"context"
	"testing"

	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/testutil"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestNewServerInitializesAuthOnce(t *testing.T) {
	provider := catalog.NewInMemoryDBProvider()
	t.Cleanup(func() {
		require.NoError(t, provider.Close())
	})

	newCtx := func() *sql.Context {
		session := backend.NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)
		return sql.NewContext(context.Background(), sql.WithSession(session))
	}

	for i, password := range []string{"", "secret"} {
		server, err := NewServer(provider, "127.0.0.1", testutil.FindFreePort(), password, newCtx)
		require.NoError(t, err)
		postgres := auth.GetRole("postgres")
		require.True(t, postgres.IsValid())
		require.True(t, postgres.CanLogin)
		require.True(t, postgres.IsSuperUser)
		require.True(t, postgres.CanCreateRoles)
		require.True(t, postgres.CanCreateDB)
		require.NotNil(t, postgres.Password)
		require.Equal(t, password != "", EnableAuthentication)
		if i == 0 {
			retained := auth.CreateDefaultRole("retained_across_server_construction")
			auth.SetRole(retained)
		} else {
			require.True(t, auth.RoleExists("retained_across_server_construction"))
		}
		server.Close()
	}
	auth.DropRole("retained_across_server_construction")
}
