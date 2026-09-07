package pgserver

import (
	"context"
	stdsql "database/sql"
	"testing"

	"github.com/apecloud/myduckserver/mycontext"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

type postgresOperationOwnerProviderProbe struct {
	legacyCalls int
	ownerCalls  int
	owner       any
}

func (p *postgresOperationOwnerProviderProbe) BeginDuckLakeOperation(context.Context) func() {
	p.legacyCalls++
	return func() {}
}

func (p *postgresOperationOwnerProviderProbe) BeginDuckLakeOperationForOwner(_ context.Context, owner any) func() {
	p.ownerCalls++
	p.owner = owner
	return func() {}
}

func TestBeginPostgresDuckLakeOperationPassesPhysicalOwner(t *testing.T) {
	physicalTx := new(stdsql.Tx)
	session := &rollbackCleanupSessionProbe{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		tx:      physicalTx,
	}
	ctx := sql.NewContext(
		mycontext.WithFrontendQuery(context.Background()),
		sql.WithSession(session),
	)
	provider := &postgresOperationOwnerProviderProbe{}

	owner := postgresDuckLakeOperationOwner(ctx, nil)
	require.Same(t, physicalTx, owner)
	release := beginPostgresDuckLakeOperationForProvider(ctx, &tree.Select{}, provider, owner)
	release()

	require.Equal(t, 1, provider.ownerCalls)
	require.Equal(t, 0, provider.legacyCalls)
	require.Same(t, physicalTx, provider.owner)
}

func TestBeginPostgresDuckLakeOperationFallsBackForLegacyProvider(t *testing.T) {
	physicalTx := new(stdsql.Tx)
	session := &rollbackCleanupSessionProbe{
		Session: memory.NewSession(sql.NewBaseSession(), nil),
		tx:      physicalTx,
	}
	ctx := sql.NewContext(
		mycontext.WithFrontendQuery(context.Background()),
		sql.WithSession(session),
	)
	provider := &legacyPostgresOperationProviderProbe{}

	release := beginPostgresDuckLakeOperationForProvider(ctx, &tree.Select{}, provider, physicalTx)
	release()

	require.Equal(t, 1, provider.calls)
}

type legacyPostgresOperationProviderProbe struct{ calls int }

func (p *legacyPostgresOperationProviderProbe) BeginDuckLakeOperation(context.Context) func() {
	p.calls++
	return func() {}
}
