package pgtypes

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestGoDuckDBJSONTypeMapsToPostgresJSON(t *testing.T) {
	pgType, _, _, fallback, err := GoDuckDBTypeNameToPostgresType("JSON")
	require.NoError(t, err)
	require.False(t, fallback)
	require.Equal(t, uint32(pgtype.JSONOID), pgType.OID)
}
