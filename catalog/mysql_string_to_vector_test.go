package catalog

import (
	"database/sql/driver"
	"testing"

	gmsql "github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestMySQLStringToVectorExec(t *testing.T) {
	value, err := mysqlStringToVectorExec([]driver.Value{"[1.25,-2.5]", int64(2)})
	require.NoError(t, err)
	decoded, err := gmsql.DecodeVector(value.([]byte))
	require.NoError(t, err)
	require.Equal(t, []float32{1.25, -2.5}, decoded)

	value, err = mysqlStringToVectorExec([]driver.Value{nil})
	require.NoError(t, err)
	require.Nil(t, value)

	_, err = mysqlStringToVectorExec([]driver.Value{"[1.25]", int64(2)})
	require.ErrorContains(t, err, "VECTOR dimension mismatch: expected 2, got 1")
	_, err = mysqlStringToVectorExec([]driver.Value{"[]"})
	require.EqualError(t, err, "VECTOR must contain at least one element")
	_, err = mysqlStringToVectorExec([]driver.Value{"[]", int64(0)})
	require.EqualError(t, err, "VECTOR must contain at least one element")
	_, err = mysqlStringToVectorExec([]driver.Value{"not-json"})
	require.Error(t, err)
}
