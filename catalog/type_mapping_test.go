package catalog

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
)

func TestNewEnumTypeIncludesEmptyString(t *testing.T) {
	typ, err := types.CreateEnumType([]string{"phprapporten", "excelrapporten"}, sql.Collation_Default)
	require.NoError(t, err)
	got := newEnumType(typ)
	require.Equal(t, "ENUM('', 'phprapporten', 'excelrapporten')", got.Name())
	require.Equal(t, []string{"phprapporten", "excelrapporten"}, got.MySQL().Values)
}

func TestDuckDBEnumAcceptsEmptyString(t *testing.T) {
	dir := t.TempDir()
	prov, err := NewDBProvider("", dir, "myduck")
	require.NoError(t, err)
	defer prov.Close()

	_, err = prov.Storage().Exec(`CREATE TABLE t (doel ENUM('', 'phprapporten', 'excelrapporten'))`)
	require.NoError(t, err)
	_, err = prov.Storage().Exec(`INSERT INTO t VALUES ('')`)
	require.NoError(t, err)
	_, err = prov.Storage().Exec(`INSERT INTO t VALUES ('phprapporten')`)
	require.NoError(t, err)

	var got string
	require.NoError(t, prov.Storage().QueryRow(`SELECT doel FROM t WHERE doel = ''`).Scan(&got))
	require.Equal(t, "", got)
}

func TestNewEnumTypeKeepsExistingEmptyString(t *testing.T) {
	typ, err := types.CreateEnumType([]string{"", "a", "b"}, sql.Collation_Default)
	require.NoError(t, err)
	got := newEnumType(typ)
	require.Equal(t, "ENUM('', 'a', 'b')", got.Name())
	require.Equal(t, []string{"", "a", "b"}, got.MySQL().Values)
}

func TestMySQLDataTypeBignum(t *testing.T) {
	for _, name := range []string{"VARINT", "BIGNUM"} {
		t.Run(name, func(t *testing.T) {
			got, err := mysqlDataType(AnnotatedDuckType{name: name}, 0, 0)
			require.NoError(t, err)

			decimalType, ok := got.(sql.DecimalType)
			require.True(t, ok)
			require.Equal(t, uint8(65), decimalType.Precision())
			require.Equal(t, uint8(0), decimalType.Scale())
		})
	}
}
