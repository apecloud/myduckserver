package catalog

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	vectorfn "github.com/dolthub/go-mysql-server/sql/expression/function/vector"
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

func TestVectorTypeRoundTrip(t *testing.T) {
	want, err := types.CreateVectorType(3)
	require.NoError(t, err)

	duckType, err := DuckdbDataType(want)
	require.NoError(t, err)
	require.Equal(t, "BLOB", duckType.Name())
	require.Equal(t, MySQLType{Name: "VECTOR", Length: 3}, duckType.MySQL())

	got, err := mysqlDataType(duckType, 0, 0)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestTimeTypeRoundTrip(t *testing.T) {
	for precision := 0; precision <= types.MaxTimePrecision; precision++ {
		t.Run(types.MustCreateTimeType(precision).String(), func(t *testing.T) {
			want := types.MustCreateTimeType(precision)
			duckType, err := DuckdbDataType(want)
			require.NoError(t, err)
			require.Equal(t, "INTERVAL", duckType.Name())
			require.Equal(t, uint8(precision), duckType.MySQL().Precision)

			got, err := mysqlDataType(duckType, 0, 0)
			require.NoError(t, err)
			require.True(t, want.Equals(got))
		})
	}
}

func TestVectorGeneratedExpressionRejectsOnlyUnsupportedShapes(t *testing.T) {
	vectorType, err := types.CreateVectorType(2)
	require.NoError(t, err)

	col := &sql.Column{
		Name: "generated_v",
		Generated: &sql.ColumnDefaultValue{
			Expr: expression.NewLiteral(int64(1), types.Int64),
		},
	}
	_, err = vectorGeneratedExpression(col, vectorType)
	require.EqualError(t, err, "unsupported generated VECTOR expression: 1")

	col.Generated.Expr = vectorfn.NewStringToVector(
		sql.NewEmptyContext(),
		expression.NewLiteral("[]", types.Text),
	)
	_, err = vectorGeneratedExpression(col, vectorType)
	require.EqualError(t, err, "unsupported STRING_TO_VECTOR generated argument: '[]'")

	col.Generated.Expr = vectorfn.NewStringToVector(
		sql.NewEmptyContext(),
		expression.NewGetField(0, types.JSON, "source", true),
	)
	got, err := vectorGeneratedExpression(col, vectorType)
	require.NoError(t, err)
	require.Equal(t, `STRING_TO_VECTOR("source", 2)`, got)
}
