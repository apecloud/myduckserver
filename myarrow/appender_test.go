package myarrow

import (
	"math/big"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/cockroachdb/apd/v3"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
)

func TestArrowAppenderPreservesAPDDecimals(t *testing.T) {
	schema := sql.Schema{
		&sql.Column{Name: "decimal128", Type: types.MustCreateDecimalType(38, 18), Nullable: true},
		&sql.Column{Name: "decimal256", Type: types.MustCreateDecimalType(65, 30), Nullable: true},
	}
	appender, err := NewArrowAppender(schema)
	require.NoError(t, err)
	t.Cleanup(appender.Release)

	decimal128 := mustParseDecimal(t, "12345678901234567890.123456789012345678")
	decimal256 := mustParseDecimal(t, "12345678901234567890123456789012345.123456789012345678901234567890")
	negative128 := mustParseDecimal(t, "-12345678901234567890.123456789012345678")
	negative256 := mustParseDecimal(t, "-12345678901234567890123456789012345.123456789012345678901234567890")
	require.NoError(t, appender.Append(sql.Row{decimal128, decimal256}))
	require.NoError(t, appender.Append(sql.Row{negative128, negative256}))
	require.NoError(t, appender.Append(sql.Row{nil, nil}))

	record := appender.Build()
	t.Cleanup(record.Release)
	values128 := record.Column(0).(*array.Decimal128)
	values256 := record.Column(1).(*array.Decimal256)
	require.Zero(t, decimalCoefficient(decimal128).Cmp(values128.Value(0).BigInt()))
	require.Zero(t, decimalCoefficient(decimal256).Cmp(values256.Value(0).BigInt()))
	require.Zero(t, decimalCoefficient(negative128).Cmp(values128.Value(1).BigInt()))
	require.Zero(t, decimalCoefficient(negative256).Cmp(values256.Value(1).BigInt()))
	require.Equal(t, int32(18), record.Schema().Field(0).Type.(*arrow.Decimal128Type).Scale)
	require.Equal(t, int32(30), record.Schema().Field(1).Type.(*arrow.Decimal256Type).Scale)
	require.True(t, values128.IsNull(2))
	require.True(t, values256.IsNull(2))
}

func TestArrowAppenderValidatesVectorBinaryLength(t *testing.T) {
	vectorType, err := types.CreateVectorType(2)
	require.NoError(t, err)
	appender, err := NewArrowAppender(sql.Schema{
		&sql.Column{Name: "v", Type: vectorType, Nullable: true},
	})
	require.NoError(t, err)
	t.Cleanup(appender.Release)

	valid := sql.EncodeVector([]float32{1.25, -2.5})
	require.NoError(t, appender.Append(sql.Row{valid}))
	require.NoError(t, appender.Append(sql.Row{nil}))
	require.EqualError(t, appender.Append(sql.Row{[]byte{}}), "vector column 0 dimension mismatch: expected 8 bytes, got 0")
	require.EqualError(t, appender.Append(sql.Row{[]byte{1, 2, 3, 4}}), "vector column 0 dimension mismatch: expected 8 bytes, got 4")

	record := appender.Build()
	t.Cleanup(record.Release)
	values := record.Column(0).(*array.Binary)
	require.Equal(t, valid, values.Value(0))
	require.True(t, values.IsNull(1))
}

func mustParseDecimal(t *testing.T, value string) *apd.Decimal {
	t.Helper()
	d, _, err := apd.NewFromString(value)
	require.NoError(t, err)
	return d
}

func decimalCoefficient(d *apd.Decimal) *big.Int {
	coeff := d.Coeff.MathBigInt()
	if d.Negative {
		coeff.Neg(coeff)
	}
	return coeff
}
