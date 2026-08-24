package myarrow

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
)

func TestVectorUsesArrowBinary(t *testing.T) {
	vectorType, err := types.CreateVectorType(2)
	require.NoError(t, err)
	arrowType, err := ToArrowType(vectorType)
	require.NoError(t, err)
	require.Equal(t, arrow.BINARY, arrowType.ID())
}
