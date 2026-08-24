package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeCreateIndexFields(t *testing.T) {
	columns, err := DecodeCreateindex("ALTER TABLE t ADD INDEX idx (first_col, second_col)")
	require.NoError(t, err)
	require.Equal(t, []string{"first_col", "second_col"}, columns)
}
