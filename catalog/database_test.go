package catalog

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestViewDefinitionFromMetadata(t *testing.T) {
	meta := ExtraViewInfo{
		TextDefinition:      `select public_keys."public" from public_keys`,
		CreateViewStatement: "CREATE VIEW `view1` AS select public_keys.\"public\" from public_keys",
		SqlMode:             "ANSI_QUOTES",
	}
	comment := NewCommentWithMeta("", meta).Encode()

	actual := viewDefinitionFromMetadata("view1", "mydb", "CREATE VIEW normalized", comment)
	require.Equal(t, sql.ViewDefinition{
		Name:                "view1",
		TextDefinition:      meta.TextDefinition,
		CreateViewStatement: meta.CreateViewStatement,
		SqlMode:             meta.SqlMode,
		SchemaName:          "mydb",
	}, actual)
}

func TestViewDefinitionFromMetadataFallsBackForLegacyViews(t *testing.T) {
	expected := sql.ViewDefinition{
		Name:                "view1",
		CreateViewStatement: "CREATE VIEW normalized",
		SchemaName:          "mydb",
	}

	t.Run("no comment", func(t *testing.T) {
		actual := viewDefinitionFromMetadata("view1", "mydb", "CREATE VIEW normalized", "")
		require.Equal(t, expected, actual)
	})

	t.Run("user comment", func(t *testing.T) {
		actual := viewDefinitionFromMetadata("view1", "mydb", "CREATE VIEW normalized", "user supplied comment")
		require.Equal(t, expected, actual)
	})

	t.Run("non ANSI internal metadata", func(t *testing.T) {
		meta := ExtraViewInfo{
			TextDefinition:      "SELECT * FROM t",
			CreateViewStatement: "CREATE VIEW `view1` AS SELECT * FROM t",
			SqlMode:             "ONLY_FULL_GROUP_BY",
		}
		actual := viewDefinitionFromMetadata("view1", "mydb", "CREATE VIEW normalized", NewCommentWithMeta("", meta).Encode())
		require.Equal(t, expected, actual)
	})
}
