package main

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/harness"
	"github.com/apecloud/myduckserver/myarrow"
	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
)

func TestVectorStorageSurvivesProviderCleanReopen(t *testing.T) {
	provider, err := catalog.NewDBProvider("", t.TempDir(), "myduck")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, provider.Close())
	})

	h := harness.NewDuckHarness("vector-reopen", 1, 1, true, nil).WithProvider(provider)
	setupData := []setup.SetupScript{{
		"CREATE DATABASE vector_reopen",
		"USE vector_reopen",
		`CREATE TABLE vectors (
			id INT PRIMARY KEY,
			v VECTOR(2),
			source JSON,
			generated_v VECTOR(2) NOT NULL GENERATED ALWAYS AS (STRING_TO_VECTOR(source)) STORED
		)`,
		`INSERT INTO vectors (id, v, source) VALUES (1, STRING_TO_VECTOR('[1.25,-2.5]'), '[3,4]')`,
	}}
	engine, err := harness.NewEngine(t, h, provider, setupData, memory.NewStatsProv(), false)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})

	ctx := enginetest.NewContext(h)
	require.NoError(t, provider.Restart(false))
	conn, err := provider.Pool().GetConn(ctx, ctx.ID())
	require.NoError(t, err)

	db, err := provider.Database(ctx, "vector_reopen")
	require.NoError(t, err)
	table, found, err := db.GetTableInsensitive(ctx, "vectors")
	require.NoError(t, err)
	require.True(t, found)
	schema := table.Schema(ctx)
	require.Len(t, schema, 4)

	ordinaryType, ok := schema[1].Type.(types.VectorType)
	require.True(t, ok)
	require.Equal(t, 2, ordinaryType.Dimensions)
	require.True(t, schema[1].Nullable)
	require.Nil(t, schema[1].Generated)

	generatedType, ok := schema[3].Type.(types.VectorType)
	require.True(t, ok)
	require.Equal(t, 2, generatedType.Dimensions)
	require.False(t, schema[3].Nullable)
	require.NotNil(t, schema[3].Generated)
	require.Equal(t, "STRING_TO_VECTOR(source)", strings.ReplaceAll(schema[3].Generated.Expr.String(), "`", ""))
	require.False(t, schema[3].Virtual)
	require.Equal(t, "STORED GENERATED", schema[3].Extra)

	arrowSchema, err := myarrow.ToArrowSchema(schema)
	require.NoError(t, err)
	require.Equal(t, arrow.BINARY, arrowSchema.Field(1).Type.ID())
	require.True(t, arrowSchema.Field(1).Nullable)
	require.Equal(t, arrow.BINARY, arrowSchema.Field(3).Type.ID())
	require.False(t, arrowSchema.Field(3).Nullable)

	_, err = conn.ExecContext(ctx, `USE "myduck"."vector_reopen"`)
	require.NoError(t, err)

	var constraintText string
	require.NoError(t, conn.QueryRowContext(ctx, `
		SELECT constraint_text
		FROM duckdb_constraints()
		WHERE schema_name = 'vector_reopen'
		  AND table_name = 'vectors'
		  AND constraint_type = 'CHECK'
	`).Scan(&constraintText))
	normalizedConstraint := strings.ToLower(strings.ReplaceAll(constraintText, `"`, ""))
	require.Contains(t, normalizedConstraint, "octet_length(v)")
	require.Contains(t, normalizedConstraint, "= 8")

	var generatedExpression string
	require.NoError(t, conn.QueryRowContext(ctx, `
		SELECT column_default
		FROM duckdb_columns()
		WHERE schema_name = 'vector_reopen'
		  AND table_name = 'vectors'
		  AND column_name = 'generated_v'
	`).Scan(&generatedExpression))
	require.Contains(t, strings.ToLower(generatedExpression), "string_to_vector")
	require.Contains(t, generatedExpression, "2")

	query := "SELECT v, generated_v FROM vector_reopen.vectors WHERE id = 1"
	ctx = ctx.WithQuery(query)
	_, iter, _, err := engine.Query(ctx, query)
	require.NoError(t, err)
	rows, err := sql.RowIterToRows(ctx, iter)
	require.NoError(t, err)
	require.Equal(t, []float32{1.25, -2.5}, decodeStoredVector(t, rows[0][0]))
	require.Equal(t, []float32{3, 4}, decodeStoredVector(t, rows[0][1]))

	_, err = conn.ExecContext(ctx, "INSERT INTO vectors (id, v, source) VALUES (2, STRING_TO_VECTOR('[1]'), '[5,6]')")
	require.ErrorContains(t, err, "CHECK constraint failed")
}

func decodeStoredVector(t *testing.T, value any) []float32 {
	t.Helper()
	encoded, ok := value.([]byte)
	require.True(t, ok)
	require.NotEmpty(t, encoded)
	decoded, err := sql.DecodeVector(encoded)
	require.NoError(t, err)
	return decoded
}
