package pgserver

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/pgtypes"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestPreparedInsertReturningMetadataUsesActiveExecutionSnapshot(t *testing.T) {
	parsed, err := parser.ParseOne("INSERT INTO object_rows VALUES ($1, $2) RETURNING id, name")
	require.NoError(t, err)
	insert, ok := parsed.AST.(*tree.Insert)
	require.True(t, ok)

	metadataQuery := postgresInsertReturningSchemaQuery(insert)
	require.Equal(t,
		"SELECT id, name FROM object_rows LIMIT 0",
		metadataQuery,
	)
	routedQuery, changed := backend.RewriteSQLRelations(metadataQuery, map[string]string{
		"object_rows": `"__myduck_ducklake"."app"."object_rows"`,
	})
	require.True(t, changed)
	require.Equal(t,
		`SELECT id, name FROM "__myduck_ducklake"."app"."object_rows" LIMIT 0`,
		routedQuery,
	)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	mock.ExpectQuery(regexp.QuoteMeta(routedQuery)).WillReturnRows(
		sqlmock.NewRowsWithColumnDefinition(
			sqlmock.NewColumn("id").OfType("INTEGER", int64(0)).Nullable(false),
			sqlmock.NewColumn("name").OfType("VARCHAR", "").Nullable(false),
		),
	)

	// Passing a context without a session makes an accidental fallback to
	// adapter.QueryCatalog fail immediately. The explicit *sql.Tx is the only
	// legal metadata executor for this active snapshot.
	schema, err := inferPostgresInsertReturningSchema(sql.NewEmptyContext(), routedQuery, tx)
	require.NoError(t, err)
	require.Len(t, schema, 2)
	require.Equal(t, "id", schema[0].Name)
	require.Equal(t, "name", schema[1].Name)
	idType, ok := schema[0].Type.(pgtypes.PostgresType)
	require.True(t, ok)
	require.Equal(t, uint32(pgtype.Int4OID), idType.PG.OID)

	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}
