package pgserver

import (
	"context"
	_ "embed"
	"strconv"
	"strings"
	"testing"

	"github.com/apecloud/myduckserver/testutil"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// Captured SQLAlchemy 1.4 query from Superset 6.1.0; only its OID is parameterized.
//
//go:embed testdata/sqlalchemy14-columns.sql
var reflectedColumnsSQL string

func TestSQLAlchemyPhysicalColumnReflection(t *testing.T) {
	env := testutil.NewTestEnv()
	require.NoError(t, testutil.StartDuckSqlServer(t, testutil.CreateTestDir(t), nil, env))
	defer testutil.StopDuckSqlServer(t, env.DuckProcess)
	_, err := env.MyDuckServer.Exec(`CREATE DATABASE reflection`)
	require.NoError(t, err)
	_, err = env.MyDuckServer.Exec(`CREATE TABLE reflection.orders (
        id INT AUTO_INCREMENT PRIMARY KEY,
        sku VARCHAR(32) NOT NULL COMMENT 'Merchant sku',
        amount DECIMAL(12,2) DEFAULT 12.50,
        placed TIMESTAMP NULL,
        score DOUBLE NULL,
        optional_text TEXT NULL
    )`)
	require.NoError(t, err)
	_, err = env.MyDuckServer.Exec(`INSERT INTO reflection.orders (sku) VALUES ('first'), ('second')`)
	require.NoError(t, err)
	ctx := context.Background()
	for _, mode := range []string{"simple_protocol", "cache_statement"} {
		t.Run(mode, func(t *testing.T) {
			conn, err := pgx.Connect(ctx, "postgresql://postgres@127.0.0.1:"+strconv.Itoa(env.DuckPgPort)+"/postgres?default_query_exec_mode="+mode)
			require.NoError(t, err)
			defer conn.Close(ctx)
			var oid int64
			require.NoError(t, conn.QueryRow(ctx, `SELECT table_oid FROM duckdb_tables() WHERE schema_name='reflection' AND table_name='orders'`).Scan(&oid))
			type column struct {
				name, typ string
				def       *string
				notNull   bool
				oid       int64
				comment   *string
				generated *string
				identity  *string
			}
			for attempt := 0; attempt < 2; attempt++ {
				rows, err := conn.Query(ctx, reflectedColumnsSQL, oid)
				require.NoError(t, err)
				var columns []column
				for rows.Next() {
					var c column
					require.NoError(t, rows.Scan(&c.name, &c.typ, &c.def, &c.notNull, &c.oid, &c.comment, &c.generated, &c.identity))
					require.Equal(t, oid, c.oid)
					require.Nil(t, c.generated)
					require.Nil(t, c.identity, "AUTO_INCREMENT is a sequence default, not a PG identity column")
					columns = append(columns, c)
				}
				require.NoError(t, rows.Err())
				rows.Close()
				require.Len(t, columns, 6)
				require.Equal(t, []string{"id", "sku", "amount", "placed", "score", "optional_text"}, []string{columns[0].name, columns[1].name, columns[2].name, columns[3].name, columns[4].name, columns[5].name})
				require.Equal(t, "integer", columns[0].typ)
				require.True(t, columns[0].notNull)
				require.NotNil(t, columns[0].def)
				require.Contains(t, *columns[0].def, "nextval(")
				require.Equal(t, "character varying(32)", columns[1].typ)
				require.True(t, columns[1].notNull)
				require.NotNil(t, columns[1].comment)
				require.Equal(t, "Merchant sku", *columns[1].comment)
				require.Equal(t, "numeric(12,2)", strings.ReplaceAll(columns[2].typ, " ", ""))
				require.NotNil(t, columns[2].def)
				require.Contains(t, *columns[2].def, "12.50")
				require.False(t, columns[2].notNull)
				require.Equal(t, "timestamp without time zone", columns[3].typ)
				require.Equal(t, "double precision", columns[4].typ)
				require.False(t, columns[5].notNull)
				require.Nil(t, columns[5].def)
				require.Nil(t, columns[5].comment)
			}
			rows, err := conn.Query(ctx, reflectedColumnsSQL, int64(0))
			require.NoError(t, err)
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
			rows.Close()

			domainRows, err := conn.Query(ctx, sqlAlchemyDomainsQuery)
			require.NoError(t, err)
			domains := map[string]string{}
			for domainRows.Next() {
				var name, typ, schema string
				var nullable, visible bool
				var def *string
				require.NoError(t, domainRows.Scan(&name, &typ, &nullable, &def, &visible, &schema))
				require.Equal(t, "information_schema", schema)
				require.False(t, visible)
				domains[name] = typ
			}
			require.NoError(t, domainRows.Err())
			domainRows.Close()
			require.Equal(t, "integer", domains["cardinal_number"])
			require.Equal(t, "character varying(3)", domains["yes_or_no"])
			require.Equal(t, "timestamp(2) with time zone", domains["time_stamp"])
			var count int64
			require.NoError(t, conn.QueryRow(ctx, `SELECT COUNT(id) FROM reflection.orders`).Scan(&count))
			require.Equal(t, int64(2), count)
		})
	}
}

func TestSQLAlchemyColumnQueryDoesNotRewriteOtherSQL(t *testing.T) {
	for _, query := range []string{
		`SELECT 'pg_get_serial_sequence()'`,
		strings.Replace(reflectedColumnsSQL, "'always'", "'something_else'", 1),
		strings.Replace(reflectedColumnsSQL, "ORDER BY a.attnum", "AND a.attname = 'id' ORDER BY a.attnum", 1),
		strings.Replace(reflectedColumnsSQL, "$1", "1 OR 1=1", 1),
	} {
		require.Empty(t, rewriteSQLAlchemyReflection(query))
	}
}
