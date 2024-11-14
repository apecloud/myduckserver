package pgserver

import (
	stdsql "database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/marcboeker/go-duckdb"
)

var defaultTypeMap = pgtype.NewMap()

var duckdbTypeStrToPostgresTypeStr = map[string]string{
	"INVALID":      "unknown",
	"BOOLEAN":      "bool",
	"TINYINT":      "int2",
	"SMALLINT":     "int2",
	"INTEGER":      "int4",
	"BIGINT":       "int8",
	"UTINYINT":     "int2",    // Unsigned tinyint, approximated to int2
	"USMALLINT":    "int4",    // Unsigned smallint, approximated to int4
	"UINTEGER":     "int8",    // Unsigned integer, approximated to int8
	"UBIGINT":      "numeric", // Unsigned bigint, approximated to numeric for large values
	"FLOAT":        "float4",
	"DOUBLE":       "float8",
	"TIMESTAMP":    "timestamp",
	"DATE":         "date",
	"TIME":         "time",
	"INTERVAL":     "interval",
	"HUGEINT":      "numeric",
	"UHUGEINT":     "numeric",
	"VARCHAR":      "text",
	"BLOB":         "bytea",
	"DECIMAL":      "numeric",
	"TIMESTAMP_S":  "timestamp",
	"TIMESTAMP_MS": "timestamp",
	"TIMESTAMP_NS": "timestamp",
	"ENUM":         "text",
	"UUID":         "uuid",
	"BIT":          "bit",
	"TIME_TZ":      "timetz",
	"TIMESTAMP_TZ": "timestamptz",
	"ANY":          "text",    // Generic ANY type approximated to text
	"VARINT":       "numeric", // Variable integer, mapped to numeric
}

var duckdbTypeToPostgresOID = map[duckdb.Type]uint32{
	duckdb.TYPE_INVALID:      pgtype.UnknownOID,
	duckdb.TYPE_BOOLEAN:      pgtype.BoolOID,
	duckdb.TYPE_TINYINT:      pgtype.Int2OID,
	duckdb.TYPE_SMALLINT:     pgtype.Int2OID,
	duckdb.TYPE_INTEGER:      pgtype.Int4OID,
	duckdb.TYPE_BIGINT:       pgtype.Int8OID,
	duckdb.TYPE_UTINYINT:     pgtype.Int2OID,
	duckdb.TYPE_USMALLINT:    pgtype.Int4OID,
	duckdb.TYPE_UINTEGER:     pgtype.Int8OID,
	duckdb.TYPE_UBIGINT:      pgtype.NumericOID,
	duckdb.TYPE_FLOAT:        pgtype.Float4OID,
	duckdb.TYPE_DOUBLE:       pgtype.Float8OID,
	duckdb.TYPE_DECIMAL:      pgtype.NumericOID,
	duckdb.TYPE_VARCHAR:      pgtype.TextOID,
	duckdb.TYPE_BLOB:         pgtype.ByteaOID,
	duckdb.TYPE_TIMESTAMP:    pgtype.TimestampOID,
	duckdb.TYPE_DATE:         pgtype.DateOID,
	duckdb.TYPE_TIME:         pgtype.TimeOID,
	duckdb.TYPE_INTERVAL:     pgtype.IntervalOID,
	duckdb.TYPE_HUGEINT:      pgtype.NumericOID,
	duckdb.TYPE_UHUGEINT:     pgtype.NumericOID,
	duckdb.TYPE_TIMESTAMP_S:  pgtype.TimestampOID,
	duckdb.TYPE_TIMESTAMP_MS: pgtype.TimestampOID,
	duckdb.TYPE_TIMESTAMP_NS: pgtype.TimestampOID,
	duckdb.TYPE_ENUM:         pgtype.TextOID,
	duckdb.TYPE_UUID:         pgtype.UUIDOID,
	duckdb.TYPE_BIT:          pgtype.BitOID,
	duckdb.TYPE_TIME_TZ:      pgtype.TimetzOID,
	duckdb.TYPE_TIMESTAMP_TZ: pgtype.TimestamptzOID,
	duckdb.TYPE_ANY:          pgtype.TextOID,
	duckdb.TYPE_VARINT:       pgtype.NumericOID,
}

var pgTypeSizes = map[uint32]int32{
	pgtype.BoolOID:        1,  // bool
	pgtype.ByteaOID:       -1, // bytea
	pgtype.NameOID:        -1, // name
	pgtype.Int8OID:        8,  // int8
	pgtype.Int2OID:        2,  // int2
	pgtype.Int4OID:        4,  // int4
	pgtype.TextOID:        -1, // text
	pgtype.OIDOID:         4,  // oid
	pgtype.TIDOID:         8,  // tid
	pgtype.XIDOID:         -1, // xid
	pgtype.CIDOID:         -1, // cid
	pgtype.JSONOID:        -1, // json
	pgtype.XMLOID:         -1, // xml
	pgtype.PointOID:       8,  // point
	pgtype.Float4OID:      4,  // float4
	pgtype.Float8OID:      8,  // float8
	pgtype.UnknownOID:     -1, // unknown
	pgtype.MacaddrOID:     -1, // macaddr
	pgtype.InetOID:        -1, // inet
	pgtype.BoolArrayOID:   -1, // bool[]
	pgtype.ByteaArrayOID:  -1, // bytea[]
	pgtype.NameArrayOID:   -1, // name[]
	pgtype.Int2ArrayOID:   -1, // int2[]
	pgtype.Int4ArrayOID:   -1, // int4[]
	pgtype.TextArrayOID:   -1, // text[]
	pgtype.BPCharOID:      -1, // char(n)
	pgtype.VarcharOID:     -1, // varchar
	pgtype.DateOID:        4,  // date
	pgtype.TimeOID:        8,  // time
	pgtype.TimestampOID:   8,  // timestamp
	pgtype.TimestamptzOID: 8,  // timestamptz
	pgtype.NumericOID:     -1, // numeric
	pgtype.UUIDOID:        16, // uuid
}

func inferSchema(rows *stdsql.Rows) (sql.Schema, error) {
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	schema := make(sql.Schema, len(types))
	for i, t := range types {
		pgTypeName, ok := duckdbTypeStrToPostgresTypeStr[t.DatabaseTypeName()]
		if !ok {
			return nil, fmt.Errorf("unsupported type %s", t.DatabaseTypeName())
		}
		pgType, ok := defaultTypeMap.TypeForName(pgTypeName)
		if !ok {
			return nil, fmt.Errorf("unsupported type %s", pgTypeName)
		}
		nullable, _ := t.Nullable()

		schema[i] = &sql.Column{
			Name: t.Name(),
			Type: PostgresType{
				PG:   pgType,
				Size: pgTypeSizes[pgType.OID],
			},
			Nullable: nullable,
		}
	}

	return schema, nil
}

func inferDriverSchema(rows driver.Rows) (sql.Schema, error) {
	columns := rows.Columns()
	schema := make(sql.Schema, len(columns))
	for i, colName := range columns {
		var pgTypeName string
		if colType, ok := rows.(driver.RowsColumnTypeDatabaseTypeName); ok {
			pgTypeName = duckdbTypeStrToPostgresTypeStr[colType.ColumnTypeDatabaseTypeName(i)]
		} else {
			pgTypeName = "text" // Default to text if type name is not available
		}

		pgType, ok := defaultTypeMap.TypeForName(pgTypeName)
		if !ok {
			return nil, fmt.Errorf("unsupported type %s", pgTypeName)
		}

		nullable := true
		if colNullable, ok := rows.(driver.RowsColumnTypeNullable); ok {
			nullable, _ = colNullable.ColumnTypeNullable(i)
		}

		schema[i] = &sql.Column{
			Name: colName,
			Type: PostgresType{
				PG:   pgType,
				Size: pgTypeSizes[pgType.OID],
			},
			Nullable: nullable,
		}
	}
	return schema, nil
}

type PostgresType struct {
	PG *pgtype.Type
	// https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-ROWDESCRIPTION
	Size int32
}

func (p PostgresType) Encode(v any, buf []byte) ([]byte, error) {
	return defaultTypeMap.Encode(p.PG.OID, p.PG.Codec.PreferredFormat(), v, buf)
}

var _ sql.Type = PostgresType{}

func (p PostgresType) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	panic("not implemented")
}

func (p PostgresType) Compare(v1 interface{}, v2 interface{}) (int, error) {
	panic("not implemented")
}

func (p PostgresType) Convert(v interface{}) (interface{}, sql.ConvertInRange, error) {
	panic("not implemented")
}

func (p PostgresType) Equals(t sql.Type) bool {
	panic("not implemented")
}

func (p PostgresType) MaxTextResponseByteLength(_ *sql.Context) uint32 {
	panic("not implemented")
}

func (p PostgresType) Promote() sql.Type {
	panic("not implemented")
}

func (p PostgresType) SQL(ctx *sql.Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	panic("not implemented")
}

func (p PostgresType) Type() query.Type {
	panic("not implemented")
}

func (p PostgresType) ValueType() reflect.Type {
	panic("not implemented")
}

func (p PostgresType) Zero() interface{} {
	panic("not implemented")
}

func (p PostgresType) String() string {
	return fmt.Sprintf("PostgresType(%s)", p.PG.Name)
}
