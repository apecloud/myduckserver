package pgtypes

import (
	stdsql "database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/marcboeker/go-duckdb"

	"github.com/dolthub/go-mysql-server/sql"
)

var DefaultTypeMap *pgtype.Map

func init() {
	DefaultTypeMap = pgtype.NewMap()
}

var duckdbTypeNameToPostgresTypeName = map[string]string{
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
	"TIMETZ":       "timetz",
	"TIMESTAMPTZ":  "timestamptz",
	"ANY":          "text",    // Generic ANY type approximated to text
	"VARINT":       "numeric", // Variable integer, mapped to numeric
}

var DuckdbTypeToPostgresOID = map[duckdb.Type]uint32{
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

var PostgresOIDToDuckDBTypeName = map[uint32]string{
	pgtype.BoolOID:        "BOOLEAN",
	pgtype.ByteaOID:       "BLOB",
	pgtype.QCharOID:       "UTINYINT",
	pgtype.Int2OID:        "SMALLINT",
	pgtype.Int4OID:        "INTEGER",
	pgtype.Int8OID:        "BIGINT",
	pgtype.Float4OID:      "FLOAT",
	pgtype.Float8OID:      "DOUBLE",
	pgtype.NumericOID:     "DECIMAL",
	pgtype.TextOID:        "VARCHAR",
	pgtype.VarcharOID:     "VARCHAR",
	pgtype.BPCharOID:      "VARCHAR",
	pgtype.NameOID:        "VARCHAR",
	pgtype.DateOID:        "DATE",
	pgtype.TimeOID:        "TIME",
	pgtype.TimetzOID:      "TIMETZ",
	pgtype.TimestampOID:   "TIMESTAMP",
	pgtype.TimestamptzOID: "TIMESTAMPTZ",
	pgtype.IntervalOID:    "INTERVAL",
	pgtype.UUIDOID:        "UUID",
	pgtype.BitOID:         "BIT",
	pgtype.VarbitOID:      "BIT",
	pgtype.JSONOID:        "JSON",
	pgtype.JSONBOID:       "JSON",
	pgtype.OIDOID:         "UINTEGER",
	pgtype.XIDOID:         "UINTEGER",
	pgtype.CIDOID:         "UINTEGER",
	pgtype.TIDOID:         "UBIGINT",
	// Postgres one-dimensional ARRAY types -> DuckDB LIST types
	pgtype.BoolArrayOID:        "BOOLEAN[]",
	pgtype.QCharArrayOID:       "TINYINT[]",
	pgtype.Int2ArrayOID:        "SMALLINT[]",
	pgtype.Int4ArrayOID:        "INTEGER[]",
	pgtype.Int8ArrayOID:        "BIGINT[]",
	pgtype.Float4ArrayOID:      "FLOAT[]",
	pgtype.Float8ArrayOID:      "DOUBLE[]",
	pgtype.NumericArrayOID:     "DECIMAL[]",
	pgtype.TextArrayOID:        "VARCHAR[]",
	pgtype.VarcharArrayOID:     "VARCHAR[]",
	pgtype.BPCharArrayOID:      "VARCHAR[]",
	pgtype.NameArrayOID:        "VARCHAR[]",
	pgtype.UUIDArrayOID:        "UUID[]",
	pgtype.DateArrayOID:        "DATE[]",
	pgtype.TimeArrayOID:        "TIME[]",
	pgtype.TimetzArrayOID:      "TIMETZ[]",
	pgtype.TimestampArrayOID:   "TIMESTAMP[]",
	pgtype.TimestamptzArrayOID: "TIMESTAMPTZ[]",
	pgtype.IntervalArrayOID:    "INTERVAL[]",
	pgtype.JSONArrayOID:        "JSON[]",
	pgtype.JSONBArrayOID:       "JSON[]",
	pgtype.BitArrayOID:         "BIT[]",
	// ...additional mappings as needed...
}

var postgresTypeSizes = map[uint32]int32{
	pgtype.BoolOID:        1,  // bool
	pgtype.NameOID:        64, // name
	pgtype.Int8OID:        8,  // int8
	pgtype.Int2OID:        2,  // int2
	pgtype.Int4OID:        4,  // int4
	pgtype.OIDOID:         4,  // oid
	pgtype.TIDOID:         6,  // tid
	pgtype.XIDOID:         4,  // xid
	pgtype.CIDOID:         4,  // cid
	pgtype.PointOID:       8,  // point
	pgtype.Float4OID:      4,  // float4
	pgtype.Float8OID:      8,  // float8
	pgtype.UnknownOID:     -2, // unknown
	pgtype.MacaddrOID:     6,  // macaddr
	pgtype.Macaddr8OID:    8,  // macaddr8
	pgtype.DateOID:        4,  // date
	pgtype.TimeOID:        8,  // time
	pgtype.TimetzOID:      12, // timetz
	pgtype.TimestampOID:   8,  // timestamp
	pgtype.TimestamptzOID: 8,  // timestamptz
	pgtype.IntervalOID:    16, // interval
	pgtype.UUIDOID:        16, // uuid
}

func PostgresTypeToArrowType(p PostgresType) arrow.DataType {
	return pgTypeToArrowType(p.PG, p.Precision, p.Scale)
}

func pgTypeToArrowType(pt *pgtype.Type, precision, scale int32) arrow.DataType {
	switch pt.OID {
	case pgtype.BoolOID:
		return arrow.FixedWidthTypes.Boolean
	case pgtype.QCharOID:
		return arrow.PrimitiveTypes.Uint8
	case pgtype.ByteaOID:
		return arrow.BinaryTypes.Binary
	case pgtype.NameOID, pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID, pgtype.JSONOID, pgtype.JSONBOID, pgtype.XMLOID:
		return arrow.BinaryTypes.String
	case pgtype.Int8OID:
		return arrow.PrimitiveTypes.Int64
	case pgtype.Int2OID:
		return arrow.PrimitiveTypes.Int16
	case pgtype.Int4OID:
		return arrow.PrimitiveTypes.Int32
	case pgtype.OIDOID:
		return arrow.PrimitiveTypes.Uint32
	case pgtype.TIDOID:
		return arrow.PrimitiveTypes.Uint64
	case pgtype.Float4OID:
		return arrow.PrimitiveTypes.Float32
	case pgtype.Float8OID:
		return arrow.PrimitiveTypes.Float64
	case pgtype.DateOID:
		return arrow.FixedWidthTypes.Date32
	case pgtype.TimeOID, pgtype.TimetzOID:
		return arrow.FixedWidthTypes.Time64ns
	case pgtype.TimestampOID:
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}
	case pgtype.TimestamptzOID:
		return arrow.FixedWidthTypes.Timestamp_us
	case pgtype.NumericOID:
		if precision > 0 && scale >= 0 {
			if precision <= 38 {
				return &arrow.Decimal128Type{Precision: precision, Scale: scale}
				// 256-bit decimal is not supported in DuckDB
				// } else if precision <= 76 {
				// 	return &arrow.Decimal256Type{Precision: precision, Scale: scale}
			} else {
				// Precision too large, default to string
				return arrow.BinaryTypes.String
			}
		} else {
			// Unknown precision and scale, default to string
			return arrow.BinaryTypes.String
		}
	case pgtype.UUIDOID:
		// TODO(fan): Currently, DuckDB does not support BLOB -> UUID conversion,
		//   so we use a string type for UUIDs.
		// return &arrow.FixedSizeBinaryType{ByteWidth: 16}
		return arrow.BinaryTypes.String
	case pgtype.PointOID:
		return arrow.StructOf(
			arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64},
			arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64},
		)
	default:
		// Check for array types
		if ac, ok := pt.Codec.(*pgtype.ArrayCodec); ok {
			// Map the element type recursively
			return arrow.ListOf(pgTypeToArrowType(ac.ElementType, precision, scale))
		}

		return arrow.BinaryTypes.Binary // fall back for unknown types
	}
}

func PostgresTypeSize(oid uint32) int32 {
	if s, ok := postgresTypeSizes[oid]; ok {
		return s
	}
	return -1
}

// GoDuckDBTypeNameToPostgresType parses a type name reported by the go-duckdb driver
// into a corresponding pgtype.Type with its precision and scale (if applicable).
// Unknown types are fallback to text.
//
// TODO(fan): Make this function more rigorous for nested types.
func GoDuckDBTypeNameToPostgresType(name string) (pt *pgtype.Type, precision, scale int32, fallback bool, err error) {
	var list bool
	if strings.HasSuffix(name, "[]") {
		// LIST type
		// Ref: logicalTypeNameList in go-duckdb
		name = strings.TrimSuffix(name, "[]")
		list = true
	}

	if strings.HasPrefix(name, "DECIMAL") {
		// Scan precision and scale from the type name
		// Ref: logicalTypeNameDecimal in go-duckdb
		if _, err = fmt.Sscanf(name, "DECIMAL(%d,%d)", &precision, &scale); err != nil {
			return nil, 0, 0, false, err
		}
		name = "DECIMAL"
	}
	pgTypeName, ok := duckdbTypeNameToPostgresTypeName[name]
	if !ok {
		pgTypeName, fallback = "text", true // Default to text if it is an unknown type
	}
	if list {
		// the pgx/pgtype package prefixes array type names with an underscore:
		// https://github.com/jackc/pgx/blob/master/pgtype/pgtype_default.go
		pgTypeName = `_` + pgTypeName
	}
	pt, ok = DefaultTypeMap.TypeForName(pgTypeName)
	if !ok {
		pt, ok = DefaultTypeMap.TypeForName("text")
		fallback = true
	}
	if !ok {
		return nil, 0, 0, fallback, fmt.Errorf("unsupported type %s", name)
	}
	return
}

func InferSchema(rows *stdsql.Rows) (sql.Schema, error) {
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	schema := make(sql.Schema, len(types))
	for i, t := range types {
		pgType, precision, scale, fallback, err := GoDuckDBTypeNameToPostgresType(t.DatabaseTypeName())
		if err != nil {
			return nil, err
		}
		nullable, _ := t.Nullable()

		switch pgType.OID {
		case pgtype.NumericOID, pgtype.NumericArrayOID:
			// Currently, ok is always false
			if p, s, ok := t.DecimalSize(); ok {
				precision = int32(p)
				scale = int32(s)
			}
		}

		schema[i] = &sql.Column{
			Name: t.Name(),
			Type: PostgresType{
				PG:        pgType,
				Size:      PostgresTypeSize(pgType.OID),
				Precision: precision,
				Scale:     scale,
				Fallback:  fallback,
			},
			Nullable: nullable,
		}
	}

	return schema, nil
}

func InferDriverSchema(rows driver.Rows) (sql.Schema, error) {
	columns := rows.Columns()
	schema := make(sql.Schema, len(columns))
	for i, colName := range columns {
		var dbTypeName string
		if colType, ok := rows.(driver.RowsColumnTypeDatabaseTypeName); ok {
			dbTypeName = colType.ColumnTypeDatabaseTypeName(i)
		} else {
			return nil, fmt.Errorf("driver does not support RowsColumnTypeDatabaseTypeName")
		}

		pgType, precision, scale, fallback, err := GoDuckDBTypeNameToPostgresType(dbTypeName)
		if err != nil {
			return nil, err
		}

		nullable := true
		if colNullable, ok := rows.(driver.RowsColumnTypeNullable); ok {
			nullable, _ = colNullable.ColumnTypeNullable(i)
		}

		switch pgType.OID {
		case pgtype.NumericOID, pgtype.NumericArrayOID:
			// Currently, ok is always false
			if colPrecisionScale, ok := rows.(driver.RowsColumnTypePrecisionScale); ok {
				p, s, ok := colPrecisionScale.ColumnTypePrecisionScale(i)
				if ok {
					precision = int32(p)
					scale = int32(s)
				}
			}
		}

		schema[i] = &sql.Column{
			Name: colName,
			Type: PostgresType{
				PG:        pgType,
				Size:      PostgresTypeSize(pgType.OID),
				Precision: precision,
				Scale:     scale,
				Fallback:  fallback,
			},
			Nullable: nullable,
		}
	}
	return schema, nil
}

type PostgresType struct {
	PG        *pgtype.Type
	Precision int32
	Scale     int32
	// https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-ROWDESCRIPTION
	Size int32
	// Fallback indicates that the type is not supported by the pgtype package and is approximated to the `text` fallback type.
	Fallback bool
}

func NewPostgresType(registry *pgtype.Map, oid uint32, modifier int32) (PostgresType, error) {
	t, ok := registry.TypeForOID(oid)
	if !ok {
		return PostgresType{}, fmt.Errorf("unsupported type OID %d", oid)
	}

	var precision, scale int32
	switch oid {
	case pgtype.NumericOID, pgtype.NumericArrayOID:
		precision, scale, _ = DecodePrecisionScale(int(modifier))
	}

	return PostgresType{
		PG:        t,
		Precision: precision,
		Scale:     scale,
		Size:      PostgresTypeSize(oid),
	}, nil
}

func (p PostgresType) Encode(v any, buf []byte) ([]byte, error) {
	return DefaultTypeMap.Encode(p.PG.OID, p.PG.Codec.PreferredFormat(), v, buf)
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

func DecodePrecisionScale(typmod int) (precision, scale int32, ok bool) {
	if typmod > 0 {
		typmod -= 4 // remove VARHDRSZ
		precision = int32((typmod >> 16) & 0xFFFF)
		scale = int32(typmod & 0xFFFF)
		ok = true
	}
	// The max precision is 131072, which is larger than the max int16 value.
	// Therefore, if the precision is larger than the max int16 value, we default to unknown precision and scale.
	return
}
