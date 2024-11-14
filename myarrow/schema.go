package myarrow

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apecloud/myduckserver/pgtypes"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

func ToArrowSchema(s sql.Schema, dictionary ...int) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(s))
	for i, col := range s {
		at, err := ToArrowType(col.Type)
		if err != nil {
			return nil, err
		}
		fields[i] = arrow.Field{
			Name:     col.Name,
			Type:     at,
			Nullable: col.Nullable,
		}
	}
	for _, i := range dictionary {
		fields[i].Type = &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Uint32,
			ValueType: fields[i].Type,
		}
	}
	return arrow.NewSchema(fields, nil), nil
}

// ToArrowType translates the MySQL Type to Arrow Type.
func ToArrowType(t sql.Type) (arrow.DataType, error) {
	at := toArrowType(t)
	if at == nil {
		return nil, sql.ErrInvalidType.New(t)
	}
	return at, nil
}

func toArrowType(t sql.Type) arrow.DataType {
	if pgType, ok := t.(pgtypes.PostgresType); ok {
		return pgtypes.PostgresTypeToArrowType(pgType.PG.OID)
	}
	switch t.Type() {
	case query.Type_UINT8:
		return arrow.PrimitiveTypes.Uint8
	case query.Type_INT8:
		return arrow.PrimitiveTypes.Int8
	case query.Type_UINT16:
		return arrow.PrimitiveTypes.Uint16
	case query.Type_INT16:
		return arrow.PrimitiveTypes.Int16
	case query.Type_UINT24:
		return arrow.PrimitiveTypes.Uint32
	case query.Type_INT24:
		return arrow.PrimitiveTypes.Int32
	case query.Type_UINT32:
		return arrow.PrimitiveTypes.Uint32
	case query.Type_INT32:
		return arrow.PrimitiveTypes.Int32
	case query.Type_UINT64:
		return arrow.PrimitiveTypes.Uint64
	case query.Type_INT64:
		return arrow.PrimitiveTypes.Int64
	case query.Type_FLOAT32:
		return arrow.PrimitiveTypes.Float32
	case query.Type_FLOAT64:
		return arrow.PrimitiveTypes.Float64
	case query.Type_TIMESTAMP:
		switch t.(sql.DatetimeType).Precision() {
		case 0:
			return arrow.FixedWidthTypes.Timestamp_s
		case 1, 2, 3:
			return arrow.FixedWidthTypes.Timestamp_ms
		default:
			return arrow.FixedWidthTypes.Timestamp_us
		}
	case query.Type_DATE:
		return arrow.FixedWidthTypes.Date32
	case query.Type_TIME:
		return arrow.FixedWidthTypes.Duration_us
	case query.Type_DATETIME:
		var unit arrow.TimeUnit
		switch t.(sql.DatetimeType).Precision() {
		case 0:
			unit = arrow.Second
		case 1, 2, 3:
			unit = arrow.Millisecond
		default:
			unit = arrow.Microsecond
		}
		return &arrow.TimestampType{Unit: unit, TimeZone: ""}
	case query.Type_YEAR:
		return arrow.PrimitiveTypes.Uint16
	case query.Type_DECIMAL:
		dt := t.(sql.DecimalType)
		if dt.Precision() > 38 {
			return &arrow.Decimal256Type{
				Precision: int32(dt.Precision()),
				Scale:     int32(dt.Scale()),
			}
		}
		return &arrow.Decimal128Type{
			Precision: int32(dt.Precision()),
			Scale:     int32(dt.Scale()),
		}
	case query.Type_TEXT:
		return arrow.BinaryTypes.String
	case query.Type_BLOB:
		return arrow.BinaryTypes.Binary
	case query.Type_VARCHAR:
		return arrow.BinaryTypes.String
	case query.Type_VARBINARY:
		return arrow.BinaryTypes.Binary
	case query.Type_CHAR:
		return arrow.BinaryTypes.String
	case query.Type_BINARY:
		return arrow.BinaryTypes.Binary
	case query.Type_BIT:
		return arrow.PrimitiveTypes.Uint64
	case query.Type_ENUM:
		return arrow.BinaryTypes.String
	case query.Type_SET:
		return arrow.BinaryTypes.String
	case query.Type_JSON:
		return arrow.BinaryTypes.String
	case query.Type_GEOMETRY:
		return arrow.BinaryTypes.Binary
	default:
		panic("unsupported data type")
	}
}
