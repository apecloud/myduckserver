package logrepl

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/jackc/pgx/v5/pgtype"
)

// decodeAndAppend decodes Postgres text format data and appends directly to Arrow builder
func decodeAndAppend(typeMap *pgtype.Map, data []byte, dataType uint32, builder array.Builder) (int, error) {
	if data == nil {
		builder.AppendNull()
		return 0, nil
	}

	if dt, ok := typeMap.TypeForOID(dataType); ok {
		format := pgtype.TextFormatCode
		switch dataType {
		case pgtype.BoolOID:
			if b, ok := builder.(*array.BooleanBuilder); ok {
				var v bool
				if err := pgtype.BoolCodec.PlanScan(typeMap, dataType, format, &v).Scan(data, &v); err != nil {
					return 0, err
				}
				b.Append(v)
				return 1, nil
			}

		case pgtype.Int2OID:
			if b, ok := builder.(*array.Int16Builder); ok {
				var v int16
				if err := pgtype.Int2Codec.PlanScan(typeMap, dataType, format, &v).Scan(data, &v); err != nil {
					return 0, err
				}
				b.Append(v)
				return 2, nil
			}

		case pgtype.Int4OID:
			if b, ok := builder.(*array.Int32Builder); ok {
				var v int32
				if err := pgtype.Int4Codec.PlanScan(typeMap, dataType, format, &v).Scan(data, &v); err != nil {
					return 0, err
				}
				b.Append(v)
				return 4, nil
			}

		case pgtype.Int8OID:
			if b, ok := builder.(*array.Int64Builder); ok {
				var v int64
				if err := pgtype.Int8Codec.PlanScan(typeMap, dataType, format, &v).Scan(data, &v); err != nil {
					return 0, err
				}
				b.Append(v)
				return 8, nil
			}

		case pgtype.Float4OID:
			if b, ok := builder.(*array.Float32Builder); ok {
				var v float32
				if err := pgtype.Float4Codec.PlanScan(typeMap, dataType, format, &v).Scan(data, &v); err != nil {
					return 0, err
				}
				b.Append(v)
				return 4, nil
			}

		case pgtype.Float8OID:
			if b, ok := builder.(*array.Float64Builder); ok {
				var v float64
				if err := pgtype.Float8Codec.PlanScan(typeMap, dataType, format, &v).Scan(data, &v); err != nil {
					return 0, err
				}
				b.Append(v)
				return 8, nil
			}

		case pgtype.TimestampOID, pgtype.TimestamptzOID:
			if b, ok := builder.(*array.TimestampBuilder); ok {
				var v time.Time
				if err := pgtype.TimestamptzCodec.PlanScan(typeMap, dataType, format, &v).Scan(data, &v); err != nil {
					return 0, err
				}
				b.Append(v)
				return 8, nil
			}

		case pgtype.DateOID:
			if b, ok := builder.(*array.Date32Builder); ok {
				var v time.Time
				if err := pgtype.DateCodec.PlanScan(typeMap, dataType, format, &v).Scan(data, &v); err != nil {
					return 0, err
				}
				b.Append(arrow.Date32FromTime(v))
				return 4, nil
			}

		case pgtype.NumericOID:
			if b, ok := builder.(*array.StringBuilder); ok {
				var v pgtype.Numeric
				if err := pgtype.NumericCodec.PlanScan(typeMap, dataType, format, &v).Scan(data, &v); err != nil {
					return 0, err
				}
				b.Append(v.String())
				return len(data), nil
			}

		case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID, pgtype.NameOID:
			if b, ok := builder.(*array.StringBuilder); ok {
				var v string
				if err := pgtype.TextCodec.PlanScan(typeMap, dataType, format, &v).Scan(data, &v); err != nil {
					return 0, err
				}
				b.Append(v)
				return len(v), nil
			}

		case pgtype.ByteaOID:
			if b, ok := builder.(*array.BinaryBuilder); ok {
				var v []byte
				if err := pgtype.ByteaCodec.PlanScan(typeMap, dataType, format, &v).Scan(data, &v); err != nil {
					return 0, err
				}
				b.Append(v)
				return len(v), nil
			}
		}

		// Fallback to interface{} for unsupported types
		var v interface{}
		if err := dt.Codec.PlanScan(typeMap, dataType, format, &v).Scan(data, &v); err != nil {
			return 0, err
		}
		return writeValue(builder, v)
	}

	// Unknown type, store as string
	if b, ok := builder.(*array.StringBuilder); ok {
		b.Append(string(data))
		return len(data), nil
	}

	return 0, fmt.Errorf("unsupported type conversion for OID %d to %T", dataType, builder)
}

// Keep writeValue as a fallback for handling Go values from pgtype codec
func writeValue(builder array.Builder, val interface{}) (int, error) {
	switch b := builder.(type) {
	case *array.BooleanBuilder:
		if v, ok := val.(bool); ok {
			b.Append(v)
			return 1, nil
		}
	case *array.Int8Builder:
		if v, ok := val.(int8); ok {
			b.Append(v)
			return 1, nil
		}
	case *array.Int16Builder:
		if v, ok := val.(int16); ok {
			b.Append(v)
			return 2, nil
		}
	case *array.Int32Builder:
		if v, ok := val.(int32); ok {
			b.Append(v)
			return 4, nil
		}
	case *array.Int64Builder:
		if v, ok := val.(int64); ok {
			b.Append(v)
			return 8, nil
		}
	case *array.Uint8Builder:
		if v, ok := val.(uint8); ok {
			b.Append(v)
			return 1, nil
		}
	case *array.Uint16Builder:
		if v, ok := val.(uint16); ok {
			b.Append(v)
			return 2, nil
		}
	case *array.Uint32Builder:
		if v, ok := val.(uint32); ok {
			b.Append(v)
			return 4, nil
		}
	case *array.Uint64Builder:
		if v, ok := val.(uint64); ok {
			b.Append(v)
			return 8, nil
		}
	case *array.Float32Builder:
		if v, ok := val.(float32); ok {
			b.Append(v)
			return 4, nil
		}
	case *array.Float64Builder:
		if v, ok := val.(float64); ok {
			b.Append(v)
			return 8, nil
		}
	case *array.StringBuilder:
		if v, ok := val.(string); ok {
			b.Append(v)
			return len(v), nil
		}
	case *array.BinaryBuilder:
		if v, ok := val.([]byte); ok {
			b.Append(v)
			return len(v), nil
		}
	case *array.TimestampBuilder:
		if v, ok := val.(time.Time); ok {
			b.Append(v)
			return 8, nil
		}
	case *array.Date32Builder:
		if v, ok := val.(time.Time); ok {
			b.Append(arrow.Date32FromTime(v))
			return 4, nil
		}
	case *array.DurationBuilder:
		if v, ok := val.(time.Duration); ok {
			b.Append(arrow.Duration(v))
			return 8, nil
		}
	case *array.Decimal128Builder:
		if v, ok := val.(decimal128.Num); ok {
			b.Append(v)
			return 16, nil
		}
	case *array.Decimal256Builder:
		if v, ok := val.(decimal256.Num); ok {
			b.Append(v)
			return 32, nil
		}
	}
	return 0, fmt.Errorf("unsupported type conversion: %T -> %T", val, builder)
}
