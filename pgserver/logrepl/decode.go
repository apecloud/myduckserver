package logrepl

import (
	"fmt"
	"math/big"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apecloud/myduckserver/pgtypes"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// decodeToArrow decodes Postgres text format data and appends directly to Arrow builder
func decodeToArrow(typeMap *pgtype.Map, columnType *pglogrepl.RelationMessageColumn, data []byte, format int16, builder array.Builder) (int, error) {
	if data == nil {
		builder.AppendNull()
		return 0, nil
	}

	dt, ok := typeMap.TypeForOID(columnType.DataType)
	if !ok {
		// Unknown type, store as string
		if b, ok := builder.(*array.StringBuilder); ok {
			b.Append(string(data))
			return len(data), nil
		}
		return 0, fmt.Errorf("column %s: unsupported type conversion for OID %d to %T", columnType.Name, columnType.DataType, builder)
	}

	var (
		oid   = dt.OID
		scale int32
	)
	if oid == pgtype.NumericOID {
		_, scale, _ = pgtypes.DecodePrecisionScale(int(columnType.TypeModifier))
	}

	// StringBuilder.Append is just StringBuilder.BinaryBuilder.Append
	if b, ok := builder.(*array.StringBuilder); ok {
		builder = b.BinaryBuilder
	}

	switch oid {
	case pgtype.BoolOID:
		if b, ok := builder.(*array.BooleanBuilder); ok {
			var v bool
			var codec pgtype.BoolCodec
			if err := codec.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			b.Append(v)
			return 1, nil
		}

	case pgtype.Int2OID:
		if b, ok := builder.(*array.Int16Builder); ok {
			var v int16
			var codec pgtype.Int2Codec
			if err := codec.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			b.Append(v)
			return 2, nil
		}

	case pgtype.Int4OID:
		if b, ok := builder.(*array.Int32Builder); ok {
			var v int32
			var codec pgtype.Int4Codec
			if err := codec.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			b.Append(v)
			return 4, nil
		}

	case pgtype.Int8OID:
		if b, ok := builder.(*array.Int64Builder); ok {
			var v int64
			var codec pgtype.Int8Codec
			if err := codec.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			b.Append(v)
			return 8, nil
		}

	case pgtype.Float4OID:
		if b, ok := builder.(*array.Float32Builder); ok {
			var v float32
			var codec pgtype.Float4Codec
			if err := codec.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			b.Append(v)
			return 4, nil
		}

	case pgtype.Float8OID:
		if b, ok := builder.(*array.Float64Builder); ok {
			var v float64
			var codec pgtype.Float8Codec
			if err := codec.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			b.Append(v)
			return 8, nil
		}

	case pgtype.TimestampOID:
		if b, ok := builder.(*array.TimestampBuilder); ok {
			var v pgtype.Timestamp
			codec := pgtype.TimestampCodec{ScanLocation: time.UTC}
			if err := codec.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			b.AppendTime(v.Time)
			return 8, nil
		}

	case pgtype.TimestamptzOID:
		if b, ok := builder.(*array.TimestampBuilder); ok {
			var v pgtype.Timestamptz
			codec := pgtype.TimestamptzCodec{ScanLocation: time.UTC}
			if err := codec.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			b.AppendTime(v.Time)
			return 8, nil
		}

	case pgtype.DateOID:
		if b, ok := builder.(*array.Date32Builder); ok {
			var v pgtype.Date
			var codec pgtype.DateCodec
			if err := codec.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			b.Append(arrow.Date32FromTime(v.Time))
			return 4, nil
		}

	case pgtype.TimeOID, pgtype.TimetzOID:
		if b, ok := builder.(*array.Time64Builder); ok {
			var v pgtype.Time
			var codec pgtype.TimeCodec
			if err := codec.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			b.Append(arrow.Time64(v.Microseconds * 1000))
			return 8, nil
		}

	case pgtype.NumericOID:
		// Fast path for text format & string destination
		if format == pgtype.TextFormatCode {
			switch b := builder.(type) {
			case *array.BinaryBuilder:
				b.Append(data)
				return len(data), nil
			}
		}

		// TODO(fan): Write a custom decoder for numeric to achieve better performance
		var v pgtype.Numeric
		var codec pgtype.NumericCodec
		if err := codec.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
			return 0, err
		}
		if v.NaN || v.InfinityModifier != 0 {
			builder.AppendNull() // Arrow doesn't support NaN or Infinity
			return 0, nil
		}

		switch b := builder.(type) {
		case *array.Decimal128Builder:
			if exp := v.Exp + scale; exp != 0 {
				ten := big.NewInt(10)
				if exp > 0 { // e.g., v.Int = 123, v.Exp = -2, scale = 3 (i.e., target = 1.230), exp = 1, we need to scale up by 10: 123 x 10 = 1230
					for range exp {
						v.Int.Mul(v.Int, ten)
					}
				} else { // e.g., v.Int = 1230, v.Exp = -3, scale = 2 (i.e., target = 1.23), exp = -1, we need to scale down by 10: 1230 / 10 = 123
					for range -exp {
						v.Int.Div(v.Int, ten)
					}
				}
			}
			b.Append(decimal128.FromBigInt(v.Int))
			return 16, nil

		case *array.BinaryBuilder: // This is for very large numbers that can't fit into Decimal128, so we pre-allocate a larger buffer
			var buf [64]byte
			res, err := codec.PlanEncode(typeMap, oid, pgtype.TextFormatCode, v).Encode(v, buf[:0])
			if err != nil {
				return 0, err
			}
			b.Append(res)
			return len(res), nil
		}

	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID, pgtype.NameOID:
		var buf [32]byte // Stack-allocated buffer for small string
		v := pgtype.PreallocBytes(buf[:])
		var codec pgtype.TextCodec
		if err := codec.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
			return 0, err
		}
		switch b := builder.(type) {
		case *array.BinaryBuilder:
			b.Append(v)
			return len(v), nil
		}

	case pgtype.ByteaOID:
		if b, ok := builder.(*array.BinaryBuilder); ok {
			var buf [32]byte // Stack-allocated buffer for small byte array
			v := pgtype.PreallocBytes(buf[:])
			var codec pgtype.ByteaCodec
			if err := codec.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			b.Append(v)
			return len(v), nil
		}

	case pgtype.UUIDOID:
		var v pgtype.UUID
		var codec pgtype.UUIDCodec
		if err := codec.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
			return 0, err
		}
		switch b := builder.(type) {
		case *array.FixedSizeBinaryBuilder:
			b.Append(v.Bytes[:])
			return 16, nil
		case *array.BinaryBuilder:
			var buf [36]byte
			codec.PlanEncode(typeMap, oid, pgtype.TextFormatCode, &v).Encode(&v, buf[:0])
			b.Append(buf[:])
			return 36, nil
		}

	case pgtype.JSONOID:
		if b, ok := builder.(*array.BinaryBuilder); ok {
			var buf [32]byte // Stack-allocated buffer for small JSON
			v := pgtype.PreallocBytes(buf[:])
			var codec pgtype.JSONCodec
			if err := codec.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			b.Append(v)
			return len(v), nil
		}

	case pgtype.JSONBOID:
		if b, ok := builder.(*array.BinaryBuilder); ok {
			var buf [32]byte // Stack-allocated buffer for small JSON
			v := pgtype.PreallocBytes(buf[:])
			var codec pgtype.JSONBCodec
			if err := codec.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			b.Append(v)
			return len(v), nil
		}
	}

	// TODO(fan): add support for other types

	// Fallback
	v, err := dt.Codec.DecodeValue(typeMap, oid, format, data)
	if err != nil {
		return 0, err
	}
	return writeValue(builder, v)
}

// Keep writeValue as a fallback for handling Go values from pgtype codec
func writeValue(builder array.Builder, val any) (int, error) {
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
		if v, ok := val.(pgtype.Timestamp); ok {
			b.AppendTime(v.Time)
			return 8, nil
		}
	case *array.DurationBuilder:
		if v, ok := val.(time.Duration); ok {
			b.Append(arrow.Duration(v))
			return 8, nil
		}
	}
	return 0, fmt.Errorf("unsupported type conversion: %T -> %T", val, builder)
}
