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

var (
	// pow10s holds precomputed power of 10
	pow10s [40]big.Int
)

func init() {
	pow10s[0].SetInt64(1)
	ten := big.NewInt(10)
	for i := 1; i < len(pow10s); i++ {
		pow10s[i].Mul(&pow10s[i-1], ten)
	}
}

// decodeToArrow decodes Postgres text format data and appends directly to Arrow builder
func decodeToArrow(typeMap *pgtype.Map, columnType *pglogrepl.RelationMessageColumn, data []byte, format int16, builder array.Builder) (int, error) {
	if data == nil {
		builder.AppendNull()
		return 0, nil
	}

	dt, ok := typeMap.TypeForOID(columnType.DataType)
	if !ok {
		// Unknown type, store as string if possible
		if b, ok := builder.(*array.StringBuilder); ok && format == pgtype.TextFormatCode {
			b.BinaryBuilder.Append(data)
			return len(data), nil
		}
		return 0, fmt.Errorf("column %s: unsupported type conversion for OID %d to %T", columnType.Name, columnType.DataType, builder)
	}

	var (
		oid   = dt.OID
		scale int32
	)
	switch oid {
	case pgtype.NumericOID, pgtype.NumericArrayOID:
		_, scale, _ = pgtypes.DecodePrecisionScale(int(columnType.TypeModifier))
	}

	if ac, ok := dt.Codec.(*pgtype.ArrayCodec); ok {
		if builder, ok := builder.(*array.ListBuilder); ok {
			return decodeArrayToArrow(typeMap, ac, data, format, scale, builder)
		}
		return 0, fmt.Errorf("column %s: unexpected Arrow array builder %T for Postgres array", columnType.Name, builder)
	}

	// StringBuilder.Append is just StringBuilder.BinaryBuilder.Append
	if b, ok := builder.(*array.StringBuilder); ok {
		builder = b.BinaryBuilder
	}

	// TODO(fan): add dedicated decoder for missing types
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

	case pgtype.QCharOID:
		if b, ok := builder.(*array.Uint8Builder); ok {
			var v byte
			var codec pgtype.QCharCodec
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
				if exp >= 40 || exp <= -40 {
					return 0, fmt.Errorf("column %s: unsupported scale %d for Decimal128", columnType.Name, exp)
				}
				if exp > 0 { // e.g., v.Int = 123, v.Exp = -2, scale = 3 (i.e., target = 1.230), exp = 1, we need to scale up by 10: 123 x 10 = 1230
					v.Int.Mul(v.Int, &pow10s[exp])
				} else { // e.g., v.Int = 1230, v.Exp = -3, scale = 2 (i.e., target = 1.23), exp = -1, we need to scale down by 10: 1230 / 10 = 123
					v.Int.Div(v.Int, &pow10s[-exp])
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
		if b, ok := builder.(*array.BinaryBuilder); ok {
			var v pgtype.DriverBytes // raw reference to the data without copying
			var codec pgtype.TextCodec
			if err := codec.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			b.Append(v)
			return len(v), nil
		}

	case pgtype.ByteaOID:
		if b, ok := builder.(*array.BinaryBuilder); ok {
			var v pgtype.DriverBytes // raw reference to the data without copying
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
			var v pgtype.DriverBytes // raw reference to the data without copying
			var codec pgtype.JSONCodec
			if err := codec.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			b.Append(v)
			return len(v), nil
		}

	case pgtype.JSONBOID:
		if b, ok := builder.(*array.BinaryBuilder); ok {
			var v pgtype.DriverBytes // raw reference to the data without copying
			var codec pgtype.JSONBCodec
			if err := codec.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			b.Append(v)
			return len(v), nil
		}
	}

	// Fallback
	v, err := dt.Codec.DecodeValue(typeMap, oid, format, data)
	if err != nil {
		return 0, err
	}
	return writeValue(builder, v)
}

// decodeArrayToArrow decodes Postgres array data and appends directly to Arrow builder
func decodeArrayToArrow(typeMap *pgtype.Map, ac *pgtype.ArrayCodec, data []byte, format int16, scale int32, builder *array.ListBuilder) (int, error) {
	if data == nil {
		builder.AppendNull()
		return 0, nil
	}

	// This must be called before (NOT after!) appending elements to the value builder
	builder.Append(true)

	var (
		et     = ac.ElementType
		oid    = et.OID
		values = builder.ValueBuilder()
	)

	// StringBuilder.Append is just StringBuilder.BinaryBuilder.Append
	if b, ok := values.(*array.StringBuilder); ok {
		values = b.BinaryBuilder
	}

	// Heap allocations are unavoidable if we stick to ArrayCodec,
	// but it should not be a bottleneck here.
	switch oid {
	case pgtype.BoolOID:
		if values, ok := values.(*array.BooleanBuilder); ok {
			var v pgtype.FlatArray[pgtype.Bool]
			if err := ac.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			for _, e := range v {
				if e.Valid {
					values.Append(e.Bool)
				} else {
					values.AppendNull()
				}
			}
			return len(v) * 1, nil
		}

	case pgtype.Int2OID:
		if values, ok := values.(*array.Int16Builder); ok {
			var v pgtype.FlatArray[pgtype.Int2]
			if err := ac.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			for _, e := range v {
				if e.Valid {
					values.Append(e.Int16)
				} else {
					values.AppendNull()
				}
			}
			return len(v) * 2, nil
		}

	case pgtype.Int4OID:
		if values, ok := values.(*array.Int32Builder); ok {
			var v pgtype.FlatArray[pgtype.Int4]
			if err := ac.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			for _, e := range v {
				if e.Valid {
					values.Append(e.Int32)
				} else {
					values.AppendNull()
				}
			}
			return len(v) * 4, nil
		}

	case pgtype.Int8OID:
		if values, ok := values.(*array.Int64Builder); ok {
			var v pgtype.FlatArray[pgtype.Int8]
			if err := ac.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			for _, e := range v {
				if e.Valid {
					values.Append(e.Int64)
				} else {
					values.AppendNull()
				}
			}
			return len(v) * 8, nil
		}

	case pgtype.Float4OID:
		if values, ok := values.(*array.Float32Builder); ok {
			var v pgtype.FlatArray[pgtype.Float4]
			if err := ac.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			for _, e := range v {
				if e.Valid {
					values.Append(e.Float32)
				} else {
					values.AppendNull()
				}
			}
			return len(v) * 4, nil
		}

	case pgtype.Float8OID:
		if values, ok := values.(*array.Float64Builder); ok {
			var v pgtype.FlatArray[pgtype.Float8]
			if err := ac.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			for _, e := range v {
				if e.Valid {
					values.Append(e.Float64)
				} else {
					values.AppendNull()
				}
			}
			return len(v) * 8, nil
		}

	case pgtype.DateOID:
		if values, ok := values.(*array.Date32Builder); ok {
			var v pgtype.FlatArray[pgtype.Date]
			if err := ac.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			for _, e := range v {
				if e.Valid {
					values.Append(arrow.Date32FromTime(e.Time))
				} else {
					values.AppendNull()
				}
			}
			return len(v) * 4, nil
		}

	case pgtype.TimeOID, pgtype.TimetzOID:
		if values, ok := values.(*array.Time64Builder); ok {
			var v pgtype.FlatArray[pgtype.Time]
			if err := ac.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			for _, e := range v {
				if e.Valid {
					values.Append(arrow.Time64(e.Microseconds * 1000))
				} else {
					values.AppendNull()
				}
			}
			return len(v) * 8, nil
		}

	case pgtype.TimestampOID:
		if values, ok := values.(*array.TimestampBuilder); ok {
			var v pgtype.FlatArray[pgtype.Timestamp]
			if err := ac.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			for _, e := range v {
				if e.Valid {
					values.AppendTime(e.Time)
				} else {
					values.AppendNull()
				}
			}
			return len(v) * 8, nil
		}

	case pgtype.TimestamptzOID:
		if values, ok := values.(*array.TimestampBuilder); ok {
			var v pgtype.FlatArray[pgtype.Timestamptz]
			if err := ac.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			for _, e := range v {
				if e.Valid {
					values.AppendTime(e.Time)
				} else {
					values.AppendNull()
				}
			}
			return len(v) * 8, nil
		}

	case pgtype.ByteaOID, pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID, pgtype.NameOID, pgtype.JSONOID, pgtype.JSONBOID:
		if values, ok := values.(*array.BinaryBuilder); ok {
			var v pgtype.FlatArray[pgtype.DriverBytes]
			if err := ac.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
				return 0, err
			}
			var total int
			for _, e := range v {
				if e != nil {
					values.Append(e)
					total += len(e)
				} else {
					values.AppendNull()
				}
			}
			return total, nil
		}

	case pgtype.NumericOID:
		var v pgtype.FlatArray[pgtype.Numeric]
		if err := ac.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
			return 0, err
		}

		switch values := values.(type) {
		case *array.Decimal128Builder:
			for _, e := range v {
				if e.NaN || e.InfinityModifier != 0 || !e.Valid {
					values.AppendNull()
					continue
				}
				if exp := e.Exp + scale; exp != 0 {
					if exp >= 40 || exp <= -40 {
						return 0, fmt.Errorf("unsupported scale %d for Decimal128 array", exp)
					}
					if exp > 0 {
						e.Int.Mul(e.Int, &pow10s[exp])
					} else {
						e.Int.Div(e.Int, &pow10s[-exp])
					}
				}
				values.Append(decimal128.FromBigInt(e.Int))
			}
			return len(v) * 16, nil

		case *array.BinaryBuilder:
			codec := pgtype.NumericCodec{}
			var total int
			var buf [64]byte
			for _, e := range v {
				if e.NaN || e.InfinityModifier != 0 || !e.Valid {
					values.AppendNull()
					continue
				}
				res, err := codec.PlanEncode(typeMap, oid, pgtype.TextFormatCode, e).Encode(e, buf[:0])
				if err != nil {
					return 0, err
				}
				values.Append(res)
				total += len(res)
			}
			return total, nil
		}

	case pgtype.UUIDOID:
		var v pgtype.FlatArray[pgtype.UUID]
		if err := ac.PlanScan(typeMap, oid, format, &v).Scan(data, &v); err != nil {
			return 0, err
		}
		switch values := values.(type) {
		case *array.FixedSizeBinaryBuilder:
			for _, e := range v {
				if e.Valid {
					values.Append(e.Bytes[:])
				} else {
					values.AppendNull()
				}
			}
			return len(v) * 16, nil
		case *array.BinaryBuilder:
			codec := pgtype.UUIDCodec{}
			var buf [36]byte
			for _, e := range v {
				if e.Valid {
					codec.PlanEncode(typeMap, oid, pgtype.TextFormatCode, &e).Encode(&e, buf[:0])
					values.Append(buf[:])
				} else {
					values.AppendNull()
				}
			}
			return len(v) * 36, nil
		}
	}

	return 0, fmt.Errorf("unsupported array type: cannot decode elements (type %s) to Arrow %T", et.Name, values)
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
