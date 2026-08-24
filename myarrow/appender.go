package myarrow

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/cockroachdb/apd/v3"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type ArrowAppender struct {
	*array.RecordBuilder
	schema sql.Schema
}

func NewArrowAppender(schema sql.Schema, dictionary ...int) (ArrowAppender, error) {
	pool := memory.NewGoAllocator()
	arrowSchema, err := ToArrowSchema(schema, dictionary...)
	if err != nil {
		return ArrowAppender{}, err
	}
	return ArrowAppender{RecordBuilder: array.NewRecordBuilder(pool, arrowSchema), schema: schema}, nil
}

// Build creates a new arrow.Record from the memory buffers and resets the builder.
// The returned Record must be Release()'d after use.
func (a *ArrowAppender) Build() arrow.Record {
	return a.RecordBuilder.NewRecord()
}

// Grow increases the capacity of the builder to at least n rows.
// This method is intended to be used to preallocate memory for the builder
// after Build() has been called.
func (a *ArrowAppender) Grow(n int) {
	for _, b := range a.RecordBuilder.Fields() {
		b.Reserve(n)
	}
}

func (a *ArrowAppender) Append(row sql.Row) error {
	for i, b := range a.RecordBuilder.Fields() {
		v := row[i]
		if v == nil {
			b.AppendNull()
			continue
		}
		if vectorType, ok := a.schema[i].Type.(types.VectorType); ok {
			encoded, ok := v.([]byte)
			if !ok {
				return fmt.Errorf("vector column %d has value of type %T", i, v)
			}
			expectedBytes := vectorType.Dimensions * 4
			if len(encoded) != expectedBytes {
				return fmt.Errorf("vector column %d dimension mismatch: expected %d bytes, got %d", i, expectedBytes, len(encoded))
			}
		}
		switch b.Type().ID() {
		case arrow.UINT8:
			b.(*array.Uint8Builder).Append(v.(uint8))
		case arrow.INT8:
			b.(*array.Int8Builder).Append(v.(int8))
		case arrow.UINT16:
			b.(*array.Uint16Builder).Append(v.(uint16))
		case arrow.INT16:
			b.(*array.Int16Builder).Append(v.(int16))
		case arrow.UINT32:
			b.(*array.Uint32Builder).Append(v.(uint32))
		case arrow.INT32:
			b.(*array.Int32Builder).Append(v.(int32))
		case arrow.UINT64:
			b.(*array.Uint64Builder).Append(v.(uint64))
		case arrow.INT64:
			b.(*array.Int64Builder).Append(v.(int64))
		case arrow.FLOAT32:
			b.(*array.Float32Builder).Append(v.(float32))
		case arrow.FLOAT64:
			b.(*array.Float64Builder).Append(v.(float64))
		case arrow.STRING:
			b.(*array.StringBuilder).Append(v.(string))
		case arrow.BINARY:
			b.(*array.BinaryBuilder).Append(v.([]byte))
		case arrow.DECIMAL128, arrow.DECIMAL256:
			dv, ok := v.(*apd.Decimal)
			if !ok || dv == nil {
				return fmt.Errorf("decimal column %d has value of type %T", i, v)
			}
			if err := b.AppendValueFromString(dv.String()); err != nil {
				return fmt.Errorf("append decimal column %d: %w", i, err)
			}
		case arrow.TIMESTAMP:
			tv := v.(time.Time)
			at, err := arrow.TimestampFromTime(tv, b.Type().(*arrow.TimestampType).Unit)
			if err != nil {
				return err
			}
			b.(*array.TimestampBuilder).Append(at)
		case arrow.DATE32:
			tv := v.(time.Time)
			b.(*array.Date32Builder).Append(arrow.Date32FromTime(tv))
		case arrow.DURATION:
			sv := v.(string)
			duration, err := types.Time.ConvertToTimeDuration(sv)
			if err != nil {
				return err
			}
			b.(*array.DurationBuilder).Append(arrow.Duration(duration.Microseconds()))
		case arrow.DICTIONARY:
			switch v := v.(type) {
			case string:
				b.(*array.BinaryDictionaryBuilder).AppendString(v)
			case []byte:
				b.(*array.BinaryDictionaryBuilder).Append(v)
			default:
				b.AppendValueFromString(fmt.Sprint(v))
			}
		default:
			b.AppendValueFromString(fmt.Sprint(v))
		}
	}
	return nil
}
