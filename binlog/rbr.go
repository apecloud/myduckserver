/*
Copyright 2023 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package binlog

import (
	"encoding/binary"
	"math"
	"math/big"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apecloud/myduckserver/charset"
	"github.com/cockroachdb/apd/v3"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/proto/vtrpc"
	"github.com/dolthub/vitess/go/vt/vterrors"
	vtbinlog "vitess.io/vitess/go/mysql/binlog"
)

// ZeroTimestamp is the special value 0 for a timestamp.
var ZeroTimestamp = []byte("0000-00-00 00:00:00")

var dig2bytes = []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}

var powerOf10s = [20]uint64{
	1,
	10,
	100,
	1000,
	10000,
	1_00000,
	10_00000,
	100_00000,
	1000_00000,
	10000_00000,
	1_00000_00000,
	10_00000_00000,
	100_00000_00000,
	1000_00000_00000,
	10000_00000_00000,
	1_00000_00000_00000,
	10_00000_00000_00000,
	100_00000_00000_00000,
	1000_00000_00000_00000,
	10000_00000_00000_00000,
}

// CellLength returns the new position after the field with the given
// type is read.
func CellLength(data []byte, pos int, typ byte, metadata uint16) (int, error) {
	switch typ {
	case TypeNull:
		return 0, nil
	case TypeTiny, TypeYear:
		return 1, nil
	case TypeShort:
		return 2, nil
	case TypeInt24:
		return 3, nil
	case TypeLong, TypeFloat, TypeTimestamp:
		return 4, nil
	case TypeLongLong, TypeDouble:
		return 8, nil
	case TypeDate, TypeTime, TypeNewDate:
		return 3, nil
	case TypeDateTime:
		return 8, nil
	case TypeVarchar, TypeVarString:
		// Length is encoded in 1 or 2 bytes.
		if metadata > 255 {
			l := int(uint64(data[pos]) |
				uint64(data[pos+1])<<8)
			return l + 2, nil
		}
		l := int(data[pos])
		return l + 1, nil
	case TypeBit:
		// bitmap length is in metadata, as:
		// upper 8 bits: bytes length
		// lower 8 bits: bit length
		nbits := ((metadata >> 8) * 8) + (metadata & 0xFF)
		return (int(nbits) + 7) / 8, nil
	case TypeTimestamp2:
		// metadata has number of decimals. One byte encodes
		// two decimals.
		return 4 + (int(metadata)+1)/2, nil
	case TypeDateTime2:
		// metadata has number of decimals. One byte encodes
		// two decimals.
		return 5 + (int(metadata)+1)/2, nil
	case TypeTime2:
		// metadata has number of decimals. One byte encodes
		// two decimals.
		return 3 + (int(metadata)+1)/2, nil
	case TypeNewDecimal:
		precision := int(metadata >> 8)
		scale := int(metadata & 0xff)
		// Example:
		//   NNNNNNNNNNNN.MMMMMM
		//     12 bytes     6 bytes
		// precision is 18
		// scale is 6
		// storage is done by groups of 9 digits:
		// - 32 bits are used to store groups of 9 digits.
		// - any leftover digit is stored in:
		//   - 1 byte for 1 and 2 digits
		//   - 2 bytes for 3 and 4 digits
		//   - 3 bytes for 5 and 6 digits
		//   - 4 bytes for 7 and 8 digits (would also work for 9)
		// both sides of the dot are stored separately.
		// In this example, we'd have:
		// - 2 bytes to store the first 3 full digits.
		// - 4 bytes to store the next 9 full digits.
		// - 3 bytes to store the 6 fractional digits.
		intg := precision - scale
		intg0 := intg / 9
		frac0 := scale / 9
		intg0x := intg - intg0*9
		frac0x := scale - frac0*9
		return intg0*4 + dig2bytes[intg0x] + frac0*4 + dig2bytes[frac0x], nil
	case TypeEnum, TypeSet:
		return int(metadata & 0xff), nil
	case TypeJSON, TypeTinyBlob, TypeMediumBlob, TypeLongBlob, TypeBlob, TypeGeometry, TypeVector:
		// Of the Blobs, only TypeBlob is used in binary logs,
		// but supports others just in case.
		switch metadata {
		case 1:
			return 1 + int(uint32(data[pos])), nil
		case 2:
			return 2 + int(uint32(data[pos])|
				uint32(data[pos+1])<<8), nil
		case 3:
			return 3 + int(uint32(data[pos])|
				uint32(data[pos+1])<<8|
				uint32(data[pos+2])<<16), nil
		case 4:
			return 4 + int(uint32(data[pos])|
				uint32(data[pos+1])<<8|
				uint32(data[pos+2])<<16|
				uint32(data[pos+3])<<24), nil
		default:
			return 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "unsupported blob/geometry metadata value %v (data: %v pos: %v)", metadata, data, pos)
		}
	case TypeString:
		// This may do String, Enum, and Set. The type is in
		// metadata. If it's a string, then there will be more bits.
		// This will give us the maximum length of the field.
		t := metadata >> 8
		if t == TypeEnum || t == TypeSet {
			return int(metadata & 0xff), nil
		}
		max := int((((metadata >> 4) & 0x300) ^ 0x300) + (metadata & 0xff))
		// Length is encoded in 1 or 2 bytes.
		if max > 255 {
			l := int(uint64(data[pos]) |
				uint64(data[pos+1])<<8)
			return l + 2, nil
		}
		l := int(data[pos])
		return l + 1, nil

	default:
		return 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "unsupported type %v (data: %v pos: %v)", typ, data, pos)
	}
}

// CellValue returns the data for a cell as a sqltypes.Value, and how
// many bytes it takes. It uses source type in querypb.Type and vitess type
// byte to determine general shared aspects of types and the querypb.Field to
// determine other info specifically about its underlying column (SQL column
// type, column length, charset, etc)
func CellValue(data []byte, pos int, typ byte, metadata uint16, column *sql.Column, builder array.Builder) (int, error) {
	// logrus.Infof("CellValue: binlog type: %s, column: %v, type: %v, builder: %T", TypeNames[typ], column.Name, column.Type, builder)
	ftype := querypb.Type(column.Type.Type())
	switch typ {
	case TypeTiny:
		if sqltypes.IsSigned(ftype) {
			builder.(*array.Int8Builder).Append(int8(data[pos]))
		} else {
			builder.(*array.Uint8Builder).Append(data[pos])
		}
		return 1, nil
	case TypeYear:
		val := data[pos]
		if val == 0 {
			builder.(*array.Uint16Builder).Append(0)
		} else {
			builder.(*array.Uint16Builder).Append(uint16(data[pos]) + 1900)
		}
		return 1, nil
	case TypeShort:
		val := binary.LittleEndian.Uint16(data[pos : pos+2])
		if sqltypes.IsSigned(ftype) {
			builder.(*array.Int16Builder).Append(int16(val))
		} else {
			builder.(*array.Uint16Builder).Append(val)
		}
		return 2, nil
	case TypeInt24:
		if sqltypes.IsSigned(ftype) && data[pos+2]&128 > 0 {
			// Negative number, have to extend the sign.
			val := int32(uint32(data[pos]) +
				uint32(data[pos+1])<<8 +
				uint32(data[pos+2])<<16 +
				uint32(255)<<24)
			builder.(*array.Int32Builder).Append(val)
		} else {
			// Positive number.
			val := uint64(data[pos]) +
				uint64(data[pos+1])<<8 +
				uint64(data[pos+2])<<16
			switch builder := builder.(type) {
			case *array.Int32Builder:
				builder.Append(int32(val))
			case *array.Uint32Builder:
				builder.Append(uint32(val))
			default:
				return 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected Arrow builder type %T", builder)
			}
		}
		return 3, nil
	case TypeLong:
		val := binary.LittleEndian.Uint32(data[pos : pos+4])
		if sqltypes.IsSigned(ftype) {
			builder.(*array.Int32Builder).Append(int32(val))
		} else {
			builder.(*array.Uint32Builder).Append(val)
		}
		return 4, nil
	case TypeFloat:
		val := binary.LittleEndian.Uint32(data[pos : pos+4])
		fval := math.Float32frombits(val)
		builder.(*array.Float32Builder).Append(fval)
		return 4, nil
	case TypeDouble:
		val := binary.LittleEndian.Uint64(data[pos : pos+8])
		fval := math.Float64frombits(val)
		builder.(*array.Float64Builder).Append(fval)
		return 8, nil
	case TypeTimestamp:
		val := binary.LittleEndian.Uint32(data[pos : pos+4])
		builder.(*array.TimestampBuilder).AppendTime(time.Unix(int64(val), 0).UTC())
		return 4, nil
	case TypeLongLong:
		val := binary.LittleEndian.Uint64(data[pos : pos+8])
		if sqltypes.IsSigned(ftype) {
			builder.(*array.Int64Builder).Append(int64(val))
		} else {
			builder.(*array.Uint64Builder).Append(val)
		}
		return 8, nil
	case TypeDate, TypeNewDate:
		val := uint32(data[pos]) +
			uint32(data[pos+1])<<8 +
			uint32(data[pos+2])<<16
		day := val & 31
		month := val >> 5 & 15
		year := val >> 9
		t := time.Date(int(year), time.Month(month), int(day), 0, 0, 0, 0, time.UTC)
		builder.(*array.Date32Builder).Append(arrow.Date32FromTime(t))
		return 3, nil
	case TypeTime:
		var hour, minute, second int32
		if data[pos+2]&128 > 0 {
			// Negative number, have to extend the sign.
			val := int32(uint32(data[pos]) +
				uint32(data[pos+1])<<8 +
				uint32(data[pos+2])<<16 +
				uint32(255)<<24)
			hour = val / 10000
			minute = -((val % 10000) / 100)
			second = -(val % 100)
		} else {
			val := int32(data[pos]) +
				int32(data[pos+1])<<8 +
				int32(data[pos+2])<<16
			hour = val / 10000
			minute = (val % 10000) / 100
			second = val % 100
		}
		duration := time.Duration(hour*3600+minute*60+second) * time.Second
		builder.(*array.DurationBuilder).Append(arrow.Duration(duration.Microseconds()))
		return 3, nil
	case TypeDateTime:
		val := binary.LittleEndian.Uint64(data[pos : pos+8])
		d := val / 1000000
		t := val % 1000000
		year := d / 10000
		month := (d % 10000) / 100
		day := d % 100
		hour := t / 10000
		minute := (t % 10000) / 100
		second := t % 100
		builder.(*array.TimestampBuilder).AppendTime(time.Date(int(year), time.Month(month), int(day), int(hour), int(minute), int(second), 0, time.UTC))
		return 8, nil
	case TypeVarchar, TypeVarString:
		// We trust that typ is compatible with the ftype
		// Length is encoded in 1 or 2 bytes.
		typeToUse := querypb.Type_VARCHAR
		if ftype == querypb.Type_VARBINARY || ftype == querypb.Type_BINARY || ftype == querypb.Type_BLOB {
			typeToUse = ftype
		}
		var (
			size int
			src  []byte
		)
		if metadata > 255 {
			l := int(uint64(data[pos]) |
				uint64(data[pos+1])<<8)
			size = l + 2
			src = data[pos+2 : pos+2+l]
		} else {
			l := int(data[pos])
			size = l + 1
			src = data[pos+1 : pos+1+l]
		}
		if typeToUse == querypb.Type_VARCHAR {
			utf8str, err := charset.DecodeBytes(column.Type.(sql.StringType).CharacterSet(), src)
			if err != nil {
				return size, err
			}
			builder.(*array.StringBuilder).BinaryBuilder.Append(utf8str)
		} else {
			builder.(*array.BinaryBuilder).Append(src)
		}
		return size, nil
	case TypeBit:
		// The contents is just the bytes, quoted.
		nbits := ((metadata >> 8) * 8) + (metadata & 0xFF)
		l := (int(nbits) + 7) / 8
		var buf [8]byte
		copy(buf[8-l:], data[pos:pos+l])
		builder.(*array.Uint64Builder).Append(binary.BigEndian.Uint64(buf[:]))
		return l, nil
	case TypeTimestamp2:
		second := binary.BigEndian.Uint32(data[pos : pos+4])
		size := 4
		frac := 0
		mul := 0
		switch metadata {
		case 1:
			decimals := int(data[pos+4])
			frac = decimals / 10
			mul = 100000
			size = 5
		case 2:
			decimals := int(data[pos+4])
			frac = decimals
			mul = 10000
			size = 5
		case 3:
			decimals := int(data[pos+4])<<8 +
				int(data[pos+5])
			frac = decimals / 10
			mul = 1000
			size = 6
		case 4:
			decimals := int(data[pos+4])<<8 +
				int(data[pos+5])
			frac = decimals
			mul = 100
			size = 6
		case 5:
			decimals := int(data[pos+4])<<16 +
				int(data[pos+5])<<8 +
				int(data[pos+6])
			frac = decimals / 10
			mul = 10
			size = 7
		case 6:
			decimals := int(data[pos+4])<<16 +
				int(data[pos+5])<<8 +
				int(data[pos+6])
			frac = decimals
			mul = 1
			size = 7
		}
		frac *= mul
		t := time.Unix(int64(second), int64(frac*1000)).UTC()
		builder.(*array.TimestampBuilder).AppendTime(t)
		return size, nil
	case TypeDateTime2:
		ymdhms := (uint64(data[pos])<<32 |
			uint64(data[pos+1])<<24 |
			uint64(data[pos+2])<<16 |
			uint64(data[pos+3])<<8 |
			uint64(data[pos+4])) - uint64(0x8000000000)
		ymd := ymdhms >> 17
		ym := ymd >> 5
		hms := ymdhms % (1 << 17)

		day := ymd % (1 << 5)
		month := ym % 13
		year := ym / 13

		second := hms % (1 << 6)
		minute := (hms >> 6) % (1 << 6)
		hour := hms >> 12

		size := 5
		frac := 0
		mul := 0

		switch metadata {
		case 1:
			decimals := int(data[pos+5])
			frac = decimals / 10
			mul = 100000
			size = 6
		case 2:
			decimals := int(data[pos+5])
			frac = decimals
			mul = 10000
			size = 6
		case 3:
			decimals := int(data[pos+5])<<8 +
				int(data[pos+6])
			frac = decimals / 10
			mul = 1000
			size = 7
		case 4:
			decimals := int(data[pos+5])<<8 +
				int(data[pos+6])
			frac = decimals
			mul = 100
			size = 7
		case 5:
			decimals := int(data[pos+5])<<16 +
				int(data[pos+6])<<8 +
				int(data[pos+7])
			frac = decimals / 10
			mul = 10
			size = 8
		case 6:
			decimals := int(data[pos+5])<<16 +
				int(data[pos+6])<<8 +
				int(data[pos+7])
			frac = decimals
			mul = 1
			size = 8
		}
		frac *= mul
		t := time.Date(int(year), time.Month(month), int(day), int(hour), int(minute), int(second), int(frac*1000), time.UTC)
		builder.(*array.TimestampBuilder).AppendTime(t)
		return size, nil
	case TypeTime2:
		hms := (int64(data[pos])<<16 |
			int64(data[pos+1])<<8 |
			int64(data[pos+2])) - 0x800000
		sign := 1
		if hms < 0 {
			hms = -hms
			sign = -1
		}

		frac := 0
		mul := 0
		switch metadata {
		case 1:
			frac = int(data[pos+3])
			if sign == -1 && frac != 0 {
				hms--
				frac = 0x100 - frac
			}
			frac /= 10
			mul = 100000
		case 2:
			frac = int(data[pos+3])
			if sign == -1 && frac != 0 {
				hms--
				frac = 0x100 - frac
			}
			mul = 10000
		case 3:
			frac = int(data[pos+3])<<8 |
				int(data[pos+4])
			if sign == -1 && frac != 0 {
				hms--
				frac = 0x10000 - frac
			}
			frac /= 10
			mul = 1000
		case 4:
			frac = int(data[pos+3])<<8 |
				int(data[pos+4])
			if sign == -1 && frac != 0 {
				hms--
				frac = 0x10000 - frac
			}
			mul = 100
		case 5:
			frac = int(data[pos+3])<<16 |
				int(data[pos+4])<<8 |
				int(data[pos+5])
			if sign == -1 && frac != 0 {
				hms--
				frac = 0x1000000 - frac
			}
			frac /= 10
			mul = 10
		case 6:
			frac = int(data[pos+3])<<16 |
				int(data[pos+4])<<8 |
				int(data[pos+5])
			if sign == -1 && frac != 0 {
				hms--
				frac = 0x1000000 - frac
			}
			mul = 1
		}
		frac *= mul

		hour := (hms >> 12) % (1 << 10)
		minute := (hms >> 6) % (1 << 6)
		second := hms % (1 << 6)
		duration := time.Duration(hour*3600+minute*60+second)*time.Second + time.Duration(frac)*time.Microsecond
		micros := int64(sign) * duration.Microseconds()
		builder.(*array.DurationBuilder).Append(arrow.Duration(micros))
		return 3 + (int(metadata)+1)/2, nil

	case TypeNewDecimal:
		precision := int(metadata >> 8) // total digits number
		scale := int(metadata & 0xff)   // number of fractional digits
		intg := precision - scale       // number of full digits
		intg0 := intg / 9               // number of 32-bits digits
		intg0x := intg - intg0*9        // leftover full digits
		frac0 := scale / 9              // number of 32 bits fractionals
		frac0x := scale - frac0*9       // leftover fractionals

		l := intg0*4 + dig2bytes[intg0x] + frac0*4 + dig2bytes[frac0x]

		// Copy the data so we can change it. Otherwise
		// decoding is just too hard.
		// Using a constant capacity to ensure stack allocation:
		//   https://github.com/golang/go/issues/27625
		d := make([]byte, l, 40)
		copy(d, data[pos:pos+l])

		// txt := &bytes.Buffer{}

		isNegative := (d[0] & 0x80) == 0
		d[0] ^= 0x80 // First bit is inverted.
		if isNegative {
			// Negative numbers are just inverted bytes.
			// txt.WriteByte('-')
			for i := range d {
				d[i] ^= 0xff
			}
		}

		// the initial 128 bits are stack-allocated
		var coeff apd.BigInt

		// first we have the leftover full digits
		var val uint32
		switch dig2bytes[intg0x] {
		case 0:
			// nothing to do
		case 1:
			// one byte, up to two digits
			val = uint32(d[0])
		case 2:
			// two bytes, up to 4 digits
			val = uint32(d[0])<<8 +
				uint32(d[1])
		case 3:
			// 3 bytes, up to 6 digits
			val = uint32(d[0])<<16 +
				uint32(d[1])<<8 +
				uint32(d[2])
		case 4:
			// 4 bytes, up to 8 digits (9 digits would be a full)
			val = uint32(d[0])<<24 +
				uint32(d[1])<<16 +
				uint32(d[2])<<8 +
				uint32(d[3])
		}
		pos = dig2bytes[intg0x]
		if val > 0 {
			// txt.Write(strconv.AppendUint(nil, uint64(val), 10))
			coeff.SetUint64(uint64(val))
		}

		var multiplier, tmp apd.BigInt
		multiplier.SetUint64(1_000_000_000) // 9 digits

		// now the full digits, 32 bits each, 9 digits
		for range intg0 {
			val = binary.BigEndian.Uint32(d[pos : pos+4])
			// fmt.Fprintf(txt, "%09d", val)
			tmp.SetUint64(uint64(val))
			coeff.Mul(&coeff, &multiplier)
			coeff.Add(&coeff, &tmp)
			pos += 4
		}

		// now see if we have a fraction
		if scale == 0 {
			// When the field is a DECIMAL using a scale of 0, e.g.
			// DECIMAL(5,0), a binlogged value of 0 is almost treated
			// like the NULL byte and we get a 0 byte length value.
			// In this case let's return the correct value of 0.
			// if txt.Len() == 0 {
			// 	txt.WriteRune('0')
			// }

			// keep stack-allocated if possible
			var bi big.Int
			bi.SetBits(coeff.Bits())

			switch b := builder.(type) {
			case *array.Decimal128Builder:
				num := decimal128.FromBigInt(&bi)
				if isNegative {
					num = num.Negate()
				}
				b.Append(num)
			case *array.Decimal256Builder:
				num := decimal256.FromBigInt(&bi)
				if isNegative {
					num = num.Negate()
				}
				b.Append(num)
			default:
				return l, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected Arrow builder type: %T", builder)
			}
			return l, nil
		}

		// txt.WriteByte('.')
		fp := 0

		// now the full fractional digits
		for range frac0 {
			val = binary.BigEndian.Uint32(d[pos : pos+4])
			// fmt.Fprintf(txt, "%09d", val)
			tmp.SetUint64(uint64(val))
			coeff.Mul(&coeff, &multiplier)
			coeff.Add(&coeff, &tmp)
			fp += 9
			pos += 4
		}

		// then the partial fractional digits
		switch dig2bytes[frac0x] {
		case 0:
			// Nothing to do
			break
		case 1:
			// one byte, 1 or 2 digits
			val = uint32(d[pos])
			if frac0x == 1 {
				// fmt.Fprintf(txt, "%1d", val)
				multiplier.SetUint64(10)
				tmp.SetUint64(uint64(val))
				coeff.Mul(&coeff, &multiplier)
				coeff.Add(&coeff, &tmp)
			} else {
				// fmt.Fprintf(txt, "%02d", val)
				multiplier.SetUint64(100)
				tmp.SetUint64(uint64(val))
				coeff.Mul(&coeff, &multiplier)
				coeff.Add(&coeff, &tmp)
			}
		case 2:
			// two bytes, 3 or 4 digits
			val = uint32(d[pos])<<8 +
				uint32(d[pos+1])
			if frac0x == 3 {
				// fmt.Fprintf(txt, "%03d", val)
				multiplier.SetUint64(1_000)
				tmp.SetUint64(uint64(val))
				coeff.Mul(&coeff, &multiplier)
				coeff.Add(&coeff, &tmp)
			} else {
				// fmt.Fprintf(txt, "%04d", val)
				multiplier.SetUint64(10_000)
				tmp.SetUint64(uint64(val))
				coeff.Mul(&coeff, &multiplier)
				coeff.Add(&coeff, &tmp)
			}
		case 3:
			// 3 bytes, 5 or 6 digits
			val = uint32(d[pos])<<16 +
				uint32(d[pos+1])<<8 +
				uint32(d[pos+2])
			if frac0x == 5 {
				// fmt.Fprintf(txt, "%05d", val)
				multiplier.SetUint64(100_000)
				tmp.SetUint64(uint64(val))
				coeff.Mul(&coeff, &multiplier)
				coeff.Add(&coeff, &tmp)
			} else {
				// fmt.Fprintf(txt, "%06d", val)
				multiplier.SetUint64(1_000_000)
				tmp.SetUint64(uint64(val))
				coeff.Mul(&coeff, &multiplier)
				coeff.Add(&coeff, &tmp)
			}
		case 4:
			// 4 bytes, 7 or 8 digits (9 digits would be a full)
			val = uint32(d[pos])<<24 +
				uint32(d[pos+1])<<16 +
				uint32(d[pos+2])<<8 +
				uint32(d[pos+3])
			if frac0x == 7 {
				// fmt.Fprintf(txt, "%07d", val)
				multiplier.SetUint64(10_000_000)
				tmp.SetUint64(uint64(val))
				coeff.Mul(&coeff, &multiplier)
				coeff.Add(&coeff, &tmp)
			} else {
				// fmt.Fprintf(txt, "%08d", val)
				multiplier.SetUint64(100_000_000)
				tmp.SetUint64(uint64(val))
				coeff.Mul(&coeff, &multiplier)
				coeff.Add(&coeff, &tmp)
			}
		}
		fp += frac0x

		// Pad with zero digits if necessary:
		// the arrow array shares a common scale for all values,
		// so we need to ensure that the number of fractional digits is as expected.
		desired := int(builder.Type().(arrow.DecimalType).GetScale())
		if fp < desired {
			// Pad 19 zero digits at a time
			multiplier.SetUint64(10000_00000_00000_00000)
			for fp+19 < desired {
				coeff.Mul(&coeff, &multiplier)
				fp += 19
			}
			// Add the remaining zero digits
			multiplier.SetUint64(powerOf10s[desired-fp])
			coeff.Mul(&coeff, &multiplier)
			fp = desired
		} else if fp > desired {
			return l, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected fractional digits: %v > %v", fp, desired)
		}

		// keep stack-allocated if possible
		var bi big.Int
		bi.SetBits(coeff.Bits())
		switch b := builder.(type) {
		case *array.Decimal128Builder:
			num := decimal128.FromBigInt(&bi)
			if isNegative {
				num = num.Negate()
			}
			b.Append(num)
		case *array.Decimal256Builder:
			num := decimal256.FromBigInt(&bi)
			if isNegative {
				num = num.Negate()
			}
			b.Append(num)
		default:
			return l, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected Arrow builder type: %T", builder)
		}
		return l, nil

	case TypeEnum:
		var idx int
		l := int(metadata & 0xff)
		switch l {
		case 1:
			// One byte storage.
			idx = int(data[pos])
		case 2:
			// Two bytes storage.
			idx = int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		default:
			return 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected enum size: %v", metadata&0xff)
		}
		val, ok := column.Type.(sql.EnumType).At(idx)
		if !ok {
			return l, vterrors.Errorf(vtrpc.Code_INTERNAL, "enum value %v not found in %v", idx, column.Type)
		}
		builder.(*array.StringBuilder).Append(val)
		return l, nil

	case TypeSet:
		l := int(metadata & 0xff)
		var val uint64
		for i := range l {
			val += uint64(data[pos+i]) << (uint(i) * 8)
		}
		s, err := column.Type.(sql.SetType).BitsToString(val)
		if err != nil {
			return l, vterrors.Errorf(vtrpc.Code_INTERNAL, "invalid bit value %x for set %v", val, column.Type)
		}
		builder.(*array.StringBuilder).Append(s)
		return l, nil

	case TypeJSON, TypeTinyBlob, TypeMediumBlob, TypeLongBlob, TypeBlob, TypeVector:
		// Only TypeBlob and TypeVector is used in binary logs,
		// but supports others just in case.
		l := 0
		switch metadata {
		case 1:
			l = int(uint32(data[pos]))
		case 2:
			l = int(uint32(data[pos]) |
				uint32(data[pos+1])<<8)
		case 3:
			l = int(uint32(data[pos]) |
				uint32(data[pos+1])<<8 |
				uint32(data[pos+2])<<16)
		case 4:
			l = int(uint32(data[pos]) |
				uint32(data[pos+1])<<8 |
				uint32(data[pos+2])<<16 |
				uint32(data[pos+3])<<24)
		default:
			return 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "unsupported blob metadata value %v (data: %v pos: %v)", metadata, data, pos)
		}
		pos += int(metadata)

		// For JSON, we parse the data, and emit SQL.
		if typ == TypeJSON {
			var err error
			jsonData := data[pos : pos+l]
			jsonVal, err := vtbinlog.ParseBinaryJSON(jsonData)
			if err != nil {
				panic(err)
			}
			var buf [64]byte
			d := jsonVal.MarshalTo(buf[:0])
			builder.(*array.StringBuilder).BinaryBuilder.Append(d)
			return l + int(metadata), nil
		}

		// For blobs, we just copy the bytes.
		switch builder := builder.(type) {
		case *array.BinaryBuilder:
			builder.Append(data[pos : pos+l])
		case *array.StringBuilder:
			utf8str, err := charset.DecodeBytes(column.Type.(sql.StringType).CharacterSet(), data[pos:pos+l])
			if err != nil {
				return l, vterrors.Errorf(vtrpc.Code_INTERNAL, "failed to decode string: %v", err)
			}
			builder.BinaryBuilder.Append(utf8str)
		default:
			return 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected Arrow builder type: %T", builder)
		}
		return l + int(metadata), nil

	case TypeString:
		// This may do String, Enum, and Set. The type is in
		// metadata. If it's a string, then there will be more bits.
		t := metadata >> 8
		if t == TypeEnum {
			// We don't know the string values. So just use the
			// numbers.
			l := int(metadata & 0xff)
			var idx int
			switch metadata & 0xff {
			case 1:
				// One byte storage.
				idx = int(data[pos])
			case 2:
				// Two bytes storage.
				idx = int(binary.LittleEndian.Uint16(data[pos : pos+2]))
			default:
				return 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected enum size: %v", metadata&0xff)
			}
			str, ok := column.Type.(sql.EnumType).At(idx)
			if !ok {
				return l, vterrors.Errorf(vtrpc.Code_INTERNAL, "enum value %v not found in %v", data[pos], column.Type)
			}
			builder.(*array.StringBuilder).Append(str)
			return l, nil
		}
		if t == TypeSet {
			// We don't know the set values. So just use the
			// numbers.
			l := int(metadata & 0xff)
			var val uint64
			for i := range l {
				val += uint64(data[pos+i]) << (uint(i) * 8)
			}
			str, err := column.Type.(sql.SetType).BitsToString(val)
			if err != nil {
				return l, vterrors.Errorf(vtrpc.Code_INTERNAL, "invalid bit value %x for set %v", val, column.Type)
			}
			builder.(*array.StringBuilder).Append(str)
			return l, nil
		}
		// This is a real string. The length is weird.
		max := int((((metadata >> 4) & 0x300) ^ 0x300) + (metadata & 0xff))
		// Length is encoded in 1 or 2 bytes.
		if max > 255 {
			// This code path exists due to https://bugs.mysql.com/bug.php?id=37426.
			// CHAR types need to allocate 3 bytes per char. So, the length for CHAR(255)
			// cannot be represented in 1 byte. This also means that this rule does not
			// apply to BINARY data.
			l := int(uint64(data[pos]) |
				uint64(data[pos+1])<<8)
			utf8str, err := charset.DecodeBytes(column.Type.(sql.StringType).CharacterSet(), data[pos+2:pos+2+l])
			if err != nil {
				return l + 2, vterrors.Errorf(vtrpc.Code_INTERNAL, "failed to decode string: %v", err)
			}
			builder.(*array.StringBuilder).BinaryBuilder.Append(utf8str)
			return l + 2, nil
		}
		l := int(data[pos])
		mdata := data[pos+1 : pos+1+l]
		if sqltypes.IsBinary(ftype) {
			// For binary(n) column types, mysql pads the data on the right with nulls. However the binlog event contains
			// the data without this padding. This causes several issues:
			//    * if a binary(n) column is part of the sharding key, the keyspace_id() returned during the copy phase
			//      (where the value is the result of a mysql query) is different from the one during replication
			//      (where the value is the one from the binlogs)
			//    * mysql where clause comparisons do not do the right thing without padding
			// So for fixed length BINARY columns we right-pad it with nulls if necessary to match what MySQL returns.
			// Because CHAR columns with a binary collation (e.g. utf8mb4_bin) have the same metadata as a BINARY column
			// in binlog events, we also need to check for this case based on the underlying column type.
			if l < max && ftype == querypb.Type_BINARY {
				paddedData := make([]byte, max)
				copy(paddedData[:l], mdata)
				mdata = paddedData
			}
			if builder, ok := builder.(*array.BinaryBuilder); ok {
				builder.Append(mdata)
				return l + 1, nil
			} // Otherwise, fall through to handle (VAR)CHAR/TEXT columns.
		}
		utf8str, err := charset.DecodeBytes(column.Type.(sql.StringType).CharacterSet(), mdata)
		if err != nil {
			return l + 1, vterrors.Errorf(vtrpc.Code_INTERNAL, "failed to decode string: %v", err)
		}
		builder.(*array.StringBuilder).BinaryBuilder.Append(utf8str)
		return l + 1, nil

	case TypeGeometry:
		l := 0
		switch metadata {
		case 1:
			l = int(uint32(data[pos]))
		case 2:
			l = int(uint32(data[pos]) |
				uint32(data[pos+1])<<8)
		case 3:
			l = int(uint32(data[pos]) |
				uint32(data[pos+1])<<8 |
				uint32(data[pos+2])<<16)
		case 4:
			l = int(uint32(data[pos]) |
				uint32(data[pos+1])<<8 |
				uint32(data[pos+2])<<16 |
				uint32(data[pos+3])<<24)
		default:
			return 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "unsupported geometry metadata value %v (data: %v pos: %v)", metadata, data, pos)
		}
		pos += int(metadata)
		builder.(*array.BinaryBuilder).Append(data[pos : pos+l])
		return l + int(metadata), nil

	default:
		return 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "unsupported type %v", typ)
	}
}
