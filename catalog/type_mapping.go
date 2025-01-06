package catalog

import (
	"fmt"
	"strings"

	"github.com/apecloud/myduckserver/transpiler"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/sqlparser"
)

// TODO(ysg): Refactor this implementation by using interface{} to represent a DuckDB type,
// and implement the interface{} type for each corresponding MySQL type.
// The current large mapping function is error-prone and difficult to maintain.

type AnnotatedDuckType struct {
	name  string
	mysql MySQLType
}

func (t AnnotatedDuckType) Name() string {
	return t.name
}

func (t AnnotatedDuckType) MySQL() MySQLType {
	return t.mysql
}

type MySQLType struct {
	Name          string
	Length        uint32   `json:",omitempty"`
	Precision     uint8    `json:",omitempty"`
	Scale         uint8    `json:",omitempty"`
	Unsigned      bool     `json:",omitempty"`
	Display       uint8    `json:",omitempty"` // Display width for integer types
	Collation     uint16   `json:",omitempty"` // For string types
	Values        []string `json:",omitempty"` // For ENUM and SET
	Default       string   `json:",omitempty"` // Default value of column
	AutoIncrement bool     `json:",omitempty"` // Auto increment flag
}

func newCommonType(name string) AnnotatedDuckType {
	return AnnotatedDuckType{name, MySQLType{Name: name}}
}

func newSimpleType(duckName, mysqlName string) AnnotatedDuckType {
	return AnnotatedDuckType{duckName, MySQLType{Name: mysqlName}}
}

func newStringType(duckName, mysqlName string, typ sql.StringType) AnnotatedDuckType {
	return AnnotatedDuckType{
		duckName,
		MySQLType{Name: mysqlName, Length: uint32(typ.Length()), Collation: uint16(typ.Collation())},
	}
}

func newPrecisionType(duckName, mysqlName string, precision uint8) AnnotatedDuckType {
	return AnnotatedDuckType{duckName, MySQLType{Name: mysqlName, Precision: precision}}
}

func newDecimalType(precision, scale uint8) AnnotatedDuckType {
	return AnnotatedDuckType{
		fmt.Sprintf("DECIMAL(%d, %d)", precision, scale),
		MySQLType{Name: "DECIMAL", Precision: precision, Scale: scale},
	}
}

func newNumberType(name string, displayWidth int) AnnotatedDuckType {
	return AnnotatedDuckType{name, MySQLType{Name: name, Display: uint8(displayWidth)}}
}

func newUnsignedNumberType(name string, displayWidth int) AnnotatedDuckType {
	return AnnotatedDuckType{name, MySQLType{Name: name, Display: uint8(displayWidth), Unsigned: true}}
}

func newMediumIntType(displayWidth int) AnnotatedDuckType {
	return AnnotatedDuckType{"INTEGER", MySQLType{Name: "MEDIUMINT", Display: uint8(displayWidth)}}
}

func newUnsignedMediumIntType(displayWidth int) AnnotatedDuckType {
	return AnnotatedDuckType{"UINTEGER", MySQLType{Name: "MEDIUMINT", Display: uint8(displayWidth), Unsigned: true}}
}

func newDateTimeType(mysqlName string, precision int) AnnotatedDuckType {
	// precision is [0, 6], round up to 3

	name := "TIMESTAMP"
	if precision == 0 {
		name = "TIMESTAMP_S"
	} else if precision <= 3 {
		name = "TIMESTAMP_MS"
	} else if precision <= 6 {
		name = "TIMESTAMP" // us
	}

	return AnnotatedDuckType{name, MySQLType{Name: mysqlName, Precision: uint8(precision)}}
}

func newEnumType(typ sql.EnumType) AnnotatedDuckType {
	// For ENUM type, we need to escape single quotes in values
	escapedValues := make([]string, len(typ.Values()))
	for i, v := range typ.Values() {
		// Replace each single quote with two single quotes to escape it
		escapedValues[i] = strings.ReplaceAll(v, "'", "''")
	}
	typeString := `ENUM('` + strings.Join(escapedValues, `', '`) + `')`
	return AnnotatedDuckType{typeString, MySQLType{Name: "ENUM", Values: typ.Values(), Collation: uint16(typ.Collation())}}
}

func newSetType(typ sql.SetType) AnnotatedDuckType {
	// TODO: DuckDB does not support `SET` type. We store it as a string.
	//   We may use a `VARCHAR` type with a check constraint to enforce the values.
	return AnnotatedDuckType{"VARCHAR", MySQLType{Name: "SET", Values: typ.Values(), Collation: uint16(typ.Collation())}}
}

const DuckDBDecimalTypeMaxPrecision = 38

func DuckdbDataType(mysqlType sql.Type) (AnnotatedDuckType, error) {
	switch mysqlType.Type() {
	case sqltypes.Int8:
		return newNumberType("TINYINT", mysqlType.(sql.NumberType).DisplayWidth()), nil
	case sqltypes.Uint8:
		return newNumberType("UTINYINT", mysqlType.(sql.NumberType).DisplayWidth()), nil
	case sqltypes.Int16:
		return newNumberType("SMALLINT", mysqlType.(sql.NumberType).DisplayWidth()), nil
	case sqltypes.Uint16:
		return newNumberType("USMALLINT", mysqlType.(sql.NumberType).DisplayWidth()), nil
	case sqltypes.Int24:
		return newMediumIntType(mysqlType.(sql.NumberType).DisplayWidth()), nil
	case sqltypes.Uint24:
		return newUnsignedMediumIntType(mysqlType.(sql.NumberType).DisplayWidth()), nil
	case sqltypes.Int32:
		return newNumberType("INTEGER", mysqlType.(sql.NumberType).DisplayWidth()), nil
	case sqltypes.Uint32:
		return newNumberType("UINTEGER", mysqlType.(sql.NumberType).DisplayWidth()), nil
	case sqltypes.Int64:
		return newNumberType("BIGINT", mysqlType.(sql.NumberType).DisplayWidth()), nil
	case sqltypes.Uint64:
		return newNumberType("UBIGINT", mysqlType.(sql.NumberType).DisplayWidth()), nil
	case sqltypes.Float32:
		return newCommonType("FLOAT"), nil
	case sqltypes.Float64:
		return newCommonType("DOUBLE"), nil
	case sqltypes.Timestamp:
		return newDateTimeType("TIMESTAMP", mysqlType.(sql.DatetimeType).Precision()), nil // TODO: check if this is correct
	case sqltypes.Date:
		return newCommonType("DATE"), nil
	case sqltypes.Time:
		// https://dev.mysql.com/doc/refman/8.4/en/time.html
		// MySQL's TIME type can store a value within the range of '-838:59:59.000000' to '838:59:59.000000'.
		return newSimpleType("INTERVAL", "TIME"), nil
	case sqltypes.Datetime:
		return newDateTimeType("DATETIME", mysqlType.(sql.DatetimeType).Precision()), nil
	case sqltypes.Year:
		return newSimpleType("SMALLINT", "YEAR"), nil
	case sqltypes.Decimal:
		decimal := mysqlType.(sql.DecimalType)
		prec := decimal.Precision()
		scale := decimal.Scale()
		// truncate precision to max supported by DuckDB
		if prec > DuckDBDecimalTypeMaxPrecision {
			prec = DuckDBDecimalTypeMaxPrecision
			// scale must be less than or equal to precision
			if scale > prec {
				scale = prec
			}
		}
		return newDecimalType(prec, scale), nil
	// the logic is based on https://github.com/dolthub/go-mysql-server/blob/ed8de8d3a4e6a3c3f76788821fd3890aca4806bc/sql/types/strings.go#L570
	case sqltypes.Text:
		return newStringType("VARCHAR", "TEXT", mysqlType.(sql.StringType)), nil
	case sqltypes.Blob:
		return newStringType("BLOB", "BLOB", mysqlType.(sql.StringType)), nil
	case sqltypes.VarChar:
		return newStringType("VARCHAR", "VARCHAR", mysqlType.(sql.StringType)), nil
	case sqltypes.VarBinary:
		return newStringType("BLOB", "VARBINARY", mysqlType.(sql.StringType)), nil
	case sqltypes.Char:
		return newStringType("VARCHAR", "CHAR", mysqlType.(sql.StringType)), nil
	case sqltypes.Binary:
		return newStringType("BLOB", "BINARY", mysqlType.(sql.StringType)), nil
	case sqltypes.Bit:
		// https://dev.mysql.com/doc/refman/8.4/en/bit-type.html
		// We store it as a 64-bit unsigned integer because the BIT type is not supported by go-duckdb currently.
		return newPrecisionType("UBIGINT", "BIT", mysqlType.(types.BitType).NumberOfBits()), nil
	case sqltypes.TypeJSON:
		return newCommonType("JSON"), nil
	case sqltypes.Enum:
		return newEnumType(mysqlType.(types.EnumType)), nil
	case sqltypes.Set:
		return newSetType(mysqlType.(types.SetType)), nil
	case sqltypes.Geometry, sqltypes.Expression:
		return newCommonType(""), fmt.Errorf("unsupported MySQL type: %s", mysqlType.String())
	default:
		panic(fmt.Sprintf("encountered unknown MySQL type(%v). This is likely a bug - please check the duckdbDataType function for missing type mappings", mysqlType.Type()))
	}
}

func mysqlDataType(duckType AnnotatedDuckType, numericPrecision uint8, numericScale uint8) sql.Type {
	// TODO: The current type mappings are not lossless. We need to store the original type in the column comments.
	duckName := strings.TrimSpace(strings.ToUpper(duckType.name))

	if strings.HasPrefix(duckName, "DECIMAL") {
		duckName = "DECIMAL"
	} else if strings.HasPrefix(duckName, "ENUM") {
		duckName = "ENUM"
	}

	mysqlName := duckType.mysql.Name

	// process integer types, they all have displayWidth
	intBaseType := sqltypes.Null
	switch duckName {
	case "TINYINT":
		intBaseType = sqltypes.Int8
	case "UTINYINT":
		intBaseType = sqltypes.Uint8
	case "SMALLINT":
		if mysqlName == "YEAR" {
			return types.Year
		}
		intBaseType = sqltypes.Int16
	case "USMALLINT":
		intBaseType = sqltypes.Uint16
	case "INTEGER":
		if mysqlName == "MEDIUMINT" {
			intBaseType = sqltypes.Int24
		} else {
			intBaseType = sqltypes.Int32
		}
	case "UINTEGER":
		if mysqlName == "MEDIUMINT" {
			intBaseType = sqltypes.Uint24
		} else {
			intBaseType = sqltypes.Uint32
		}
	case "BIGINT":
		intBaseType = sqltypes.Int64
	case "UBIGINT":
		if mysqlName == "BIT" {
			return types.MustCreateBitType(duckType.mysql.Precision)
		}
		intBaseType = sqltypes.Uint64
	}

	if intBaseType != sqltypes.Null {
		return types.MustCreateNumberTypeWithDisplayWidth(intBaseType, int(duckType.mysql.Display))
	}

	length := int64(duckType.mysql.Length)
	precision := int(duckType.mysql.Precision)
	collation := sql.CollationID(duckType.mysql.Collation)

	switch duckName {
	case "FLOAT":
		return types.Float32
	case "DOUBLE":
		return types.Float64

	case "TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS":
		if mysqlName == "DATETIME" {
			return types.MustCreateDatetimeType(sqltypes.Datetime, precision)
		}
		return types.MustCreateDatetimeType(sqltypes.Timestamp, precision)

	case "DATE":
		return types.Date
	case "INTERVAL", "TIME":
		return types.Time

	case "DECIMAL":
		return types.MustCreateDecimalType(numericPrecision, numericScale)

	case "VARCHAR":
		if mysqlName == "TEXT" {
			if length <= types.TinyTextBlobMax {
				return types.TinyText
			} else if length <= types.TextBlobMax {
				return types.Text
			} else if length <= types.MediumTextBlobMax {
				return types.MediumText
			} else {
				return types.LongText
			}
		} else if mysqlName == "VARCHAR" {
			return types.MustCreateString(sqltypes.VarChar, length, collation)
		} else if mysqlName == "CHAR" {
			return types.MustCreateString(sqltypes.Char, length, collation)
		} else if mysqlName == "SET" {
			return types.MustCreateSetType(duckType.mysql.Values, collation)
		}
		return types.Text

	case "BLOB":
		if mysqlName == "BLOB" {
			if length <= types.TinyTextBlobMax {
				return types.TinyBlob
			} else if length <= types.TextBlobMax {
				return types.Blob
			} else if length <= types.MediumTextBlobMax {
				return types.MediumBlob
			} else {
				return types.LongBlob
			}
		} else if mysqlName == "VARBINARY" {
			return types.MustCreateBinary(sqltypes.VarBinary, length)
		} else if mysqlName == "BINARY" {
			return types.MustCreateBinary(sqltypes.Binary, length)
		}
		return types.Blob

	case "JSON":
		return types.JSON
	case "ENUM":
		return types.MustCreateEnumType(duckType.mysql.Values, collation)
	case "SET":
		return types.MustCreateSetType(duckType.mysql.Values, collation)
	default:
		panic(fmt.Sprintf("encountered unknown DuckDB type(%v). This is likely a bug - please check the duckdbDataType function for missing type mappings", duckType))
	}
}

func parseDefaultValue(defaultValue string) (string, error) {
	parsed, err := sqlparser.Parse("SELECT " + defaultValue)
	if err != nil {
		return "", err
	}
	selectStmt, ok := parsed.(*sqlparser.Select)
	if !ok {
		return "", fmt.Errorf("expected SELECT statement, got %T", parsed)
	}
	expr := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr).Expr
	switch expr := expr.(type) {
	case *sqlparser.FuncExpr:
		if expr.Name.Lowered() == "current_timestamp" {
			return "CURRENT_TIMESTAMP", nil
		}
	}
	normalized := transpiler.NormalizeStrings(defaultValue)
	return normalized, nil
}
