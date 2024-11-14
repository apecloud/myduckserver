package pgtest

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"
)

var defaultMap = pgtype.NewMap()

// ScriptTestAssertion are the assertions upon which the script executes its main "testing" logic.
type ScriptTestAssertion struct {
	Query       string
	Expected    []sql.Row
	ExpectedErr string

	BindVars []any

	// SkipResultsCheck is used to skip assertions on the expected rows returned from a query. For now, this is
	// included as some messages do not have a full logical implementation. Skipping the results check allows us to
	// force the test client to not send of those messages.
	SkipResultsCheck bool

	// Skip is used to completely skip a test, not execute its query at all, and record it as a skipped test
	// in the test suite results.
	Skip bool

	// Username specifies the user's name to use for the command. This creates a new connection, using the given name.
	// By default (when the string is empty), the `postgres` superuser account is used. Any consecutive queries that
	// have the same username and password will reuse the same connection. The `postgres` superuser account will always
	// reuse the same connection. Do note that specifying the `postgres` account manually will create a connection
	// that is different from the primary one.
	Username string
	// Password specifies the password that will be used alongside the given username. This field is essentially ignored
	// when no username is given. If a username is given and the password is empty, then it is assumed that the password
	// is the empty string.
	Password string

	// ExpectedTag is used to check the command tag returned from the server.
	// This is checked only if no Expected is defined
	ExpectedTag string

	// Cols is used to check the column names returned from the server.
	Cols []string
}

// ReadRows reads all of the given rows into a slice, then closes the rows. If `normalizeRows` is true, then the rows
// will be normalized such that all integers are int64, etc.
func ReadRows(rows pgx.Rows, normalizeRows bool) (readRows []sql.Row, err error) {
	defer func() {
		err = errors.Join(err, rows.Err())
	}()
	var slice []sql.Row
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			return nil, err
		}
		slice = append(slice, row)
	}
	return NormalizeRows(rows.FieldDescriptions(), slice, normalizeRows), nil
}

// NormalizeRows normalizes each value's type within each row, as the tests only want to compare values. Returns a new
// set of rows in the same order.
func NormalizeRows(fds []pgconn.FieldDescription, rows []sql.Row, normalize bool) []sql.Row {
	newRows := make([]sql.Row, len(rows))
	for i := range rows {
		newRows[i] = NormalizeRow(fds, rows[i], normalize)
	}
	return newRows
}

// NormalizeRow normalizes each value's type, as the tests only want to compare values.
// Returns a new row.
func NormalizeRow(fds []pgconn.FieldDescription, row sql.Row, normalize bool) sql.Row {
	if len(row) == 0 {
		return nil
	}
	newRow := make(sql.Row, len(row))
	for i := range row {
		typ, ok := defaultMap.TypeForOID(fds[i].DataTypeOID)
		if !ok {
			panic(fmt.Sprintf("unknown oid: %v", fds[i].DataTypeOID))
		}
		newRow[i] = NormalizeValToString(typ, row[i])
		if normalize {
			newRow[i] = NormalizeIntsAndFloats(newRow[i])
		}
	}
	return newRow
}

// NormalizeExpectedRow normalizes each value's type, as the tests only want to compare values. Returns a new row.
func NormalizeExpectedRow(fds []pgconn.FieldDescription, rows []sql.Row) []sql.Row {
	newRows := make([]sql.Row, len(rows))
	for ri, row := range rows {
		if len(row) == 0 {
			newRows[ri] = nil
		} else if len(row) != len(fds) {
			// Return if the expected row count does not match the field description count, we'll error elsewhere
			return rows
		} else {
			newRow := make(sql.Row, len(row))
			for i := range row {
				oid := fds[i].DataTypeOID
				typ, ok := defaultMap.TypeForOID(oid)
				if !ok {
					panic(fmt.Sprintf("unknown oid: %v", fds[i].DataTypeOID))
				}
				if strings.EqualFold(typ.Name, "json") {
					newRow[i] = UnmarshalAndMarshalJsonString(row[i].(string))
				} else if strings.EqualFold(typ.Name, "_json") { // Array of JSON
					bytes, err := defaultMap.Encode(oid, pgtype.TextFormatCode, row[i], nil)
					if err != nil {
						panic(fmt.Errorf("failed to encode json array: %w", err))
					}
					var arr []string
					if err := defaultMap.Scan(oid, pgtype.TextFormatCode, bytes, &arr); err != nil {
						panic(fmt.Errorf("failed to scan json array: %w", err))
					}
					newArr := make([]string, len(arr))
					for j, el := range arr {
						newArr[j] = UnmarshalAndMarshalJsonString(el)
					}

					bytes, err = defaultMap.Encode(oid, pgtype.TextFormatCode, newArr, nil)
					if err != nil {
						panic(fmt.Errorf("failed to encode json array: %w", err))
					}

					newRow[i] = string(bytes)
				} else {
					newRow[i] = NormalizeIntsAndFloats(row[i])
				}
			}
			newRows[ri] = newRow
		}
	}
	return newRows
}

// UnmarshalAndMarshalJsonString is used to normalize expected json type value to compare the actual value.
// JSON type value is in string format, and since Postrges JSON type preserves the input string if valid,
// it cannot be compared to the returned map as json.Marshal method space padded key value pair.
// To allow result matching, we unmarshal and marshal the expected string. This causes missing check
// for the identical format as the input of the json string.
func UnmarshalAndMarshalJsonString(val string) string {
	var decoded any
	err := json.Unmarshal([]byte(val), &decoded)
	if err != nil {
		panic(err)
	}
	ret, err := json.Marshal(decoded)
	if err != nil {
		panic(err)
	}
	return string(ret)
}

// NormalizeValToString normalizes values into types that can be compared.
// JSON types, any pg types and time and decimal type values are converted into string value.
// |normalizeNumeric| defines whether to normalize Numeric values into either Numeric type or string type.
// There are an infinite number of ways to represent the same value in-memory,
// so we must at least normalize Numeric values.
func NormalizeValToString(typ *pgtype.Type, v any) any {
	switch strings.ToLower(typ.Name) {
	case "json":
		str, err := json.Marshal(v)
		if err != nil {
			panic(err)
		}
		bytes, err := defaultMap.Encode(typ.OID, pgtype.TextFormatCode, string(str), nil)
		if err != nil {
			panic(err)
		}
		return string(bytes)
	case "jsonb":
		bytes, err := defaultMap.Encode(typ.OID, pgtype.TextFormatCode, v, nil)
		if err != nil {
			panic(err)
		}
		var s string
		if err := defaultMap.Scan(typ.OID, pgtype.TextFormatCode, bytes, &s); err != nil {
			panic(err)
		}
		return s
	case "interval", "time", "timestamp", "date", "uuid":
		// These values need to be normalized into the appropriate types
		// before being converted to string type using the Doltgres
		// IoOutput method.
		if v == nil {
			return nil
		}
		v = NormalizeVal(typ, v)
		bytes, err := defaultMap.Encode(typ.OID, pgtype.TextFormatCode, v, nil)
		if err != nil {
			panic(err)
		}
		return string(bytes)

	case "timestamptz":
		// timestamptz returns a value in server timezone
		_, offset := v.(time.Time).Zone()
		if offset%3600 != 0 {
			return v.(time.Time).Format("2006-01-02 15:04:05.999999999-07:00")
		} else {
			return v.(time.Time).Format("2006-01-02 15:04:05.999999999-07")
		}
	}

	switch val := v.(type) {
	case bool:
		if val {
			return "t"
		} else {
			return "f"
		}
	case pgtype.Numeric:
		if val.NaN {
			return math.NaN()
		} else if val.InfinityModifier != pgtype.Finite {
			return math.Inf(int(val.InfinityModifier))
		} else if !val.Valid {
			return nil
		} else {
			decStr := decimal.NewFromBigInt(val.Int, val.Exp).StringFixed(val.Exp * -1)
			return Numeric(decStr)
		}
	case []any:
		if strings.HasPrefix(typ.Name, "_") {
			return NormalizeArrayType(typ, val)
		}
	}
	return v
}

// NormalizeArrayType normalizes array types by normalizing its elements first,
// then to a string using the type IoOutput method.
func NormalizeArrayType(dta *pgtype.Type, arr []any) any {
	baseType := dta.Codec.(*pgtype.ArrayCodec).ElementType
	newVal := make([]any, len(arr))
	for i, el := range arr {
		newVal[i] = NormalizeVal(baseType, el)
	}
	bytes, err := defaultMap.Encode(dta.OID, pgtype.TextFormatCode, newVal, nil)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

// NormalizeVal normalizes values to the Doltgres type expects, so it can be used to
// convert the values using the given Doltgres type. This is used to normalize array
// types as the type conversion expects certain type values.
func NormalizeVal(typ *pgtype.Type, v any) any {
	switch strings.ToLower(typ.Name) {
	case "json":
		str, err := json.Marshal(v)
		if err != nil {
			panic(err)
		}
		return string(str)
	case "jsonb":
		bytes, err := defaultMap.Encode(typ.OID, pgtype.TextFormatCode, v, nil)
		if err != nil {
			panic(err)
		}
		var s string
		if err := defaultMap.Scan(typ.OID, pgtype.TextFormatCode, bytes, &s); err != nil {
			panic(err)
		}
		return s
	}

	switch val := v.(type) {
	case pgtype.Numeric:
		if val.NaN {
			return math.NaN()
		} else if val.InfinityModifier != pgtype.Finite {
			return math.Inf(int(val.InfinityModifier))
		} else if !val.Valid {
			return nil
		} else {
			return decimal.NewFromBigInt(val.Int, val.Exp)
		}
	case pgtype.Time:
		// This value type is used for TIME type.
		var zero time.Time
		return zero.Add(time.Duration(val.Microseconds) * time.Microsecond)
	case pgtype.Interval:
		// This value type is used for INTERVAL type.
		// TODO(fan): Months
		var zero time.Time
		return zero.Add(time.Duration(val.Microseconds)*time.Microsecond).AddDate(0, 0, int(val.Days))
	case [16]byte:
		// This value type is used for UUID type.
		u, err := uuid.FromBytes(val[:])
		if err != nil {
			panic(err)
		}
		return u
	case []any:
		baseType := typ.Codec.(*pgtype.ArrayCodec).ElementType
		newVal := make([]any, len(val))
		for i, el := range val {
			newVal[i] = NormalizeVal(baseType, el)
		}
		return newVal
	}
	return v
}

// NormalizeIntsAndFloats normalizes all int and float types
// to int64 and float64, respectively.
func NormalizeIntsAndFloats(v any) any {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case uint:
		return int64(val)
	case uint8:
		return int64(val)
	case uint16:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		// PostgreSQL does not support an uint64 type, so we can always convert this to an int64 safely.
		return int64(val)
	// case float32:
	// 	return float64(val)
	default:
		return val
	}
}

// Numeric creates a numeric value from a string.
func Numeric(str string) pgtype.Numeric {
	numeric := pgtype.Numeric{}
	if err := numeric.Scan(str); err != nil {
		panic(err)
	}
	return numeric
}
