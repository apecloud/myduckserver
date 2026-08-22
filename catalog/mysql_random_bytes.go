package catalog

import (
	crand "crypto/rand"
	stdsql "database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/duckdb/duckdb-go/v2"
)

// mysqlRandomBytesUDF implements MySQL RANDOM_BYTES(n): n cryptographically
// random bytes. n must be 1..1024.
type mysqlRandomBytesUDF struct{}

func mysqlRandomBytesExec(values []driver.Value) (any, error) {
	if len(values) == 0 || values[0] == nil {
		return nil, nil
	}
	var n int64
	switch v := values[0].(type) {
	case int64:
		n = v
	case int32:
		n = int64(v)
	case int16:
		n = int64(v)
	case int8:
		n = int64(v)
	case float64:
		n = int64(v)
	case float32:
		n = int64(v)
	default:
		return nil, fmt.Errorf("RANDOM_BYTES: invalid length")
	}
	if n < 1 || n > 1024 {
		return nil, fmt.Errorf("RANDOM_BYTES: length must be between 1 and 1024")
	}
	b := make([]byte, n)
	if _, err := crand.Read(b); err != nil {
		return nil, err
	}
	return b, nil
}

func (*mysqlRandomBytesUDF) Config() duckdb.ScalarFuncConfig {
	in, err := duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	if err != nil {
		panic(err)
	}
	out, err := duckdb.NewTypeInfo(duckdb.TYPE_BLOB)
	if err != nil {
		panic(err)
	}
	return duckdb.ScalarFuncConfig{
		InputTypeInfos: []duckdb.TypeInfo{in},
		ResultTypeInfo: out,
	}
}

func (*mysqlRandomBytesUDF) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{RowExecutor: mysqlRandomBytesExec}
}

func registerMySQLRandomBytes(conn *stdsql.Conn) error {
	return duckdb.RegisterScalarUDF(conn, "mysql_random_bytes", &mysqlRandomBytesUDF{})
}
