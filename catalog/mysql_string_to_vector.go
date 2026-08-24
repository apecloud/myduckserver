package catalog

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"fmt"

	gmsql "github.com/dolthub/go-mysql-server/sql"
	"github.com/duckdb/duckdb-go/v2"
)

type mysqlStringToVectorUDF struct{}

func mysqlStringToVectorExec(values []driver.Value) (any, error) {
	if len(values) == 0 || values[0] == nil {
		return nil, nil
	}
	if len(values) > 2 {
		return nil, fmt.Errorf("STRING_TO_VECTOR: expected one or two arguments")
	}

	input := values[0]
	if bytes, ok := input.([]byte); ok {
		input = string(bytes)
	}
	floats, err := gmsql.ConvertToVector(context.Background(), input)
	if err != nil {
		return nil, err
	}
	if len(floats) == 0 {
		return nil, fmt.Errorf("VECTOR must contain at least one element")
	}
	if len(values) == 2 {
		dimensions, ok := values[1].(int64)
		if !ok {
			return nil, fmt.Errorf("STRING_TO_VECTOR: invalid dimension type %T", values[1])
		}
		if len(floats) != int(dimensions) {
			return nil, fmt.Errorf("VECTOR dimension mismatch: expected %d, got %d", dimensions, len(floats))
		}
	}
	return gmsql.EncodeVector(floats), nil
}

func (*mysqlStringToVectorUDF) Config() duckdb.ScalarFuncConfig {
	varchar, err := duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
	if err != nil {
		panic(err)
	}
	bigint, err := duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	if err != nil {
		panic(err)
	}
	blob, err := duckdb.NewTypeInfo(duckdb.TYPE_BLOB)
	if err != nil {
		panic(err)
	}
	return duckdb.ScalarFuncConfig{
		InputTypeInfos:   []duckdb.TypeInfo{varchar},
		VariadicTypeInfo: bigint,
		ResultTypeInfo:   blob,
	}
}

func (*mysqlStringToVectorUDF) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{RowExecutor: mysqlStringToVectorExec}
}

func registerMySQLStringToVector(conn *stdsql.Conn) error {
	return duckdb.RegisterScalarUDF(conn, "string_to_vector", &mysqlStringToVectorUDF{})
}
