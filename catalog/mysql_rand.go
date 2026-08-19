package catalog

import (
	stdsql "database/sql"
	"database/sql/driver"
	"math/rand"

	"github.com/marcboeker/go-duckdb"
)

// mysqlRandUDF matches go-mysql-server RAND(seed):
// rand.New(rand.NewSource(int64(seed))).Float64().
type mysqlRandUDF struct{}

func mysqlRandFromSeed(seed int64) float64 {
	return rand.New(rand.NewSource(seed)).Float64()
}

func mysqlRandExec(values []driver.Value) (any, error) {
	var seed int64
	if len(values) > 0 && values[0] != nil {
		switch v := values[0].(type) {
		case int64:
			seed = v
		case int32:
			seed = int64(v)
		case int16:
			seed = int64(v)
		case int8:
			seed = int64(v)
		case float64:
			seed = int64(v)
		case float32:
			seed = int64(v)
		}
	}
	return mysqlRandFromSeed(seed), nil
}

func (*mysqlRandUDF) Config() duckdb.ScalarFuncConfig {
	in, err := duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	if err != nil {
		panic(err)
	}
	out, err := duckdb.NewTypeInfo(duckdb.TYPE_DOUBLE)
	if err != nil {
		panic(err)
	}
	return duckdb.ScalarFuncConfig{
		InputTypeInfos: []duckdb.TypeInfo{in},
		ResultTypeInfo: out,
	}
}

func (*mysqlRandUDF) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{RowExecutor: mysqlRandExec}
}

func registerMySQLRand(conn *stdsql.Conn) error {
	return duckdb.RegisterScalarUDF(conn, "mysql_rand", &mysqlRandUDF{})
}
