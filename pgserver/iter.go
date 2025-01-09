package pgserver

import (
	stdsql "database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"math/big"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/marcboeker/go-duckdb"
	"github.com/sirupsen/logrus"
)

// DriverRowIter wraps a standard driver.Rows as a RowIter.
type DriverRowIter struct {
	rows    driver.Rows
	columns []string
	schema  sql.Schema
	buffer  []driver.Value // pre-allocated buffer for scanning values
	row     []any
}

func NewDriverRowIter(rows driver.Rows, schema sql.Schema) (*DriverRowIter, error) {
	columns := rows.Columns()
	width := max(len(columns), len(schema))
	buf := make([]driver.Value, width)
	row := make([]any, width)

	iter := &DriverRowIter{rows, columns, schema, buf, row}
	if logrus.GetLevel() >= logrus.DebugLevel {
		logrus.Debugf("New " + iter.String() + "\n")
	}
	return iter, nil
}

func (iter *DriverRowIter) String() string {
	var sb strings.Builder
	for i, col := range iter.schema {
		if i > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(col.Type.String())
	}
	return fmt.Sprintf("DriverRowIter: columns=%v, schema=[%s]", iter.columns, formatSchema(iter.schema))
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
func (iter *DriverRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if err := iter.rows.Next(iter.buffer[:len(iter.columns)]); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, err
	}

	// Prune or fill the values to match the schema
	width := len(iter.schema) // the desired width
	if width == 0 {
		width = len(iter.columns)
	} else if len(iter.columns) < width {
		for i := len(iter.columns); i < width; i++ {
			iter.buffer[i] = nil
		}
	}

	for i := 0; i < width; i++ {
		iter.row[i] = iter.buffer[i]
	}

	return sql.NewRow(iter.row[:width]...), nil
}

// Close closes the underlying driver.Rows.
func (iter *DriverRowIter) Close(ctx *sql.Context) error {
	return iter.rows.Close()
}

// SqlRowIter wraps a standard sql.Rows as a RowIter.
type SqlRowIter struct {
	rows     *stdsql.Rows
	columns  []*stdsql.ColumnType
	schema   sql.Schema
	buffer   []any // pre-allocated buffer for scanning values
	pointers []any // pointers to the buffer

	decimals []int
	lists    []int
	hugeInts []int
}

func NewSqlRowIter(rows *stdsql.Rows, schema sql.Schema) (*SqlRowIter, error) {
	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	width := max(len(columns), len(schema))
	buf := make([]any, width)
	ptrs := make([]any, width)
	for i := range buf {
		ptrs[i] = &buf[i]
	}

	var decimals []int
	for i, c := range columns {
		if strings.HasPrefix(c.DatabaseTypeName(), "DECIMAL") {
			decimals = append(decimals, i)
		}
	}

	var lists []int
	for i, t := range columns {
		if strings.HasSuffix(t.DatabaseTypeName(), "[]") {
			lists = append(lists, i)
		}
	}

	var hugeInts []int
	for i, t := range columns {
		if t.DatabaseTypeName() == "HUGEINT" {
			hugeInts = append(hugeInts, i)
		}
	}

	iter := &SqlRowIter{rows, columns, schema, buf, ptrs, decimals, lists, hugeInts}
	if logrus.GetLevel() >= logrus.DebugLevel {
		logrus.Debugf("New " + iter.String() + "\n")
	}
	return iter, nil
}

func (iter *SqlRowIter) String() string {
	return fmt.Sprintf("SqlRowIter: columns=[%s], schema=[%s]", formatColumnTypes(iter.columns), formatSchema(iter.schema))
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
func (iter *SqlRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if !iter.rows.Next() {
		if err := iter.rows.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}

	// Scan the values into the buffer
	if err := iter.rows.Scan(iter.pointers[:len(iter.columns)]...); err != nil {
		return nil, err
	}

	// logrus.Debugf("iter.decimals=%v, iter.lists=%v iter.buffer=%#v\n", iter.decimals, iter.lists, iter.buffer)

	// Process decimal values
	for _, idx := range iter.decimals {
		switch v := iter.buffer[idx].(type) {
		case nil:
			continue
		case duckdb.Decimal:
			iter.buffer[idx] = pgtype.Numeric{Int: v.Value, Exp: -int32(v.Scale), Valid: true}
		case string:
			var n pgtype.Numeric
			if err := n.Scan(v); err != nil {
				return nil, err
			}
			iter.buffer[idx] = n
		case []any:
			array := make([]pgtype.Numeric, len(v))
			for i, x := range v {
				switch y := x.(type) {
				case nil:
					array[i] = pgtype.Numeric{}
				case duckdb.Decimal:
					array[i] = pgtype.Numeric{Int: y.Value, Exp: -int32(y.Scale), Valid: true}
				case string:
					if err := array[i].Scan(y); err != nil {
						return nil, err
					}
				default:
					return nil, fmt.Errorf("unexpected type %T for decimal value", x)
				}
			}
			iter.buffer[idx] = array
		default:
			return nil, fmt.Errorf("unexpected type %T for decimal value", v)
		}
	}

	// Process list values
	for _, idx := range iter.lists {
		var list []any
		switch v := iter.buffer[idx].(type) {
		case nil:
			list = nil
		case []pgtype.Numeric: // from the previous decimal processing step
			iter.buffer[idx] = pgtype.FlatArray[pgtype.Numeric](v)
			continue
		case []any:
			list = v
		case duckdb.Composite[[]any]:
			list = v.Get()
		default:
			return nil, fmt.Errorf("unexpected type %T for list value", v)
		}
		iter.buffer[idx] = pgtype.FlatArray[any](list)
	}

	for _, idx := range iter.hugeInts {
		switch v := iter.buffer[idx].(type) {
		case nil:
			continue
		case *big.Int:
			iter.buffer[idx] = pgtype.Numeric{Int: v, Valid: true}
		default:
			return nil, fmt.Errorf("unexpected type %T for big.Int value", v)
		}
	}

	// Prune or fill the values to match the schema
	width := len(iter.schema) // the desired width
	if width == 0 {
		width = len(iter.columns)
	} else if len(iter.columns) < width {
		for i := len(iter.columns); i < width; i++ {
			iter.buffer[i] = nil
		}
	}

	return sql.NewRow(iter.buffer[:width]...), nil
}

// Close closes the underlying sql.Rows.
func (iter *SqlRowIter) Close(ctx *sql.Context) error {
	return iter.rows.Close()
}

func formatSchema(schema sql.Schema) string {
	var sb strings.Builder
	for i, col := range schema {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(col.Name)
		sb.WriteString(": ")
		sb.WriteString(col.Type.String())
	}
	return sb.String()
}

func formatColumnTypes(columns []*stdsql.ColumnType) string {
	var sb strings.Builder
	for i, col := range columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(col.Name())
		sb.WriteString(": ")
		sb.WriteString(col.DatabaseTypeName())
	}
	return sb.String()
}
