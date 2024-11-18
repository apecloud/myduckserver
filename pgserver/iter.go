package pgserver

import (
	"database/sql/driver"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
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

	var sb strings.Builder
	for i, col := range schema {
		if i > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(col.Type.String())
	}
	logrus.Debugf("New DriverRowIter: columns=%v, schema=[%s]\n", columns, sb.String())

	return &DriverRowIter{rows, columns, schema, buf, row}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
func (iter *DriverRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if err := iter.rows.Next(iter.buffer); err != nil {
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
