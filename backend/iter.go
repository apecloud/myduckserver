// Copyright 2024-2025 ApeCloud, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	stdsql "database/sql"
	"io"
	"strings"

	"github.com/apecloud/myduckserver/charset"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/marcboeker/go-duckdb"
	"github.com/shopspring/decimal"
)

var _ sql.RowIter = (*SQLRowIter)(nil)

// SQLRowIter wraps a standard sql.Rows as a RowIter.
type SQLRowIter struct {
	rows      *stdsql.Rows
	columns   []*stdsql.ColumnType
	schema    sql.Schema
	buffer    []any // pre-allocated buffer for scanning values
	pointers  []any // pointers to the buffer
	decimals  []int
	intervals []int
	nonUTF8   []int
	charsets  []sql.CharacterSetID
}

func NewSQLRowIter(rows *stdsql.Rows, schema sql.Schema) (*SQLRowIter, error) {
	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	var decimals []int
	for i, c := range columns {
		if strings.HasPrefix(c.DatabaseTypeName(), "DECIMAL") {
			decimals = append(decimals, i)
		}
	}

	var intervals []int
	for i, t := range columns {
		if strings.HasPrefix(t.DatabaseTypeName(), "INTERVAL") {
			intervals = append(intervals, i)
		}
	}

	var (
		nonUTF8  []int
		charsets []sql.CharacterSetID
	)
	for i, c := range schema {
		if t, ok := c.Type.(sql.StringType); ok && types.IsTextOnly(c.Type) && charset.IsSupportedNonUTF8(t.CharacterSet()) {
			nonUTF8 = append(nonUTF8, i)
			charsets = append(charsets, t.CharacterSet())
		}
	}

	width := max(len(columns), len(schema))
	buf := make([]any, width)
	ptrs := make([]any, width)
	for i := range buf {
		ptrs[i] = &buf[i]
	}

	return &SQLRowIter{rows, columns, schema, buf, ptrs, decimals, intervals, nonUTF8, charsets}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
func (iter *SQLRowIter) Next(ctx *sql.Context) (sql.Row, error) {
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

	// Process decimal values
	for _, idx := range iter.decimals {
		switch v := iter.buffer[idx].(type) {
		case duckdb.Decimal:
			iter.buffer[idx] = decimal.NewFromBigInt(v.Value, -int32(v.Scale))
		case string:
			iter.buffer[idx], _ = decimal.NewFromString(v)
		}
	}

	// Process interval values
	for _, idx := range iter.intervals {
		t := types.TimespanType_{}
		switch v := iter.buffer[idx].(type) {
		case duckdb.Interval:
			iter.buffer[idx] = t.MicrosecondsToTimespan(v.Micros + int64(v.Days)*24*60*60*1000000) // ignore the month part, which does not appear in MySQL
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

	// Encode UTF-8 strings into the desired charset
	for i, idx := range iter.nonUTF8 {
		switch v := iter.buffer[idx].(type) {
		case string:
			iter.buffer[idx], _ = charset.Encode(iter.charsets[i], v)
		}
	}

	return sql.NewRow(iter.buffer[:width]...), nil
}

// Close closes the underlying sql.Rows.
func (iter *SQLRowIter) Close(ctx *sql.Context) error {
	return iter.rows.Close()
}
