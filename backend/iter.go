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
	"errors"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"strings"

	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/charset"
	"github.com/apecloud/myduckserver/pgtypes"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"
)

var _ sql.RowIter = (*SQLRowIter)(nil)

const queryRowLimitError = "query returned more than the configured row limit of %d"

type queryRowLimitIter struct {
	child       sql.RowIter
	limit       uint64
	rows        uint64
	closed      bool
	closeErr    error
	overflowErr error
}

// ApplyQueryRowLimit wraps result rows with the limit configured on the
// current session. Non-row results and sessions with no limit are unchanged.
func ApplyQueryRowLimit(ctx *sql.Context, schema sql.Schema, iter sql.RowIter) sql.RowIter {
	if iter == nil || schema == nil || types.IsOkResultSchema(schema) {
		return iter
	}
	sess, ok := ctx.Session.(interface{ QueryRowLimit() uint64 })
	if !ok || sess.QueryRowLimit() == 0 {
		return iter
	}
	return &queryRowLimitIter{child: iter, limit: sess.QueryRowLimit()}
}

func (iter *queryRowLimitIter) Next(ctx *sql.Context) (sql.Row, error) {
	if iter.overflowErr != nil {
		return nil, iter.overflowErr
	}
	if iter.closed {
		return nil, io.EOF
	}
	if iter.rows < iter.limit {
		row, err := iter.child.Next(ctx)
		if err == nil {
			iter.rows++
		}
		return row, err
	}

	_, err := iter.child.Next(ctx)
	if err != nil {
		return nil, err
	}

	limitErr := fmt.Errorf(queryRowLimitError, iter.limit)
	iter.overflowErr = errors.Join(limitErr, iter.Close(ctx))
	return nil, iter.overflowErr
}

func (iter *queryRowLimitIter) Close(ctx *sql.Context) error {
	if iter.closed {
		return nil
	}
	iter.closed = true
	iter.closeErr = iter.child.Close(ctx)
	return iter.closeErr
}

type typeConversion struct {
	idx  int
	kind reflect.Kind
}

// SQLRowIter wraps a standard sql.Rows as a RowIter.
type SQLRowIter struct {
	rows        *stdsql.Rows
	columns     []*stdsql.ColumnType
	schema      sql.Schema
	buffer      []any // pre-allocated buffer for scanning values
	pointers    []any // pointers to the buffer
	decimals    []int
	intervals   []int
	jsons       []int
	nonUTF8     []int
	charsets    []sql.CharacterSetID
	conversions []typeConversion
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

	var jsons []int
	for i, t := range columns {
		if t.DatabaseTypeName() == "JSON" || (i < len(schema) && isJSONType(schema[i].Type)) {
			jsons = append(jsons, i)
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

	var conversions []typeConversion
	for i, c := range columns {
		if c.DatabaseTypeName() == "HUGEINT" {
			expectedType := schema[i].Type
			if ok := types.IsFloat(expectedType); ok {
				conversions = append(conversions, typeConversion{idx: i, kind: reflect.Float64})
			} else {
				conversions = append(conversions, typeConversion{idx: i, kind: reflect.Int64})
			}
		}
		if c.DatabaseTypeName() == "DOUBLE" || c.DatabaseTypeName() == "FLOAT" {
			expectedType := schema[i].Type
			if ok := types.IsInteger(expectedType); ok {
				conversions = append(conversions, typeConversion{idx: i, kind: reflect.Int64})
			}
		}
	}

	width := max(len(columns), len(schema))
	buf := make([]any, width)
	ptrs := make([]any, width)
	for i := range buf {
		ptrs[i] = &buf[i]
	}

	return &SQLRowIter{rows, columns, schema, buf, ptrs, decimals, intervals, jsons, nonUTF8, charsets, conversions}, nil
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

	// Normalize JSON strings and native driver values to the representation GMS
	// expects. SQL NULL remains nil; QueryForJSONScan preserves JSON null as text.
	for _, idx := range iter.jsons {
		if iter.buffer[idx] == nil {
			continue
		}
		converted, _, err := types.JSON.Convert(iter.buffer[idx])
		if err != nil {
			return nil, err
		}
		iter.buffer[idx] = converted
	}

	// Process type conversions
	for _, targetType := range iter.conversions {
		idx := targetType.idx
		rawValue := iter.buffer[idx]
		if targetType.kind == reflect.Float64 {
			switch v := rawValue.(type) {
			case *big.Int:
				iter.buffer[idx], _ = v.Float64()
			}
		}
		if targetType.kind == reflect.Int64 {
			switch v := rawValue.(type) {
			case float64:
				iter.buffer[idx] = int64(v)
			case float32:
				iter.buffer[idx] = int64(v)
			case *big.Int:
				iter.buffer[idx] = v.Int64()
			}
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

// QueryForJSONScan casts JSON result columns to VARCHAR before the DuckDB Go
// driver can collapse JSON null into SQL NULL.
func QueryForJSONScan(query string, schema sql.Schema) string {
	hasJSON := false
	for _, column := range schema {
		if isJSONType(column.Type) {
			hasJSON = true
			break
		}
	}
	if !hasJSON {
		return query
	}

	aliases := make([]string, len(schema))
	projections := make([]string, len(schema))
	for i, column := range schema {
		aliases[i] = fmt.Sprintf("__myduck_col_%d", i)
		qualified := "__myduck_json_source." + aliases[i]
		if isJSONType(column.Type) {
			projections[i] = "CAST(" + qualified + " AS VARCHAR)"
		} else {
			projections[i] = qualified
		}
		if column.Name != "" {
			projections[i] += " AS " + catalog.QuoteIdentifierANSI(column.Name)
		}
	}

	query = strings.TrimSuffix(strings.TrimSpace(query), ";")
	return "SELECT " + strings.Join(projections, ", ") +
		" FROM (" + query + ") AS __myduck_json_source(" + strings.Join(aliases, ", ") + ")"
}

func isJSONType(t sql.Type) bool {
	if types.IsJSON(t) {
		return true
	}
	postgresType, ok := t.(pgtypes.PostgresType)
	return ok && postgresType.PG != nil &&
		(postgresType.PG.OID == pgtype.JSONOID || postgresType.PG.OID == pgtype.JSONBOID)
}

// Close closes the underlying sql.Rows.
func (iter *SQLRowIter) Close(ctx *sql.Context) error {
	return iter.rows.Close()
}
