package catalog

import (
	"strings"

	"github.com/apecloud/myduckserver/transpiler"

	"github.com/dolthub/vitess/go/vt/sqlparser"
)

func FullSchemaName(catalog, schema string) string {
	if catalog == "" {
		return `"` + schema + `"`
	}
	// why?
	if schema == "" {
		return `"` + catalog + `"`
	}
	return `"` + catalog + `"."` + schema + `"`
}

func FullTableName(catalog, schema, table string) string {
	return FullSchemaName(catalog, schema) + `."` + table + `"`
}

func FullIndexName(catalog, schema, index string) string {
	return FullTableName(catalog, schema, index)
}

func FullColumnName(catalog, schema, table, column string) string {
	return FullTableName(catalog, schema, table) + `."` + column + `"`
}

// EncodeIndexName uses a simple encoding scheme (table$$index) for better visibility which is useful for debugging.
func EncodeIndexName(table, index string) string {
	return table + "$$" + index
}

func DecodeIndexName(encodedName string) (string, string) {
	parts := strings.Split(encodedName, "$$")
	// without "$$", the encodedName is the index name
	if len(parts) != 2 {
		return "", encodedName
	}
	return parts[0], parts[1]
}

func DecodeCreateindex(createIndexSQL string) ([]string, error) {

	denormalizedQuery := transpiler.DenormalizeStrings(createIndexSQL)
	stmt, err := sqlparser.Parse(denormalizedQuery)

	if err != nil {
		return nil, err
	}

	switch stmt := stmt.(type) {
	case *sqlparser.AlterTable:
		var columnNames []string
		for _, column := range stmt.Statements[0].IndexSpec.Columns {
			columnNames = append(columnNames, column.Column.String())
		}
		return columnNames, nil
	}

	return nil, nil
}

func QuoteIdentifierANSI(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func ConnectIdentifiersANSI(identifiers ...string) string {
	for i, id := range identifiers {
		identifiers[i] = QuoteIdentifierANSI(id)
	}
	return strings.Join(identifiers, ".")
}
