package meta

import (
	"strings"
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

// DecodeCreateindex extracts column names from a SQL string, Only consider single-column indexes or multi-column indexes
func DecodeCreateindex(sql_string string) []string {
	leftParen := strings.Index(sql_string, "(")
	rightParen := strings.Index(sql_string, ")")
	if leftParen != -1 && rightParen != -1 {
		content := sql_string[leftParen+1 : rightParen]
		columns := strings.Split(content, ",")
		for i, col := range columns {
			columns[i] = strings.TrimSpace(col)
		}
		return columns
	}
	return []string{}
}
