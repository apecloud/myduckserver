package pgserver

import (
	"bytes"
	"github.com/apecloud/myduckserver/catalog"
	"regexp"
	"strings"
	"sync"
	"unicode"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/marcboeker/go-duckdb"
)

var wellKnownStatementTags = map[string]struct{}{
	"SELECT":  {},
	"INSERT":  {},
	"UPDATE":  {},
	"DELETE":  {},
	"CALL":    {},
	"PRAGMA":  {},
	"COPY":    {},
	"ALTER":   {},
	"CREATE":  {},
	"DROP":    {},
	"PREPARE": {},
	"EXECUTE": {},
	"ATTACH":  {},
	"DETACH":  {},
}

func IsWellKnownStatementTag(tag string) bool {
	_, ok := wellKnownStatementTags[tag]
	return ok
}

func GetStatementTag(stmt *duckdb.Stmt) string {
	switch stmt.StatementType() {
	case duckdb.DUCKDB_STATEMENT_TYPE_SELECT:
		return "SELECT"
	case duckdb.DUCKDB_STATEMENT_TYPE_INSERT:
		return "INSERT"
	case duckdb.DUCKDB_STATEMENT_TYPE_UPDATE:
		return "UPDATE"
	case duckdb.DUCKDB_STATEMENT_TYPE_DELETE:
		return "DELETE"
	case duckdb.DUCKDB_STATEMENT_TYPE_CALL:
		return "CALL"
	case duckdb.DUCKDB_STATEMENT_TYPE_PRAGMA:
		return "PRAGMA"
	case duckdb.DUCKDB_STATEMENT_TYPE_COPY:
		return "COPY"
	case duckdb.DUCKDB_STATEMENT_TYPE_ALTER:
		return "ALTER"
	case duckdb.DUCKDB_STATEMENT_TYPE_CREATE:
		return "CREATE"
	case duckdb.DUCKDB_STATEMENT_TYPE_CREATE_FUNC:
		return "CREATE FUNCTION"
	case duckdb.DUCKDB_STATEMENT_TYPE_DROP:
		return "DROP"
	case duckdb.DUCKDB_STATEMENT_TYPE_PREPARE:
		return "PREPARE"
	case duckdb.DUCKDB_STATEMENT_TYPE_EXECUTE:
		return "EXECUTE"
	case duckdb.DUCKDB_STATEMENT_TYPE_ATTACH:
		return "ATTACH"
	case duckdb.DUCKDB_STATEMENT_TYPE_DETACH:
		return "DETACH"
	case duckdb.DUCKDB_STATEMENT_TYPE_TRANSACTION:
		return "TRANSACTION"
	case duckdb.DUCKDB_STATEMENT_TYPE_ANALYZE:
		return "ANALYZE"
	case duckdb.DUCKDB_STATEMENT_TYPE_EXPLAIN:
		return "EXPLAIN"
	case duckdb.DUCKDB_STATEMENT_TYPE_SET:
		return "SET"
	case duckdb.DUCKDB_STATEMENT_TYPE_VARIABLE_SET:
		return "SET VARIABLE"
	case duckdb.DUCKDB_STATEMENT_TYPE_EXPORT:
		return "EXPORT"
	case duckdb.DUCKDB_STATEMENT_TYPE_LOAD:
		return "LOAD"
	default:
		return "UNKNOWN"
	}
}

func GuessStatementTag(query string) string {
	// Remove leading comments
	query = RemoveLeadingComments(query)
	// Remove trailing semicolon
	query = sql.RemoveSpaceAndDelimiter(query, ';')

	// Guess the statement tag by looking for the first non-identifier character
	for i, c := range query {
		if !unicode.IsLetter(c) && c != '_' {
			return strings.ToUpper(query[:i])
		}
	}
	return strings.ToUpper(query)
}

func RemoveLeadingComments(query string) string {
	i := 0
	n := len(query)

	for i < n {
		if strings.HasPrefix(query[i:], "--") {
			// Skip line comment
			end := strings.Index(query[i:], "\n")
			if end == -1 {
				return ""
			}
			i += end + 1
		} else if strings.HasPrefix(query[i:], "/*") {
			// Skip block comment with nesting support
			nestLevel := 1
			pos := i + 2
			for pos < n && nestLevel > 0 {
				if pos+1 < n {
					if query[pos] == '/' && query[pos+1] == '*' {
						nestLevel++
						pos += 2
						continue
					}
					if query[pos] == '*' && query[pos+1] == '/' {
						nestLevel--
						pos += 2
						continue
					}
				}
				pos++
			}
			if nestLevel > 0 {
				return ""
			}
			i = pos
		} else if unicode.IsSpace(rune(query[i])) {
			// Skip whitespace
			i++
		} else {
			break
		}
	}
	return query[i:]
}

// RemoveComments removes comments from a query string.
// It supports line comments (--), block comments (/* ... */), and quoted strings.
// Author: Claude Sonnet 3.5
func RemoveComments(query string) string {
	var buf bytes.Buffer
	runes := []rune(query)
	length := len(runes)
	pos := 0

	for pos < length {
		// Handle line comments
		if pos+1 < length && runes[pos] == '-' && runes[pos+1] == '-' {
			pos += 2
			for pos < length && runes[pos] != '\n' {
				pos++
			}
			if pos < length {
				buf.WriteRune('\n')
				pos++
			}
			continue
		}

		// Handle block comments
		if pos+1 < length && runes[pos] == '/' && runes[pos+1] == '*' {
			nestLevel := 1
			pos += 2
			for pos < length && nestLevel > 0 {
				if pos+1 < length {
					if runes[pos] == '/' && runes[pos+1] == '*' {
						nestLevel++
						pos += 2
						continue
					}
					if runes[pos] == '*' && runes[pos+1] == '/' {
						nestLevel--
						pos += 2
						continue
					}
				}
				pos++
			}
			continue
		}

		// Handle string literals
		if runes[pos] == '\'' || (pos+1 < length && runes[pos] == 'E' && runes[pos+1] == '\'') {
			if runes[pos] == 'E' {
				buf.WriteRune('E')
				pos++
			}
			buf.WriteRune('\'')
			pos++
			for pos < length {
				if runes[pos] == '\'' {
					buf.WriteRune('\'')
					pos++
					break
				}
				if pos+1 < length && runes[pos] == '\\' {
					buf.WriteRune('\\')
					buf.WriteRune(runes[pos+1])
					pos += 2
					continue
				}
				buf.WriteRune(runes[pos])
				pos++
			}
			continue
		}

		// Handle dollar-quoted strings
		if runes[pos] == '$' {
			start := pos
			tagEnd := pos + 1
			for tagEnd < length && (unicode.IsLetter(runes[tagEnd]) || unicode.IsDigit(runes[tagEnd]) || runes[tagEnd] == '_') {
				tagEnd++
			}
			if tagEnd < length && runes[tagEnd] == '$' {
				tag := string(runes[start : tagEnd+1])
				buf.WriteString(tag)
				pos = tagEnd + 1
				for pos < length {
					if pos+len(tag) <= length && string(runes[pos:pos+len(tag)]) == tag {
						buf.WriteString(tag)
						pos += len(tag)
						break
					}
					buf.WriteRune(runes[pos])
					pos++
				}
				continue
			}
		}

		// Handle quoted identifiers
		if runes[pos] == '"' {
			buf.WriteRune('"')
			pos++
			for pos < length {
				if runes[pos] == '"' {
					buf.WriteRune('"')
					pos++
					break
				}
				buf.WriteRune(runes[pos])
				pos++
			}
			continue
		}

		buf.WriteRune(runes[pos])
		pos++
	}

	return buf.String()
}

var (
	pgCatalogRegex     *regexp.Regexp
	initPgCatalogRegex sync.Once
)

// get the regex to match any table in pg_catalog in the query.
func getPgCatalogRegex() *regexp.Regexp {
	initPgCatalogRegex.Do(func() {
		var tableNames []string
		for _, table := range catalog.GetInternalTables() {
			if table.Schema != "__sys__" {
				continue
			}
			tableNames = append(tableNames, table.Name)
		}
		pgCatalogRegex = regexp.MustCompile(`(?i)\b(?:FROM|JOIN)\s+(?:pg_catalog\.)?(` + strings.Join(tableNames, "|") + `)`)
	})
	return pgCatalogRegex
}

func ConvertToSys(sql string) string {
	return getPgCatalogRegex().ReplaceAllString(RemoveComments(sql), " __sys__.$1")
}
