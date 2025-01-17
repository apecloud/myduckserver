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
		var internalNames []string
		for _, table := range catalog.GetInternalTables() {
			if table.Schema != "__sys__" {
				continue
			}
			internalNames = append(internalNames, table.Name)
		}
		for _, view := range catalog.InternalViews {
			if view.Schema != "__sys__" {
				continue
			}
			internalNames = append(internalNames, view.Name)
		}
		pgCatalogRegex = regexp.MustCompile(
			`(?i)\b(FROM|JOIN|INTO)\s+(?:pg_catalog\.)?(?:"?(` + strings.Join(internalNames, "|") + `)"?)`)
	})
	return pgCatalogRegex
}

func ConvertToSys(sql string) string {
	return getPgCatalogRegex().ReplaceAllString(RemoveComments(sql), "$1 __sys__.$2")
}

var (
	pgAnyOpRegex     *regexp.Regexp
	initPgAnyOpRegex sync.Once
)

// get the regex to match the operator 'ANY'
func getPgAnyOpRegex() *regexp.Regexp {
	initPgAnyOpRegex.Do(func() {
		pgAnyOpRegex = regexp.MustCompile(`(?i)([^\s(]+)\s*=\s*any\s*\(\s*([^)]*)\s*\)`)
	})
	return pgAnyOpRegex
}

// Replace the operator 'ANY' with a function call.
func ConvertAnyOp(sql string) string {
	re := getPgAnyOpRegex()
	return re.ReplaceAllString(sql, catalog.SchemaNameSYS+"."+catalog.MacroNameMyListContains+"($2, $1)")
}

var (
	simpleStrMatchingRegex     *regexp.Regexp
	initSimpleStrMatchingRegex sync.Once
)

// TODO(sean): This is a temporary solution. We need to find a better way to handle type cast conversion and column conversion. e.g. Iterating the AST with a visitor pattern.
// The Key must be in lowercase. Because the key used for value retrieval is in lowercase.
var simpleStringsConversion = map[string]string{
	// type cast conversion
	"::regclass": "::varchar",
	"::regtype":  "::varchar",

	// column conversion
	"proallargtypes": catalog.SchemaNameSYS + "." + catalog.MacroNameMySplitListStr + "(proallargtypes)",
	"proargtypes":    catalog.SchemaNameSYS + "." + catalog.MacroNameMySplitListStr + "(proargtypes)",
}

// This function will return a regex that matches all type casts in the query.
func getSimpleStringMatchingRegex() *regexp.Regexp {
	initSimpleStrMatchingRegex.Do(func() {
		var simpleStrings []string
		for simpleString := range simpleStringsConversion {
			simpleStrings = append(simpleStrings, regexp.QuoteMeta(simpleString))
		}
		simpleStrMatchingRegex = regexp.MustCompile(`(?i)(` + strings.Join(simpleStrings, "|") + `)`)
	})
	return simpleStrMatchingRegex
}

// This function will replace all type casts in the query with the corresponding type cast in the simpleStringsConversion map.
func SimpleStrReplacement(sql string) string {
	return getSimpleStringMatchingRegex().ReplaceAllStringFunc(sql, func(m string) string {
		return simpleStringsConversion[strings.ToLower(m)]
	})
}

var (
	renameMacroRegex     *regexp.Regexp
	initRenameMacroRegex sync.Once
	macroRegex           *regexp.Regexp
	initMacroRegex       sync.Once
)

// This function will return a regex that matches all function names
// in the list of InternalMacros. And they will have optional "pg_catalog." prefix.
// However, if the schema is not "pg_catalog", it will not be matched.
// e.g.
// SELECT pg_catalog.abc(123, 'test') AS result1,
//
//	defg('hello', world) AS result2,
//	user.abc(1) AS result3,
//	pg_catalog.xyz(456) AS result4
//
// FROM my_table;
// If the function names in the list of InternalMacros are "pg_catalog.abc" and "pg_catalog.defg",
// Then the matched function names will be "pg_catalog.abc" and "defg".
// The "user.abc" and "pg_catalog.xyz" will not be matched. Because for "user.abc", the schema is "user" and for
// "pg_catalog.xyz", the function name is "xyz".
func getRenamePgCatalogFuncRegex() *regexp.Regexp {
	initRenameMacroRegex.Do(func() {
		var internalNames []string
		for _, view := range catalog.InternalMacros {
			if strings.ToLower(view.Schema) != "pg_catalog" {
				continue
			}
			// Quote the function name to ensure safe regex usage
			internalNames = append(internalNames, regexp.QuoteMeta(view.Name))
		}

		namesAlt := strings.Join(internalNames, "|")

		// Compile the regex
		// The pattern matches:
		// - Branch A: "pg_catalog.<funcName>("
		// - Branch B: "<funcName>(" without a preceding "."
		pattern := `(?i)(?:pg_catalog\.("?(?:` + namesAlt + `)"?)\(|(^|[^\.])("?(?:` + namesAlt + `)"?)\()`
		renameMacroRegex = regexp.MustCompile(pattern)
	})
	return renameMacroRegex
}

// Replaces all matching function names in the query with "__sys__.<funcName>".
// e.g.
// SELECT pg_catalog.abc(123, 'test') AS result1,
//
//	defg('hello', world) AS result2,
//	user.abc(1) AS result3,
//	pg_catalog.xyz(456) AS result4
//
// If the function names in the list of InternalMacros are "pg_catalog.abc" and "pg_catalog.defg".
// After the replacement, the query will be:
// SELECT __sys__.abc(123, 'test') AS result1,
//
//	__sys__.defg('hello', world) AS result2,
//	user.abc(1) AS result3,
//	pg_catalog.xyz(456) AS result4
func ConvertPgCatalogFuncToSys(sql string) string {
	re := getRenamePgCatalogFuncRegex()
	return re.ReplaceAllStringFunc(sql, func(m string) string {
		sub := re.FindStringSubmatch(m)
		// sub[1]  => Function name from branch A (pg_catalog.<func>)
		// sub[2]  => Matches from branch B (^|[^.]), not the function name
		// sub[3]  => Function name from branch B
		var funcName string
		if sub[1] != "" {
			// Matched branch A
			funcName = sub[1]
		} else {
			// Matched branch B
			funcName = sub[3]
		}
		// Return __sys__.<funcName>(
		return "__sys__." + funcName + "("
	})
}

// This function will return a regex that matches all function names
// in the list of InternalMacros. And the Macro must be a table macro.
// e.g.
//
// * A scalar macro:
// CREATE OR REPLACE MACRO udf.mul
//
//	(a, b) AS a * b,
//	(a, b, c) AS a * b * c;
//
// * A table macro:
// CREATE OR REPLACE MACRO information_schema._pg_expandarray(a) AS TABLE
// SELECT STRUCT_PACK(
//
//	x := unnest(a),
//	n := generate_series(1, array_length(a))
//
// ) AS item;
//
// SQL string:
// SELECT
//
//	(information_schema._pg_expandarray(my_key_indexes)).x,
//	information_schema._pg_expandarray(my_col_indexes),
//	udf.mul(a, b, c)
//
// FROM my_table;
//
// Then the matched function names will be "information_schema._pg_expandarray".
// The "udf.mul" will not be matched. Because it is a scalar macro.
func getPgFuncRegex() *regexp.Regexp {
	initMacroRegex.Do(func() {
		// Collect the fully qualified names of all macros.
		var macroPatterns []string
		for _, macro := range catalog.InternalMacros {
			if macro.IsTableMacro {
				qualified := regexp.QuoteMeta(macro.QualifiedName())
				macroPatterns = append(macroPatterns, qualified)
			}
		}

		// Build the regular expression:
		// (\(*) - Captures leading parentheses.
		// (schema.name\s*\([^)]*\)) - Captures the macro invocation itself.
		// (\)*) - Captures trailing parentheses.
		pattern := `(?i)(\(*)(\b(?:` + strings.Join(macroPatterns, "|") + `)\([^)]*\))(\)*)`
		macroRegex = regexp.MustCompile(pattern)
	})
	return macroRegex
}

// Wraps all table macro calls in "(FROM ...)".
// e.g.
// If the function names in the list of InternalMacros are "information_schema._pg_expandarray"(Table Macro)
// and "udf.mul"(Scalar Macro).
//
// For the SQL string:
// SELECT
//
//	(information_schema._pg_expandarray(my_key_indexes)).x,
//	information_schema._pg_expandarray(my_col_indexes),
//	udf.mul(a, b, c)
//
// FROM my_table;
//
// After the replacement, the query will be:
// SELECT
//
//	(FROM information_schema._pg_expandarray(my_key_indexes)).x,
//	(FROM information_schema._pg_expandarray(my_col_indexes)),
//	udf.mul(a, b, c)
//
// FROM my_table;
func ConvertToDuckDBMacro(sql string) string {
	return getPgFuncRegex().ReplaceAllStringFunc(sql, func(match string) string {
		// Split the match into components using the regex's capturing groups.
		parts := getPgFuncRegex().FindStringSubmatch(match)
		if len(parts) != 4 {
			return match // Return the original match if it doesn't conform to the expected structure.
		}

		leftParens := parts[1]  // Leading parentheses.
		macroCall := parts[2]   // The macro invocation.
		rightParens := parts[3] // Trailing parentheses.

		// If the macro call is already wrapped in "(FROM ...)", skip wrapping it again.
		if strings.HasPrefix(macroCall, "(FROM ") {
			return match
		}

		// Wrap the macro call in "(FROM ...)" and preserve surrounding parentheses.
		return leftParens + "(FROM " + macroCall + ")" + rightParens
	})
}
