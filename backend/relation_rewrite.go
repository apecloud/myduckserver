package backend

import (
	"strings"
	"unicode"
)

// RewriteSQLRelations replaces table references in relation positions. It is
// intentionally a small lexer rather than strings.Replace: quoted literals
// and comments may contain table-looking text, and must remain untouched.
//
// routes are keyed by an unquoted, case-insensitive relation name. A qualified
// key (for example, "app.orders") takes precedence over a bare table key.
// Values are emitted verbatim and are expected to be trusted, already-quoted
// DuckDB identifiers supplied by the catalog layer.
func RewriteSQLRelations(query string, routes map[string]string) (string, bool) {
	if query == "" || len(routes) == 0 {
		return query, false
	}
	tokens := lexSQLForRelations(query)
	if len(tokens) == 0 {
		return query, false
	}

	var out strings.Builder
	out.Grow(len(query))
	last := 0
	changed := false
	for i := 0; i < len(tokens); i++ {
		if !isRelationKeyword(tokens[i]) || i+1 >= len(tokens) {
			continue
		}
		// A FROM clause can contain a comma-separated list of relations. Keep
		// consuming that list so each table is routed independently.
		for nameStart := i + 1; nameStart < len(tokens); {
			if !isRelationNameToken(tokens[nameStart]) {
				break
			}
			nameEnd := nameStart + 1
			for nameEnd+1 < len(tokens) && tokens[nameEnd].kind == relationDot && isRelationNameToken(tokens[nameEnd+1]) {
				nameEnd += 2
			}

			parts := make([]string, 0, (nameEnd-nameStart+1)/2)
			for j := nameStart; j < nameEnd; j += 2 {
				parts = append(parts, tokens[j].value)
			}
			key := strings.ToLower(strings.Join(parts, "."))
			replacement, ok := routes[key]
			if !ok && len(parts) > 1 {
				replacement, ok = routes[strings.ToLower(parts[len(parts)-1])]
			}
			if ok {
				out.WriteString(query[last:tokens[nameStart].start])
				out.WriteString(replacement)
				last = tokens[nameEnd-1].end
				changed = true
			}

			// Skip an optional alias before looking for the next relation. The
			// alias is left in the output because last still ends at nameEnd.
			listEnd := nameEnd
			if listEnd < len(tokens) && tokens[listEnd].kind == relationWord && tokens[listEnd].value == "as" {
				if listEnd+1 < len(tokens) && isRelationNameToken(tokens[listEnd+1]) {
					listEnd += 2
				}
			} else if listEnd < len(tokens) && isRelationNameToken(tokens[listEnd]) {
				listEnd++
			}
			// Only a literal comma starts another relation in the same list.
			i = listEnd - 1
			if listEnd >= len(tokens) || tokens[listEnd].kind != relationOther || query[tokens[listEnd].start] != ',' {
				break
			}
			nameStart = listEnd + 1
		}
	}
	if !changed {
		return query, false
	}
	out.WriteString(query[last:])
	return out.String(), true
}

type relationTokenKind uint8

const (
	relationWord relationTokenKind = iota
	relationIdentifier
	relationDot
	relationOther
)

type relationToken struct {
	kind       relationTokenKind
	value      string
	start, end int
}

func isRelationKeyword(token relationToken) bool {
	if token.kind != relationWord {
		return false
	}
	switch token.value {
	case "from", "join", "using", "update", "into":
		return true
	default:
		return false
	}
}

func isRelationNameToken(token relationToken) bool {
	if token.kind == relationIdentifier {
		return true
	}
	if token.kind != relationWord {
		return false
	}
	// An unquoted word is an identifier unless it is a SQL keyword that can
	// begin the next clause. Quoted keywords are relationIdentifier tokens and
	// remain valid table names.
	switch token.value {
	case "select", "with", "where", "on", "join", "left", "right", "full", "inner", "cross", "group", "order", "limit", "having", "union", "returning", "set", "values":
		return false
	default:
		return true
	}
}

func lexSQLForRelations(query string) []relationToken {
	var tokens []relationToken
	for i := 0; i < len(query); {
		if isSQLSpace(query[i]) {
			i++
			continue
		}
		start := i
		switch query[i] {
		case '\'', '"', '`':
			quote := query[i]
			i++
			for i < len(query) {
				if query[i] == '\\' && quote != '`' && i+1 < len(query) {
					i += 2
					continue
				}
				if query[i] == quote {
					i++
					if i < len(query) && query[i] == quote {
						i++
						continue
					}
					break
				}
				i++
			}
			if quote == '"' || quote == '`' {
				value := query[start+1 : i-1]
				value = strings.ReplaceAll(value, string(quote)+string(quote), string(quote))
				tokens = append(tokens, relationToken{kind: relationIdentifier, value: value, start: start, end: i})
			} else {
				tokens = append(tokens, relationToken{kind: relationOther, start: start, end: i})
			}
		case '-', '#':
			if query[i] == '#' || (i+1 < len(query) && query[i+1] == '-') {
				for i < len(query) && query[i] != '\n' {
					i++
				}
				continue
			}
			i++
		case '/':
			if i+1 < len(query) && query[i+1] == '*' {
				i += 2
				for i+1 < len(query) && !(query[i] == '*' && query[i+1] == '/') {
					i++
				}
				if i+1 < len(query) {
					i += 2
				}
				continue
			}
			fallthrough
		case '.':
			i++
			kind := relationOther
			if query[start] == '.' {
				kind = relationDot
			}
			tokens = append(tokens, relationToken{kind: kind, start: start, end: i})
		default:
			if isSQLIdentStart(query[i]) {
				i++
				for i < len(query) && isSQLIdentPart(query[i]) {
					i++
				}
				value := strings.ToLower(query[start:i])
				tokens = append(tokens, relationToken{kind: relationWord, value: value, start: start, end: i})
				continue
			}
			i++
			tokens = append(tokens, relationToken{kind: relationOther, start: start, end: i})
		}
	}
	return tokens
}

func isSQLSpace(c byte) bool { return unicode.IsSpace(rune(c)) }

func isSQLIdentStart(c byte) bool {
	return c == '_' || c == '$' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func isSQLIdentPart(c byte) bool {
	return isSQLIdentStart(c) || c >= '0' && c <= '9'
}
