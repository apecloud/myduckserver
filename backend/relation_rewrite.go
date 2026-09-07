package backend

import (
	"sort"
	"strings"
	"unicode"
)

// RewriteSQLRelations replaces table references in relation positions. It is
// intentionally a small lexer rather than strings.Replace: quoted literals,
// dollar-quoted bodies, and comments may contain table-looking text and must
// remain untouched.
//
// Routes are keyed by an unquoted, case-insensitive relation name. A
// qualified key (for example, "app.orders") takes precedence over a bare
// table key. Values are emitted verbatim and are expected to be trusted,
// already-quoted DuckDB identifiers supplied by the catalog layer.
func RewriteSQLRelations(query string, routes map[string]string) (string, bool) {
	if query == "" || len(routes) == 0 {
		return query, false
	}
	tokens := lexSQLForRelations(query)
	if len(tokens) == 0 {
		return query, false
	}
	spans := relationSpans(tokens)
	if len(spans) == 0 {
		return query, false
	}

	// A statement can expose the same relation through more than one context
	// (for example an INSERT ... SELECT). Keep spans ordered and reject an
	// accidental overlap before constructing the output.
	sort.SliceStable(spans, func(i, j int) bool { return spans[i].start < spans[j].start })
	var out strings.Builder
	out.Grow(len(query))
	last := 0
	changed := false
	for _, span := range spans {
		if span.start < last || span.start < 0 || span.end > len(query) || span.end <= span.start {
			continue
		}
		key := strings.ToLower(strings.Join(span.parts, "."))
		replacement, ok := routes[key]
		if !ok && len(span.parts) > 1 {
			replacement, ok = routes[strings.ToLower(span.parts[len(span.parts)-1])]
		}
		if !ok || replacement == "" {
			continue
		}
		out.WriteString(query[last:span.start])
		out.WriteString(replacement)
		last = span.end
		changed = true
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

type relationSpan struct {
	start, end int
	parts      []string
}

func relationSpans(tokens []relationToken) []relationSpan {
	var spans []relationSpan
	for i := 0; i < len(tokens); i++ {
		token := tokens[i]
		if token.kind != relationWord {
			continue
		}
		switch token.value {
		case "from", "join", "using", "update", "into":
			span, next := relationList(tokens, i+1, true)
			spans = append(spans, span...)
			if next > i+1 {
				i = next - 1
			}
		case "truncate":
			j := i + 1
			j = skipWords(tokens, j, "table", "only")
			span, next := relationList(tokens, j, false)
			spans = append(spans, span...)
			if next > j {
				i = next - 1
			}
		case "drop":
			j := i + 1
			if !wordAt(tokens, j, "table") {
				continue
			}
			j++
			j = skipWords(tokens, j, "if", "exists", "only")
			span, next := relationList(tokens, j, false)
			spans = append(spans, span...)
			if next > j {
				i = next - 1
			}
		case "alter":
			j := i + 1
			if !wordAt(tokens, j, "table") {
				continue
			}
			j++
			j = skipWords(tokens, j, "if", "exists", "only")
			span, _ := relationList(tokens, j, false)
			if len(span) > 0 {
				spans = append(spans, span[0])
			}
		case "copy":
			// COPY (SELECT ...) ... is a query source, not a table target.
			j := i + 1
			if tokenAt(tokens, j).kind == relationOther && tokenAt(tokens, j).value == "(" {
				continue
			}
			span, _ := relationList(tokens, j, false)
			if len(span) > 0 {
				spans = append(spans, span[0])
			}
		case "create":
			// CREATE INDEX ... ON table (...). Restrict this to INDEX so an
			// ON clause in another CREATE statement is not mistaken for a
			// relation target.
			if !wordAt(tokens, i+1, "unique") && !wordAt(tokens, i+1, "index") {
				continue
			}
			j := i + 1
			if wordAt(tokens, j, "unique") {
				j++
			}
			for j < len(tokens) && !(tokens[j].kind == relationWord && tokens[j].value == "on") {
				j++
			}
			if j < len(tokens) {
				span, _ := relationList(tokens, j+1, false)
				if len(span) > 0 {
					spans = append(spans, span[0])
				}
			}
		}
	}
	return spans
}

func relationList(tokens []relationToken, start int, aliases bool) ([]relationSpan, int) {
	var spans []relationSpan
	i := start
	for {
		i = skipWords(tokens, i, "only", "lateral")
		nameStart := i
		if !isRelationNameToken(tokenAt(tokens, i)) {
			break
		}
		parts := []string{tokens[i].value}
		nameEnd := i + 1
		for nameEnd+1 < len(tokens) && tokens[nameEnd].kind == relationDot && isRelationNameToken(tokens[nameEnd+1]) {
			parts = append(parts, tokens[nameEnd+1].value)
			nameEnd += 2
		}
		spans = append(spans, relationSpan{
			start: tokens[nameStart].start,
			end:   tokens[nameEnd-1].end,
			parts: parts,
		})

		// Skip an optional alias, but leave it outside the replacement span.
		listEnd := nameEnd
		if aliases && wordAt(tokens, listEnd, "as") {
			if isRelationNameToken(tokenAt(tokens, listEnd+1)) {
				listEnd += 2
			}
		} else if aliases && isRelationNameToken(tokenAt(tokens, listEnd)) {
			listEnd++
		}
		if !commaAt(tokens, listEnd) {
			return spans, listEnd
		}
		i = listEnd + 1
	}
	return spans, i
}

func skipWords(tokens []relationToken, i int, words ...string) int {
	for _, word := range words {
		if wordAt(tokens, i, word) {
			i++
		}
	}
	return i
}

func tokenAt(tokens []relationToken, i int) relationToken {
	if i < 0 || i >= len(tokens) {
		return relationToken{kind: relationOther}
	}
	return tokens[i]
}

func wordAt(tokens []relationToken, i int, value string) bool {
	token := tokenAt(tokens, i)
	return token.kind == relationWord && token.value == value
}

func commaAt(tokens []relationToken, i int) bool {
	token := tokenAt(tokens, i)
	return token.kind == relationOther && token.value == ","
}

func isRelationNameToken(token relationToken) bool {
	if token.kind == relationIdentifier {
		return true
	}
	if token.kind != relationWord {
		return false
	}
	// An unquoted word is an identifier unless it begins the next clause.
	// Quoted keywords are relationIdentifier tokens and remain valid names.
	switch token.value {
	case "select", "with", "where", "on", "join", "left", "right", "full", "inner", "cross", "natural", "group", "order", "limit", "having", "union", "returning", "set", "values", "from", "using", "into", "table", "if", "exists", "only", "as", "cascade", "restrict", "add", "drop", "alter", "rename", "column", "to":
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
		if query[i] == '$' {
			if end, ok := dollarQuoteEnd(query, i); ok {
				i = end
				tokens = append(tokens, relationToken{kind: relationOther, start: start, end: i})
				continue
			}
		}
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
				end := i
				if end > start+1 && query[end-1] == quote {
					end--
				}
				value := strings.ReplaceAll(query[start+1:end], string(quote)+string(quote), string(quote))
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
				depth := 1
				for i < len(query) && depth > 0 {
					if i+1 < len(query) && query[i] == '/' && query[i+1] == '*' {
						depth++
						i += 2
						continue
					}
					if i+1 < len(query) && query[i] == '*' && query[i+1] == '/' {
						depth--
						i += 2
						continue
					}
					i++
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
			value := ""
			if kind == relationOther {
				value = query[start:i]
			}
			tokens = append(tokens, relationToken{kind: kind, value: value, start: start, end: i})
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
			value := query[start:i]
			tokens = append(tokens, relationToken{kind: relationOther, value: value, start: start, end: i})
		}
	}
	return tokens
}

func dollarQuoteEnd(query string, start int) (int, bool) {
	if start >= len(query) || query[start] != '$' {
		return 0, false
	}
	i := start + 1
	if i < len(query) && query[i] == '$' {
		i++
	} else {
		if i >= len(query) || !(query[i] == '_' || query[i] >= 'a' && query[i] <= 'z' || query[i] >= 'A' && query[i] <= 'Z') {
			return 0, false
		}
		i++
		for i < len(query) && (query[i] == '_' || query[i] >= 'a' && query[i] <= 'z' || query[i] >= 'A' && query[i] <= 'Z' || query[i] >= '0' && query[i] <= '9') {
			i++
		}
		if i >= len(query) || query[i] != '$' {
			return 0, false
		}
		i++
	}
	tag := query[start:i]
	if end := strings.Index(query[i:], tag); end >= 0 {
		return i + end + len(tag), true
	}
	// An unterminated dollar quote is treated as a non-relation token so
	// identifiers inside it cannot be rewritten accidentally.
	return len(query), true
}

func isSQLSpace(c byte) bool { return unicode.IsSpace(rune(c)) }

func isSQLIdentStart(c byte) bool {
	return c == '_' || c == '$' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func isSQLIdentPart(c byte) bool {
	return isSQLIdentStart(c) || c >= '0' && c <= '9'
}
