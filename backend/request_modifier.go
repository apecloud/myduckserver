package backend

import (
	"strings"
	"unicode"
)

// RequestModifier is a function type that transforms a query string
type RequestModifier func(string, *[]ResultModifier) string

// skipUnsupportedDDL is a no-op that the parser accepts.
// Used for CREATE/DROP/ALTER FUNCTION|PROCEDURE|TRIGGER|EVENT.
const skipUnsupportedDDL = "SELECT 1"

// default request modifier list
var defaultRequestModifiers = []RequestModifier{
	skipUnsupportedRoutineDDL,
	unwrapCreateViewParens,
	replaceMariaDBCollation,
}

// Newer MariaDB versions use utf8mb4_uca1400_ai_ci as the default collation,
// which is not supported by go-mysql-server.
// This function replaces the collation with the MySQL default utf8mb4_0900_ai_ci.
func replaceMariaDBCollation(query string, _ *[]ResultModifier) string {
	return strings.ReplaceAll(query, "utf8mb4_uca1400_ai_ci", "utf8mb4_0900_ai_ci")
}

// skipUnsupportedRoutineDDL turns stored routines, triggers, and events
// into a no-op. DuckDB cannot host them; replica snapshot dumps still emit
// /*!50003 DROP FUNCTION ... */ and abort the whole schema if we parse-fail.
func skipUnsupportedRoutineDDL(query string, _ *[]ResultModifier) string {
	if isUnsupportedRoutineDDL(query) {
		return skipUnsupportedDDL
	}
	return query
}

func isUnsupportedRoutineDDL(query string) bool {
	q := unwrapVersionComment(query)
	q = strings.TrimSpace(q)
	if q == "" {
		return false
	}

	upperStart := strings.ToUpper(q)
	if !hasWordPrefix(upperStart, "CREATE") && !hasWordPrefix(upperStart, "ALTER") && !hasWordPrefix(upperStart, "DROP") {
		return false
	}

	// Stop at the first '(' so CREATE TABLE t (function INT) is not treated as a routine.
	if idx := indexUnquoted(q, '('); idx >= 0 {
		q = q[:idx]
	}

	words := strings.FieldsFunc(q, func(r rune) bool {
		return unicode.IsSpace(r) || r == '`' || r == '"' || r == '\'' || r == '=' || r == '.' || r == '@'
	})
	if len(words) < 2 {
		return false
	}

	for _, w := range words[1:] {
		switch strings.ToUpper(w) {
		case "FUNCTION", "PROCEDURE", "TRIGGER", "EVENT":
			return true
		case "TABLE", "VIEW", "INDEX", "DATABASE", "SCHEMA", "USER", "ROLE", "SERVER":
			return false
		}
	}
	return false
}

// unwrapVersionComment turns /*!50003 DROP FUNCTION IF EXISTS `toDate` */
// into DROP FUNCTION IF EXISTS `toDate`.
func unwrapVersionComment(query string) string {
	q := strings.TrimSpace(query)
	if !strings.HasPrefix(q, "/*!") {
		return q
	}
	i := 3
	for i < len(q) && q[i] >= '0' && q[i] <= '9' {
		i++
	}
	body := strings.TrimSpace(q[i:])
	body = strings.TrimSuffix(body, "*/")
	return strings.TrimSpace(body)
}

func hasWordPrefix(upper, word string) bool {
	if !strings.HasPrefix(upper, word) {
		return false
	}
	if len(upper) == len(word) {
		return true
	}
	return !isIdentChar(rune(upper[len(word)]))
}

func isIdentChar(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}

func indexUnquoted(s string, target byte) int {
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == '\\' && quote != '`' {
				i++
				continue
			}
			if c == quote {
				quote = 0
			}
			continue
		}
		if c == '\'' || c == '"' || c == '`' {
			quote = c
			continue
		}
		if c == target {
			return i
		}
	}
	return -1
}

// unwrapCreateViewParens rewrites
//
//	CREATE ... VIEW name AS ( SELECT ... )
//
// to
//
//	CREATE ... VIEW name AS SELECT ...
//
// go-mysql-server rejects the parenthesized form (issue #329).
func unwrapCreateViewParens(query string, _ *[]ResultModifier) string {
	asEnd, bodyStart, ok := findCreateViewBody(query)
	if !ok {
		return query
	}
	i := skipSpace(query, bodyStart)
	if i >= len(query) || query[i] != '(' {
		return query
	}
	closeIdx, ok := matchingParen(query, i)
	if !ok {
		return query
	}
	inner := strings.TrimSpace(query[i+1 : closeIdx])
	return query[:asEnd] + " " + inner + query[closeIdx+1:]
}

func findCreateViewBody(query string) (asEnd, bodyStart int, ok bool) {
	i := skipSpace(query, 0)
	if !matchKeywordAt(query, i, "CREATE") {
		return 0, 0, false
	}
	i = skipToken(query, i)

	foundView := false
	for i < len(query) {
		i = skipSpace(query, i)
		if i >= len(query) {
			return 0, 0, false
		}
		if matchKeywordAt(query, i, "VIEW") {
			foundView = true
			i = skipToken(query, i)
			break
		}
		if matchKeywordAt(query, i, "TABLE") ||
			matchKeywordAt(query, i, "INDEX") ||
			matchKeywordAt(query, i, "FUNCTION") ||
			matchKeywordAt(query, i, "PROCEDURE") ||
			matchKeywordAt(query, i, "TRIGGER") ||
			matchKeywordAt(query, i, "EVENT") ||
			matchKeywordAt(query, i, "DATABASE") ||
			matchKeywordAt(query, i, "SCHEMA") {
			return 0, 0, false
		}
		i = skipToken(query, i)
	}
	if !foundView {
		return 0, 0, false
	}

	i = skipSpace(query, i)
	i = skipQualifiedName(query, i)
	i = skipSpace(query, i)
	if i < len(query) && query[i] == '(' {
		end, matched := matchingParen(query, i)
		if !matched {
			return 0, 0, false
		}
		i = end + 1
		i = skipSpace(query, i)
	}
	if !matchKeywordAt(query, i, "AS") {
		return 0, 0, false
	}
	asEnd = i + 2
	return asEnd, skipSpace(query, asEnd), true
}

func skipSpace(s string, i int) int {
	for i < len(s) && unicode.IsSpace(rune(s[i])) {
		i++
	}
	return i
}

func matchKeywordAt(s string, i int, word string) bool {
	if i+len(word) > len(s) {
		return false
	}
	if !strings.EqualFold(s[i:i+len(word)], word) {
		return false
	}
	if i+len(word) < len(s) && isIdentChar(rune(s[i+len(word)])) {
		return false
	}
	return true
}

func skipToken(s string, i int) int {
	i = skipSpace(s, i)
	if i >= len(s) {
		return i
	}
	if s[i] == '`' || s[i] == '"' || s[i] == '\'' {
		q := s[i]
		i++
		for i < len(s) {
			if s[i] == q {
				return i + 1
			}
			i++
		}
		return i
	}
	if isIdentChar(rune(s[i])) {
		i++
		for i < len(s) && isIdentChar(rune(s[i])) {
			i++
		}
		return i
	}
	return i + 1
}

func skipQualifiedName(s string, i int) int {
	i = skipToken(s, i)
	i = skipSpace(s, i)
	if i < len(s) && s[i] == '.' {
		i++
		i = skipSpace(s, i)
		i = skipToken(s, i)
	}
	return i
}

func matchingParen(s string, open int) (int, bool) {
	if open >= len(s) || s[open] != '(' {
		return -1, false
	}
	depth := 0
	var quote byte
	for i := open; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == '\\' && quote != '`' {
				i++
				continue
			}
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i, true
			}
		}
	}
	return -1, false
}

// RewriteIncomingQuery applies the default request modifiers.
// Tests and protocol handlers use this so replica-dump DDL is rewritten
// the same way as live MySQL connections.
func RewriteIncomingQuery(query string) string {
	rewritten, _ := applyRequestModifiers(query, defaultRequestModifiers)
	return rewritten
}

func isCreateViewStmt(query string) bool {
	_, _, ok := findCreateViewBody(query)
	return ok
}

func shouldIgnoreFailedView(query string, snapshot bool) bool {
	return snapshot && isCreateViewStmt(query)
}

// applyRequestModifiers applies request modifiers to a query
func applyRequestModifiers(query string, requestModifiers []RequestModifier) (string, []ResultModifier) {
	resultModifiers := make([]ResultModifier, 0)
	for _, modifier := range requestModifiers {
		query = modifier(query, &resultModifiers)
	}
	return query, resultModifiers
}
