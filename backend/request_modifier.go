package backend

import "strings"

// RequestModifier is a function type that transforms a query string
type RequestModifier func(string, *[]ResultModifier) string

// default request modifier list
var defaultRequestModifiers = []RequestModifier{
	replaceMariaDBCollation,
}

// Newer MariaDB versions use utf8mb4_uca1400_ai_ci as the default collation,
// which is not supported by go-mysql-server.
// This function replaces the collation with the MySQL default utf8mb4_0900_ai_ci.
func replaceMariaDBCollation(query string, _ *[]ResultModifier) string {
	return strings.ReplaceAll(query, "utf8mb4_uca1400_ai_ci", "utf8mb4_0900_ai_ci")
}

// applyRequestModifiers applies request modifiers to a query
func applyRequestModifiers(query string, requestModifiers []RequestModifier) (string, []ResultModifier) {
	resultModifiers := make([]ResultModifier, 0)
	for _, modifier := range requestModifiers {
		query = modifier(query, &resultModifiers)
	}
	return query, resultModifiers
}
