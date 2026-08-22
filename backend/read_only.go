package backend

import (
	"strings"
	"unicode"
)

// IsWriteQueryText classifies statements that could not be parsed by the
// protocol-specific engine. Parsed statements should use their AST instead.
func IsWriteQueryText(query string) bool {
	keyword := firstQueryKeyword(query)
	switch keyword {
	case "INSERT", "UPDATE", "DELETE", "REPLACE", "MERGE",
		"CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME",
		"GRANT", "REVOKE", "CALL", "LOAD", "IMPORT":
		return true
	default:
		return false
	}
}

func firstQueryKeyword(query string) string {
	for {
		query = strings.TrimLeftFunc(query, unicode.IsSpace)
		switch {
		case strings.HasPrefix(query, "/*!"):
			end := strings.Index(query[3:], "*/")
			if end < 0 {
				return ""
			}
			body := query[3 : 3+end]
			body = strings.TrimLeftFunc(body, unicode.IsDigit)
			query = body
			continue
		case strings.HasPrefix(query, "--"):
			if newline := strings.IndexByte(query, '\n'); newline >= 0 {
				query = query[newline+1:]
				continue
			}
			return ""
		case strings.HasPrefix(query, "#"):
			if newline := strings.IndexByte(query, '\n'); newline >= 0 {
				query = query[newline+1:]
				continue
			}
			return ""
		case strings.HasPrefix(query, "/*"):
			if end := strings.Index(query[2:], "*/"); end >= 0 {
				query = query[end+4:]
				continue
			}
			return ""
		}
		break
	}

	end := 0
	for end < len(query) && (unicode.IsLetter(rune(query[end])) || query[end] == '_') {
		end++
	}
	return strings.ToUpper(query[:end])
}
