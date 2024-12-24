package backend

// RequestModifier is a function type that transforms a query string
type RequestModifier func(string, *[]ResultModifier) string

// default request modifier list
var defaultRequestModifiers = []RequestModifier{}

// applyRequestModifiers applies request modifiers to a query
func applyRequestModifiers(query string, requestModifiers []RequestModifier) (string, []ResultModifier) {
	resultModifiers := make([]ResultModifier, 0)
	for _, modifier := range requestModifiers {
		query = modifier(query, &resultModifiers)
	}
	return query, resultModifiers
}
