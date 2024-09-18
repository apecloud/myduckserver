package transpiler

import (
	"strings"
)

func ConvertQuery(query string) []string {
	query = NormalizeStrings(query)
	return []string{query}
}

// little state machine for turning MySQL quote characters into their postgres equivalents:
/*
               ┌───────────────────*─────────────────────────┐
               │                   ┌─*─┐                     *
               │               ┌───┴───▼──────┐         ┌────┴─────────┐
               │     ┌────"───►│ In double    │◄───"────┤End double    │
               │     │         │ quoted string│────"───►│quoted string?│
               │     │         └──────────────┘         └──────────────┘
               ├─────(──────────────────*───────────────────┐
      ┌─*──┐   ▼     │                                      *
      │    ├─────────┴┐            ┌─*─┐                    │
      └───►│ Not in   │        ┌───┴───▼─────┐          ┌───┴──────────┐
           │ string   ├───'───►│In single    │◄────'────┤End single    │
  ────────►└─────────┬┘        │quoted string│─────'───►│quoted string?│
  START        ▲     │         └─────────────┘          └──────────────┘
               └─────(──────────────────*───────────────────┐
                     │            ┌─*──┐                    *
                     │        ┌───┴────▼────┐           ┌───┴──────────┐
                     └───`───►│In backtick  │◄─────`────┤End backtick  │
                              │quoted string│──────`───►│quoted string?│
                              └─────────────┘           └──────────────┘
*/
type stringParserState byte

const (
	notInString stringParserState = iota
	inDoubleQuote
	maybeEndDoubleQuote
	inSingleQuote
	maybeEndSingleQuote
	inBackticks
	maybeEndBackticks
)

const singleQuote = '\''
const doubleQuote = '"'
const backtick = '`'
const backslash = '\\'

// NormalizeStrings normalizes a query string to convert any MySQL syntax to Postgres syntax
func NormalizeStrings(q string) string {
	state := notInString
	lastCharWasBackslash := false
	normalized := strings.Builder{}

	for _, c := range q {
		switch state {
		case notInString:
			switch c {
			case singleQuote:
				state = inSingleQuote
				normalized.WriteRune(singleQuote)
			case doubleQuote:
				state = inDoubleQuote
				normalized.WriteRune(singleQuote)
			case backtick:
				state = inBackticks
				normalized.WriteRune(doubleQuote)
			default:
				normalized.WriteRune(c)
			}
		case inDoubleQuote:
			switch c {
			case backslash:
				if lastCharWasBackslash {
					normalized.WriteRune(c)
				}
				lastCharWasBackslash = !lastCharWasBackslash
			case doubleQuote:
				if lastCharWasBackslash {
					normalized.WriteRune(c)
					lastCharWasBackslash = false
				} else {
					state = maybeEndDoubleQuote
				}
			case singleQuote:
				normalized.WriteRune(singleQuote)
				normalized.WriteRune(singleQuote)
				lastCharWasBackslash = false
			default:
				lastCharWasBackslash = false
				normalized.WriteRune(c)
			}
		case maybeEndDoubleQuote:
			switch c {
			case doubleQuote:
				state = inDoubleQuote
				normalized.WriteRune(doubleQuote)
			default:
				state = notInString
				normalized.WriteRune(singleQuote)
				normalized.WriteRune(c)
			}
		case inSingleQuote:
			switch c {
			case backslash:
				if lastCharWasBackslash {
					normalized.WriteRune(c)
				}
				lastCharWasBackslash = !lastCharWasBackslash
			case singleQuote:
				if lastCharWasBackslash {
					normalized.WriteRune(c)
					normalized.WriteRune(c)
					lastCharWasBackslash = false
				} else {
					state = maybeEndSingleQuote
				}
			default:
				lastCharWasBackslash = false
				normalized.WriteRune(c)
			}
		case maybeEndSingleQuote:
			switch c {
			case singleQuote:
				state = inSingleQuote
				normalized.WriteRune(singleQuote)
				normalized.WriteRune(singleQuote)
			default:
				state = notInString
				normalized.WriteRune(singleQuote)
				normalized.WriteRune(c)
			}
		case inBackticks:
			switch c {
			case backtick:
				state = maybeEndBackticks
			default:
				normalized.WriteRune(c)
			}
		case maybeEndBackticks:
			switch c {
			case backtick:
				state = inBackticks
				normalized.WriteRune(backtick)
			default:
				state = notInString
				normalized.WriteRune(doubleQuote)
				normalized.WriteRune(c)
			}
		default:
			panic("unknown state")
		}
	}

	// If reached the end of input unsure whether to unquote a string, do so now
	switch state {
	case maybeEndDoubleQuote:
		normalized.WriteRune(singleQuote)
	case maybeEndSingleQuote:
		normalized.WriteRune(singleQuote)
	case maybeEndBackticks:
		normalized.WriteRune(doubleQuote)
	default: // do nothing
	}

	return normalized.String()
}

// normalizes a query string to convert any Postgres syntax to MySQL syntax
func DenormalizeStrings(q string) string {
	state := notInString
	lastCharWasBackslash := false
	denormalized := strings.Builder{}

	for _, c := range q {
		switch state {
		case notInString:
			switch c {
			case singleQuote:
				state = inSingleQuote
				denormalized.WriteRune(singleQuote)
			case doubleQuote:
				state = inDoubleQuote
				denormalized.WriteRune(backtick)
			default:
				denormalized.WriteRune(c)
			}
		case inSingleQuote:
			switch c {
			case backslash:
				if lastCharWasBackslash {
					denormalized.WriteRune(c)
				} else {
					lastCharWasBackslash = !lastCharWasBackslash
				}
			case singleQuote:
				if lastCharWasBackslash {
					denormalized.WriteRune(c)
					lastCharWasBackslash = false
				} else {
					state = maybeEndSingleQuote
				}
			default:
				lastCharWasBackslash = false
				denormalized.WriteRune(c)
			}
		case maybeEndSingleQuote:
			switch c {
			case singleQuote:
				state = inSingleQuote
				denormalized.WriteRune(singleQuote)
				denormalized.WriteRune(singleQuote)
			default:
				state = notInString
				denormalized.WriteRune(singleQuote)
				denormalized.WriteRune(c)
			}
		case inDoubleQuote:
			switch c {
			case backslash:
				if lastCharWasBackslash {
					denormalized.WriteRune(c)
				} else {
					lastCharWasBackslash = !lastCharWasBackslash
				}
			case doubleQuote:
				if lastCharWasBackslash {
					denormalized.WriteRune(c)
					lastCharWasBackslash = !lastCharWasBackslash
				} else {
					state = maybeEndDoubleQuote
				}
			case backtick:
				denormalized.WriteRune(backtick)
				denormalized.WriteRune(backtick)
			default:
				lastCharWasBackslash = false
				denormalized.WriteRune(c)
			}
		case maybeEndDoubleQuote:
			switch c {
			case doubleQuote:
				state = inDoubleQuote
				denormalized.WriteRune(doubleQuote)
			default:
				state = notInString
				denormalized.WriteRune(backtick)
				denormalized.WriteRune(c)
			}
		default:
			panic("unknown state")
		}
	}
	switch state {
	case maybeEndSingleQuote:
		denormalized.WriteRune(singleQuote)
	case maybeEndDoubleQuote:
		denormalized.WriteRune(backtick)
	default: // do nothing

	}
	return denormalized.String()
}
