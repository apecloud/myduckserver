package pgserver

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
)

const (
	CopyFormatParquet = tree.CopyFormatCSV + 1
	CopyFormatJSON    = tree.CopyFormatCSV + 2
	CopyFormatArrow   = tree.CopyFormatCSV + 3
)

var (
	// We are supporting the parquet|json|arrow formats for COPY TO, but
	// COPY ... TO STDOUT [WITH] (FORMAT PARQUET, OPT1 v1, OPT2, OPT3 v3, ...)
	// cannot be parsed by the PG parser.
	// Let's match them with regex and extract the ... part.
	reCopyToFormat = regexp.MustCompile(`(?i)^COPY\s+(.*?)\s+TO\s+STDOUT(?:\s+(?:WITH\s*)?\(\s*(?:FORMAT\s+(\w+)\s*,?\s*)?(.*?)\s*\))?$`)
	// Also for COPY ... FROM STDIN [WITH] (FORMAT PARQUET, OPT1 v1, OPT2, OPT3 v3, ...)
	reCopyFromFormat = regexp.MustCompile(`(?i)^COPY\s+(.*?)\s+FROM\s+STDIN(?:\s+(?:WITH\s*)?\(\s*(?:FORMAT\s+(\w+)\s*,?\s*)?(.*?)\s*\))?$`)
)

func ParseFormat(s string) (format tree.CopyFormat, ok bool) {
	switch strings.ToUpper(s) {
	case "PARQUET":
		format = CopyFormatParquet
	case "JSON":
		format = CopyFormatJSON
	case "ARROW":
		format = CopyFormatArrow
	case "CSV":
		format = tree.CopyFormatCSV
	case "BINARY":
		format = tree.CopyFormatBinary
	case "", "TEXT":
		format = tree.CopyFormatText
	default:
		return 0, false
	}
	return format, true
}

// ParseCopyTo parses a COPY TO statement and returns the query, format, and options.
func ParseCopyTo(stmt string) (query string, format tree.CopyFormat, options string, ok bool) {
	stmt = RemoveComments(stmt)
	stmt = sql.RemoveSpaceAndDelimiter(stmt, ';')
	m := reCopyToFormat.FindStringSubmatch(stmt)
	if m == nil {
		return "", 0, "", false
	}
	query = strings.TrimSpace(m[1])
	format, ok = ParseFormat(strings.TrimSpace(m[2]))
	options = strings.TrimSpace(m[3])
	return
}

// ParseCopyFrom parses a COPY FROM statement and returns the query, format, and options.
func ParseCopyFrom(stmt string) (target string, format tree.CopyFormat, options string, ok bool) {
	stmt = RemoveComments(stmt)
	stmt = sql.RemoveSpaceAndDelimiter(stmt, ';')
	m := reCopyFromFormat.FindStringSubmatch(stmt)
	if m == nil {
		return "", 0, "", false
	}
	target = strings.TrimSpace(m[1])
	format, ok = ParseFormat(strings.TrimSpace(m[2]))
	options = strings.TrimSpace(m[3])
	return
}

type OptionValueType uint8

const (
	OptionValueTypeBool   OptionValueType = iota // bool
	OptionValueTypeInt                           // int
	OptionValueTypeFloat                         // float64
	OptionValueTypeString                        // string
)

// ParseCopyOptions parses the options string and returns the CopyOptions.
// The options string is a comma-separated list of key-value pairs: `OPT1 1, OPT2, OPT3 'v3', OPT4 E'v4', ...`.
// The allowed map specifies the allowed options and their types. Its keys are the option names in uppercase.
func ParseCopyOptions(options string, allowed map[string]OptionValueType) (result map[string]any, err error) {
	result = make(map[string]any)
	var key, value string
	inQuotes := false
	expectComma := false
	readingKey := true
	var sb strings.Builder

	parseOption := func() error {
		k := strings.TrimSpace(key)
		if k == "" {
			return nil
		}
		k = strings.ToUpper(k)
		if _, ok := allowed[k]; !ok {
			return fmt.Errorf("unsupported option: %s", k)
		}
		v := strings.TrimSpace(value)

		switch allowed[k] {
		case OptionValueTypeBool:
			if v == "" {
				result[k] = true
			} else {
				val, err := strconv.ParseBool(v)
				if err != nil {
					return fmt.Errorf("invalid bool value for %s: %v", k, err)
				}
				result[k] = val
			}
		case OptionValueTypeInt:
			val, err := strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("invalid int value for %s: %v", k, err)
			}
			result[k] = val
		case OptionValueTypeFloat:
			val, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return fmt.Errorf("invalid float value for %s: %v", k, err)
			}
			result[k] = val
		case OptionValueTypeString:
			if strings.HasPrefix(v, `E'`) && strings.HasSuffix(v, `'`) {
				// Remove the 'E' prefix and unescape the value
				unquoted, err := strconv.Unquote(`"` + v[2:len(v)-1] + `"`)
				if err != nil {
					return fmt.Errorf("invalid escaped string value for %s: %v", k, err)
				}
				v = unquoted
			} else if strings.HasPrefix(v, "'") && strings.HasSuffix(v, "'") {
				// Trim the single quotes
				v = v[1 : len(v)-1]
				// Replace double single quotes with a single quote
				v = strings.ReplaceAll(v, "''", "'")
			} else {
				return fmt.Errorf("invalid string value for %s: %q", k, v)
			}
			result[k] = v
		}
		key, value = "", ""
		readingKey = true
		return nil
	}

	for i, c := range options {
		if expectComma && c != ',' && !unicode.IsSpace(c) {
			return nil, fmt.Errorf("expected comma before %q", options[i:])
		}
		switch c {
		case '\'':
			inQuotes = !inQuotes
			sb.WriteRune(c)
		case ',':
			if !inQuotes {
				if readingKey {
					key = sb.String()
				} else {
					value = sb.String()
				}
				if err := parseOption(); err != nil {
					return nil, err
				}
				expectComma = false
				sb.Reset()
			} else {
				sb.WriteRune(c)
			}
		default:
			if unicode.IsSpace(c) {
				if !inQuotes {
					if sb.Len() > 0 {
						if readingKey {
							key = sb.String()
							sb.Reset()
							readingKey = false
						} else {
							expectComma = true
						}
					}
				} else {
					sb.WriteRune(c)
				}
			} else {
				sb.WriteRune(c)
			}
		}
	}

	if sb.Len() > 0 {
		if readingKey {
			key = sb.String()
		} else {
			value = sb.String()
		}
		if err := parseOption(); err != nil {
			return nil, err
		}
	}

	return result, nil
}
