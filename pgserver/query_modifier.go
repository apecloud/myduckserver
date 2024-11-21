package pgserver

import "regexp"

type QueryModifier func(string) string

func NewQueryReplacer(regex string, replacement string) QueryModifier {
	r := regexp.MustCompile(regex)
	return func(query string) string {
		return r.ReplaceAllString(query, replacement)
	}

}

func NewQueryRemover(regex string) QueryModifier {
	return NewQueryReplacer(regex, "")
}

var (
	removeLocaleProvider = NewQueryRemover(`(?i)LOCALE_PROVIDER = [^ ;]*`)
	removeLocale         = NewQueryRemover(`(?i)LOCALE = [^ ;]*`)
)
