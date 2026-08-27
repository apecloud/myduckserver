package catalog

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"
)

// ErrSensitiveSQL is returned for user statements that would bypass the
// service-owned DuckLake/S3 credential boundary. It intentionally contains no
// statement text or credential-shaped data.
var ErrSensitiveSQL = errors.New("statement uses a service-managed SQL feature")

// RejectSensitiveSQL rejects service-control and credential-inspection SQL
// before protocol parsing, tracing, auditing, or prepared-statement handling.
// Internal service code does not call this function and can use parameterized
// APIs instead.
func RejectSensitiveSQL(query string) error {
	if IsSensitiveSQL(query) {
		return ErrSensitiveSQL
	}
	return nil
}

// IsSensitiveSQL reports whether query contains a DuckLake/S3 control or
// credential-inspection operation. The scanner is deliberately lexical: the
// MySQL and PostgreSQL parsers accept different SQL dialects, while comments,
// quoted identifiers, dollar-quoted strings, and multi-statements must all be
// handled consistently at the protocol boundary.
func IsSensitiveSQL(query string) bool {
	for _, statement := range splitSQLStatements(query) {
		if sensitiveStatement(statement) {
			return true
		}
	}
	return false
}

// RedactedSensitiveSQL is the stable audit value used when a service-managed
// statement must not be retained in logs.
const RedactedSensitiveSQL = "[redacted service-managed SQL]"

// RedactSensitiveSQL is a defensive logging helper for callers that receive a
// statement before they can reject it. Non-sensitive text is returned as-is;
// sensitive text is replaced wholesale so no literal can reach a log field.
func RedactSensitiveSQL(query string) string {
	if IsSensitiveSQL(query) || isLegacyObjectStorageSQL(query) {
		return RedactedSensitiveSQL
	}
	return query
}

type sqlToken struct {
	text   string
	quoted bool
	string bool
}

func splitSQLStatements(query string) [][]sqlToken {
	var statements [][]sqlToken
	current := make([]sqlToken, 0, 16)
	flush := func() {
		if len(current) > 0 {
			statements = append(statements, current)
			current = make([]sqlToken, 0, 16)
		}
	}

	for i := 0; i < len(query); {
		r, size := utf8.DecodeRuneInString(query[i:])
		if size == 0 {
			break
		}
		switch {
		case unicode.IsSpace(r):
			i += size
		case r == ';':
			flush()
			i++
		case r == '#' || isDashCommentStart(query, i):
			i = skipLineComment(query, i)
		case r == '/' && i+1 < len(query) && query[i+1] == '*':
			if i+2 < len(query) && query[i+2] == '!' {
				// MySQL executable comments are parsed and executed by the
				// server. Strip the optional version prefix and scan the embedded
				// SQL in the same statement context. This lets compatibility
				// clauses such as /*!32312 IF NOT EXISTS*/ pass while still
				// rejecting a sensitive statement hidden inside the comment.
				end := strings.Index(query[i+3:], "*/")
				if end < 0 {
					// An unterminated executable comment is malformed input. Keep
					// the conservative rejection used by the protocol boundary.
					current = append(current, sqlToken{text: "__executable_comment__"})
					i = len(query)
					continue
				}
				end += i + 3
				body := query[i+3 : end]
				body = stripMySQLExecutableVersion(body)
				embeddedStatements := splitSQLStatements(body)
				for n, embedded := range embeddedStatements {
					if sensitiveStatement(embedded) {
						// Keep a marker when the embedded statement itself is
						// service-managed. The surrounding statement may begin with
						// a harmless verb (for example SELECT), so checking only the
						// combined token stream would otherwise hide this operation.
						current = append(current, sqlToken{text: "__executable_comment__"})
					} else {
						current = append(current, embedded...)
					}
					if n+1 < len(embeddedStatements) {
						flush()
					}
				}
				i = end + 2
				continue
			}
			i = skipBlockComment(query, i)
		case r == '\'' || r == '"' || r == '`':
			tok, next := scanQuoted(query, i, byte(r))
			current = append(current, tok)
			i = next
		case r == '$':
			if tagEnd := dollarQuoteStart(query, i); tagEnd > i {
				tok, next := scanDollarQuoted(query, i, tagEnd)
				current = append(current, tok)
				i = next
			} else {
				current = append(current, sqlToken{text: "$"})
				i++
			}
		case isWordStartRune(r):
			start := i
			for i < len(query) {
				part, partSize := utf8.DecodeRuneInString(query[i:])
				if !isWordPartRune(part) {
					break
				}
				i += partSize
			}
			current = append(current, sqlToken{text: query[start:i]})
		default:
			// Keep punctuation as a token so SET (...) and function calls can
			// be recognized without attempting to parse dialect-specific SQL.
			current = append(current, sqlToken{text: query[i : i+size]})
			i += size
		}
	}
	flush()
	return statements
}

func skipLineComment(query string, start int) int {
	for start < len(query) && query[start] != '\n' && query[start] != '\r' {
		start++
	}
	return start
}

// isDashCommentStart follows the SQL rule that a double-dash comment starts
// only when the dashes are followed by whitespace or the end of the input.
// Treating every "--" pair as a comment can hide a later statement in input
// such as "SELECT 1--x; CREATE SECRET ...".
func isDashCommentStart(query string, start int) bool {
	if start+1 >= len(query) || query[start] != '-' || query[start+1] != '-' {
		return false
	}
	if start+2 >= len(query) {
		return true
	}
	r, _ := utf8.DecodeRuneInString(query[start+2:])
	return unicode.IsSpace(r)
}

func skipBlockComment(query string, start int) int {
	// Treat the first closing delimiter as the end of the comment. PostgreSQL
	// accepts nested block comments, but MySQL does not; recognizing nested
	// openers here could hide a sensitive statement after the first `*/` when
	// the same text is routed through both protocol parsers.
	for i := start + 2; i+1 < len(query); i++ {
		if query[i] == '*' && query[i+1] == '/' {
			return i + 2
		}
	}
	return len(query)
}

func stripMySQLExecutableVersion(body string) string {
	body = strings.TrimLeftFunc(body, unicode.IsSpace)
	if len(body) >= 5 {
		version := body[:5]
		for i := range version {
			if version[i] < '0' || version[i] > '9' {
				return body
			}
		}
		body = body[5:]
	}
	return body
}

func scanQuoted(query string, start int, quote byte) (sqlToken, int) {
	var b strings.Builder
	for i := start + 1; i < len(query); i++ {
		c := query[i]
		if c == quote {
			if i+1 < len(query) && query[i+1] == quote {
				b.WriteByte(quote)
				i++
				continue
			}
			return sqlToken{text: b.String(), quoted: quote != '\'', string: quote == '\''}, i + 1
		}
		b.WriteByte(c)
	}
	// Standard-conforming PostgreSQL strings treat a backslash as ordinary
	// text. Do not consume the following quote as a backslash escape here: the
	// same input is also accepted by MySQL, but swallowing that quote could hide
	// a second statement containing a service credential operation. The gate is
	// intentionally conservative at this dialect boundary.
	// Unterminated literals are still represented as a token; a conservative
	// rejection is preferable if they contain a service-control keyword.
	return sqlToken{text: b.String(), quoted: quote != '\'', string: quote == '\''}, len(query)
}

func dollarQuoteStart(query string, start int) int {
	for i := start + 1; i < len(query); i++ {
		if query[i] == '$' {
			if i == start+1 || validDollarTag(query[start+1:i]) {
				return i + 1
			}
			return 0
		}
		if !isDollarTagPart(query[i]) {
			return 0
		}
	}
	return 0
}

func validDollarTag(tag string) bool {
	for i := range tag {
		if i == 0 {
			if !(tag[i] == '_' || isASCIIAlpha(tag[i])) {
				return false
			}
		} else if !(tag[i] == '_' || isASCIIAlpha(tag[i]) || isASCIIDigit(tag[i])) {
			return false
		}
	}
	return true
}

func isDollarTagPart(c byte) bool {
	return c == '_' || isASCIIAlpha(c) || isASCIIDigit(c)
}

func scanDollarQuoted(query string, start, tagEnd int) (sqlToken, int) {
	tag := query[start:tagEnd]
	if end := strings.Index(query[tagEnd:], tag); end >= 0 {
		end += tagEnd
		return sqlToken{text: query[tagEnd:end], string: true}, end + len(tag)
	}
	return sqlToken{text: query[tagEnd:], string: true}, len(query)
}

func isWordStart(c byte) bool {
	return isASCIIAlpha(c) || c == '_' || c >= 0x80
}

func isWordPart(c byte) bool {
	return isWordStart(c) || isASCIIDigit(c) || c == '$'
}

func isWordStartRune(r rune) bool {
	return r == '_' || unicode.IsLetter(r)
}

func isWordPartRune(r rune) bool {
	return isWordStartRune(r) || unicode.IsDigit(r) || r == '$'
}

func isASCIIAlpha(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isASCIIDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func sensitiveStatement(tokens []sqlToken) bool {
	if len(tokens) == 0 {
		return false
	}
	first := tokenLower(tokens[0])
	words := make([]string, 0, len(tokens))
	for _, token := range tokens {
		words = append(words, tokenLower(token))
	}
	// BACKUP/RESTORE are legacy MyDuck commands. They intentionally keep
	// their ObjectStorage implementation, including the user-supplied
	// endpoint and credentials. Keep the recognized form on that path while
	// still rejecting malformed or generic remote-storage statements below.
	if isLegacyObjectStorageStatement(first, tokens, words) {
		return false
	}

	// Inspection functions/views are sensitive only in their SQL role. A table
	// or column named `secret`/`duckdb_settings` is otherwise an ordinary user
	// identifier and must not be rejected by a substring match.
	if containsExecutableComment(tokens) || containsSensitiveInspection(tokens) {
		return true
	}

	switch first {
	case "install", "prepare", "execute":
		return true
	case "load":
		// LOAD DATA is a normal MySQL data-import operation. Other LOAD
		// forms are DuckDB extension loading and must stay service-owned.
		return len(words) < 2 || words[1] != "data" || containsRemoteURI(tokens)
	case "create":
		return sensitiveCreateStatement(tokens, words)
	case "set", "reset":
		return sensitiveSettingStatement(tokens, 1)
	case "alter", "drop":
		return sensitiveAlterOrDropStatement(tokens, words)
	case "attach":
		return containsRemoteURI(tokens) || containsWord(words, "ducklake")
	case "copy":
		return containsRemoteURI(tokens) || containsSensitiveOption(tokens)
	case "backup", "restore":
		return containsRemoteURI(tokens) || containsSensitiveOption(tokens)
	case "pragma":
		return containsSensitiveOption(tokens)
	}

	// A bare URI expression is an external-storage operation even when it uses
	// a dialect-specific verb unknown to this lexer. URI string literals are
	// checked by commands that interpret them as paths; a normal SELECT of a
	// string such as 'https://example.test' is harmless.
	return containsBareRemoteURI(tokens)
}

func tokenLower(token sqlToken) string {
	return strings.ToLower(strings.TrimSpace(token.text))
}

func sensitiveName(word string) bool {
	return sensitiveSettingName(word)
}

func sensitiveOptionName(word string) bool {
	if sensitiveSettingName(word) {
		return true
	}
	switch word {
	case "endpoint", "region", "url_style", "use_ssl", "access_key_id", "secret_access_key":
		return true
	default:
		return false
	}
}

func currentSettingIsSensitive(tokens []sqlToken, index int) bool {
	open := index + 1
	if open >= len(tokens) || tokens[open].text != "(" {
		return false
	}
	arg := open + 1
	if arg >= len(tokens) || !tokens[arg].string {
		// A parameter, identifier, expression, or malformed call can resolve to
		// a credential setting at execution time. Reject it conservatively.
		return true
	}
	return sensitiveSettingName(tokenLower(tokens[arg]))
}

func isFunctionCall(tokens []sqlToken, index int) bool {
	for i := index + 1; i < len(tokens); i++ {
		if tokens[i].text == "." {
			continue
		}
		return tokens[i].text == "("
	}
	return false
}

// sensitiveSettingName is intentionally limited to service-owned settings and
// credential fields. It is used only after a statement role has identified a
// setting target or a current_setting argument; applying it to every token
// would reject ordinary columns and aliases named "secret".
func sensitiveSettingName(word string) bool {
	word = strings.Trim(strings.ToLower(strings.TrimSpace(word)), "`\"")
	if strings.HasPrefix(word, "s3_") || strings.HasPrefix(word, "aws_") {
		return true
	}
	switch word {
	case "secret_access_key", "access_key_id", "session_token",
		"http_proxy_password", "enable_external_access", "allow_unsigned_extensions",
		"custom_extension_repository", "autoinstall_known_extensions",
		"autoinstall_extension_repository":
		return true
	default:
		return false
	}
}

func containsExecutableComment(tokens []sqlToken) bool {
	for _, token := range tokens {
		if token.text == "__executable_comment__" {
			return true
		}
	}
	return false
}

func containsSensitiveInspection(tokens []sqlToken) bool {
	for i, token := range tokens {
		word := tokenLower(token)
		if word == "current_setting" && isFunctionCall(tokens, i) {
			if currentSettingIsSensitive(tokens, i) {
				return true
			}
		}
		if (word == "duckdb_secrets" || word == "duckdb_settings") &&
			(isFunctionCall(tokens, i) || relationReferenceAt(tokens, i)) {
			return true
		}
		if word == "load_extension" && !token.quoted && isFunctionCall(tokens, i) {
			return true
		}
	}
	return false
}

func relationReferenceAt(tokens []sqlToken, index int) bool {
	// Walk over an optional schema qualifier (`main`.`duckdb_settings`) and
	// inspect the relation-introducing keyword. Aliases after AS are not
	// relations and therefore remain allowed.
	for i := index - 1; i >= 0 && i >= index-5; i-- {
		if keywordToken(tokens[i], "from", "join", "update", "into", "using", "table", "copy", "describe") {
			return true
		}
		if keywordToken(tokens[i], "as", "select", "where", "on", "and", "or") {
			return false
		}
	}
	return false
}

func keywordToken(token sqlToken, words ...string) bool {
	if token.string || token.quoted {
		return false
	}
	word := tokenLower(token)
	for _, want := range words {
		if word == want {
			return true
		}
	}
	return false
}

func sensitiveCreateStatement(tokens []sqlToken, words []string) bool {
	// Only the first object keyword is relevant. Scanning the whole statement
	// would reject harmless identifiers such as CREATE TABLE secret (...).
	for i := 1; i < len(words); i++ {
		if tokens[i].string || tokens[i].quoted {
			continue
		}
		switch words[i] {
		case "or", "replace", "temporary", "temp", "persistent", "if", "not", "exists":
			continue
		case "secret", "ducklake", "httpfs", "extension":
			return true
		default:
			return false
		}
	}
	return false
}

func sensitiveSettingStatement(tokens []sqlToken, start int) bool {
	// Restrict matching to the setting target, never its RHS. This avoids
	// treating a value or ordinary identifier named "secret" as a control
	// operation while still handling SET (..., s3_endpoint=...).
	for start < len(tokens) && isSettingModifier(tokens[start]) {
		start++
	}
	if start >= len(tokens) {
		return false
	}
	if tokens[start].text == "(" {
		for i := start + 1; i < len(tokens) && tokens[i].text != ")" && tokens[i].text != "="; i++ {
			if settingTargetSensitiveAt(tokens, i) {
				return true
			}
		}
		return false
	}
	// SET TIME ZONE/NAMES/ROLE and SET SESSION CHARACTERISTICS are standard
	// PostgreSQL session controls, not DuckLake settings.
	if keywordToken(tokens[start], "time", "names", "schema", "role", "constraints", "characteristics", "transaction") {
		return false
	}
	return settingTargetSensitiveAt(tokens, start)
}

func isSettingModifier(token sqlToken) bool {
	if token.string || token.quoted {
		return false
	}
	switch tokenLower(token) {
	case "local", "session", "global", "variable", "persistent", "system":
		return true
	default:
		return false
	}
}

func settingTargetSensitiveAt(tokens []sqlToken, index int) bool {
	if index < 0 || index >= len(tokens) || tokens[index].string {
		return false
	}
	word := tokenLower(tokens[index])
	if word == "." || word == "," || word == "=" || word == "to" || word == "from" {
		return false
	}
	if sensitiveSettingName(word) {
		return true
	}
	// Accept qualified spellings such as s3.endpoint and aws.secret_access_key
	// without broad matching of the unrelated value tokens that follow them.
	if index+2 < len(tokens) && tokens[index+1].text == "." && sensitiveSettingName(tokenLower(tokens[index+2])) {
		return true
	}
	if index > 1 && tokens[index-1].text == "." && sensitiveSettingName(tokenLower(tokens[index-2])) {
		return true
	}
	return false
}

func settingTargetName(word string) string {
	return strings.Trim(strings.ToLower(strings.TrimSpace(word)), "`\"")
}

func sensitiveAlterOrDropStatement(tokens []sqlToken, words []string) bool {
	if len(words) < 2 {
		return false
	}
	for i := 1; i < len(words); i++ {
		if tokens[i].string || tokens[i].quoted {
			continue
		}
		switch words[i] {
		case "if", "not", "exists", "only":
			continue
		case "secret", "extension":
			return true
		case "set":
			return sensitiveSettingStatement(tokens, i+1)
		default:
			return false
		}
	}
	return false
}

func isLegacyObjectStorageSQL(query string) bool {
	statements := splitSQLStatements(query)
	for _, tokens := range statements {
		if len(tokens) == 0 {
			continue
		}
		words := make([]string, 0, len(tokens))
		for _, token := range tokens {
			words = append(words, tokenLower(token))
		}
		if isLegacyObjectStorageStatement(words[0], tokens, words) {
			return true
		}
	}
	return false
}

func isLegacyObjectStorageStatement(first string, tokens []sqlToken, words []string) bool {
	if first != "backup" && first != "restore" {
		return false
	}
	direction := "to"
	if first == "restore" {
		direction = "from"
	}
	if !containsUnquotedWord(tokens, "database") || !containsLegacyRemoteLiteral(tokens, direction) {
		return false
	}
	return true
}

func containsLegacyRemoteLiteral(tokens []sqlToken, direction string) bool {
	for i := 0; i+1 < len(tokens); i++ {
		if !keywordToken(tokens[i], direction) || !tokens[i+1].string {
			continue
		}
		value := strings.ToLower(strings.TrimSpace(tokens[i+1].text))
		if strings.HasPrefix(value, "s3://") || strings.HasPrefix(value, "s3c://") {
			return true
		}
	}
	return false
}

func containsUnquotedWord(tokens []sqlToken, want string) bool {
	for _, token := range tokens {
		if !token.string && !token.quoted && tokenLower(token) == want {
			return true
		}
	}
	return false
}

func containsRemoteURI(tokens []sqlToken) bool {
	for _, token := range tokens {
		if token.string {
			value := strings.ToLower(strings.TrimSpace(token.text))
			if hasRemoteScheme(value) {
				return true
			}
		}
	}
	return containsBareRemoteURI(tokens)
}

// containsBareRemoteURI recognizes a URI written as SQL punctuation and
// identifiers (for example, SELECT s3://bucket/object). Quoted literals are
// intentionally excluded here; command-specific callers decide whether a
// quoted path is interpreted as an external storage location.
func containsBareRemoteURI(tokens []sqlToken) bool {
	for i := 0; i+3 < len(tokens); i++ {
		if tokens[i].string || tokens[i].quoted || tokens[i+1].string || tokens[i+1].quoted || tokens[i+2].string || tokens[i+2].quoted || tokens[i+3].string || tokens[i+3].quoted {
			continue
		}
		scheme := tokenLower(tokens[i])
		if (scheme != "http" && scheme != "https" && scheme != "s3" && scheme != "s3a" && scheme != "s3n" && scheme != "s3c") || tokens[i+1].text != ":" || tokens[i+2].text != "/" || tokens[i+3].text != "/" {
			continue
		}
		return true
	}
	return false
}

func hasRemoteScheme(value string) bool {
	for _, scheme := range []string{"http://", "https://", "s3://", "s3a://", "s3n://", "s3c://"} {
		if strings.HasPrefix(value, scheme) {
			return true
		}
	}
	return false
}

func containsWord(words []string, want string) bool {
	for _, word := range words {
		if word == want {
			return true
		}
	}
	return false
}

func containsSensitiveOption(tokens []sqlToken) bool {
	for i := 0; i < len(tokens); i++ {
		if tokens[i].string || tokens[i].quoted {
			continue
		}
		word := tokenLower(tokens[i])
		if sensitiveOptionName(word) || word == "ducklake" || word == "httpfs" {
			if assignmentOperatorAt(tokens, i+1) {
				return true
			}
		}
		// Keep qualified option names (`s3.endpoint`, `aws.secret_access_key`)
		// tied to the assignment target rather than matching an arbitrary RHS
		// identifier or column name.
		if i+2 < len(tokens) && tokens[i+1].text == "." &&
			sensitiveOptionName(tokenLower(tokens[i+2])) && assignmentOperatorAt(tokens, i+3) {
			return true
		}
	}
	return false
}

func assignmentOperatorAt(tokens []sqlToken, index int) bool {
	if index < 0 || index >= len(tokens) {
		return false
	}
	if tokens[index].text == "=" {
		return true
	}
	return tokens[index].text == ":" && index+1 < len(tokens) && tokens[index+1].text == "="
}
