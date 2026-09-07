package catalog

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"

	"github.com/dolthub/go-mysql-server/sql"
)

// TableStorageKind is the durable storage class for a user table. Resolver
// functions use local when no selector is present; object is the DuckLake-
// backed class selected by the 0.3 table-storage slice.
type TableStorageKind string

const (
	TableStorageLocal  TableStorageKind = "local"
	TableStorageObject TableStorageKind = "object"

	// TableStorageDuckLake is an explicit spelling for callers that think in
	// terms of the underlying implementation. It has the same durable value as
	// TableStorageObject.
	TableStorageDuckLake = TableStorageObject
	// TableStorageObjectStorage is retained as a descriptive alias for API
	// callers; object is the only persisted value.
	TableStorageObjectStorage = TableStorageObject
)

const (
	// TableStorageOptionName is the protocol-neutral PostgreSQL storage
	// parameter. MySQL clients use ENGINE=DUCKLAKE because Vitess accepts only
	// its built-in table-option grammar.
	TableStorageOptionName  = "myduck_storage"
	TableStorageObjectValue = "object"
	TableStorageLocalValue  = "local"
	TableStorageMySQLEngine = "ducklake"
)

var (
	ErrInvalidTableStorage       = errors.New("invalid table storage selection")
	ErrTableStorageConflict      = errors.New("conflicting table storage selections")
	ErrTableStorageDuplicate     = errors.New("duplicate table storage selection")
	ErrTableStorageCredentialSQL = errors.New("table storage credentials must come from service configuration")
	ErrTableStoragePathSQL       = errors.New("table storage paths must come from service configuration")
)

// TableStorageSelection is the normalized result of a CREATE TABLE storage
// selector. Explicit is false only when no selector was supplied, in which case
// Kind is always TableStorageLocal.
type TableStorageSelection struct {
	Kind     TableStorageKind
	Explicit bool
	Source   string
}

func DefaultTableStorageSelection() TableStorageSelection {
	return TableStorageSelection{Kind: TableStorageLocal, Source: "default"}
}

func (s TableStorageSelection) Validate() error {
	if s.Kind != TableStorageLocal && s.Kind != TableStorageObject {
		return fmt.Errorf("%w: unsupported kind %q", ErrInvalidTableStorage, s.Kind)
	}
	return nil
}

func (s TableStorageSelection) IsObjectStorage() bool {
	return s.Kind == TableStorageObject
}

// EffectiveKind makes the local default explicit for callers that receive a
// zero-valued selection from an optional context or metadata field.
func (s TableStorageSelection) EffectiveKind() TableStorageKind {
	if s.Kind == "" {
		return TableStorageLocal
	}
	return s.Kind
}

// TableStorageOption is a parser-neutral name/value pair. Keeping options as a
// slice lets protocol adapters preserve duplicates and reject them before a
// map-based planner silently overwrites one declaration with another.
type TableStorageOption struct {
	Name  string
	Value any
}

// NormalizeTableStorageOptions resolves the selectors that protocol parsers
// expose. Ordinary MySQL/PostgreSQL table options are ignored and retain their
// historical local behavior. Only the explicit selector and recognized
// ENGINE alias affect the result.
func NormalizeTableStorageOptions(options []TableStorageOption) (TableStorageSelection, error) {
	selection := DefaultTableStorageSelection()
	canonicalSeen := false
	canonicalKind := TableStorageLocal
	canonicalName := ""
	engineSeen := false
	engineKind := TableStorageLocal
	engineSelected := false

	for _, option := range options {
		name := normalizeTableStorageOptionName(option.Name)
		if name == "" {
			return TableStorageSelection{}, fmt.Errorf("%w: empty option name", ErrInvalidTableStorage)
		}

		if isTableStorageCredentialOption(name) {
			return TableStorageSelection{}, fmt.Errorf("%w: option %q", ErrTableStorageCredentialSQL, name)
		}
		if isTableStoragePathOption(name) {
			return TableStorageSelection{}, fmt.Errorf("%w: option %q", ErrTableStoragePathSQL, name)
		}

		switch name {
		case TableStorageOptionName:
			if canonicalSeen {
				return TableStorageSelection{}, fmt.Errorf("%w: %q and %q", ErrTableStorageDuplicate, canonicalName, name)
			}
			kind, err := parseTableStorageKind(option.Value)
			if err != nil {
				return TableStorageSelection{}, fmt.Errorf("%w: option %q: %v", ErrInvalidTableStorage, name, err)
			}
			canonicalSeen = true
			canonicalKind = kind
			canonicalName = name
		case "engine":
			kind, selected, err := parseMySQLEngine(option.Value)
			if err != nil {
				return TableStorageSelection{}, err
			}
			if engineSeen {
				if engineSelected && selected && engineKind != kind {
					return TableStorageSelection{}, fmt.Errorf("%w: ENGINE %s versus %s", ErrTableStorageConflict, engineKind, kind)
				}
				return TableStorageSelection{}, fmt.Errorf("%w: repeated ENGINE selector", ErrTableStorageDuplicate)
			}
			engineSeen = true
			engineKind = kind
			engineSelected = selected
			if !selected {
				// A normal MySQL engine (for example InnoDB) has always been
				// accepted by MyDuck and maps to the local DuckDB table.
				continue
			}
			if canonicalSeen {
				if canonicalKind != engineKind {
					return TableStorageSelection{}, fmt.Errorf("%w: %s versus ENGINE", ErrTableStorageConflict, canonicalKind)
				}
				return TableStorageSelection{}, fmt.Errorf("%w: %q and ENGINE", ErrTableStorageDuplicate, canonicalName)
			}
			selection.Kind = kind
			selection.Explicit = true
			selection.Source = "mysql-engine"
		default:
			// Do not silently accept a future MyDuck/DuckLake control option.
			// Standard PostgreSQL WITH parameters and standard MySQL options
			// remain untouched for compatibility.
			if strings.HasPrefix(name, "myduck_") || strings.HasPrefix(name, "ducklake_") || isTableStorageAliasName(name) {
				return TableStorageSelection{}, fmt.Errorf("%w: unsupported option %q", ErrInvalidTableStorage, name)
			}
		}
	}

	if canonicalSeen {
		if engineSeen {
			if engineSelected && engineKind == canonicalKind {
				return TableStorageSelection{}, fmt.Errorf("%w: %q and ENGINE", ErrTableStorageDuplicate, canonicalName)
			}
			if engineKind != canonicalKind {
				return TableStorageSelection{}, fmt.Errorf("%w: %s versus ENGINE", ErrTableStorageConflict, canonicalKind)
			}
		}
		selection.Kind = canonicalKind
		selection.Explicit = true
		selection.Source = "storage-option"
	}
	if err := selection.Validate(); err != nil {
		return TableStorageSelection{}, err
	}
	return selection, nil
}

// ResolveMySQLTableStorage consumes the map produced by go-mysql-server's
// planner. The planner has already removed duplicate keys, so protocol adapters
// that need duplicate detection should call NormalizeTableStorageOptions on a
// token-preserving slice first.
func ResolveMySQLTableStorage(options map[string]interface{}) (TableStorageSelection, error) {
	if len(options) == 0 {
		return DefaultTableStorageSelection(), nil
	}
	keys := make([]string, 0, len(options))
	for key := range options {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	normalized := make([]TableStorageOption, 0, len(keys))
	for _, key := range keys {
		normalized = append(normalized, TableStorageOption{Name: key, Value: options[key]})
	}
	return NormalizeTableStorageOptions(normalized)
}

// ResolvePostgresStorageParams consumes literal values extracted from
// Cockroach's tree.CreateTable.StorageParams. Values may retain SQL quoting;
// the strict value parser removes one matching quote pair.
func ResolvePostgresStorageParams(params map[string]string) (TableStorageSelection, error) {
	if len(params) == 0 {
		return DefaultTableStorageSelection(), nil
	}
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	options := make([]TableStorageOption, 0, len(keys))
	for _, key := range keys {
		options = append(options, TableStorageOption{Name: key, Value: params[key]})
	}
	return NormalizeTableStorageOptions(options)
}

// ResolvePostgresStorageOptions is the duplicate-preserving form for callers
// that walk tree.StorageParams directly.
func ResolvePostgresStorageOptions(options []TableStorageOption) (TableStorageSelection, error) {
	return NormalizeTableStorageOptions(options)
}

// tableStorageSelectionContextKey is deliberately private so a caller cannot
// accidentally collide with another context value. The selector is request
// scoped: callers set it immediately before invoking the generic table-create
// executor, and the database consumes it while creating that table.
type tableStorageSelectionContextKey struct{}

// tableStorageSelectionParentKey lets the request-scoped value be consumed
// without discarding cancellation, query-origin, or other context values.
// SetTableStorageSelection mutates sql.Context for the GMS hook API, so the
// original context is retained explicitly and restored by Clear...
type tableStorageSelectionParentKey struct{}

// SetTableStorageSelection attaches a validated selector to a GMS SQL
// context. It mutates only the context wrapper passed by the caller; the
// underlying request/session remains otherwise unchanged.
func SetTableStorageSelection(ctx *sql.Context, selection TableStorageSelection) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil SQL context", ErrInvalidTableStorage)
	}
	if err := selection.Validate(); err != nil {
		return err
	}
	base := ctx.Context
	if base == nil {
		base = context.Background()
	}
	parent := base
	if original, ok := base.Value(tableStorageSelectionParentKey{}).(context.Context); ok && original != nil {
		parent = original
	}
	withParent := context.WithValue(base, tableStorageSelectionParentKey{}, parent)
	ctx.Context = context.WithValue(withParent, tableStorageSelectionContextKey{}, selection)
	return nil
}

// ClearTableStorageSelection consumes the request-scoped selector installed by
// SetTableStorageSelection. This prevents a reusable sql.Context (for example
// a session transaction context) from carrying one CREATE TABLE's choice into
// a later statement.
func ClearTableStorageSelection(ctx *sql.Context) {
	if ctx == nil || ctx.Context == nil {
		return
	}
	if parent, ok := ctx.Context.Value(tableStorageSelectionParentKey{}).(context.Context); ok && parent != nil {
		ctx.Context = parent
	}
}

// WithTableStorageSelection returns a copy of ctx carrying a validated table
// selector. The copy form is useful in hooks that must not mutate a context
// owned by another execution path.
func WithTableStorageSelection(ctx *sql.Context, selection TableStorageSelection) (*sql.Context, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil SQL context", ErrInvalidTableStorage)
	}
	copy := ctx.WithContext(ctx.Context)
	if err := SetTableStorageSelection(copy, selection); err != nil {
		return nil, err
	}
	return copy, nil
}

// TableStorageSelectionFromContext retrieves a selector attached by
// SetTableStorageSelection or WithTableStorageSelection. A missing or invalid
// value is reported as absent so callers can safely fall back to local.
func TableStorageSelectionFromContext(ctx *sql.Context) (TableStorageSelection, bool) {
	if ctx == nil || ctx.Context == nil {
		return TableStorageSelection{}, false
	}
	selection, ok := ctx.Value(tableStorageSelectionContextKey{}).(TableStorageSelection)
	if !ok || selection.Validate() != nil {
		return TableStorageSelection{}, false
	}
	return selection, true
}

func normalizeTableStorageOptionName(raw string) string {
	name := strings.ToLower(strings.TrimSpace(raw))
	if len(name) >= 2 {
		first, last := name[0], name[len(name)-1]
		if (first == '`' && last == '`') || (first == '"' && last == '"') || (first == '\'' && last == '\'') {
			name = strings.TrimSpace(name[1 : len(name)-1])
		}
	}
	return name
}

func normalizeTableStorageValue(value any) (string, error) {
	var raw string
	switch v := value.(type) {
	case string:
		raw = v
	case []byte:
		raw = string(v)
	case fmt.Stringer:
		raw = v.String()
	default:
		return "", fmt.Errorf("value must be a literal string")
	}
	raw = strings.TrimSpace(raw)
	if len(raw) >= 2 {
		first, last := raw[0], raw[len(raw)-1]
		if (first == '\'' && last == '\'') || (first == '"' && last == '"') || (first == '`' && last == '`') {
			raw = strings.TrimSpace(raw[1 : len(raw)-1])
		}
	}
	if raw == "" {
		return "", fmt.Errorf("value is empty")
	}
	if strings.ContainsAny(raw, "\r\n\t;:/\\?#'\"") {
		return "", fmt.Errorf("value must be a simple storage kind")
	}
	for _, r := range raw {
		if unicode.IsSpace(r) || !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-') {
			return "", fmt.Errorf("value must be a simple storage kind")
		}
	}
	return strings.ToLower(raw), nil
}

func parseTableStorageKind(value any) (TableStorageKind, error) {
	normalized, err := normalizeTableStorageValue(value)
	if err != nil {
		return "", err
	}
	switch normalized {
	case TableStorageLocalValue:
		return TableStorageLocal, nil
	case TableStorageObjectValue:
		return TableStorageObject, nil
	default:
		return "", errors.New("unknown storage kind")
	}
}

func parseMySQLEngine(value any) (TableStorageKind, bool, error) {
	normalized, err := normalizeTableStorageValue(value)
	if err != nil {
		return "", false, fmt.Errorf("%w: ENGINE: %v", ErrInvalidTableStorage, err)
	}
	switch normalized {
	case TableStorageMySQLEngine:
		return TableStorageObject, true, nil
	case "local", "duckdb":
		return TableStorageLocal, true, nil
	}
	// These are common MySQL/Dolt engine names. MyDuck historically accepts
	// them as compatibility decorations while storing the table in DuckDB.
	switch normalized {
	case "innodb", "myisam", "memory", "heap", "merge", "archive", "csv", "blackhole", "ndbcluster", "federated", "rocksdb", "aria", "columnstore":
		return TableStorageLocal, false, nil
	default:
		// ENGINE is a long-standing MySQL compatibility decoration. GMS and
		// MyDuck have historically accepted engines they do not implement and
		// still materialized those tables locally. Preserve that behavior for
		// arbitrary simple engine identifiers; DUCKLAKE remains the sole object
		// storage spelling.
		return TableStorageLocal, false, nil
	}
}

func isTableStorageAliasName(name string) bool {
	switch name {
	case "storage", "storage_kind", "storage_type", "myduck_storage_kind", "ducklake_storage":
		return true
	default:
		return false
	}
}

func isTableStorageCredentialOption(name string) bool {
	for _, part := range []string{
		"access_key", "access_key_id", "secret", "secret_key", "secret_access_key",
		"credential", "credentials", "password", "token", "key_id", "session_token",
	} {
		if name == part || strings.HasPrefix(name, part+"_") || strings.HasSuffix(name, "_"+part) {
			return true
		}
	}
	return false
}

func isTableStoragePathOption(name string) bool {
	for _, part := range []string{
		"endpoint", "s3_endpoint", "data_path", "metadata_path", "object_path",
		"lake_path", "bucket", "path", "region", "url_style", "use_ssl", "tls",
	} {
		if name == part || strings.HasPrefix(name, part+"_") || strings.HasSuffix(name, "_"+part) {
			return true
		}
	}
	return false
}
