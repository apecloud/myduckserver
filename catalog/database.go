package catalog

import (
	stdsql "database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/configuration"
	"github.com/apecloud/myduckserver/mycontext"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	vectorfn "github.com/dolthub/go-mysql-server/sql/expression/function/vector"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

type Database struct {
	mu      *sync.RWMutex
	catalog string
	name    string
}

type ExtraViewInfo struct {
	TextDefinition      string `json:"text_definition,omitempty"`
	CreateViewStatement string `json:"create_view_statement,omitempty"`
	SqlMode             string `json:"sql_mode,omitempty"`
}

var _ sql.Database = (*Database)(nil)
var _ sql.TableCreator = (*Database)(nil)
var _ sql.TableDropper = (*Database)(nil)
var _ sql.TableRenamer = (*Database)(nil)
var _ sql.ViewDatabase = (*Database)(nil)
var _ sql.TriggerDatabase = (*Database)(nil)
var _ sql.CollatedDatabase = (*Database)(nil)
var _ sql.TemporaryTableCreator = (*Database)(nil)
var _ sql.StoredProcedureDatabase = (*Database)(nil)

func vectorGeneratedExpression(col *sql.Column, typ types.VectorType) (string, error) {
	fn, ok := col.Generated.Expr.(*vectorfn.StringToVector)
	if !ok {
		return "", fmt.Errorf("unsupported generated VECTOR expression: %s", col.Generated.Expr.String())
	}
	field, ok := fn.Child.(*expression.GetField)
	if !ok {
		return "", fmt.Errorf("unsupported STRING_TO_VECTOR generated argument: %s", fn.Child.String())
	}
	return fmt.Sprintf("STRING_TO_VECTOR(%s, %d)", QuoteIdentifierANSI(field.Name()), typ.Dimensions), nil
}

func NewDatabase(name string, catalogName string) *Database {
	return &Database{
		mu:      &sync.RWMutex{},
		name:    name,
		catalog: catalogName,
	}
}

// GetTableNames implements sql.Database.
func (d *Database) GetTableNames(ctx *sql.Context) ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	tbls, err := d.tablesInsensitive(ctx, "%")
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(tbls))
	for _, tbl := range tbls {
		names = append(names, tbl.Name())
	}
	return names, nil
}

// GetTableInsensitive implements sql.Database.
func (d *Database) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	tbls, err := d.tablesInsensitive(ctx, tblName)
	if err != nil {
		return nil, false, err
	}

	if len(tbls) == 0 {
		return nil, false, nil
	}
	return tbls[0], true, nil
}

func (d *Database) tablesInsensitive(ctx *sql.Context, pattern string) ([]*Table, error) {
	tables, err := d.findTables(ctx, pattern)
	if err != nil {
		ctx.GetLogger().WithFields(logrus.Fields{
			"catalog":  d.catalog,
			"database": d.name,
			"pattern":  pattern,
		}).WithError(err).Error("Failed to find tables")
		return nil, err
	}
	for _, t := range tables {
		if err := t.withSchema(ctx); err != nil {
			ctx.GetLogger().WithFields(logrus.Fields{
				"catalog":  d.catalog,
				"database": d.name,
				"pattern":  pattern,
				"table":    t.Name(),
			}).WithError(err).Error("Failed to get table schema")
			return nil, err
		}
	}
	return tables, nil
}

func (d *Database) findTables(ctx *sql.Context, pattern string) ([]*Table, error) {
	rows, err := adapter.QueryCatalog(ctx, "SELECT table_name, has_primary_key, comment FROM duckdb_tables() WHERE (database_name = ? AND schema_name = ? AND table_name ILIKE ?) OR (temporary IS TRUE AND table_name ILIKE ?)", d.catalog, d.name, pattern, pattern)
	if err != nil {
		return nil, ErrDuckDB.New(err)
	}
	defer rows.Close()

	var tbls []*Table
	for rows.Next() {
		var tblName string
		var hasPrimaryKey bool
		var comment stdsql.NullString
		if err := rows.Scan(&tblName, &hasPrimaryKey, &comment); err != nil {
			return nil, ErrDuckDB.New(err)
		}
		t := NewTable(d, tblName, hasPrimaryKey).withComment(DecodeComment[ExtraTableInfo](comment.String))
		tbls = append(tbls, t)
	}
	if err := rows.Err(); err != nil {
		return nil, ErrDuckDB.New(err)
	}

	return tbls, nil
}

// Name implements sql.Database.
func (d *Database) Name() string {
	return d.name
}

func (d *Database) createAllTable(ctx *sql.Context, name string, schema sql.PrimaryKeySchema, collation sql.CollationID, comment string, storage TableStorageSelection, temporary bool) error {
	if err := storage.Validate(); err != nil {
		return err
	}
	if temporary && storage.IsObjectStorage() {
		return fmt.Errorf("%w: temporary tables cannot use object storage", ErrInvalidTableStorage)
	}
	if storage.Kind == "" {
		storage = DefaultTableStorageSelection()
	}
	var columns []string
	var columnCommentSQLs []string
	var fullTableName string

	if temporary {
		fullTableName = FullTableName("temp", "main", name)
	} else {
		fullTableName = FullTableName(d.catalog, d.name, name)
	}

	var sequenceName, fullSequenceName string

	for _, col := range schema.Schema {
		typ, err := DuckdbDataType(col.Type)
		if err != nil {
			return err
		}
		colDef := fmt.Sprintf(`"%s" %s`, col.Name, typ.name)
		vectorType, isVector := col.Type.(types.VectorType)
		generatedVector := isVector && col.Generated != nil
		if generatedVector {
			generatedExpr, err := vectorGeneratedExpression(col, vectorType)
			if err != nil {
				return err
			}
			typ.mysql.Generated = col.Generated.Expr.String()
			typ.mysql.Virtual = col.Virtual
			typ.mysql.Nullable = new(bool)
			*typ.mysql.Nullable = col.Nullable
			colDef += " GENERATED ALWAYS AS (" + generatedExpr + ")"
		} else {
			if col.Nullable {
				colDef += " NULL"
			} else {
				colDef += " NOT NULL"
			}
			if isVector {
				colDef += fmt.Sprintf(` CHECK (octet_length("%s") = %d)`, col.Name, vectorType.Dimensions*4)
			}
		}

		if col.Default != nil {
			typ.mysql.Default = col.Default.String()
			defaultExpr, err := parseDefaultValue(typ.mysql.Default)
			if err != nil {
				return err
			}
			colDef += " DEFAULT " + defaultExpr
		} else if col.AutoIncrement {
			typ.mysql.AutoIncrement = true

			// Generate a random sequence name.
			// TODO(fan): Drop the sequence when the table is dropped or the column is removed.
			uuid, err := uuid.NewRandom()
			if err != nil {
				return err
			}
			sequenceName = SequenceNamePrefix + uuid.String()
			if temporary {
				fullSequenceName = `temp.main."` + sequenceName + `"`
			} else {
				fullSequenceName = InternalSchemas.SYS.Schema + `."` + sequenceName + `"`
			}

			defaultExpr := `nextval('` + fullSequenceName + `')`
			colDef += " DEFAULT " + defaultExpr
		}

		columns = append(columns, colDef)

		var fullColumnName string

		if temporary {
			fullColumnName = FullColumnName("temp", "main", name, col.Name)
		} else {
			fullColumnName = FullColumnName(d.catalog, d.name, name, col.Name)
		}

		if col.Comment != "" || typ.mysql.Name != "" || col.Default != nil {
			columnCommentSQLs = append(columnCommentSQLs,
				fmt.Sprintf(`COMMENT ON COLUMN %s IS '%s'`, fullColumnName,
					NewCommentWithMeta(col.Comment, typ.mysql).Encode()))
		}
	}

	var b strings.Builder
	b.Grow(256)

	if sequenceName != "" {
		b.WriteString(`CREATE `)
		if temporary {
			b.WriteString(`TEMP SEQUENCE "`)
			b.WriteString(sequenceName)
			b.WriteString(`"`)
		} else {
			b.WriteString(`SEQUENCE `)
			b.WriteString(fullSequenceName)
		}
		b.WriteString(`;`)
	}

	if temporary {
		b.WriteString(fmt.Sprintf(`CREATE TEMP TABLE %s (%s`, name, strings.Join(columns, ", ")))
	} else {
		b.WriteString(fmt.Sprintf(`CREATE TABLE %s (%s`, fullTableName, strings.Join(columns, ", ")))
	}

	var primaryKeys []string
	for _, pkord := range schema.PkOrdinals {
		primaryKeys = append(primaryKeys, schema.Schema[pkord].Name)
	}

	withoutIndex := isIndexCreationDisabled(ctx)

	// https://github.com/apecloud/myduckserver/issues/272
	if len(primaryKeys) > 0 && !withoutIndex {
		b.WriteString(fmt.Sprintf(", PRIMARY KEY (%s)", strings.Join(primaryKeys, ", ")))
	}

	b.WriteString(")")

	// Add comment to the table
	info := ExtraTableInfo{
		PkOrdinals: schema.PkOrdinals,
		Replicated: withoutIndex,
		Sequence:   fullSequenceName,
		Checks:     nil,
		Storage:    storage.Kind,
	}
	b.WriteString(fmt.Sprintf(
		"; COMMENT ON TABLE %s IS '%s'",
		fullTableName,
		NewCommentWithMeta(comment, info).Encode(),
	))

	// Add column comments
	for _, s := range columnCommentSQLs {
		b.WriteString(";")
		b.WriteString(s)
	}

	ddl := b.String()

	if logger := ctx.GetLogger(); logger.Logger.GetLevel() >= logrus.DebugLevel {
		logger.WithField("DuckSQL", ddl).Debug("Executing DDL")
	}

	_, err := adapter.Exec(ctx, ddl)
	if err != nil {
		if IsDuckDBTableAlreadyExistsError(err) {
			return sql.ErrTableAlreadyExists.New(name)
		}
		return ErrDuckDB.New(err)
	}

	// TODO: support collation

	return nil
}

func isIndexCreationDisabled(ctx *sql.Context) bool {
	if !configuration.IsReplicationWithoutIndex() {
		return false
	}
	if mycontext.IsReplicationQuery(ctx) {
		return true
	}
	_, vv, ok := sql.SystemVariables.GetGlobal("replica_is_loading_snapshot")
	if !ok {
		return false
	}
	if b, ok := vv.(int8); ok {
		return b != 0
	}
	return false
}

// CreateTable implements sql.TableCreator.
func (d *Database) CreateTable(ctx *sql.Context, name string, schema sql.PrimaryKeySchema, collation sql.CollationID, comment string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	storage := DefaultTableStorageSelection()
	if selected, ok := TableStorageSelectionFromContext(ctx); ok {
		storage = selected
	}
	return d.createAllTable(ctx, name, schema, collation, comment, storage, false)
}

// CreateTableWithStorage is the explicit catalog boundary for protocol
// adapters that already normalized a table selector. It is intentionally
// limited to selection propagation and metadata; object-table physical
// routing is owned by the follow-up storage implementation.
func (d *Database) CreateTableWithStorage(ctx *sql.Context, name string, schema sql.PrimaryKeySchema, collation sql.CollationID, comment string, storage TableStorageSelection) error {
	if err := storage.Validate(); err != nil {
		return err
	}
	if err := SetTableStorageSelection(ctx, storage); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.createAllTable(ctx, name, schema, collation, comment, storage, false)
}

// RecordTableStorageSelection updates the managed table metadata for a table
// created by a protocol path that bypasses sql.TableCreator (currently the
// PostgreSQL handler). It preserves the user-visible table comment and makes
// the selection available after a fresh catalog reload.
func (d *Database) RecordTableStorageSelection(ctx *sql.Context, name string, storage TableStorageSelection) error {
	if err := storage.Validate(); err != nil {
		return err
	}
	if d.catalog == "temp" && storage.IsObjectStorage() {
		return fmt.Errorf("%w: temporary tables cannot use object storage", ErrInvalidTableStorage)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	rows, err := adapter.QueryCatalog(ctx, `
		SELECT comment
		FROM duckdb_tables()
		WHERE database_name = ? AND schema_name = ? AND table_name = ?
	`, d.catalog, d.name, name)
	if err != nil {
		return ErrDuckDB.New(err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return ErrDuckDB.New(err)
		}
		return sql.ErrTableNotFound.New(name)
	}

	var rawComment stdsql.NullString
	if err := rows.Scan(&rawComment); err != nil {
		_ = rows.Close()
		return ErrDuckDB.New(err)
	}
	if err := rows.Close(); err != nil {
		return ErrDuckDB.New(err)
	}
	comment := DecodeComment[ExtraTableInfo](rawComment.String)
	info := comment.Meta
	info.Storage = storage.Kind
	encoded := NewCommentWithMeta(comment.Text, info).Encode()
	_, err = adapter.Exec(ctx, fmt.Sprintf(`COMMENT ON TABLE %s IS '%s'`, FullTableName(d.catalog, d.name, name), encoded))
	if err != nil {
		return ErrDuckDB.New(err)
	}
	return nil
}

// CreateTemporaryTable implements sql.CreateTemporaryTable.
func (d *Database) CreateTemporaryTable(ctx *sql.Context, name string, schema sql.PrimaryKeySchema, collation sql.CollationID) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	storage := DefaultTableStorageSelection()
	if selected, ok := TableStorageSelectionFromContext(ctx); ok {
		storage = selected
	}
	return d.createAllTable(ctx, name, schema, collation, "", storage, true)
}

// DropTable implements sql.TableDropper.
func (d *Database) DropTable(ctx *sql.Context, name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := adapter.Exec(ctx, fmt.Sprintf(`DROP TABLE %s`, FullTableName(d.catalog, d.name, name)))

	if err != nil {
		if IsDuckDBTableNotFoundError(err) {
			return sql.ErrTableNotFound.New(name)
		}
		return ErrDuckDB.New(err)
	}
	return nil
}

// RenameTable implements sql.TableRenamer.
func (d *Database) RenameTable(ctx *sql.Context, oldName string, newName string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := adapter.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s RENAME TO "%s"`, FullTableName(d.catalog, d.name, oldName), newName))
	if err != nil {
		if IsDuckDBTableNotFoundError(err) {
			return sql.ErrTableNotFound.New(oldName)
		}
		if IsDuckDBTableAlreadyExistsError(err) {
			return sql.ErrTableAlreadyExists.New(newName)
		}
		return ErrDuckDB.New(err)
	}
	return nil
}

// extractViewDefinitions is a helper function to extract view definitions from DuckDB
func (d *Database) extractViewDefinitions(ctx *sql.Context, schemaName string, viewName string) ([]sql.ViewDefinition, error) {
	query := `
		SELECT DISTINCT view_name, sql, comment
		FROM duckdb_views()
		WHERE schema_name = ? AND NOT internal
	`
	args := []interface{}{schemaName}

	if viewName != "" {
		query += " AND view_name = ?"
		args = append(args, viewName)
	}

	rows, err := adapter.QueryCatalog(ctx, query, args...)
	if err != nil {
		return nil, ErrDuckDB.New(err)
	}
	defer rows.Close()

	var views []sql.ViewDefinition
	for rows.Next() {
		var name, createViewStmt string
		var comment stdsql.NullString
		if err := rows.Scan(&name, &createViewStmt, &comment); err != nil {
			return nil, ErrDuckDB.New(err)
		}

		// Skip system views directly
		if IsSystemView(name) {
			continue
		}

		views = append(views, viewDefinitionFromMetadata(name, schemaName, createViewStmt, comment.String))
	}
	if err := rows.Err(); err != nil {
		return nil, ErrDuckDB.New(err)
	}
	return views, nil
}

func viewDefinitionFromMetadata(name, schemaName, createViewStmt, encodedComment string) sql.ViewDefinition {
	view := sql.ViewDefinition{
		Name:                name,
		CreateViewStatement: createViewStmt,
		SchemaName:          schemaName,
	}

	meta := DecodeComment[ExtraViewInfo](encodedComment).Meta
	if meta.CreateViewStatement != "" && meta.TextDefinition != "" && sql.NewSqlModeFromString(meta.SqlMode).AnsiQuotes() {
		view.TextDefinition = meta.TextDefinition
		view.CreateViewStatement = meta.CreateViewStatement
		view.SqlMode = meta.SqlMode
	}

	return view
}

// AllViews implements sql.ViewDatabase.
func (d *Database) AllViews(ctx *sql.Context) ([]sql.ViewDefinition, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return d.extractViewDefinitions(ctx, d.name, "")
}

// GetViewDefinition implements sql.ViewDatabase.
func (d *Database) GetViewDefinition(ctx *sql.Context, viewName string) (sql.ViewDefinition, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	views, err := d.extractViewDefinitions(ctx, d.name, viewName)
	if err != nil {
		return sql.ViewDefinition{}, false, err
	}

	if len(views) == 0 {
		return sql.ViewDefinition{}, false, nil
	}

	return views[0], true, nil
}

// CreateView implements sql.ViewDatabase.
func (d *Database) CreateView(ctx *sql.Context, name string, selectStatement string, createViewStmt string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	statement := fmt.Sprintf(`USE %s; CREATE VIEW "%s" AS %s`, FullSchemaName(d.catalog, d.name), name, selectStatement)
	sqlMode := sql.LoadSqlMode(ctx)
	if sqlMode.AnsiQuotes() {
		meta := ExtraViewInfo{
			TextDefinition:      selectStatement,
			CreateViewStatement: createViewStmt,
			SqlMode:             sqlMode.String(),
		}
		statement += fmt.Sprintf(`; COMMENT ON VIEW %s IS '%s'`,
			FullTableName(d.catalog, d.name, name), NewCommentWithMeta("", meta).Encode())
	}

	_, err := adapter.Exec(ctx, statement)
	if err != nil {
		return ErrDuckDB.New(err)
	}
	return nil
}

// DropView implements sql.ViewDatabase.
func (d *Database) DropView(ctx *sql.Context, name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := adapter.Exec(ctx, fmt.Sprintf(`USE %s; DROP VIEW "%s"`, FullSchemaName(d.catalog, d.name), name))
	if err != nil {
		if IsDuckDBViewNotFoundError(err) {
			return sql.ErrViewDoesNotExist.New(name)
		}
		return ErrDuckDB.New(err)
	}
	return nil
}

// CreateTrigger implements sql.TriggerDatabase.
func (d *Database) CreateTrigger(ctx *sql.Context, definition sql.TriggerDefinition) error {
	return sql.ErrTriggersNotSupported.New(d.name)
}

// DropTrigger implements sql.TriggerDatabase.
func (d *Database) DropTrigger(ctx *sql.Context, name string) error {
	return sql.ErrTriggersNotSupported.New(d.name)
}

// GetTriggers implements sql.TriggerDatabase.
func (d *Database) GetTriggers(ctx *sql.Context) ([]sql.TriggerDefinition, error) {
	return nil, nil
}

// GetStoredProcedure implements sql.StoredProcedureDatabase.
// Routines are not hosted in DuckDB; SHOW FUNCTION STATUS should be empty.
func (d *Database) GetStoredProcedure(ctx *sql.Context, name string) (sql.StoredProcedureDetails, bool, error) {
	return sql.StoredProcedureDetails{}, false, nil
}

// GetStoredProcedures implements sql.StoredProcedureDatabase.
func (d *Database) GetStoredProcedures(ctx *sql.Context) ([]sql.StoredProcedureDetails, error) {
	return nil, nil
}

// SaveStoredProcedure implements sql.StoredProcedureDatabase.
func (d *Database) SaveStoredProcedure(ctx *sql.Context, spd sql.StoredProcedureDetails) error {
	return sql.ErrStoredProceduresNotSupported.New(d.name)
}

// DropStoredProcedure implements sql.StoredProcedureDatabase.
func (d *Database) DropStoredProcedure(ctx *sql.Context, name string) error {
	return sql.ErrStoredProceduresNotSupported.New(d.name)
}

// GetCollation implements sql.CollatedDatabase.
func (d *Database) GetCollation(ctx *sql.Context) sql.CollationID {
	return sql.Collation_Default
}

// SetCollation implements sql.CollatedDatabase.
func (d *Database) SetCollation(ctx *sql.Context, collation sql.CollationID) error {
	return nil
}

// CopyTableData implements sql.TableCopierDatabase interface.
func (d *Database) CopyTableData(ctx *sql.Context, sourceTable string, destinationTable string) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Use INSERT INTO ... SELECT to copy data
	sql := `INSERT INTO ` + FullTableName(d.catalog, d.name, destinationTable) + ` FROM ` + FullTableName(d.catalog, d.name, sourceTable)

	res, err := adapter.Exec(ctx, sql)
	if err != nil {
		return 0, ErrDuckDB.New(err)
	}

	// Get count of affected rows
	count, err := res.RowsAffected()
	if err != nil {
		return 0, ErrDuckDB.New(err)
	}

	return uint64(count), nil
}
