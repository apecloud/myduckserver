package catalog

import (
	stdsql "database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/dolthub/go-mysql-server/sql"
)

type Database struct {
	mu      *sync.RWMutex
	catalog string
	name    string
}

var _ sql.Database = (*Database)(nil)
var _ sql.TableCreator = (*Database)(nil)
var _ sql.TableDropper = (*Database)(nil)
var _ sql.TableRenamer = (*Database)(nil)
var _ sql.ViewDatabase = (*Database)(nil)
var _ sql.TriggerDatabase = (*Database)(nil)
var _ sql.CollatedDatabase = (*Database)(nil)
var _ sql.TemporaryTableCreator = (*Database)(nil)

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
		return nil, err
	}
	for _, t := range tables {
		t.WithSchema(ctx)
	}
	return tables, nil
}

func (d *Database) findTables(ctx *sql.Context, pattern string) ([]*Table, error) {
	rows, err := adapter.QueryCatalog(ctx, "SELECT DISTINCT table_name, comment FROM duckdb_tables()  where (database_name = ? and schema_name = ? and table_name ILIKE ?) or (database_name = 'temp' and schema_name = 'main' and table_name ILIKE ?)", d.catalog, d.name, pattern, pattern)
	if err != nil {
		return nil, ErrDuckDB.New(err)
	}
	defer rows.Close()

	var tbls []*Table
	for rows.Next() {
		var tblName string
		var comment stdsql.NullString
		if err := rows.Scan(&tblName, &comment); err != nil {
			return nil, ErrDuckDB.New(err)
		}
		t := NewTable(tblName, d).WithComment(DecodeComment[any](comment.String))
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

func (d *Database) CreateAllTable(ctx *sql.Context, name string, schema sql.PrimaryKeySchema, collation sql.CollationID, comment string, is_temp bool) error {

	var columns []string
	var columnCommentSQLs []string
	var fullTableName string

	if is_temp {
		fullTableName = FullTableName("temp", "main", name)
	} else {
		fullTableName = FullTableName(d.catalog, d.name, name)
	}

	for _, col := range schema.Schema {
		typ, err := DuckdbDataType(col.Type)
		if err != nil {
			return err
		}
		colDef := fmt.Sprintf(`"%s" %s`, col.Name, typ.name)
		if col.Nullable {
			colDef += " NULL"
		} else {
			colDef += " NOT NULL"
		}

		if col.Default != nil {
			columnDefault, err := typ.mysql.withDefault(col.Default.String())
			if err != nil {
				return err
			}
			colDef += " DEFAULT " + columnDefault
		}

		columns = append(columns, colDef)

		var fullColumnName string

		if is_temp {
			fullColumnName = FullColumnName("temp", "main", name, col.Name)
		} else {
			fullColumnName = FullColumnName(d.catalog, d.name, name, col.Name)
		}

		if col.Comment != "" || typ.mysql.Name != "" || col.Default != nil {
			columnCommentSQLs = append(columnCommentSQLs,
				fmt.Sprintf(`COMMENT ON COLUMN %s IS '%s'`, fullColumnName,
					NewCommentWithMeta[MySQLType](col.Comment, typ.mysql).Encode()))
		}
	}

	var sqlsBuild strings.Builder

	if is_temp {
		sqlsBuild.WriteString(fmt.Sprintf(`CREATE TEMP TABLE %s (%s`, name, strings.Join(columns, ", ")))
	} else {
		sqlsBuild.WriteString(fmt.Sprintf(`CREATE TABLE %s (%s`, fullTableName, strings.Join(columns, ", ")))
	}

	var primaryKeys []string
	for _, pkord := range schema.PkOrdinals {
		primaryKeys = append(primaryKeys, schema.Schema[pkord].Name)
	}

	if len(primaryKeys) > 0 {
		sqlsBuild.WriteString(fmt.Sprintf(", PRIMARY KEY (%s)", strings.Join(primaryKeys, ", ")))
	}

	sqlsBuild.WriteString(")")

	// Add comment to the table
	if comment != "" {
		sqlsBuild.WriteString(fmt.Sprintf("; COMMENT ON TABLE %s IS '%s'", fullTableName, NewComment[any](comment).Encode()))
	}

	// Add column comments
	for _, s := range columnCommentSQLs {
		sqlsBuild.WriteString(";")
		sqlsBuild.WriteString(s)
	}

	_, err := adapter.Exec(ctx, sqlsBuild.String())
	if err != nil {
		if IsDuckDBTableAlreadyExistsError(err) {
			return sql.ErrTableAlreadyExists.New(name)
		}
		return ErrDuckDB.New(err)
	}

	// TODO: support collation

	return nil
}

// CreateTable implements sql.TableCreator.
func (d *Database) CreateTable(ctx *sql.Context, name string, schema sql.PrimaryKeySchema, collation sql.CollationID, comment string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.CreateAllTable(ctx, name, schema, collation, comment, false)
}

// CreateTemporaryTable implements sql.CreateTemporaryTable.
func (d *Database) CreateTemporaryTable(ctx *sql.Context, name string, schema sql.PrimaryKeySchema, collation sql.CollationID) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.CreateAllTable(ctx, name, schema, collation, "", true)
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
		SELECT DISTINCT view_name, sql
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
		if err := rows.Scan(&name, &createViewStmt); err != nil {
			return nil, ErrDuckDB.New(err)
		}

		// Skip system views directly
		if IsSystemView(name) {
			continue
		}

		views = append(views, sql.ViewDefinition{
			Name:                name,
			CreateViewStatement: createViewStmt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, ErrDuckDB.New(err)
	}
	return views, nil
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

	_, err := adapter.Exec(ctx, fmt.Sprintf(`USE %s; CREATE VIEW "%s" AS %s`, FullSchemaName(d.catalog, d.name), name, selectStatement))
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

// GetCollation implements sql.CollatedDatabase.
func (d *Database) GetCollation(ctx *sql.Context) sql.CollationID {
	return sql.Collation_Default
}

// SetCollation implements sql.CollatedDatabase.
func (d *Database) SetCollation(ctx *sql.Context, collation sql.CollationID) error {
	return nil
}
