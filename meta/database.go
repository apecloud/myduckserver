package meta

import (
	stdsql "database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

type Database struct {
	mu          *sync.RWMutex
	name        string
	storage     *stdsql.DB
	catalogName string
}

var _ sql.Database = (*Database)(nil)
var _ sql.TableCreator = (*Database)(nil)
var _ sql.TableDropper = (*Database)(nil)
var _ sql.TableRenamer = (*Database)(nil)
var _ sql.ViewDatabase = (*Database)(nil)
var _ sql.TriggerDatabase = (*Database)(nil)
var _ sql.CollatedDatabase = (*Database)(nil)

func NewDatabase(name string, storage *stdsql.DB, catalogName string) *Database {
	return &Database{
		mu:          &sync.RWMutex{},
		name:        name,
		storage:     storage,
		catalogName: catalogName,
	}
}

// GetTableNames implements sql.Database.
func (d *Database) GetTableNames(ctx *sql.Context) ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	tbls, err := d.tablesInsensitive()
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

	tbls, err := d.tablesInsensitive()
	if err != nil {
		return nil, false, err
	}

	tbl, ok := tbls[strings.ToLower(tblName)]
	return tbl, ok, nil
}

func (d *Database) tablesInsensitive() (map[string]sql.Table, error) {
	rows, err := d.storage.Query("SELECT DISTINCT table_name, comment FROM duckdb_tables() where database_name = ? and schema_name = ?", d.catalogName, d.name)
	if err != nil {
		return nil, ErrDuckDB.New(err)
	}
	defer rows.Close()

	tbls := make(map[string]sql.Table)
	for rows.Next() {
		var tblName string
		var comment stdsql.NullString
		if err := rows.Scan(&tblName, &comment); err != nil {
			return nil, ErrDuckDB.New(err)
		}
		tbls[strings.ToLower(tblName)] = NewTable(tblName, d).WithComment(DecodeComment(comment.String))
	}
	return tbls, nil
}

// Name implements sql.Database.
func (d *Database) Name() string {
	return d.name
}

// CreateTable implements sql.TableCreator.
func (d *Database) CreateTable(ctx *sql.Context, name string, schema sql.PrimaryKeySchema, collation sql.CollationID, comment string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	var columns []string
	var columnCommentSQLs []string
	for _, col := range schema.Schema {
		typ, err := duckdbDataType(col.Type)
		if err != nil {
			return err
		}
		colDef := fmt.Sprintf(`"%s" %s`, col.Name, typ.str)
		if col.Nullable {
			colDef += " NULL"
		} else {
			colDef += " NOT NULL"
		}

		if col.Default != nil {
			colDef += " DEFAULT " + col.Default.String()
		}

		columns = append(columns, colDef)

		if col.Comment != "" || typ.extra != "" {
			columnCommentSQLs = append(columnCommentSQLs,
				fmt.Sprintf(`COMMENT ON COLUMN %s IS %s`, FullColumnName(d.catalogName, d.name, name, col.Name),
					NewCommentWithMeta(col.Comment, typ.extra).Encode()))
		}
	}

	sql := fmt.Sprintf(`CREATE TABLE %s (%s`, FullTableName(d.catalogName, d.name, name), strings.Join(columns, ", "))

	var primaryKeys []string
	for pkord := range schema.PkOrdinals {
		primaryKeys = append(primaryKeys, schema.Schema[pkord].Name)
	}

	if len(primaryKeys) > 0 {
		sql += fmt.Sprintf(", PRIMARY KEY (%s)", strings.Join(primaryKeys, ", "))
	}

	sql += ")"

	// Add comment to the table
	if comment != "" {
		sql += fmt.Sprintf("; COMMENT ON TABLE %s IS %s", FullTableName(d.catalogName, d.name, name), NewComment(comment).Encode())
	}

	// Add column comments
	for _, s := range columnCommentSQLs {
		sql += ";" + s
	}

	_, err := d.storage.Exec(sql)
	if err != nil {
		return ErrDuckDB.New(err)
	}

	// TODO: support collation and comment

	return nil
}

// DropTable implements sql.TableDropper.
func (d *Database) DropTable(ctx *sql.Context, name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.storage.Exec(fmt.Sprintf(`DROP TABLE %s`, FullTableName(d.catalogName, d.name, name)))

	if err != nil {
		return ErrDuckDB.New(err)
	}
	return nil
}

// RenameTable implements sql.TableRenamer.
func (d *Database) RenameTable(ctx *sql.Context, oldName string, newName string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.storage.Exec(fmt.Sprintf(`ALTER TABLE %s RENAME TO "%s"`, FullTableName(d.catalogName, d.name, oldName), newName))
	if err != nil {
		return ErrDuckDB.New(err)
	}
	return nil
}

// extractViewDefinitions is a helper function to extract view definitions from DuckDB
func (d *Database) extractViewDefinitions(schemaName string, viewName string) ([]sql.ViewDefinition, error) {
	query := `
		SELECT view_name, sql
		FROM duckdb_views()
		WHERE schema_name = ?
	`
	args := []interface{}{schemaName}

	if viewName != "" {
		query += " AND view_name = ?"
		args = append(args, viewName)
	}

	rows, err := d.storage.Query(query, args...)
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

	return d.extractViewDefinitions(d.name, "")
}

// GetViewDefinition implements sql.ViewDatabase.
func (d *Database) GetViewDefinition(ctx *sql.Context, viewName string) (sql.ViewDefinition, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	views, err := d.extractViewDefinitions(d.name, viewName)
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

	_, err := d.storage.Exec(fmt.Sprintf(`USE %s; CREATE VIEW "%s" AS %s`, FullSchemaName(d.catalogName, d.name), name, selectStatement))
	if err != nil {
		return ErrDuckDB.New(err)
	}
	return nil
}

// DropView implements sql.ViewDatabase.
func (d *Database) DropView(ctx *sql.Context, name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.storage.Exec(fmt.Sprintf(`USE %s; DROP VIEW "%s"`, FullSchemaName(d.catalogName, d.name), name))
	if err != nil {
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
