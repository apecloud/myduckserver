package meta

import (
	stdsql "database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

type Database struct {
	mu     *sync.RWMutex
	name   string
	engine *stdsql.DB
}

var _ sql.Database = (*Database)(nil)
var _ sql.TableCreator = (*Database)(nil)
var _ sql.TableDropper = (*Database)(nil)
var _ sql.TableRenamer = (*Database)(nil)
var _ sql.ViewDatabase = (*Database)(nil)
var _ sql.TriggerDatabase = (*Database)(nil)
var _ sql.CollatedDatabase = (*Database)(nil)

func NewDatabase(name string, engine *stdsql.DB) *Database {
	return &Database{
		mu:     &sync.RWMutex{},
		name:   name,
		engine: engine}
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
	rows, err := d.engine.Query("SELECT DISTINCT table_name FROM duckdb_tables() where schema_name = ?", d.name)
	if err != nil {
		return nil, ErrDuckDB.New(err)
	}
	defer rows.Close()

	tbls := make(map[string]sql.Table)
	for rows.Next() {
		var tblName string
		if err := rows.Scan(&tblName); err != nil {
			return nil, ErrDuckDB.New(err)
		}
		tbls[strings.ToLower(tblName)] = NewTable(tblName, d)
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
	for _, col := range schema.Schema {
		typ, err := duckdbDataType(col.Type)
		if err != nil {
			return err
		}
		colDef := fmt.Sprintf(`"%s" %s`, col.Name, typ)
		if col.Nullable {
			colDef += " NULL"
		} else {
			colDef += " NOT NULL"
		}

		if col.Default != nil {
			colDef += " DEFAULT " + col.Default.String()
		}

		columns = append(columns, colDef)
	}

	createTableSQL := fmt.Sprintf(`CREATE TABLE "%s"."%s" (%s`, d.name, name, strings.Join(columns, ", "))

	var primaryKeys []string
	for pkord := range schema.PkOrdinals {
		primaryKeys = append(primaryKeys, schema.Schema[pkord].Name)
	}

	if len(primaryKeys) > 0 {
		createTableSQL += fmt.Sprintf(", PRIMARY KEY (%s)", strings.Join(primaryKeys, ", "))
	}

	createTableSQL += ")"
	_, err := d.engine.Exec(createTableSQL)
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

	_, err := d.engine.Exec(fmt.Sprintf(`DROP TABLE "%s"."%s"`, d.name, name))

	if err != nil {
		return ErrDuckDB.New(err)
	}
	return nil
}

// RenameTable implements sql.TableRenamer.
func (d *Database) RenameTable(ctx *sql.Context, oldName string, newName string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.engine.Exec(fmt.Sprintf(`ALTER TABLE "%s"."%s" RENAME TO "%s"`, d.name, oldName, newName))
	if err != nil {
		return ErrDuckDB.New(err)
	}
	return nil
}

// AllViews implements sql.ViewDatabase.
func (d *Database) AllViews(ctx *sql.Context) ([]sql.ViewDefinition, error) {
	return nil, nil
}

// CreateView implements sql.ViewDatabase.
func (d *Database) CreateView(ctx *sql.Context, name string, selectStatement string, createViewStmt string) error {
	return sql.ErrViewsNotSupported.New(d.name)
}

// DropView implements sql.ViewDatabase.
func (d *Database) DropView(ctx *sql.Context, name string) error {
	return sql.ErrViewsNotSupported.New(d.name)
}

// GetViewDefinition implements sql.ViewDatabase.
func (d *Database) GetViewDefinition(ctx *sql.Context, viewName string) (sql.ViewDefinition, bool, error) {
	return sql.ViewDefinition{}, false, nil
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
