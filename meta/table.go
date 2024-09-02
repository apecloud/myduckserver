package meta

import (
	stdsql "database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/marcboeker/go-duckdb"
	"github.com/sirupsen/logrus"
)

type Table struct {
	mu      *sync.RWMutex
	name    string
	db      *Database
	comment *Comment // save the comment to avoid querying duckdb everytime
}

var _ sql.Table = (*Table)(nil)
var _ sql.PrimaryKeyTable = (*Table)(nil)
var _ sql.AlterableTable = (*Table)(nil)
var _ sql.IndexAlterableTable = (*Table)(nil)
var _ sql.IndexAddressableTable = (*Table)(nil)
var _ sql.InsertableTable = (*Table)(nil)
var _ sql.UpdatableTable = (*Table)(nil)
var _ sql.DeletableTable = (*Table)(nil)
var _ sql.CommentedTable = (*Table)(nil)

func NewTable(name string, db *Database) *Table {
	return &Table{
		mu:   &sync.RWMutex{},
		name: name,
		db:   db}
}

func (t *Table) WithComment(comment *Comment) *Table {
	t.comment = comment
	return t
}

// Collation implements sql.Table.
func (t *Table) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Name implements sql.Table.
func (t *Table) Name() string {
	return t.name
}

// PartitionRows implements sql.Table.
func (t *Table) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	return nil, fmt.Errorf("unimplemented(PartitionRows) (table: %s, query: %s)", t.name, ctx.Query())
}

// Partitions implements sql.Table.
func (t *Table) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return nil, fmt.Errorf("unimplemented(Partitions) (table: %s, query: %s)", t.name, ctx.Query())
}

// Schema implements sql.Table.
func (t *Table) Schema() sql.Schema {
	t.mu.RLock()
	defer t.mu.RUnlock()

	schema := t.schema()
	setPrimaryKeyColumns(schema, t.primaryKeyOrdinals())
	return schema
}

func (t *Table) schema() sql.Schema {
	rows, err := t.db.storage.Query(`
		SELECT column_name, data_type, is_nullable, column_default, comment, numeric_precision, numeric_scale FROM duckdb_columns() WHERE database_name = ? AND schema_name = ? AND table_name = ?
	`, t.db.catalogName, t.db.name, t.name)
	if err != nil {
		panic(ErrDuckDB.New(err))
	}
	defer rows.Close()

	var schema sql.Schema
	for rows.Next() {
		var columnName, dataType string
		var isNullable bool
		var columnDefault stdsql.NullString
		var comment stdsql.NullString
		var numericPrecision, numericScale stdsql.NullInt32
		if err := rows.Scan(&columnName, &dataType, &isNullable, &columnDefault, &comment, &numericPrecision, &numericScale); err != nil {
			panic(ErrDuckDB.New(err))
		}

		defaultValue := (*sql.ColumnDefaultValue)(nil)
		if columnDefault.Valid {
			defaultValue = sql.NewUnresolvedColumnDefaultValue(columnDefault.String)
		}

		decodedComment := DecodeComment(comment.String)

		column := &sql.Column{
			Name:           columnName,
			Type:           mysqlDataType(newDuckType(dataType, decodedComment.Meta), uint8(numericPrecision.Int32), uint8(numericScale.Int32)),
			Nullable:       isNullable,
			Source:         t.name,
			DatabaseSource: t.db.name,
			Default:        defaultValue,
			Comment:        decodedComment.Text,
		}

		schema = append(schema, column)
	}

	if err := rows.Err(); err != nil {
		panic(ErrDuckDB.New(err))
	}

	return schema
}

func setPrimaryKeyColumns(schema sql.Schema, ordinals []int) {
	for _, idx := range ordinals {
		schema[idx].PrimaryKey = true
	}
}

// String implements sql.Table.
func (t *Table) String() string {
	return t.name
}

// PrimaryKeySchema implements sql.PrimaryKeyTable.
func (t *Table) PrimaryKeySchema() sql.PrimaryKeySchema {
	t.mu.RLock()
	defer t.mu.RUnlock()

	schema := t.schema()
	ordinals := t.primaryKeyOrdinals()
	setPrimaryKeyColumns(schema, ordinals)
	return sql.NewPrimaryKeySchema(schema, ordinals...)
}

func (t *Table) primaryKeyOrdinals() []int {
	rows, err := t.db.storage.Query(`
		SELECT constraint_column_indexes FROM duckdb_constraints() WHERE database_name = ? AND schema_name = ? AND table_name = ? AND constraint_type = 'PRIMARY KEY' LIMIT 1
	`, t.db.catalogName, t.db.name, t.name)
	if err != nil {
		panic(ErrDuckDB.New(err))
	}
	defer rows.Close()

	var ordinals []int
	if rows.Next() {
		var arr duckdb.Composite[[]int]
		if err := rows.Scan(&arr); err != nil {
			panic(ErrDuckDB.New(err))
		}
		ordinals = arr.Get()
	}
	if err := rows.Err(); err != nil {
		panic(ErrDuckDB.New(err))
	}
	return ordinals
}

// AddColumn implements sql.AlterableTable.
func (t *Table) AddColumn(ctx *sql.Context, column *sql.Column, order *sql.ColumnOrder) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	typ, err := duckdbDataType(column.Type)
	if err != nil {
		return err
	}

	sql := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN "%s" %s`, FullTableName(t.db.catalogName, t.db.name, t.name), column.Name, typ.str)

	if !column.Nullable {
		sql += " NOT NULL"
	}

	if column.Default != nil {
		sql += fmt.Sprintf(" DEFAULT %s", column.Default.String())
	}

	// add comment
	comment := NewCommentWithMeta(column.Comment, typ.extra)
	sql += fmt.Sprintf(`; COMMENT ON COLUMN %s IS %s`, FullColumnName(t.db.catalogName, t.db.name, t.name, column.Name), comment.Encode())

	_, err = t.db.storage.Exec(sql)
	if err != nil {
		return ErrDuckDB.New(err)
	}

	return nil
}

// DropColumn implements sql.AlterableTable.
func (t *Table) DropColumn(ctx *sql.Context, columnName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	sql := fmt.Sprintf(`ALTER TABLE %s DROP COLUMN "%s"`, FullTableName(t.db.catalogName, t.db.name, t.name), columnName)

	_, err := t.db.storage.Exec(sql)
	if err != nil {
		return ErrDuckDB.New(err)
	}

	return nil
}

// ModifyColumn implements sql.AlterableTable.
func (t *Table) ModifyColumn(ctx *sql.Context, columnName string, column *sql.Column, order *sql.ColumnOrder) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	typ, err := duckdbDataType(column.Type)
	if err != nil {
		return err
	}

	baseSQL := fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s"`, FullTableName(t.db.catalogName, t.db.name, t.name), columnName)
	sqls := []string{
		fmt.Sprintf(`%s TYPE %s`, baseSQL, typ.str),
	}

	if column.Nullable {
		sqls = append(sqls, fmt.Sprintf(`%s DROP NOT NULL`, baseSQL))
	} else {
		sqls = append(sqls, fmt.Sprintf(`%s SET NOT NULL`, baseSQL))
	}

	if column.Default != nil {
		sqls = append(sqls, fmt.Sprintf(`%s SET DEFAULT %s`, baseSQL, column.Default.String()))
	} else {
		sqls = append(sqls, fmt.Sprintf(`%s DROP DEFAULT`, baseSQL))
	}

	if columnName != column.Name {
		sqls = append(sqls, fmt.Sprintf(`ALTER TABLE %s RENAME "%s" TO "%s"`, FullTableName(t.db.catalogName, t.db.name, t.name), columnName, column.Name))
	}

	// alter comment
	comment := NewCommentWithMeta(column.Comment, typ.extra)
	sqls = append(sqls, fmt.Sprintf(`COMMENT ON COLUMN %s IS %s`, FullColumnName(t.db.catalogName, t.db.name, t.name, column.Name), comment.Encode()))

	joinedSQL := strings.Join(sqls, "; ")
	_, err = t.db.storage.Exec(joinedSQL)
	if err != nil {
		logrus.Errorf("run duckdb sql failed: %s", joinedSQL)
		return ErrDuckDB.New(err)
	}

	return nil
}

// Updater implements sql.AlterableTable.
func (t *Table) Updater(ctx *sql.Context) sql.RowUpdater {
	// Called when altering a table’s default value. No update needed as DuckDB handles it internally.
	return nil
}

// Inserter implements sql.InsertableTable.
func (t *Table) Inserter(*sql.Context) sql.RowInserter {
	panic("unimplemented")
}

// Deleter implements sql.DeletableTable.
func (t *Table) Deleter(*sql.Context) sql.RowDeleter {
	panic("unimplemented")
}

// CreateIndex implements sql.IndexAlterableTable.
func (t *Table) CreateIndex(ctx *sql.Context, indexDef sql.IndexDef) error {
	// Lock the table to ensure thread-safety during index creation
	t.mu.Lock()
	defer t.mu.Unlock()

	if indexDef.IsPrimary() {
		return fmt.Errorf("primary key cannot be created with CreateIndex, use ALTER TABLE ... ADD PRIMARY KEY instead")
	}

	if indexDef.IsSpatial() {
		return fmt.Errorf("spatial indexes are not supported")
	}

	if indexDef.IsFullText() {
		return fmt.Errorf("full text indexes are not supported")
	}

	// Prepare the column names for the index
	columns := make([]string, len(indexDef.Columns))
	for i, col := range indexDef.Columns {
		columns[i] = fmt.Sprintf(`"%s"`, col.Name)
	}

	unique := ""
	if indexDef.IsUnique() {
		unique = "UNIQUE"
	}

	// Construct the SQL statement for creating the index
	// DuckDB has very strange behavior that can't use full index name, so we have to switch context to the schema by USE statement
	sql := fmt.Sprintf(`USE %s; CREATE %s INDEX "%s" ON %s (%s)`,
		FullSchemaName(t.db.catalogName, t.db.name),
		unique,
		indexDef.Name,
		FullTableName(t.db.catalogName, t.db.name, t.name),
		strings.Join(columns, ", "))

	// Add the index comment if provided
	if indexDef.Comment != "" {
		sql += fmt.Sprintf("; COMMENT ON INDEX %s IS %s",
			FullIndexName(t.db.catalogName, t.db.name, indexDef.Name),
			NewComment(indexDef.Comment).Encode())
	}

	// Execute the SQL statement to create the index
	_, err := t.db.storage.Exec(sql)
	if err != nil {
		return ErrDuckDB.New(err)
	}

	return nil
}

// DropIndex implements sql.IndexAlterableTable.
func (t *Table) DropIndex(ctx *sql.Context, indexName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Construct the SQL statement for dropping the index
	// DuckDB requires switching context to the schema by USE statement
	sql := fmt.Sprintf(`USE %s; DROP INDEX "%s"`,
		FullSchemaName(t.db.catalogName, t.db.name),
		indexName)

	// Execute the SQL statement to drop the index
	_, err := t.db.storage.Exec(sql)
	if err != nil {
		return ErrDuckDB.New(err)
	}

	return nil
}

// RenameIndex implements sql.IndexAlterableTable.
func (t *Table) RenameIndex(ctx *sql.Context, fromIndexName string, toIndexName string) error {
	return sql.ErrUnsupportedFeature.New("RenameIndex is not supported")
}

// GetIndexes implements sql.IndexAddressableTable.
// This is only used for show index in SHOW INDEX and SHOW CREATE TABLE.
func (t *Table) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Query to get the indexes for the table
	rows, err := t.db.storage.Query(`SELECT index_name, is_unique, comment FROM duckdb_indexes() WHERE database_name = ? AND schema_name = ? AND table_name = ?`,
		t.db.catalogName, t.db.name, t.name)
	if err != nil {
		return nil, ErrDuckDB.New(err)
	}
	defer rows.Close()

	indexes := []sql.Index{}
	for rows.Next() {
		var indexName string
		var comment stdsql.NullString
		var isUnique bool

		if err := rows.Scan(&indexName, &isUnique, &comment); err != nil {
			return nil, ErrDuckDB.New(err)
		}

		indexes = append(indexes, NewIndex(t.db.name, t.name, indexName, isUnique, DecodeComment(comment.String)))
	}

	if err := rows.Err(); err != nil {
		return nil, ErrDuckDB.New(err)
	}

	return indexes, nil
}

// IndexedAccess implements sql.IndexAddressableTable.
func (t *Table) IndexedAccess(lookup sql.IndexLookup) sql.IndexedTable {
	panic("unimplemented")
}

// PreciseMatch implements sql.IndexAddressableTable.
func (t *Table) PreciseMatch() bool {
	panic("unimplemented")
}

// Comment implements sql.CommentedTable.
func (t *Table) Comment() string {
	return t.comment.Text
}
