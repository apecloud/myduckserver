package catalog

import (
	stdsql "database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/marcboeker/go-duckdb"
	"github.com/sirupsen/logrus"
)

type Table struct {
	mu      *sync.RWMutex
	name    string
	db      *Database
	comment *Comment[any] // save the comment to avoid querying duckdb everytime
	schema  sql.PrimaryKeySchema
}

type ColumnInfo struct {
	ColumnName    string
	ColumnIndex   int
	DataType      sql.Type
	IsNullable    bool
	ColumnDefault stdsql.NullString
	Comment       stdsql.NullString
}
type IndexedTable struct {
	*Table
	Lookup sql.IndexLookup
}

var _ sql.Table = (*Table)(nil)
var _ sql.PrimaryKeyTable = (*Table)(nil)
var _ sql.AlterableTable = (*Table)(nil)
var _ sql.IndexAlterableTable = (*Table)(nil)
var _ sql.IndexAddressableTable = (*Table)(nil)
var _ sql.InsertableTable = (*Table)(nil)
var _ sql.UpdatableTable = (*Table)(nil)
var _ sql.DeletableTable = (*Table)(nil)
var _ sql.ReplaceableTable = (*Table)(nil)
var _ sql.CommentedTable = (*Table)(nil)

func NewTable(name string, db *Database) *Table {
	return &Table{
		mu:   &sync.RWMutex{},
		name: name,
		db:   db,
	}
}

func (t *Table) WithComment(comment *Comment[any]) *Table {
	t.comment = comment
	return t
}

func (t *Table) WithSchema(ctx *sql.Context) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.schema = getPKSchema(ctx, t.db.catalog, t.db.name, t.name)
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
	return sql.PartitionsToPartitionIter(), nil
}

// Schema implements sql.Table.
func (t *Table) Schema() sql.Schema {
	return t.schema.Schema
}

func getPKSchema(ctx *sql.Context, catalogName, dbName, tableName string) sql.PrimaryKeySchema {
	var schema sql.Schema

	columns, err := queryColumns(ctx, catalogName, dbName, tableName)
	if err != nil {
		panic(ErrDuckDB.New(err))
	}

	for _, columnInfo := range columns {
		decodedComment := DecodeComment[MySQLType](columnInfo.Comment.String)

		defaultValue := (*sql.ColumnDefaultValue)(nil)
		if columnInfo.ColumnDefault.Valid {
			defaultValue = sql.NewUnresolvedColumnDefaultValue(decodedComment.Meta.Default)
		}

		column := &sql.Column{
			Name:           columnInfo.ColumnName,
			Type:           columnInfo.DataType,
			Nullable:       columnInfo.IsNullable,
			Source:         tableName,
			DatabaseSource: dbName,
			Default:        defaultValue,
			Comment:        decodedComment.Text,
		}

		schema = append(schema, column)
	}

	// Add primary key columns to the schema
	primaryKeyOrdinals := getPrimaryKeyOrdinals(ctx, catalogName, dbName, tableName)
	setPrimaryKeyColumns(schema, primaryKeyOrdinals)

	return sql.NewPrimaryKeySchema(schema, primaryKeyOrdinals...)
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
	return t.schema
}

func getPrimaryKeyOrdinals(ctx *sql.Context, catalogName, dbName, tableName string) []int {
	rows, err := adapter.QueryCatalog(ctx, `
		SELECT constraint_column_indexes FROM duckdb_constraints() WHERE ((database_name = ? AND schema_name = ? AND table_name = ?) OR (database_name = 'temp' AND schema_name = 'main' AND table_name = ?)) AND constraint_type = 'PRIMARY KEY' LIMIT 1
	`, catalogName, dbName, tableName, tableName)
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

	typ, err := DuckdbDataType(column.Type)
	if err != nil {
		return err
	}

	sql := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN "%s" %s`, FullTableName(t.db.catalog, t.db.name, t.name), column.Name, typ.name)

	if !column.Nullable {
		sql += " NOT NULL"
	}

	if column.Default != nil {
		columnDefault, err := typ.mysql.withDefault(column.Default.String())
		if err != nil {
			return err
		}
		sql += fmt.Sprintf(" DEFAULT %s", columnDefault)
	}

	// add comment
	comment := NewCommentWithMeta(column.Comment, typ.mysql)
	sql += fmt.Sprintf(`; COMMENT ON COLUMN %s IS '%s'`, FullColumnName(t.db.catalog, t.db.name, t.name, column.Name), comment.Encode())

	_, err = adapter.Exec(ctx, sql)
	if err != nil {
		return ErrDuckDB.New(err)
	}

	return nil
}

// DropColumn implements sql.AlterableTable.
func (t *Table) DropColumn(ctx *sql.Context, columnName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	sql := fmt.Sprintf(`ALTER TABLE %s DROP COLUMN "%s"`, FullTableName(t.db.catalog, t.db.name, t.name), columnName)

	_, err := adapter.Exec(ctx, sql)
	if err != nil {
		return ErrDuckDB.New(err)
	}

	return nil
}

// ModifyColumn implements sql.AlterableTable.
func (t *Table) ModifyColumn(ctx *sql.Context, columnName string, column *sql.Column, order *sql.ColumnOrder) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	typ, err := DuckdbDataType(column.Type)
	if err != nil {
		return err
	}

	baseSQL := fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s"`, FullTableName(t.db.catalog, t.db.name, t.name), columnName)
	sqls := []string{
		fmt.Sprintf(`%s TYPE %s`, baseSQL, typ.name),
	}

	if column.Nullable {
		sqls = append(sqls, fmt.Sprintf(`%s DROP NOT NULL`, baseSQL))
	} else {
		sqls = append(sqls, fmt.Sprintf(`%s SET NOT NULL`, baseSQL))
	}

	if column.Default != nil {
		columnDefault, err := typ.mysql.withDefault(column.Default.String())
		if err != nil {
			return err
		}
		sqls = append(sqls, fmt.Sprintf(`%s SET DEFAULT %s`, baseSQL, columnDefault))
	} else {
		sqls = append(sqls, fmt.Sprintf(`%s DROP DEFAULT`, baseSQL))
	}

	if columnName != column.Name {
		sqls = append(sqls, fmt.Sprintf(`ALTER TABLE %s RENAME "%s" TO "%s"`, FullTableName(t.db.catalog, t.db.name, t.name), columnName, column.Name))
	}

	// alter comment
	comment := NewCommentWithMeta(column.Comment, typ.mysql)
	sqls = append(sqls, fmt.Sprintf(`COMMENT ON COLUMN %s IS '%s'`, FullColumnName(t.db.catalog, t.db.name, t.name, column.Name), comment.Encode()))

	joinedSQL := strings.Join(sqls, "; ")
	_, err = adapter.Exec(ctx, joinedSQL)
	if err != nil {
		logrus.Errorf("run duckdb sql failed: %s", joinedSQL)
		return ErrDuckDB.New(err)
	}

	return nil
}

type EmptyTableEditor struct {
}

// Close implements sql.RowUpdater.
func (e *EmptyTableEditor) Close(*sql.Context) error {
	return nil
}

// DiscardChanges implements sql.RowUpdater.
func (e *EmptyTableEditor) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	panic("unimplemented")
}

// StatementBegin implements sql.RowUpdater.
func (e *EmptyTableEditor) StatementBegin(ctx *sql.Context) {
	panic("unimplemented")
}

// StatementComplete implements sql.RowUpdater.
func (e *EmptyTableEditor) StatementComplete(ctx *sql.Context) error {
	panic("unimplemented")
}

// Update implements sql.RowUpdater.
func (e *EmptyTableEditor) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	panic("unimplemented")
}

var _ sql.RowUpdater = (*EmptyTableEditor)(nil)

// Updater implements sql.AlterableTable.
func (t *Table) Updater(ctx *sql.Context) sql.RowUpdater {
	// Called when altering a table’s default value. No update needed as DuckDB handles it internally.
	return &EmptyTableEditor{}
}

// Inserter implements sql.InsertableTable.
func (t *Table) Inserter(*sql.Context) sql.RowInserter {
	return &rowInserter{
		db:     t.db.Name(),
		table:  t.name,
		schema: t.schema.Schema,
	}
}

// Deleter implements sql.DeletableTable.
func (t *Table) Deleter(*sql.Context) sql.RowDeleter {
	return nil
}

// Replacer implements sql.ReplaceableTable.
func (t *Table) Replacer(*sql.Context) sql.RowReplacer {
	hasKey := len(t.schema.PkOrdinals) > 0 || !sql.IsKeyless(t.schema.Schema)
	return &rowInserter{
		db:      t.db.Name(),
		table:   t.name,
		schema:  t.schema.Schema,
		replace: hasKey,
	}
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
	var sqlsBuilder strings.Builder
	sqlsBuilder.WriteString(fmt.Sprintf(`USE %s; `, FullSchemaName(t.db.catalog, "")))
	sqlsBuilder.WriteString(fmt.Sprintf(`CREATE %s INDEX "%s" ON %s (%s)`,
		unique,
		EncodeIndexName(t.name, indexDef.Name),
		FullTableName("", t.db.name, t.name),
		strings.Join(columns, ", ")))

	// Add the index comment if provided
	if indexDef.Comment != "" {
		sqlsBuilder.WriteString(fmt.Sprintf("; COMMENT ON INDEX %s IS '%s'",
			FullIndexName(t.db.catalog, t.db.name, EncodeIndexName(t.name, indexDef.Name)),
			NewComment[any](indexDef.Comment).Encode()))
	}

	// Execute the SQL statement to create the index
	_, err := adapter.Exec(ctx, sqlsBuilder.String())
	if err != nil {
		if IsDuckDBIndexAlreadyExistsError(err) {
			return sql.ErrDuplicateKey.New(indexDef.Name)
		}
		if IsDuckDBUniqueConstraintViolationError(err) {
			return sql.ErrUniqueKeyViolation.New()
		}

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
		FullSchemaName(t.db.catalog, t.db.name),
		EncodeIndexName(t.name, indexName))

	// Execute the SQL statement to drop the index
	_, err := adapter.Exec(ctx, sql)
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
	rows, err := adapter.QueryCatalog(ctx, `SELECT index_name, is_unique, comment, sql FROM duckdb_indexes() WHERE (database_name = ? AND schema_name = ? AND table_name = ?) or (database_name = 'temp' AND schema_name = 'main' AND table_name = ?)`,
		t.db.catalog, t.db.name, t.name, t.name)
	if err != nil {
		return nil, ErrDuckDB.New(err)
	}
	defer rows.Close()

	indexes := []sql.Index{}

	// Primary key is not returned by duckdb_indexes()
	sch, pkOrds := t.schema.Schema, t.schema.PkOrdinals
	if len(pkOrds) > 0 {
		pkExprs := make([]sql.Expression, len(pkOrds))
		for i, ord := range pkOrds {
			pkExprs[i] = expression.NewGetFieldWithTable(ord, 0, sch[ord].Type, t.db.name, t.name, sch[ord].Name, sch[ord].Nullable)
		}
		indexes = append(indexes, NewIndex(t.db.name, t.name, "PRIMARY", true, NewComment[any](""), pkExprs))
	}

	columnsInfo, err := queryColumns(ctx, t.db.catalog, t.db.name, t.name)
	columnsInfoMap := make(map[string]*ColumnInfo)
	for _, columnInfo := range columnsInfo {
		columnsInfoMap[columnInfo.ColumnName] = columnInfo
	}

	if err != nil {
		return nil, ErrDuckDB.New(err)
	}

	for rows.Next() {
		var encodedIndexName string
		var comment stdsql.NullString
		var isUnique bool
		var createIndexSQL string
		var exprs []sql.Expression

		if err := rows.Scan(&encodedIndexName, &isUnique, &comment, &createIndexSQL); err != nil {
			return nil, ErrDuckDB.New(err)
		}

		_, indexName := DecodeIndexName(encodedIndexName)
		columnNames, err := DecodeCreateindex(createIndexSQL)
		if err != nil {
			return nil, ErrDuckDB.New(err)
		}

		for _, columnName := range columnNames {
			if columnInfo, exists := columnsInfoMap[columnName]; exists {
				exprs = append(exprs, expression.NewGetFieldWithTable(columnInfo.ColumnIndex, 0, columnInfo.DataType, t.db.name, t.name, columnInfo.ColumnName, columnInfo.IsNullable))
			}
		}

		indexes = append(indexes, NewIndex(t.db.name, t.name, indexName, isUnique, DecodeComment[any](comment.String), exprs))
	}

	if err := rows.Err(); err != nil {
		return nil, ErrDuckDB.New(err)
	}

	return indexes, nil
}

// IndexedAccess implements sql.IndexAddressableTable.
func (t *Table) IndexedAccess(lookup sql.IndexLookup) sql.IndexedTable {
	return &IndexedTable{Table: t, Lookup: lookup}
}

// PreciseMatch implements sql.IndexAddressableTable.
func (t *Table) PreciseMatch() bool {
	return true
}

// Comment implements sql.CommentedTable.
func (t *Table) Comment() string {
	return t.comment.Text
}

func queryColumns(ctx *sql.Context, catalogName, schemaName, tableName string) ([]*ColumnInfo, error) {
	rows, err := adapter.QueryCatalog(ctx, `
		SELECT column_name, column_index, data_type, is_nullable, column_default, comment, numeric_precision, numeric_scale
		FROM duckdb_columns()
		WHERE (database_name = ? AND schema_name = ? AND table_name = ?) OR (database_name = 'temp' AND schema_name = 'main' AND table_name = ?)
	`, catalogName, schemaName, tableName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columnsInfo []*ColumnInfo

	for rows.Next() {
		var columnName, dataTypes string
		var columnIndex int
		var isNullable bool
		var comment, columnDefault stdsql.NullString
		var numericPrecision, numericScale stdsql.NullInt32

		if err := rows.Scan(&columnName, &columnIndex, &dataTypes, &isNullable, &columnDefault, &comment, &numericPrecision, &numericScale); err != nil {
			return nil, err
		}

		decodedComment := DecodeComment[MySQLType](comment.String)
		dataType := mysqlDataType(AnnotatedDuckType{dataTypes, decodedComment.Meta}, uint8(numericPrecision.Int32), uint8(numericScale.Int32))

		columnInfo := &ColumnInfo{
			ColumnName:    columnName,
			ColumnIndex:   columnIndex,
			DataType:      dataType,
			IsNullable:    isNullable,
			ColumnDefault: columnDefault,
			Comment:       comment,
		}
		columnsInfo = append(columnsInfo, columnInfo)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return columnsInfo, nil
}

func (t *IndexedTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	return nil, fmt.Errorf("unimplemented(LookupPartitions) (table: %s, query: %s)", t.name, ctx.Query())
}
