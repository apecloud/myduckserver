package catalog

import (
	stdsql "database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/configuration"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/google/uuid"
	"github.com/marcboeker/go-duckdb"
)

type Table struct {
	mu      sync.RWMutex
	db      *Database
	name    string
	comment *Comment[ExtraTableInfo] // save the comment to avoid querying duckdb every time
	schema  sql.PrimaryKeySchema

	// Whether the table has a physical primary key.
	hasPrimaryKey bool
}

type ExtraTableInfo struct {
	PkOrdinals []int
	Replicated bool
	Sequence   string
	Checks     []sql.CheckDefinition
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
var _ sql.TruncateableTable = (*Table)(nil)
var _ sql.ReplaceableTable = (*Table)(nil)
var _ sql.CommentedTable = (*Table)(nil)
var _ sql.AutoIncrementTable = (*Table)(nil)
var _ sql.CheckTable = (*Table)(nil)
var _ sql.CheckAlterableTable = (*Table)(nil)

func NewTable(db *Database, name string, hasPrimaryKey bool) *Table {
	return &Table{
		db:   db,
		name: name,

		hasPrimaryKey: hasPrimaryKey,
	}
}

func (t *Table) withComment(comment *Comment[ExtraTableInfo]) *Table {
	t.comment = comment
	return t
}

func (t *Table) withSchema(ctx *sql.Context) error {
	schema, err := getPKSchema(ctx, t.db.catalog, t.db.name, t.name)
	if err != nil {
		return err
	}

	t.schema = schema

	// https://github.com/apecloud/myduckserver/issues/272
	if len(t.schema.PkOrdinals) == 0 && configuration.IsReplicationWithoutIndex() {
		// Pretend that the primary key exists
		for _, idx := range t.comment.Meta.PkOrdinals {
			t.schema.Schema[idx].PrimaryKey = true
		}
		t.schema = sql.NewPrimaryKeySchema(t.schema.Schema, t.comment.Meta.PkOrdinals...)
	}

	return nil
}

func (t *Table) ExtraTableInfo() ExtraTableInfo {
	return t.comment.Meta
}

func (t *Table) HasPrimaryKey() bool {
	return t.hasPrimaryKey
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

func getPKSchema(ctx *sql.Context, catalogName, dbName, tableName string) (sql.PrimaryKeySchema, error) {
	var schema sql.Schema

	columns, err := queryColumns(ctx, catalogName, dbName, tableName)
	if err != nil {
		return sql.PrimaryKeySchema{}, ErrDuckDB.New(err)
	}

	for _, columnInfo := range columns {
		decodedComment := DecodeComment[MySQLType](columnInfo.Comment.String)

		defaultValue := (*sql.ColumnDefaultValue)(nil)
		if columnInfo.ColumnDefault.Valid && decodedComment.Meta.Default != "" {
			defaultValue = sql.NewUnresolvedColumnDefaultValue(decodedComment.Meta.Default)
		}

		var extra string
		if decodedComment.Meta.AutoIncrement {
			extra = "auto_increment"
		}

		column := &sql.Column{
			Name:           columnInfo.ColumnName,
			Type:           columnInfo.DataType,
			Nullable:       columnInfo.IsNullable,
			Source:         tableName,
			DatabaseSource: dbName,
			Default:        defaultValue,
			AutoIncrement:  decodedComment.Meta.AutoIncrement,
			Comment:        decodedComment.Text,
			Extra:          extra,
		}

		schema = append(schema, column)
	}

	// Add primary key columns to the schema
	primaryKeyOrdinals := getPrimaryKeyOrdinals(ctx, catalogName, dbName, tableName)
	setPrimaryKeyColumns(schema, primaryKeyOrdinals)

	return sql.NewPrimaryKeySchema(schema, primaryKeyOrdinals...), nil
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

func getCreateSequence(temporary bool, sequenceName string) (createStmt, fullName string) {
	if temporary {
		return `CREATE TEMP SEQUENCE "` + sequenceName + `"`, `temp.main."` + sequenceName + `"`
	}
	fullName = InternalSchemas.SYS.Schema + `."` + sequenceName + `"`
	return `CREATE SEQUENCE ` + fullName, fullName
}

// AddColumn implements sql.AlterableTable.
func (t *Table) AddColumn(ctx *sql.Context, column *sql.Column, order *sql.ColumnOrder) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// TODO: Column order is ignored as DuckDB does not support it.

	typ, err := DuckdbDataType(column.Type)
	if err != nil {
		return err
	}

	var sqls []string
	sql := `ALTER TABLE ` + FullTableName(t.db.catalog, t.db.name, t.name) + ` ADD COLUMN ` + QuoteIdentifierANSI(column.Name) + ` ` + typ.name

	temporary := t.db.catalog == "temp"
	var sequenceName, fullSequenceName, createSequenceStmt string

	if column.Default != nil {
		typ.mysql.Default = column.Default.String()
		defaultExpr, err := parseDefaultValue(typ.mysql.Default)
		if err != nil {
			return err
		}
		sql += " DEFAULT " + defaultExpr
	} else if column.AutoIncrement {
		typ.mysql.AutoIncrement = true

		// Generate a random sequence name.
		uuid, err := uuid.NewRandom()
		if err != nil {
			return err
		}
		sequenceName = SequenceNamePrefix + uuid.String()
		createSequenceStmt, fullSequenceName = getCreateSequence(temporary, sequenceName)
		sqls = append(sqls, createSequenceStmt)

		defaultExpr := `nextval('` + fullSequenceName + `')`
		sql += " DEFAULT " + defaultExpr
	}

	sqls = append(sqls, sql)

	// DuckDB does not support constraints in ALTER TABLE ADD COLUMN statement,
	// so we need to add NOT NULL constraint separately.
	// > Parser Error: Adding columns with constraints not yet supported
	if !column.Nullable {
		sqls = append(sqls, `ALTER TABLE `+FullTableName(t.db.catalog, t.db.name, t.name)+` ALTER COLUMN `+QuoteIdentifierANSI(column.Name)+` SET NOT NULL`)
	}

	// Add column comment
	comment := NewCommentWithMeta(column.Comment, typ.mysql)
	sqls = append(sqls, `COMMENT ON COLUMN `+FullColumnName(t.db.catalog, t.db.name, t.name, column.Name)+` IS '`+comment.Encode()+`'`)

	// Add table comment if it is AUTO_INCREMENT or PRIMARY KEY
	tableInfo := t.comment.Meta
	tableInfoChanged := false
	if column.AutoIncrement {
		tableInfo.Sequence = fullSequenceName
		tableInfoChanged = true
	}
	if column.PrimaryKey {
		sqls = append(sqls, `ALTER TABLE `+FullTableName(t.db.catalog, t.db.name, t.name)+` ADD PRIMARY KEY (`+QuoteIdentifierANSI(column.Name)+`)`)
		tableInfo.PkOrdinals = []int{len(t.schema.Schema)}
		tableInfoChanged = true
	}
	if tableInfoChanged {
		comment := NewCommentWithMeta(t.comment.Text, tableInfo)
		sqls = append(sqls, `COMMENT ON TABLE `+FullTableName(t.db.catalog, t.db.name, t.name)+` IS '`+comment.Encode()+`'`)
	}

	_, err = adapter.Exec(ctx, strings.Join(sqls, "; "))
	if err != nil {
		return ErrDuckDB.New(err)
	}

	// Update the sequence name only after the column is successfully added.
	if column.AutoIncrement {
		t.comment.Meta.Sequence = tableInfo.Sequence
	}
	// Update the PK ordinals only after the column is successfully added.
	if column.PrimaryKey {
		t.hasPrimaryKey = true
		t.comment.Meta.PkOrdinals = tableInfo.PkOrdinals
	}
	return t.withSchema(ctx)
}

// DropColumn implements sql.AlterableTable.
func (t *Table) DropColumn(ctx *sql.Context, columnName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if the column is AUTO_INCREMENT
	autoIncrement := false
	for _, column := range t.schema.Schema {
		if column.AutoIncrement && strings.EqualFold(column.Name, columnName) {
			autoIncrement = true
			break
		}
	}

	sql := `ALTER TABLE ` + FullTableName(t.db.catalog, t.db.name, t.name) + ` DROP COLUMN ` + QuoteIdentifierANSI(columnName)

	if autoIncrement {
		// Drop the sequence
		sql += `; DROP SEQUENCE IF EXISTS ` + t.comment.Meta.Sequence
		// Remove the sequence name from the table comment
		extraInfo := t.comment.Meta
		extraInfo.Sequence = ""
		comment := NewCommentWithMeta(t.comment.Text, extraInfo)
		sql += `; COMMENT ON TABLE ` + FullTableName(t.db.catalog, t.db.name, t.name) + ` IS '` + comment.Encode() + `'`
	}

	_, err := adapter.Exec(ctx, sql)
	if err != nil {
		return ErrDuckDB.New(err)
	}

	// Update the sequence name only after the column is successfully dropped.
	if autoIncrement {
		t.comment.Meta.Sequence = ""
	}
	return t.withSchema(ctx)
}

// ModifyColumn implements sql.AlterableTable.
func (t *Table) ModifyColumn(ctx *sql.Context, columnName string, column *sql.Column, order *sql.ColumnOrder) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	typ, err := DuckdbDataType(column.Type)
	if err != nil {
		return err
	}

	// Find existing column to check for AUTO_INCREMENT and PRIMARY KEY
	var oldColumn *sql.Column
	var oldColumnIndex int
	for i, col := range t.schema.Schema {
		if strings.EqualFold(col.Name, columnName) {
			oldColumnIndex, oldColumn = i, col
			break
		}
	}
	if oldColumn == nil {
		return sql.ErrColumnNotFound.New(columnName)
	}

	baseSQL := `ALTER TABLE ` + FullTableName(t.db.catalog, t.db.name, t.name) + ` ALTER COLUMN ` + QuoteIdentifierANSI(columnName)
	var sqls []string

	// Add type modification
	if !oldColumn.Type.Equals(column.Type) {
		sqls = append(sqls, baseSQL+` TYPE `+typ.name)
	}

	// Handle nullability
	if oldColumn.Nullable && !column.Nullable {
		sqls = append(sqls, baseSQL+` SET NOT NULL`)
	} else if !oldColumn.Nullable && column.Nullable {
		sqls = append(sqls, baseSQL+` DROP NOT NULL`)
	}

	// Handle default value if not AUTO_INCREMENT
	if !column.AutoIncrement && column.Default != nil {
		typ.mysql.Default = column.Default.String()
		defaultExpr, err := parseDefaultValue(typ.mysql.Default)
		if err != nil {
			return err
		}
		sqls = append(sqls, baseSQL+` SET DEFAULT `+defaultExpr)
	}

	tableInfo := t.comment.Meta
	tableInfoChanged := false

	temporary := t.db.catalog == "temp"
	var sequenceName, fullSequenceName, createSequenceStmt string

	// Handle AUTO_INCREMENT changes
	if !oldColumn.AutoIncrement && column.AutoIncrement {
		// Adding AUTO_INCREMENT
		typ.mysql.AutoIncrement = true
		uuid, err := uuid.NewRandom()
		if err != nil {
			return err
		}
		sequenceName = SequenceNamePrefix + uuid.String()
		createSequenceStmt, fullSequenceName = getCreateSequence(temporary, sequenceName)
		sqls = append(sqls, createSequenceStmt)
		sqls = append(sqls, baseSQL+` SET DEFAULT nextval('`+fullSequenceName+`')`)

		// Update table comment with sequence info
		tableInfo.Sequence = fullSequenceName
		tableInfoChanged = true

	} else if oldColumn.AutoIncrement && !column.AutoIncrement {
		// Removing AUTO_INCREMENT
		sqls = append(sqls, baseSQL+` DROP DEFAULT`)

		// https://github.com/duckdb/duckdb/issues/15399
		// sqls = append(sqls, `DROP SEQUENCE IF EXISTS `+t.comment.Meta.Sequence)

		// Update table comment to remove sequence info
		tableInfo.Sequence = ""
		tableInfoChanged = true
	}

	// Handle column rename
	if columnName != column.Name {
		sqls = append(sqls, `ALTER TABLE `+FullTableName(t.db.catalog, t.db.name, t.name)+` RENAME `+QuoteIdentifierANSI(columnName)+` TO `+QuoteIdentifierANSI(column.Name))
	}

	// Update column comment
	comment := NewCommentWithMeta(column.Comment, typ.mysql)
	sqls = append(sqls, `COMMENT ON COLUMN `+FullColumnName(t.db.catalog, t.db.name, t.name, column.Name)+` IS '`+comment.Encode()+`'`)

	// Handle PRIMARY KEY changes
	if !oldColumn.PrimaryKey && column.PrimaryKey {
		// Adding PRIMARY KEY
		// This feature will be available in DuckDB 1.2.0:
		// https://github.com/duckdb/duckdb/pull/14419
		sqls = append(sqls, `ALTER TABLE `+FullTableName(t.db.catalog, t.db.name, t.name)+` ADD PRIMARY KEY (`+QuoteIdentifierANSI(column.Name)+`)`)

		// Update table comment with PK ordinals
		tableInfo.PkOrdinals = []int{oldColumnIndex}
		tableInfoChanged = true

	} else if oldColumn.PrimaryKey && !column.PrimaryKey {
		// Remove PRIMARY KEY?
	}

	if tableInfoChanged {
		comment := NewCommentWithMeta(t.comment.Text, tableInfo)
		sqls = append(sqls, `COMMENT ON TABLE `+FullTableName(t.db.catalog, t.db.name, t.name)+` IS '`+comment.Encode()+`'`)
	}

	joinedSQL := strings.Join(sqls, "; ")
	_, err = adapter.Exec(ctx, joinedSQL)
	if err != nil {
		ctx.GetLogger().WithError(err).Errorf("Failed to execute DuckDB SQL: %s", joinedSQL)
		return ErrDuckDB.New(err)
	}

	// Update table metadata
	if column.PrimaryKey {
		t.hasPrimaryKey = true
		t.comment.Meta.PkOrdinals = []int{oldColumnIndex}
	}
	if !oldColumn.AutoIncrement && column.AutoIncrement {
		t.comment.Meta.Sequence = fullSequenceName
	} else if oldColumn.AutoIncrement && !column.AutoIncrement {
		t.comment.Meta.Sequence = ""
	}

	return t.withSchema(ctx)
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
		hasPK:  t.hasPrimaryKey,
	}
}

// Deleter implements sql.DeletableTable.
func (t *Table) Deleter(*sql.Context) sql.RowDeleter {
	return nil
}

// Truncate implements sql.TruncateableTable.
func (t *Table) Truncate(ctx *sql.Context) (int, error) {
	result, err := adapter.ExecCatalog(ctx, `TRUNCATE TABLE `+FullTableName(t.db.catalog, t.db.name, t.name))
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	return int(affected), err
}

// Replacer implements sql.ReplaceableTable.
func (t *Table) Replacer(*sql.Context) sql.RowReplacer {
	hasKey := len(t.schema.PkOrdinals) > 0 || !sql.IsKeyless(t.schema.Schema)
	return &rowInserter{
		db:      t.db.Name(),
		table:   t.name,
		schema:  t.schema.Schema,
		hasPK:   t.hasPrimaryKey,
		replace: hasKey,
	}
}

// CreateIndex implements sql.IndexAlterableTable.
func (t *Table) CreateIndex(ctx *sql.Context, indexDef sql.IndexDef) error {
	// Lock the table to ensure thread-safety during index creation
	t.mu.Lock()
	defer t.mu.Unlock()

	// https://github.com/apecloud/myduckserver/issues/272
	if isIndexCreationDisabled(ctx) {
		return nil
	}

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
	var b strings.Builder
	b.WriteString(fmt.Sprintf(`USE %s; `, FullSchemaName(t.db.catalog, "")))
	b.WriteString(fmt.Sprintf(`CREATE %s INDEX "%s" ON %s (%s)`,
		unique,
		EncodeIndexName(t.name, indexDef.Name),
		FullTableName("", t.db.name, t.name),
		strings.Join(columns, ", ")))

	// Add the index comment if provided
	if indexDef.Comment != "" {
		b.WriteString(fmt.Sprintf("; COMMENT ON INDEX %s IS '%s'",
			FullIndexName(t.db.catalog, t.db.name, EncodeIndexName(t.name, indexDef.Name)),
			NewComment[any](indexDef.Comment).Encode()))
	}

	// Execute the SQL statement to create the index
	_, err := adapter.Exec(ctx, b.String())
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
	t.mu.RLock()
	defer t.mu.RUnlock()

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

	var columns []*ColumnInfo

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
		columns = append(columns, columnInfo)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return columns, nil
}

func (t *IndexedTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	return nil, fmt.Errorf("unimplemented(LookupPartitions) (table: %s, query: %s)", t.name, ctx.Query())
}

// PeekNextAutoIncrementValue implements sql.AutoIncrementTable.
func (t *Table) PeekNextAutoIncrementValue(ctx *sql.Context) (uint64, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.comment.Meta.Sequence == "" {
		return 0, sql.ErrNoAutoIncrementCol
	}
	return t.getNextAutoIncrementValue(ctx)
}

func (t *Table) getNextAutoIncrementValue(ctx *sql.Context) (uint64, error) {
	// For PeekNextAutoIncrementValue, we want to see what the next value would be
	// without actually incrementing. We can do this by getting currval + 1.
	var val uint64
	err := adapter.QueryRowCatalog(ctx, `SELECT currval('`+t.comment.Meta.Sequence+`') + 1`).Scan(&val)
	if err != nil {
		// https://duckdb.org/docs/sql/statements/create_sequence.html#selecting-the-current-value
		// > Note that the nextval function must have already been called before calling currval,
		// > otherwise a Serialization Error (sequence is not yet defined in this session) will be thrown.
		if !strings.Contains(err.Error(), "sequence is not yet defined in this session") {
			return 0, ErrDuckDB.New(err)
		}
		// If the sequence has not been used yet, we can get the start value from the sequence.
		// See getCreateSequence() for the sequence name format.
		err = adapter.QueryRowCatalog(ctx, `SELECT start_value FROM duckdb_sequences() WHERE concat(schema_name, '."', sequence_name, '"') = '`+t.comment.Meta.Sequence+`'`).Scan(&val)
		if err != nil {
			return 0, ErrDuckDB.New(err)
		}
	}

	return val, nil
}

// GetNextAutoIncrementValue implements sql.AutoIncrementTable.
func (t *Table) GetNextAutoIncrementValue(ctx *sql.Context, insertVal any) (uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.comment.Meta.Sequence == "" {
		return 0, sql.ErrNoAutoIncrementCol
	}

	nextVal, err := t.getNextAutoIncrementValue(ctx)
	if err != nil {
		return 0, err
	}

	// If insertVal is provided and greater than the next sequence value, update sequence
	if insertVal != nil {
		var start uint64
		switch v := insertVal.(type) {
		case uint64:
			start = v
		case int64:
			if v > 0 {
				start = uint64(v)
			}
		}
		if start > 0 && start > nextVal {
			err := t.setAutoIncrementValue(ctx, start)
			if err != nil {
				return 0, err
			}
			return start, nil
		}
	}

	// Get next value from sequence
	var val uint64
	err = adapter.QueryRowCatalog(ctx, `SELECT nextval('`+t.comment.Meta.Sequence+`')`).Scan(&val)
	if err != nil {
		return 0, ErrDuckDB.New(err)
	}

	return val, nil
}

// AutoIncrementSetter implements sql.AutoIncrementTable.
func (t *Table) AutoIncrementSetter(ctx *sql.Context) sql.AutoIncrementSetter {
	if t.comment.Meta.Sequence == "" {
		return nil
	}
	return &autoIncrementSetter{t: t}
}

// setAutoIncrementValue is a helper function to update the sequence value
func (t *Table) setAutoIncrementValue(ctx *sql.Context, value uint64) error {
	// DuckDB does not support setting the sequence value directly,
	// so we need to recreate the sequence with the new start value.
	//
	// _, err := adapter.ExecCatalog(ctx, `CREATE OR REPLACE SEQUENCE `+t.comment.Meta.Sequence+` START WITH `+strconv.FormatUint(value, 10))
	//
	// However, `CREATE OR REPLACE` leads to a Dependency Error,
	// while `ALTER TABLE ... ALTER COLUMN ... DROP DEFAULT` deos not remove the dependency:
	// https://github.com/duckdb/duckdb/issues/15399
	// So we create a new sequence with the new start value and change the auto_increment column to use the new sequence.

	// Find the column with the auto_increment property
	var autoIncrementColumn *sql.Column
	for _, column := range t.schema.Schema {
		if column.AutoIncrement {
			autoIncrementColumn = column
			break
		}
	}
	if autoIncrementColumn == nil {
		return sql.ErrNoAutoIncrementCol
	}

	// Generate a random sequence name.
	uuid, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	sequenceName := SequenceNamePrefix + uuid.String()

	// Create a new sequence with the new start value
	temporary := t.db.catalog == "temp"
	createSequenceStmt, fullSequenceName := getCreateSequence(temporary, sequenceName)
	_, err = adapter.Exec(ctx, createSequenceStmt+` START WITH `+strconv.FormatUint(value, 10))
	if err != nil {
		return ErrDuckDB.New(err)
	}

	// Update the auto_increment column to use the new sequence
	alterStmt := `ALTER TABLE ` + FullTableName(t.db.catalog, t.db.name, t.name) +
		` ALTER COLUMN ` + QuoteIdentifierANSI(autoIncrementColumn.Name) +
		` SET DEFAULT nextval('` + fullSequenceName + `')`
	if _, err = adapter.Exec(ctx, alterStmt); err != nil {
		return ErrDuckDB.New(err)
	}

	// Drop the old sequence
	// https://github.com/duckdb/duckdb/issues/15399
	// if _, err = adapter.Exec(ctx, "DROP SEQUENCE " + t.comment.Meta.Sequence); err != nil {
	// 	return ErrDuckDB.New(err)
	// }

	// Update the table comment with the new sequence name
	if err = t.updateExtraTableInfo(ctx, func(info *ExtraTableInfo) {
		info.Sequence = fullSequenceName
	}); err != nil {
		return err
	}

	return t.withSchema(ctx)
}

// autoIncrementSetter implements the AutoIncrementSetter interface
type autoIncrementSetter struct {
	t *Table
}

func (s *autoIncrementSetter) SetAutoIncrementValue(ctx *sql.Context, value uint64) error {
	return s.t.setAutoIncrementValue(ctx, value)
}

func (s *autoIncrementSetter) Close(ctx *sql.Context) error {
	return nil
}

func (s *autoIncrementSetter) AcquireAutoIncrementLock(ctx *sql.Context) (func(), error) {
	s.t.mu.Lock()
	return s.t.mu.Unlock, nil
}

func (t *Table) updateExtraTableInfo(ctx *sql.Context, updater func(*ExtraTableInfo)) error {
	tableInfo := t.comment.Meta
	updater(&tableInfo)
	comment := NewCommentWithMeta(t.comment.Text, tableInfo)
	_, err := adapter.Exec(ctx, `COMMENT ON TABLE `+FullTableName(t.db.catalog, t.db.name, t.name)+` IS '`+comment.Encode()+`'`)
	if err != nil {
		return ErrDuckDB.New(err)
	}
	t.comment.Meta = tableInfo // Update the in-memory metadata
	return nil
}

// CheckConstraints implements sql.CheckTable.
func (t *Table) GetChecks(ctx *sql.Context) ([]sql.CheckDefinition, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.comment.Meta.Checks, nil
}

// AddCheck implements sql.CheckAlterableTable.
func (t *Table) CreateCheck(ctx *sql.Context, check *sql.CheckDefinition) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// TODO(fan): Implement this once DuckDB supports modifying check constraints.
	// https://duckdb.org/docs/sql/statements/alter_table.html#add--drop-constraint
	// https://github.com/duckdb/duckdb/issues/57
	// Just record the check constraint for now.
	return t.updateExtraTableInfo(ctx, func(info *ExtraTableInfo) {
		info.Checks = append(info.Checks, *check)
	})
}

// DropCheck implements sql.CheckAlterableTable.
func (t *Table) DropCheck(ctx *sql.Context, checkName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	checks := make([]sql.CheckDefinition, 0, max(len(t.comment.Meta.Checks)-1, 0))
	found := false
	for i, check := range t.comment.Meta.Checks {
		if check.Name == checkName {
			found = true
			continue
		}
		checks = append(checks, t.comment.Meta.Checks[i])
	}
	if !found {
		return sql.ErrUnknownConstraint.New(checkName)
	}
	return t.updateExtraTableInfo(ctx, func(info *ExtraTableInfo) {
		info.Checks = checks
	})
}
