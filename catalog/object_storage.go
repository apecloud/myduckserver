package catalog

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"strings"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/dolthub/go-mysql-server/sql"
)

// objectColumnDefinition is deliberately a small, trusted representation of a
// column read from the local shadow table. DuckLake's first slice supports
// ordinary columns/defaults/nullability, but not generated columns, keys, or
// checks.
type objectColumnDefinition struct {
	name         string
	dataType     string
	nullable     bool
	defaultValue string
}

func (d *Database) createObjectTable(ctx *sql.Context, name string, schema sql.PrimaryKeySchema, _ sql.CollationID, comment string, storage TableStorageSelection) error {
	if d.provider == nil || d.provider.duckLake == nil {
		return fmt.Errorf("%w: DuckLake service configuration is disabled", ErrInvalidTableStorage)
	}
	if err := validateObjectSchema(schema); err != nil {
		return err
	}
	conn, err := adapter.GetCatalogConn(ctx)
	if err != nil {
		return err
	}
	if err := d.provider.EnsureDuckLakeConnection(ctx, conn); err != nil {
		return err
	}
	physical := d.objectPhysicalTableName(name)
	if err := d.ensureObjectSchema(ctx, conn); err != nil {
		return err
	}
	ddl, err := objectCreateSQL(physical, schema)
	if err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, ddl); err != nil {
		if IsDuckDBTableAlreadyExistsError(err) {
			return sql.ErrTableAlreadyExists.New(name)
		}
		return ErrDuckDB.New(err)
	}

	// Keep a managed local shadow for GMS catalog discovery. If shadow creation
	// fails, remove the just-created lake relation so no unregistered files are
	// left behind.
	if err := d.createLocalTable(ctx, name, schema, sql.Collation_Default, comment, storage, false); err != nil {
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+physical)
		return err
	}
	return nil
}

func validateObjectSchema(schema sql.PrimaryKeySchema) error {
	if len(schema.PkOrdinals) > 0 {
		return fmt.Errorf("%w: object tables do not support primary keys", ErrInvalidTableStorage)
	}
	for _, col := range schema.Schema {
		if col == nil {
			continue
		}
		if col.PrimaryKey || col.AutoIncrement || col.Generated != nil {
			return fmt.Errorf("%w: object tables do not support key, generated, or auto-increment columns", ErrInvalidTableStorage)
		}
	}
	return nil
}

func objectCreateSQL(physical string, schema sql.PrimaryKeySchema) (string, error) {
	columns := make([]string, 0, len(schema.Schema))
	for _, col := range schema.Schema {
		typ, err := DuckdbDataType(col.Type)
		if err != nil {
			return "", err
		}
		part := QuoteIdentifierANSI(col.Name) + " " + typ.name
		if col.Nullable {
			part += " NULL"
		} else {
			part += " NOT NULL"
		}
		if col.Default != nil {
			expr, err := parseDefaultValue(col.Default.String())
			if err != nil {
				return "", err
			}
			part += " DEFAULT " + expr
		}
		columns = append(columns, part)
	}
	return "CREATE TABLE " + physical + " (" + strings.Join(columns, ", ") + ")", nil
}

func (d *Database) ensureObjectSchema(ctx *sql.Context, conn interface {
	ExecContext(context.Context, string, ...interface{}) (stdsql.Result, error)
}) error {
	// This interface is intentionally satisfied by *sql.Conn and keeps this
	// helper independent from a concrete database/sql wrapper in tests.
	schema := d.provider.LakeSchemaName(d.catalog, d.name)
	_, err := conn.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+FullSchemaName(DuckLakeCatalogName, schema))
	if err != nil {
		return fmt.Errorf("create DuckLake schema failed")
	}
	return nil
}

// objectPhysicalTableName returns the fixed lake-catalog relation for a table
// in this logical database. Callers must have validated/attached the runtime
// before executing it.
func (d *Database) objectPhysicalTableName(name string) string {
	return FullTableName(DuckLakeCatalogName, d.provider.LakeSchemaName(d.catalog, d.name), name)
}

// physicalTableName resolves the durable object-table relation for a table
// handle and initializes DuckLake on the same session connection that will
// execute the caller's statement. Local tables continue to use their managed
// catalog relation unchanged.
func (t *Table) physicalTableName(ctx *sql.Context) (string, error) {
	if t == nil || t.db == nil {
		return "", fmt.Errorf("table is unavailable")
	}
	if t.ExtraTableInfo().StorageKind() != TableStorageObject {
		return FullTableName(t.db.catalog, t.db.name, t.name), nil
	}
	if t.db.provider == nil || t.db.provider.duckLake == nil {
		return "", fmt.Errorf("%w: DuckLake service configuration is disabled", ErrInvalidTableStorage)
	}
	conn, err := adapter.GetConn(ctx)
	if err != nil {
		return "", err
	}
	if err := t.db.provider.EnsureDuckLakeConnection(ctx, conn); err != nil {
		return "", err
	}
	physical, found, err := t.db.provider.ObjectTableName(ctx, t.db.catalog, t.db.name, t.name)
	if err != nil {
		return "", err
	}
	if !found {
		return "", sql.ErrTableNotFound.New(t.name)
	}
	return physical, nil
}

func (t *Table) shadowTableName() string {
	return FullTableName(t.db.catalog, t.db.name, t.name)
}

func (t *Table) objectStorage() bool {
	return t != nil && t.ExtraTableInfo().StorageKind() == TableStorageObject
}

func objectColumnSQL(column *sql.Column) (string, error) {
	if column == nil {
		return "", fmt.Errorf("column is unavailable")
	}
	if column.PrimaryKey || column.AutoIncrement || column.Generated != nil {
		return "", fmt.Errorf("%w: object tables do not support key, generated, or auto-increment columns", ErrInvalidTableStorage)
	}
	typ, err := DuckdbDataType(column.Type)
	if err != nil {
		return "", err
	}
	part := QuoteIdentifierANSI(column.Name) + " " + typ.name
	if column.Nullable {
		part += " NULL"
	} else {
		part += " NOT NULL"
	}
	if column.Default != nil {
		expr, err := parseDefaultValue(column.Default.String())
		if err != nil {
			return "", err
		}
		part += " DEFAULT " + expr
	}
	return part, nil
}

func (t *Table) addObjectColumn(ctx *sql.Context, column *sql.Column) error {
	part, err := objectColumnSQL(column)
	if err != nil {
		return err
	}
	physical, err := t.physicalTableName(ctx)
	if err != nil {
		return err
	}
	physicalSQL := "ALTER TABLE " + physical + " ADD COLUMN " + part
	if _, err := adapter.Exec(ctx, physicalSQL); err != nil {
		return ErrDuckDB.New(err)
	}
	// Keep the shadow schema in lockstep; it is never used as the object table's
	// data source but remains the durable GMS catalog representation.
	shadowSQL := "ALTER TABLE " + t.shadowTableName() + " ADD COLUMN " + part
	if _, err := adapter.Exec(ctx, shadowSQL); err != nil {
		return ErrDuckDB.New(err)
	}
	if !column.Nullable {
		if _, err := adapter.Exec(ctx, "ALTER TABLE "+physical+" ALTER COLUMN "+QuoteIdentifierANSI(column.Name)+" SET NOT NULL"); err != nil {
			return ErrDuckDB.New(err)
		}
		if _, err := adapter.Exec(ctx, "ALTER TABLE "+t.shadowTableName()+" ALTER COLUMN "+QuoteIdentifierANSI(column.Name)+" SET NOT NULL"); err != nil {
			return ErrDuckDB.New(err)
		}
	}
	comment := NewCommentWithMeta(column.Comment, MySQLType{})
	if _, err := adapter.Exec(ctx, "COMMENT ON COLUMN "+FullColumnName(t.db.catalog, t.db.name, t.name, column.Name)+" IS '"+comment.Encode()+"'"); err != nil {
		return ErrDuckDB.New(err)
	}
	return t.withSchema(ctx)
}

func (t *Table) dropObjectColumn(ctx *sql.Context, columnName string) error {
	physical, err := t.physicalTableName(ctx)
	if err != nil {
		return err
	}
	if _, err := adapter.Exec(ctx, "ALTER TABLE "+physical+" DROP COLUMN "+QuoteIdentifierANSI(columnName)); err != nil {
		return ErrDuckDB.New(err)
	}
	if _, err := adapter.Exec(ctx, "ALTER TABLE "+t.shadowTableName()+" DROP COLUMN "+QuoteIdentifierANSI(columnName)); err != nil {
		return ErrDuckDB.New(err)
	}
	return t.withSchema(ctx)
}

func (t *Table) modifyObjectColumn(ctx *sql.Context, columnName string, column *sql.Column) error {
	if column == nil || column.PrimaryKey || column.AutoIncrement || column.Generated != nil {
		return fmt.Errorf("%w: object tables do not support key, generated, or auto-increment columns", ErrInvalidTableStorage)
	}
	var old *sql.Column
	for _, candidate := range t.schema.Schema {
		if strings.EqualFold(candidate.Name, columnName) {
			old = candidate
			break
		}
	}
	if old == nil {
		return sql.ErrColumnNotFound.New(columnName)
	}
	typ, err := DuckdbDataType(column.Type)
	if err != nil {
		return err
	}
	physical, err := t.physicalTableName(ctx)
	if err != nil {
		return err
	}
	build := func(tableName string) ([]string, error) {
		base := "ALTER TABLE " + tableName + " ALTER COLUMN " + QuoteIdentifierANSI(columnName)
		var statements []string
		if !old.Type.Equals(column.Type) {
			statements = append(statements, base+" TYPE "+typ.name)
		}
		if old.Nullable && !column.Nullable {
			statements = append(statements, base+" SET NOT NULL")
		} else if !old.Nullable && column.Nullable {
			statements = append(statements, base+" DROP NOT NULL")
		}
		if column.Default != nil {
			expr, err := parseDefaultValue(column.Default.String())
			if err != nil {
				return nil, err
			}
			statements = append(statements, base+" SET DEFAULT "+expr)
		} else if old.Default != nil {
			statements = append(statements, base+" DROP DEFAULT")
		}
		if columnName != column.Name {
			statements = append(statements, "ALTER TABLE "+tableName+" RENAME "+QuoteIdentifierANSI(columnName)+" TO "+QuoteIdentifierANSI(column.Name))
		}
		return statements, nil
	}
	physicalStatements, err := build(physical)
	if err != nil {
		return err
	}
	if len(physicalStatements) > 0 {
		if _, err := adapter.Exec(ctx, strings.Join(physicalStatements, "; ")); err != nil {
			return ErrDuckDB.New(err)
		}
	}
	shadowStatements, err := build(t.shadowTableName())
	if err != nil {
		return err
	}
	if len(shadowStatements) > 0 {
		if _, err := adapter.Exec(ctx, strings.Join(shadowStatements, "; ")); err != nil {
			return ErrDuckDB.New(err)
		}
	}
	commentName := column.Name
	if _, err := adapter.Exec(ctx, "COMMENT ON COLUMN "+FullColumnName(t.db.catalog, t.db.name, t.name, commentName)+" IS '"+NewCommentWithMeta(column.Comment, MySQLType{}).Encode()+"'"); err != nil {
		return ErrDuckDB.New(err)
	}
	return t.withSchema(ctx)
}

// materializeObjectTable is used by the PostgreSQL handler, which has already
// executed its parser-normalized CREATE TABLE against the local shadow. The
// local catalog is authoritative for the accepted DuckDB column types; the
// resulting lake relation deliberately omits constraints unsupported by
// DuckLake.
func (d *Database) materializeObjectTable(ctx *sql.Context, name string) error {
	if d.provider == nil || d.provider.duckLake == nil {
		return fmt.Errorf("%w: DuckLake service configuration is disabled", ErrInvalidTableStorage)
	}
	conn, err := adapter.GetCatalogConn(ctx)
	if err != nil {
		return err
	}
	if err := d.provider.EnsureDuckLakeConnection(ctx, conn); err != nil {
		return err
	}
	defs, err := d.localObjectColumns(ctx, name)
	if err != nil {
		return err
	}
	if len(defs) == 0 {
		return fmt.Errorf("%w: object table has no columns", ErrInvalidTableStorage)
	}
	if err := d.rejectLocalObjectConstraints(ctx, name); err != nil {
		return err
	}
	if err := d.ensureObjectSchema(ctx, conn); err != nil {
		return err
	}
	physical := d.objectPhysicalTableName(name)
	parts := make([]string, 0, len(defs))
	for _, def := range defs {
		if !validObjectTypeText(def.dataType) || strings.Contains(strings.ToLower(def.defaultValue), "nextval(") {
			return fmt.Errorf("%w: unsupported column definition", ErrInvalidTableStorage)
		}
		part := QuoteIdentifierANSI(def.name) + " " + def.dataType
		if def.nullable {
			part += " NULL"
		} else {
			part += " NOT NULL"
		}
		if strings.TrimSpace(def.defaultValue) != "" {
			part += " DEFAULT " + def.defaultValue
		}
		parts = append(parts, part)
	}
	if _, err := conn.ExecContext(ctx, "CREATE TABLE "+physical+" ("+strings.Join(parts, ", ")+")"); err != nil {
		if IsDuckDBTableAlreadyExistsError(err) {
			return nil
		}
		return ErrDuckDB.New(err)
	}
	return nil
}

// MaterializeObjectTable exposes the PostgreSQL parser bridge without
// exposing the internal shadow-table representation.
func (d *Database) MaterializeObjectTable(ctx *sql.Context, name string) error {
	return d.materializeObjectTable(ctx, name)
}

func (d *Database) localObjectColumns(ctx *sql.Context, name string) ([]objectColumnDefinition, error) {
	rows, err := adapter.QueryCatalog(ctx, `
		SELECT column_name, data_type, is_nullable, column_default
		FROM duckdb_columns()
		WHERE database_name = ? AND schema_name = ? AND table_name = ?
		ORDER BY column_index
	`, d.catalog, d.name, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var defs []objectColumnDefinition
	for rows.Next() {
		var def objectColumnDefinition
		var nullable bool
		var defaultValue stdsql.NullString
		if err := rows.Scan(&def.name, &def.dataType, &nullable, &defaultValue); err != nil {
			return nil, err
		}
		def.nullable = nullable
		if defaultValue.Valid {
			def.defaultValue = defaultValue.String
		}
		defs = append(defs, def)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return defs, nil
}

func (d *Database) rejectLocalObjectConstraints(ctx *sql.Context, name string) error {
	rows, err := adapter.QueryCatalog(ctx, `
		SELECT constraint_type
		FROM duckdb_constraints()
		WHERE database_name = ? AND schema_name = ? AND table_name = ?
	`, d.catalog, d.name, name)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var kind string
		if err := rows.Scan(&kind); err != nil {
			return err
		}
		if strings.EqualFold(kind, "PRIMARY KEY") || strings.EqualFold(kind, "UNIQUE") || strings.EqualFold(kind, "CHECK") || strings.EqualFold(kind, "FOREIGN KEY") {
			return fmt.Errorf("%w: object tables do not support %s constraints", ErrInvalidTableStorage, kind)
		}
	}
	return rows.Err()
}

func validObjectTypeText(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || strings.ContainsAny(value, "\x00;\r\n") {
		return false
	}
	return true
}

// PhysicalTableNameForTable is used by protocol COPY helpers and by callers
// that already resolved a GMS table. Local tables return their normal fully
// qualified name; object tables return the fixed lake relation.
func (prov *DatabaseProvider) PhysicalTableNameForTable(ctx *sql.Context, table sql.Table) (string, bool, error) {
	t, ok := table.(*Table)
	if !ok || t == nil {
		return "", false, nil
	}
	info := t.ExtraTableInfo()
	if info.StorageKind() != TableStorageObject {
		return FullTableName(t.db.catalog, t.db.name, t.name), false, nil
	}
	physical, found, err := prov.ObjectTableName(ctx, t.db.catalog, t.db.name, t.name)
	if err != nil || !found {
		return "", false, err
	}
	return physical, true, nil
}
