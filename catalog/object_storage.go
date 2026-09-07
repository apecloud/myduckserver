package catalog

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"strings"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/mycontext"
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
	lakeCtx, lakeConn, err := d.acquireDedicatedLakeConn()
	if err != nil {
		return err
	}
	defer lakeConn.Close()
	physical := d.objectPhysicalTableName(name)
	if err := d.ensureObjectSchema(lakeCtx, lakeConn); err != nil {
		return err
	}
	ddl, err := objectCreateSQL(physical, schema)
	if err != nil {
		return err
	}
	if _, err := lakeConn.ExecContext(lakeCtx, ddl); err != nil {
		if IsDuckDBTableAlreadyExistsError(err) {
			return sql.ErrTableAlreadyExists.New(name)
		}
		return ErrDuckDB.New(err)
	}
	d.registerUncommittedLakeRelation(ctx, physical)

	execer, _, _, release, err := adapter.GetCatalogExecutionSnapshotWithLease(ctx)
	if err != nil {
		d.dropLakeRelation(lakeCtx, lakeConn, ctx, physical)
		return err
	}
	defer release()
	// Keep a managed local shadow for GMS catalog discovery. If shadow creation
	// fails, remove the just-created lake relation so no unregistered files are
	// left behind. Lake DDL uses a dedicated connection because DuckDB allows
	// one transaction to write only one attached database.
	if err := d.createLocalTableWithExecutor(ctx, name, schema, sql.Collation_Default, comment, storage, false, execer); err != nil {
		d.dropLakeRelation(lakeCtx, lakeConn, ctx, physical)
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

func (d *Database) ensureObjectSchema(ctx context.Context, execer adapter.SQLExecutor) error {
	// This interface is intentionally satisfied by *sql.Conn and keeps this
	// helper independent from a concrete database/sql wrapper in tests.
	schema := d.provider.LakeSchemaName(d.catalog, d.name)
	query := "CREATE SCHEMA IF NOT EXISTS " + FullSchemaName(DuckLakeCatalogName, schema)
	_, err := execer.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("create DuckLake schema failed")
	}
	return nil
}

func lakeTransactionInactive(err error) bool {
	if err == nil {
		return true
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "no transaction") ||
		(strings.Contains(lower, "transaction") && strings.Contains(lower, "not active")) ||
		adapter.IsTransactionInactiveError(err)
}

func (d *Database) acquireDedicatedLakeConn() (context.Context, *stdsql.Conn, error) {
	if d == nil || d.provider == nil {
		return nil, nil, fmt.Errorf("ducklake storage is unavailable")
	}
	storageDB := d.provider.Storage()
	if storageDB == nil {
		return nil, nil, fmt.Errorf("ducklake storage is unavailable")
	}
	lakeCtx := mycontext.WithMaintenanceQuery(context.Background())
	lakeConn, err := storageDB.Conn(lakeCtx)
	if err != nil {
		return nil, nil, err
	}
	if err := d.provider.EnsureDuckLakeConnectionWithExecutor(lakeCtx, lakeConn, lakeConn); err != nil {
		_ = lakeConn.Close()
		return nil, nil, err
	}
	if _, err := lakeConn.ExecContext(lakeCtx, "COMMIT"); err != nil && !lakeTransactionInactive(err) {
		_ = lakeConn.Close()
		return nil, nil, err
	}
	return lakeCtx, lakeConn, nil
}

func sessionTxnBinding(ctx *sql.Context) (*stdsql.Conn, *stdsql.Tx) {
	if ctx == nil || ctx.Session == nil {
		return nil, nil
	}
	if _, ok := ctx.Session.(adapter.ConnectionHolder); !ok {
		return nil, nil
	}
	return adapter.TryGetTxnBinding(ctx)
}

func (d *Database) registerUncommittedLakeRelation(ctx *sql.Context, physical string) {
	if d == nil || d.provider == nil {
		return
	}
	conn, tx := sessionTxnBinding(ctx)
	d.provider.registerUncommittedLakeRelation(tx, conn, physical)
}

func (d *Database) dropLakeRelation(lakeCtx context.Context, lakeConn *stdsql.Conn, sessionCtx *sql.Context, physical string) {
	if strings.TrimSpace(physical) == "" {
		return
	}
	if sessionCtx != nil {
		_, tx := sessionTxnBinding(sessionCtx)
		if d != nil && d.provider != nil {
			d.provider.forgetUncommittedLakeRelation(tx, physical)
		}
	}
	if lakeConn != nil && lakeCtx != nil {
		_, _ = lakeConn.ExecContext(lakeCtx, "DROP TABLE IF EXISTS "+physical)
		return
	}
	if d == nil {
		return
	}
	dropCtx, dropConn, err := d.acquireDedicatedLakeConn()
	if err != nil {
		return
	}
	defer dropConn.Close()
	_, _ = dropConn.ExecContext(dropCtx, "DROP TABLE IF EXISTS "+physical)
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
	execer, conn, _, release, err := adapter.GetExecutionSnapshotWithLease(ctx)
	if err != nil {
		return "", err
	}
	defer release()
	return t.physicalTableNameWithExecutor(ctx, execer, conn)
}

// physicalTableNameWithExecutor resolves a table using an already captured
// execution snapshot. Object metadata lookup and the statement that follows
// must use the same transaction/connection pair; acquiring a fresh snapshot
// here can otherwise observe a different catalog generation while a request is
// in flight.
func (t *Table) physicalTableNameWithExecutor(ctx *sql.Context, execer adapter.SQLExecutor, conn *stdsql.Conn) (string, error) {
	if t == nil || t.db == nil {
		return "", fmt.Errorf("table is unavailable")
	}
	if t.ExtraTableInfo().StorageKind() != TableStorageObject {
		return FullTableName(t.db.catalog, t.db.name, t.name), nil
	}
	if t.db.provider == nil || t.db.provider.duckLake == nil {
		return "", fmt.Errorf("%w: DuckLake service configuration is disabled", ErrInvalidTableStorage)
	}
	if execer == nil {
		return "", fmt.Errorf("object-table metadata executor is unavailable")
	}
	if err := t.db.provider.EnsureDuckLakeConnectionWithExecutor(ctx, execer, conn); err != nil {
		return "", err
	}
	physical, found, err := t.db.provider.ObjectTableNameWithExecutor(ctx, execer, t.db.catalog, t.db.name, t.name)
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
	execer, conn, _, release, err := adapter.GetExecutionSnapshotWithLease(ctx)
	if err != nil {
		return err
	}
	defer release()
	physical, err := t.physicalTableNameWithExecutor(ctx, execer, conn)
	if err != nil {
		return err
	}
	physicalSQL := "ALTER TABLE " + physical + " ADD COLUMN " + part
	if _, err := execer.ExecContext(ctx, physicalSQL); err != nil {
		return ErrDuckDB.New(err)
	}
	// Keep the shadow schema in lockstep; it is never used as the object table's
	// data source but remains the durable GMS catalog representation.
	shadowSQL := "ALTER TABLE " + t.shadowTableName() + " ADD COLUMN " + part
	if _, err := execer.ExecContext(ctx, shadowSQL); err != nil {
		return ErrDuckDB.New(err)
	}
	if !column.Nullable {
		if _, err := execer.ExecContext(ctx, "ALTER TABLE "+physical+" ALTER COLUMN "+QuoteIdentifierANSI(column.Name)+" SET NOT NULL"); err != nil {
			return ErrDuckDB.New(err)
		}
		if _, err := execer.ExecContext(ctx, "ALTER TABLE "+t.shadowTableName()+" ALTER COLUMN "+QuoteIdentifierANSI(column.Name)+" SET NOT NULL"); err != nil {
			return ErrDuckDB.New(err)
		}
	}
	comment := NewCommentWithMeta(column.Comment, MySQLType{})
	if _, err := execer.ExecContext(ctx, "COMMENT ON COLUMN "+FullColumnName(t.db.catalog, t.db.name, t.name, column.Name)+" IS '"+comment.Encode()+"'"); err != nil {
		return ErrDuckDB.New(err)
	}
	return t.withSchemaWithExecutor(ctx, execer)
}

func (t *Table) dropObjectColumn(ctx *sql.Context, columnName string) error {
	execer, conn, _, release, err := adapter.GetExecutionSnapshotWithLease(ctx)
	if err != nil {
		return err
	}
	defer release()
	physical, err := t.physicalTableNameWithExecutor(ctx, execer, conn)
	if err != nil {
		return err
	}
	if _, err := execer.ExecContext(ctx, "ALTER TABLE "+physical+" DROP COLUMN "+QuoteIdentifierANSI(columnName)); err != nil {
		return ErrDuckDB.New(err)
	}
	if _, err := execer.ExecContext(ctx, "ALTER TABLE "+t.shadowTableName()+" DROP COLUMN "+QuoteIdentifierANSI(columnName)); err != nil {
		return ErrDuckDB.New(err)
	}
	return t.withSchemaWithExecutor(ctx, execer)
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
	execer, conn, _, release, err := adapter.GetExecutionSnapshotWithLease(ctx)
	if err != nil {
		return err
	}
	defer release()
	physical, err := t.physicalTableNameWithExecutor(ctx, execer, conn)
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
		if _, err := execer.ExecContext(ctx, strings.Join(physicalStatements, "; ")); err != nil {
			return ErrDuckDB.New(err)
		}
	}
	shadowStatements, err := build(t.shadowTableName())
	if err != nil {
		return err
	}
	if len(shadowStatements) > 0 {
		if _, err := execer.ExecContext(ctx, strings.Join(shadowStatements, "; ")); err != nil {
			return ErrDuckDB.New(err)
		}
	}
	commentName := column.Name
	if _, err := execer.ExecContext(ctx, "COMMENT ON COLUMN "+FullColumnName(t.db.catalog, t.db.name, t.name, commentName)+" IS '"+NewCommentWithMeta(column.Comment, MySQLType{}).Encode()+"'"); err != nil {
		return ErrDuckDB.New(err)
	}
	return t.withSchemaWithExecutor(ctx, execer)
}

// materializeObjectTable is used by the PostgreSQL handler, which has already
// executed its parser-normalized CREATE TABLE against the local shadow. The
// local catalog is authoritative for the accepted DuckDB column types; the
// resulting lake relation deliberately omits constraints unsupported by
// DuckLake.
func (d *Database) materializeObjectTable(ctx *sql.Context, name string) error {
	execer, conn, _, release, err := adapter.GetCatalogExecutionSnapshotWithLease(ctx)
	if err != nil {
		return err
	}
	defer release()
	return d.materializeObjectTableWithExecutor(ctx, name, execer, conn)
}

// materializeObjectTableWithExecutor is the executor-affine form used by
// metadata publication. The caller owns the snapshot (and, when present, its
// transaction), so the physical CREATE cannot silently move to another pool
// connection between the local-shadow lookup and the durable marker update.
func (d *Database) materializeObjectTableWithExecutor(
	ctx *sql.Context,
	name string,
	execer adapter.SQLExecutor,
	conn *stdsql.Conn,
) error {
	if d.provider == nil || d.provider.duckLake == nil {
		return fmt.Errorf("%w: DuckLake service configuration is disabled", ErrInvalidTableStorage)
	}
	if execer == nil {
		return fmt.Errorf("object-table metadata executor is unavailable")
	}
	if conn == nil {
		return fmt.Errorf("object-table metadata connection is unavailable")
	}
	if d.provider.Storage() == nil {
		if err := d.provider.EnsureDuckLakeConnectionWithExecutor(ctx, execer, conn); err != nil {
			return err
		}
	}
	defs, err := d.localObjectColumnsWithExecutor(ctx, name, execer)
	if err != nil {
		return err
	}
	if len(defs) == 0 {
		return fmt.Errorf("%w: object table has no columns", ErrInvalidTableStorage)
	}
	if err := d.rejectLocalObjectConstraintsWithExecutor(ctx, name, execer); err != nil {
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
	createSQL := "CREATE TABLE " + physical + " (" + strings.Join(parts, ", ") + ")"
	lakeExecer := execer
	lakeCtx := context.Context(ctx)
	var lakeConn *stdsql.Conn
	var lakeDDLCtx context.Context
	if d.provider.Storage() != nil {
		// PostgreSQL already wrote the local shadow in the session transaction.
		// DuckDB cannot also write the attached lake catalog in that transaction.
		lakeDDLCtx, lakeConn, err = d.acquireDedicatedLakeConn()
		if err != nil {
			return err
		}
		defer lakeConn.Close()
		lakeExecer = lakeConn
		lakeCtx = lakeDDLCtx
	}
	if err := d.ensureObjectSchema(lakeCtx, lakeExecer); err != nil {
		return err
	}
	if _, err := lakeExecer.ExecContext(lakeCtx, createSQL); err != nil {
		if IsDuckDBTableAlreadyExistsError(err) {
			return nil
		}
		return ErrDuckDB.New(err)
	}
	if lakeConn != nil {
		d.registerUncommittedLakeRelation(ctx, physical)
	}
	return nil
}

// MaterializeObjectTable exposes the PostgreSQL parser bridge without
// exposing the internal shadow-table representation.
func (d *Database) MaterializeObjectTable(ctx *sql.Context, name string) error {
	return d.materializeObjectTable(ctx, name)
}

func (d *Database) localObjectColumns(ctx *sql.Context, name string) ([]objectColumnDefinition, error) {
	execer, _, _, release, err := adapter.GetCatalogExecutionSnapshotWithLease(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return d.localObjectColumnsWithExecutor(ctx, name, execer)
}

func (d *Database) localObjectColumnsWithExecutor(ctx *sql.Context, name string, execer adapter.SQLExecutor) ([]objectColumnDefinition, error) {
	if execer == nil {
		return nil, fmt.Errorf("object-table metadata executor is unavailable")
	}
	rows, err := execer.QueryContext(ctx, `
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
	execer, _, _, release, err := adapter.GetCatalogExecutionSnapshotWithLease(ctx)
	if err != nil {
		return err
	}
	defer release()
	return d.rejectLocalObjectConstraintsWithExecutor(ctx, name, execer)
}

func (d *Database) rejectLocalObjectConstraintsWithExecutor(ctx *sql.Context, name string, execer adapter.SQLExecutor) error {
	if execer == nil {
		return fmt.Errorf("object-table metadata executor is unavailable")
	}
	rows, err := execer.QueryContext(ctx, `
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
	execer, conn, _, release, err := adapter.GetExecutionSnapshotWithLease(ctx)
	if err != nil {
		return "", false, err
	}
	defer release()
	return prov.PhysicalTableNameForTableWithExecutor(ctx, table, execer, conn)
}

// PhysicalTableNameForTableWithExecutor resolves a table through the supplied
// execution snapshot. Metadata lookup and the eventual statement execution
// therefore remain on one transaction/connection pair.
func (prov *DatabaseProvider) PhysicalTableNameForTableWithExecutor(
	ctx *sql.Context,
	table sql.Table,
	execer adapter.SQLExecutor,
	conn *stdsql.Conn,
) (string, bool, error) {
	var t *Table
	switch typed := table.(type) {
	case *Table:
		t = typed
	case *IndexedTable:
		if typed != nil {
			t = typed.Table
		}
	}
	if t == nil {
		return "", false, nil
	}
	info := t.ExtraTableInfo()
	if info.StorageKind() != TableStorageObject {
		return FullTableName(t.db.catalog, t.db.name, t.name), false, nil
	}
	if prov == nil || prov.duckLake == nil {
		return "", false, fmt.Errorf("%w: DuckLake service configuration is disabled", ErrInvalidTableStorage)
	}
	if execer == nil {
		return "", false, fmt.Errorf("object-table metadata executor is unavailable")
	}
	if conn != nil {
		if err := prov.EnsureDuckLakeConnectionWithExecutor(ctx, execer, conn); err != nil {
			return "", false, err
		}
	}
	physical, found, err := prov.ObjectTableNameWithExecutor(ctx, execer, t.db.catalog, t.db.name, t.name)
	if err != nil || !found {
		return "", false, err
	}
	return physical, true, nil
}
