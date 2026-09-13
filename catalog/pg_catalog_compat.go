package catalog

import (
	"context"
	"fmt"

	"github.com/apecloud/myduckserver/adapter"
)

// migrateLegacyPGCatalogTable renames a leftover __sys__ base table so a
// compatibility view can occupy the original PostgreSQL catalog name.
// New data directories skip this path because the table is created under
// newName directly.
func migrateLegacyPGCatalogTable(execer adapter.SQLExecutor, oldName, newName string) error {
	if execer == nil {
		return fmt.Errorf("catalog executor is unavailable")
	}

	var oldTables int
	if err := execer.QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM duckdb_tables() WHERE schema_name = '__sys__' AND table_name = ?`,
		oldName,
	).Scan(&oldTables); err != nil {
		return fmt.Errorf("failed to inspect __sys__.%s: %w", oldName, err)
	}
	if oldTables == 0 {
		return nil
	}

	var newTables int
	if err := execer.QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM duckdb_tables() WHERE schema_name = '__sys__' AND table_name = ?`,
		newName,
	).Scan(&newTables); err != nil {
		return fmt.Errorf("failed to inspect __sys__.%s: %w", newName, err)
	}
	if newTables > 0 {
		return nil
	}

	if _, err := execer.ExecContext(
		context.Background(),
		fmt.Sprintf(`ALTER TABLE __sys__.%s RENAME TO %s`, oldName, newName),
	); err != nil {
		return fmt.Errorf("failed to rename __sys__.%s to %s: %w", oldName, newName, err)
	}
	return nil
}
