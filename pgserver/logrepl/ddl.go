package logrepl

import (
	"fmt"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/pgtypes"
)

func generateCreateTableStmt(msg *pglogrepl.RelationMessageV2) (string, error) {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE IF NOT EXISTS ")
	sb.WriteString(catalog.ConnectIdentifiersANSI(msg.Namespace, msg.RelationName))
	sb.WriteString(" (")
	var keyColumns []string
	for i, col := range msg.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(col.Name)
		sb.WriteString(" ")
		sb.WriteString(pgTypeName(col))
		if col.Flags == 1 {
			keyColumns = append(keyColumns, col.Name)
		}
	}
	if len(keyColumns) > 0 {
		sb.WriteString(", PRIMARY KEY (")
		sb.WriteString(strings.Join(keyColumns, ", "))
		sb.WriteString(")")
	}
	sb.WriteString(");")
	return sb.String(), nil
}

func pgTypeName(col *pglogrepl.RelationMessageColumn) string {
	if duckdbType, ok := pgtypes.PostgresOIDToDuckDBTypeName[col.DataType]; ok {
		if col.DataType == pgtype.NumericOID {
			precision, scale := decodePrecisionScale(int(col.TypeModifier))
			return fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)
		}
		return duckdbType
	}
	return "VARCHAR" // default to VARCHAR if type is unknown
}

func decodePrecisionScale(typmod int) (precision int, scale int) {
	if typmod >= 0 {
		precision = ((typmod - 4) >> 16) & 0xFFFF
		scale = (typmod - 4) & 0xFFFF
	} else {
		precision = 0
		scale = 0
	}
	return
}
