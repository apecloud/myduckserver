package pgserver

import (
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
)

const (
	CopyFormatParquet = tree.CopyFormatCSV + 1
	CopyFormatJSON    = tree.CopyFormatCSV + 2
)

var (
	// We are supporting the parquet/... formats for COPY TO, but
	// COPY ... TO STDOUT [WITH] (FORMAT PARQUET, OPT1 v1, OPT2, OPT3 v3, ...)
	// Let's match them with regex and extract the ... part.
	// Update regex to capture FORMAT and other options
	reCopyToFormat = regexp.MustCompile(`(?i)^COPY\s+(.*?)\s+TO\s+STDOUT(?:\s+(?:WITH\s*)?\(\s*(?:FORMAT\s+(\w+)\s*,?\s*)?(.*?)\s*\))?$`)
)

func ParseCopy(stmt string) (query string, format tree.CopyFormat, options string, ok bool) {
	stmt = RemoveComments(stmt)
	stmt = sql.RemoveSpaceAndDelimiter(stmt, ';')
	m := reCopyToFormat.FindStringSubmatch(stmt)
	if m == nil {
		return "", 0, "", false
	}
	query = strings.TrimSpace(m[1])

	var formatStr string
	if m[2] != "" {
		formatStr = strings.ToUpper(m[2])
	} else {
		formatStr = "TEXT"
	}

	options = strings.TrimSpace(m[3])

	switch formatStr {
	case "PARQUET":
		format = CopyFormatParquet
	case "JSON":
		format = CopyFormatJSON
	case "CSV":
		format = tree.CopyFormatCSV
	case "BINARY":
		format = tree.CopyFormatBinary
	case "", "TEXT":
		format = tree.CopyFormatText
	}

	return query, format, options, true
}
