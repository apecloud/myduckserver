package backend

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

const isUnixSystem = runtime.GOOS == "linux" ||
	runtime.GOOS == "darwin" ||
	runtime.GOOS == "freebsd"

func isRewritableLoadData(node *plan.LoadData) bool {
	return !(node.Local && !isUnixSystem) && // pipe syscall is not available on Windows
		len(node.FieldsTerminatedBy) == 1 &&
		len(node.FieldsEnclosedBy) <= 1 &&
		len(node.FieldsEscapedBy) <= 1 &&
		len(node.LinesStartingBy) == 0 &&
		isSupportedLineTerminator(node.LinesTerminatedBy) &&
		areAllExpressionsNil(node.SetExprs) &&
		areAllExpressionsNil(node.UserVars) &&
		isSupportedFileCharacterSet(node.Charset)
}

func areAllExpressionsNil(exprs []sql.Expression) bool {
	for _, expr := range exprs {
		if expr != nil {
			return false
		}
	}
	return true
}

func isSupportedFileCharacterSet(charset string) bool {
	return len(charset) == 0 ||
		strings.HasPrefix(strings.ToLower(charset), "utf8") ||
		strings.EqualFold(charset, "ascii") ||
		strings.EqualFold(charset, "binary")
}

func isSupportedLineTerminator(terminator string) bool {
	return terminator == "\n" || terminator == "\r" || terminator == "\r\n"
}

// buildLoadData translates a MySQL LOAD DATA statement
// into a DuckDB INSERT INTO statement and executes it.
func (db *DuckBuilder) buildLoadData(ctx *sql.Context, root sql.Node, insert *plan.InsertInto, dst sql.InsertableTable, load *plan.LoadData) (sql.RowIter, error) {
	if load.Local {
		return db.buildClientSideLoadData(ctx, insert, dst, load)
	}
	return db.buildServerSideLoadData(ctx, insert, dst, load)
}

// Since the data is sent to the server in the form of a byte stream,
// we use a Unix pipe to stream the data to DuckDB.
func (db *DuckBuilder) buildClientSideLoadData(ctx *sql.Context, insert *plan.InsertInto, dst sql.InsertableTable, load *plan.LoadData) (sql.RowIter, error) {
	_, localInfile, ok := sql.SystemVariables.GetGlobal("local_infile")
	if !ok {
		return nil, fmt.Errorf("error: local_infile variable was not found")
	}

	if localInfile.(int8) == 0 {
		return nil, fmt.Errorf("local_infile needs to be set to 1 to use LOCAL")
	}

	reader, err := ctx.LoadInfile(load.File)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	pipePath, err := db.CreatePipe(ctx, "load-data")
	if err != nil {
		return nil, err
	}
	defer os.Remove(pipePath)

	// Write the data to the FIFO pipe.
	go func() {
		pipe, err := os.OpenFile(pipePath, os.O_WRONLY, os.ModeNamedPipe)
		if err != nil {
			return
		}
		defer pipe.Close()
		io.Copy(pipe, reader)
	}()

	return db.executeLoadData(ctx, insert, dst, load, pipePath)
}

// In the non-local case, we can directly use the file path to read the data.
func (db *DuckBuilder) buildServerSideLoadData(ctx *sql.Context, insert *plan.InsertInto, dst sql.InsertableTable, load *plan.LoadData) (sql.RowIter, error) {
	_, secureFileDir, ok := sql.SystemVariables.GetGlobal("secure_file_priv")
	if !ok {
		return nil, fmt.Errorf("error: secure_file_priv variable was not found")
	}

	if err := isUnderSecureFileDir(secureFileDir, load.File); err != nil {
		return nil, sql.ErrLoadDataCannotOpen.New(err.Error())
	}
	return db.executeLoadData(ctx, insert, dst, load, load.File)
}

func (db *DuckBuilder) executeLoadData(ctx *sql.Context, insert *plan.InsertInto, dst sql.InsertableTable, load *plan.LoadData, filePath string) (sql.RowIter, error) {
	// Build the DuckDB INSERT INTO statement.
	var b strings.Builder
	b.Grow(256)

	keyless := sql.IsKeyless(dst.Schema())
	// TODO(fan): This is an ugly hack for MySQL Shell's utilities to work properly.
	if !keyless {
		if t, ok := dst.(*catalog.Table); ok {
			// Replicated tables do not have physical primary keys.
			// Their logical primary keys are fake and should not be used in INSERT INTO statements.
			// https://github.com/apecloud/myduckserver/issues/272
			keyless = t.ExtraTableInfo().Replicated
		}
	}

	b.WriteString("INSERT")
	if load.IsIgnore && !keyless {
		b.WriteString(" OR IGNORE")
	} else if load.IsReplace && !keyless {
		b.WriteString(" OR REPLACE")
	}
	b.WriteString(" INTO ")

	qualifiedTableName := catalog.ConnectIdentifiersANSI(insert.Database().Name(), dst.Name())
	b.WriteString(qualifiedTableName)

	if len(load.ColNames) > 0 {
		b.WriteString(" (")
		b.WriteString(catalog.QuoteIdentifierANSI(load.ColNames[0]))
		for _, col := range load.ColNames[1:] {
			b.WriteString(", ")
			b.WriteString(catalog.QuoteIdentifierANSI(col))
		}
		b.WriteString(")")
	}

	b.WriteString(" FROM ")
	b.WriteString("read_csv('")
	b.WriteString(filePath)
	b.WriteString("'")

	b.WriteString(", auto_detect = false")
	b.WriteString(", header = false")
	b.WriteString(", null_padding = true")

	b.WriteString(", new_line = ")
	if len(load.LinesTerminatedBy) == 1 {
		b.WriteString(singleQuotedDuckChar(load.LinesTerminatedBy))
	} else {
		b.WriteString(`'\r\n'`)
	}

	b.WriteString(", sep = ")
	b.WriteString(singleQuotedDuckChar(load.FieldsTerminatedBy))

	b.WriteString(", quote = ")
	b.WriteString(singleQuotedDuckChar(load.FieldsEnclosedBy))

	// TODO(fan): DuckDB does not support the `\` escape mode of MySQL yet.
	if load.FieldsEscapedBy == `\` {
		b.WriteString(`, escape = ''`)
	} else {
		b.WriteString(", escape = ")
		b.WriteString(singleQuotedDuckChar(load.FieldsEscapedBy))
	}

	// > If FIELDS ENCLOSED BY is not empty, a field containing
	// > the literal word NULL as its value is read as a NULL value.
	// > If FIELDS ESCAPED BY is empty, NULL is written as the word NULL.
	b.WriteString(", allow_quoted_nulls = false, nullstr = ")
	if len(load.FieldsEnclosedBy) > 0 || len(load.FieldsEscapedBy) == 0 {
		b.WriteString(`'NULL'`)
	} else {
		b.WriteString(`'\N'`)
	}

	if load.IgnoreNum > 0 {
		b.WriteString(", skip = ")
		b.WriteString(strconv.FormatInt(load.IgnoreNum, 10))
	}

	b.WriteString(", columns = ")
	if err := columnTypeHints(&b, dst, dst.Schema(), load.ColNames); err != nil {
		return nil, err
	}

	b.WriteString(")")

	// Execute the DuckDB INSERT INTO statement.
	duckSQL := b.String()
	ctx.GetLogger().Trace(duckSQL)

	result, err := adapter.Exec(ctx, duckSQL)
	if err != nil {
		return nil, err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}

	insertId, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(sql.NewRow(types.OkResult{
		RowsAffected: uint64(affected),
		InsertID:     uint64(insertId),
	})), nil
}

func singleQuotedDuckChar(s string) string {
	if len(s) == 0 {
		return `''`
	}
	r := []rune(s)[0]
	if r == '\\' {
		return `'\'` // Slash does not need to be escaped in DuckDB
	}
	return strconv.QuoteRune(r) // e.g., tab -> '\t'
}

func columnTypeHints(b *strings.Builder, dst sql.Table, schema sql.Schema, colNames []string) error {
	b.WriteString("{")

	if len(colNames) == 0 {
		for i, col := range schema {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(catalog.QuoteIdentifierANSI(col.Name))
			b.WriteString(": ")
			if dt, err := catalog.DuckdbDataType(col.Type); err != nil {
				return err
			} else {
				b.WriteString(`'`)
				b.WriteString(dt.Name())
				b.WriteString(`'`)
			}
		}
		b.WriteString("}")
		return nil
	}

	for i, col := range colNames {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(catalog.QuoteIdentifierANSI(col))
		b.WriteString(": ")
		idx := schema.IndexOf(col, dst.Name()) // O(n^2) but n := # of columns is usually small
		if idx < 0 {
			return sql.ErrTableColumnNotFound.New(dst.Name(), col)
		}
		if dt, err := catalog.DuckdbDataType(schema[idx].Type); err != nil {
			return err
		} else {
			b.WriteString(`'`)
			b.WriteString(dt.Name())
			b.WriteString(`'`)
		}
	}

	b.WriteString("}")
	return nil
}

// isUnderSecureFileDir ensures that fileStr is under secureFileDir or a subdirectory of secureFileDir, errors otherwise
// Copied from https://github.com/dolthub/go-mysql-server/blob/main/sql/rowexec/rel.go
func isUnderSecureFileDir(secureFileDir interface{}, fileStr string) error {
	if secureFileDir == nil || secureFileDir == "" {
		return nil
	}
	sStat, err := os.Stat(secureFileDir.(string))
	if err != nil {
		return err
	}
	fStat, err := os.Stat(filepath.Dir(fileStr))
	if err != nil {
		return err
	}
	if os.SameFile(sStat, fStat) {
		return nil
	}

	fileAbsPath, filePathErr := filepath.Abs(fileStr)
	if filePathErr != nil {
		return filePathErr
	}
	secureFileDirAbsPath, _ := filepath.Abs(secureFileDir.(string))
	if strings.HasPrefix(fileAbsPath, secureFileDirAbsPath) {
		return nil
	}
	return sql.ErrSecureFilePriv.New()
}
