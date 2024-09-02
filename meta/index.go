package meta

import "github.com/dolthub/go-mysql-server/sql"

type Index struct {
	DbName     string
	TableName  string
	Exprs      []sql.Expression
	Name       string
	Unique     bool
	CommentObj *Comment
	PrefixLens []uint16
}

var _ sql.Index = (*Index)(nil)

// TODO: DuckDB doesn't have a convenient way to get the expressions from an index
// so we need to implement our own. Storing it in the index comment is a good idea.
func NewIndex(dbName, tableName, name string, unique bool, comment *Comment) *Index {
	return &Index{
		DbName:     dbName,
		TableName:  tableName,
		Name:       name,
		Unique:     unique,
		CommentObj: comment,
	}
}

// ID returns the identifier of the index.
func (idx *Index) ID() string {
	return idx.Name
}

// Database returns the database name this index belongs to.
func (idx *Index) Database() string {
	return idx.DbName
}

// Table returns the table name this index belongs to.
func (idx *Index) Table() string {
	return idx.TableName
}

// Expressions returns the indexed expressions. If the result is more than
// one expression, it means the index has multiple columns indexed. If it's
// just one, it means it may be an expression or a column.
func (idx *Index) Expressions() []string {
	exprs := make([]string, len(idx.Exprs))
	for i, expr := range idx.Exprs {
		exprs[i] = expr.String()
	}
	return exprs
}

// IsUnique returns whether this index is unique
func (idx *Index) IsUnique() bool {
	return idx.Unique
}

// IsSpatial returns whether this index is a spatial index
func (idx *Index) IsSpatial() bool {
	return false
}

// IsFullText returns whether this index is a Full-Text index
func (idx *Index) IsFullText() bool {
	return false
}

// Comment returns the comment for this index
func (idx *Index) Comment() string {
	return idx.CommentObj.Text
}

// IndexType returns the type of this index, e.g. BTREE
func (idx *Index) IndexType() string {
	// duckdb uses Adaptive Radix Tree (ART) as its index implementation
	return "ART"
}

// IsGenerated returns whether this index was generated. Generated indexes
// are used for index access, but are not displayed (such as with SHOW INDEXES).
func (idx *Index) IsGenerated() bool {
	// Assuming false as default
	return false
}

// ColumnExpressionTypes returns each expression and its associated Type.
// Each expression string should exactly match the string returned from
// Index.Expressions().
func (idx *Index) ColumnExpressionTypes() []sql.ColumnExpressionType {
	// Assuming empty slice as default
	return []sql.ColumnExpressionType{}
}

// CanSupport returns whether this index supports lookups on the given
// range filters.
func (idx *Index) CanSupport(ranges ...sql.Range) bool {
	// Assuming true as default
	return true
}

// PrefixLengths returns the prefix lengths for each column in this index
func (idx *Index) PrefixLengths() []uint16 {
	return idx.PrefixLens
}
