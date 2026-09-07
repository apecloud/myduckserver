package backend

import "github.com/dolthub/go-mysql-server/sql"

// WrapRowIterWithRelease keeps a caller-owned lifecycle lease until the
// wrapped iterator has been fully consumed or explicitly closed. The release
// callback is invoked at most once, after the child iterator is closed, and
// the wrapper preserves the mutable-child and ValueRowIter behavior used by
// GMS execution nodes.
//
// This is intentionally a thin export of the same wrapper used for the
// provider's DuckLake operation lease. PostgreSQL protocol paths can therefore
// retain an execution snapshot without introducing a second iterator lifetime
// implementation.
func WrapRowIterWithRelease(iter sql.RowIter, release func()) sql.RowIter {
	return wrapDuckLakeOperationIter(iter, release)
}
