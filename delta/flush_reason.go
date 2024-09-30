package delta

type FlushReason uint8

const (
	// UnknownFlushReason means that the changes have to be flushed for an unknown reason.
	UnknownFlushReason FlushReason = iota
	// DDLStmtFlushReason means that the changes have to be flushed because of a DDL statement.
	DDLStmtFlushReason
	// RowCountLimitFlushReason means that the changes have to be flushed because the row count limit is reached.
	RowCountLimitFlushReason
	// MemoryLimitFlushReason means that the changes have to be flushed because the memory limit is reached.
	MemoryLimitFlushReason
	// TimeTickFlushReason means that the changes have to be flushed because a time ticker is fired.
	TimeTickFlushReason
	// QueryFlushReason means that the changes have to be flushed because some tables are queried.
	QueryFlushReason
)

func (r FlushReason) String() string {
	switch r {
	case DDLStmtFlushReason:
		return "DDLStmt"
	case RowCountLimitFlushReason:
		return "RowCountLimit"
	case MemoryLimitFlushReason:
		return "MemoryLimit"
	case TimeTickFlushReason:
		return "TimeTick"
	case QueryFlushReason:
		return "Query"
	default:
		return "Unknown"
	}
}
