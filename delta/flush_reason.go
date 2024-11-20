package delta

type FlushReason uint8

const (
	// UnknownFlushReason means that the changes have to be flushed for an unknown reason.
	UnknownFlushReason FlushReason = iota
	// DDLStmtFlushReason means that the changes have to be flushed because of a DDL statement.
	DDLStmtFlushReason
	// DMLStmtFlushReason means that the changes have to be flushed because of a DML statement.
	DMLStmtFlushReason
	// RowCountLimitFlushReason means that the changes have to be flushed because the row count limit is reached.
	RowCountLimitFlushReason
	// MemoryLimitFlushReason means that the changes have to be flushed because the memory limit is reached.
	MemoryLimitFlushReason
	// TimeTickFlushReason means that the changes have to be flushed because a time ticker is fired.
	TimeTickFlushReason
	// QueryFlushReason means that the changes have to be flushed because some tables are queried.
	QueryFlushReason
	// InitFlushReason means that the changes have to be flushed because the controller is initialized.
	InitFlushReason
	// OnCloseFlushReason means that the changes have to be flushed because the controller is closed.
	OnCloseFlushReason
)

func (r FlushReason) String() string {
	switch r {
	case DDLStmtFlushReason:
		return "DDLStmt"
	case DMLStmtFlushReason:
		return "DMLStmt"
	case RowCountLimitFlushReason:
		return "RowCountLimit"
	case MemoryLimitFlushReason:
		return "MemoryLimit"
	case TimeTickFlushReason:
		return "TimeTick"
	case QueryFlushReason:
		return "Query"
	case InitFlushReason:
		return "Init"
	case OnCloseFlushReason:
		return "OnClose"
	default:
		return "Unknown"
	}
}
