package binlogreplication

// MySQL binlog event types that are safe to ignore at the replica.
// Header byte 4 is Log_event_type. See include/mysql/binlog_event.h
const (
	binlogEventStop      = 0x03 // STOP_EVENT
	binlogEventHeartbeat = 0x1b // HEARTBEAT_LOG_EVENT (network only)
	binlogEventIgnorable = 0x1c // IGNORABLE_LOG_EVENT
	binlogEventRowsQuery = 0x1d // ROWS_QUERY_LOG_EVENT
)

func isIgnorableHeaderEventType(eventType byte) bool {
	switch eventType {
	case binlogEventStop, binlogEventHeartbeat, binlogEventIgnorable, binlogEventRowsQuery:
		return true
	default:
		return false
	}
}
