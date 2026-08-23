package backend

import (
	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"
)

// QueryAudit records the outcome of one ordinary protocol query.
type QueryAudit struct {
	entry *logrus.Entry
	rows  uint64
}

// NewQueryAudit starts an audit record for a user-facing protocol query.
func NewQueryAudit(conn *mysql.Conn, protocol, query string) *QueryAudit {
	fields := logrus.Fields{
		"audit":    "query",
		"protocol": protocol,
		"query":    query,
		"user":     "",
	}
	if conn != nil {
		fields["user"] = conn.User
	}
	return &QueryAudit{entry: logrus.WithFields(fields)}
}

// AddRows records rows successfully handed to the protocol callback.
func (audit *QueryAudit) AddRows(rows int) {
	audit.rows += uint64(rows)
}

// Complete emits the query's single structured audit record.
func (audit *QueryAudit) Complete(err error) {
	entry := audit.entry.WithField("rows", audit.rows)
	if err != nil {
		entry = entry.WithError(err)
	}
	entry.Info("query audit")
}
