package backend

import (
	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"
)

// QueryAudit records the outcome of one ordinary protocol query.
type QueryAudit struct {
	entry        *logrus.Entry
	rows         uint64
	redactErrors bool
}

// NewQueryAudit starts an audit record for a user-facing protocol query.
func NewQueryAudit(conn *mysql.Conn, protocol, query string) *QueryAudit {
	query = catalog.RedactSensitiveSQL(query)
	fields := logrus.Fields{
		"audit":    "query",
		"protocol": protocol,
		"query":    query,
		"user":     "",
	}
	if conn != nil {
		fields["user"] = conn.User
	}
	return &QueryAudit{
		entry:        logrus.WithFields(fields),
		redactErrors: query == catalog.RedactedSensitiveSQL,
	}
}

// AddRows records rows successfully handed to the protocol callback.
func (audit *QueryAudit) AddRows(rows int) {
	audit.rows += uint64(rows)
}

// Complete emits the query's single structured audit record.
func (audit *QueryAudit) Complete(err error) {
	entry := audit.entry.WithField("rows", audit.rows)
	if err != nil {
		if audit.redactErrors {
			// Legacy BACKUP/RESTORE keeps its historical storage path, but its
			// downstream errors can echo endpoint or credential material. Keep the
			// audit useful without retaining the raw error alongside the redacted
			// query.
			entry = entry.WithField("error", "service-managed operation failed")
		} else {
			entry = entry.WithError(err)
		}
	}
	entry.Info("query audit")
}
