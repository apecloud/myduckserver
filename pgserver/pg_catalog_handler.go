package pgserver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/pgserver/pgconfig"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5/pgproto3"
)

// precompile a regex to match "select pg_catalog.pg_is_in_recovery();"
var pgIsInRecoveryRegex = regexp.MustCompile(`(?i)^\s*select\s+pg_catalog\.pg_is_in_recovery\(\s*\)\s*;?\s*$`)

// precompile a regex to match "select pg_catalog.pg_current_wal_lsn();" or "select pg_catalog.pg_last_wal_replay_lsn();"
var pgWALLSNRegex = regexp.MustCompile(`(?i)^\s*select\s+pg_catalog\.(pg_current_wal_lsn|pg_last_wal_replay_lsn)\(\s*\)\s*;?\s*$`)

// precompile a regex to match "select pg_catalog.current_setting('xxx');".
var currentSettingRegex = regexp.MustCompile(`(?i)^\s*select\s+(pg_catalog.)?current_setting\(\s*'([^']+)'\s*\)\s*;?\s*$`)

// precompile a regex to match any "from pg_catalog.pg_stat_replication" in the query.
var pgCatalogRegex = regexp.MustCompile(`(?i)\s+from\s+pg_catalog\.(pg_stat_replication)`)

// isInRecovery will get the count of
func (h *ConnectionHandler) isInRecovery() (string, error) {
	// Grab a sql.Context.
	ctx, err := h.duckHandler.NewContext(context.Background(), h.mysqlConn, "")
	if err != nil {
		return "f", err
	}
	var count int
	if err := adapter.QueryRow(ctx, catalog.InternalTables.PgSubscription.CountAllStmt()).Scan(&count); err != nil {
		return "f", err
	}

	if count == 0 {
		return "f", nil
	} else {
		return "t", nil
	}
}

// readOneWALPositionStr reads one of the recorded WAL position from the WAL position table
func (h *ConnectionHandler) readOneWALPositionStr() (string, error) {
	// Grab a sql.Context.
	ctx, err := h.duckHandler.NewContext(context.Background(), h.mysqlConn, "")
	if err != nil {
		return "0/0", err
	}

	// TODO(neo.zty): needs to be fixed
	var subscription, conn, publication, lsn string
	var enabled bool

	if err := adapter.QueryRow(ctx, catalog.InternalTables.PgSubscription.SelectAllStmt()).Scan(&subscription, &conn, &publication, &lsn, &enabled); err != nil {
		if errors.Is(err, stdsql.ErrNoRows) {
			// if no lsn is stored, return 0
			return "0/0", nil
		}
		return "0/0", err
	}

	return lsn, nil
}

// queryPGSetting will query the system variable value from the system variable map
func (h *ConnectionHandler) queryPGSetting(name string) (any, error) {
	sysVar, _, ok := sql.SystemVariables.GetGlobal(name)
	if !ok {
		return nil, fmt.Errorf("error: %s variable was not found", name)
	}
	ctx, err := h.duckHandler.NewContext(context.Background(), h.mysqlConn, "")
	if err != nil {
		return nil, fmt.Errorf("error creating context: %w", err)
	}
	v, err := sysVar.GetSessionScope().GetValue(ctx, name, sql.Collation_Default)
	if err != nil {
		return nil, fmt.Errorf("error: %s variable was not found, err: %w", name, err)
	}
	return v, nil
}

// setPgSessionVar will set the session variable to the value provided for pg.
// And reply with the CommandComplete and ParameterStatus messages.
func (h *ConnectionHandler) setPgSessionVar(name string, value any, useDefault bool, tag string) (bool, error) {
	sysVar, _, ok := sql.SystemVariables.GetGlobal(name)
	if !ok {
		return false, fmt.Errorf("error: %s variable was not found", name)
	}
	ctx, err := h.duckHandler.NewContext(context.Background(), h.mysqlConn, "")
	if err != nil {
		return false, err
	}
	if useDefault {
		value = sysVar.GetDefault()
	}
	err = sysVar.GetSessionScope().SetValue(ctx, name, value)
	if err != nil {
		return false, err
	}
	v, err := sysVar.GetSessionScope().GetValue(ctx, name, sql.Collation_Default)
	if err != nil {
		return false, fmt.Errorf("error: %s variable was not found, err: %w", name, err)
	}
	// Sent CommandComplete message
	err = h.send(makeCommandComplete(tag, 0))
	if err != nil {
		return true, err
	}
	// Sent ParameterStatus message
	if err := h.send(&pgproto3.ParameterStatus{
		Name:  name,
		Value: fmt.Sprintf("%v", v),
	}); err != nil {
		return true, err
	}
	return true, nil
}

// handler for pgIsInRecovery
func (h *ConnectionHandler) handleIsInRecovery() (bool, error) {
	isInRecovery, err := h.isInRecovery()
	if err != nil {
		return false, err
	}
	return true, h.query(ConvertedQuery{
		String:       fmt.Sprintf(`SELECT '%s' AS "pg_is_in_recovery";`, isInRecovery),
		StatementTag: "SELECT",
	})
}

// handler for pgWALLSN
func (h *ConnectionHandler) handleWALSN() (bool, error) {
	lsnStr, err := h.readOneWALPositionStr()
	if err != nil {
		return false, err
	}
	return true, h.query(ConvertedQuery{
		String:       fmt.Sprintf(`SELECT '%s' AS "%s";`, lsnStr, "pg_current_wal_lsn"),
		StatementTag: "SELECT",
	})
}

// handler for currentSetting
func (h *ConnectionHandler) handleCurrentSetting(query ConvertedQuery) (bool, error) {
	sql := RemoveComments(query.String)
	matches := currentSettingRegex.FindStringSubmatch(sql)
	if len(matches) != 3 {
		return false, fmt.Errorf("error: invalid current_setting query")
	}
	setting, err := h.queryPGSetting(matches[2])
	if err != nil {
		return false, err
	}
	return true, h.query(ConvertedQuery{
		String:       fmt.Sprintf(`SELECT '%s' AS "current_setting";`, fmt.Sprintf("%v", setting)),
		StatementTag: "SELECT",
	})
}

// handler for pgCatalog
func (h *ConnectionHandler) handlePgCatalog(query ConvertedQuery) (bool, error) {
	sql := RemoveComments(query.String)
	return true, h.query(ConvertedQuery{
		String:       pgCatalogRegex.ReplaceAllString(sql, " FROM __sys__.$1"),
		StatementTag: "SELECT",
	})
}

type PGCatalogHandler struct {
	// HandledInPlace is a function that determines if the query should be handled in place and not passed to the engine.
	HandledInPlace func(ConvertedQuery) (bool, error)
	Handler        func(*ConnectionHandler, ConvertedQuery) (bool, error)
}

func isPgIsInRecovery(query ConvertedQuery) bool {
	sql := RemoveComments(query.String)
	return pgIsInRecoveryRegex.MatchString(sql)
}

func isPgWALSN(query ConvertedQuery) bool {
	sql := RemoveComments(query.String)
	return pgWALLSNRegex.MatchString(sql)
}

func isPgCurrentSetting(query ConvertedQuery) bool {
	sql := RemoveComments(query.String)
	if !currentSettingRegex.MatchString(sql) {
		return false
	}
	matches := currentSettingRegex.FindStringSubmatch(sql)
	if len(matches) != 3 {
		return false
	}
	if !pgconfig.IsValidPostgresConfigParameter(matches[2]) {
		// This is a configuration of DuckDB, it should be bypassed to DuckDB
		return false
	}
	return true
}

func isSpecialPgCatalog(query ConvertedQuery) bool {
	sql := RemoveComments(query.String)
	return pgCatalogRegex.MatchString(sql)
}

// The key is the statement tag of the query.
var pgCatalogHandlers = map[string]PGCatalogHandler{
	"SELECT": {
		HandledInPlace: func(query ConvertedQuery) (bool, error) {
			// TODO(sean): Evaluate the conditions by iterating over the AST.
			if isPgIsInRecovery(query) {
				return true, nil
			}
			if isPgWALSN(query) {
				return true, nil
			}
			if isPgCurrentSetting(query) {
				return true, nil
			}
			if isSpecialPgCatalog(query) {
				return true, nil
			}
			return false, nil
		},
		Handler: func(h *ConnectionHandler, query ConvertedQuery) (bool, error) {
			if isPgIsInRecovery(query) {
				return h.handleIsInRecovery()
			}
			if isPgWALSN(query) {
				return h.handleWALSN()
			}
			if isPgCurrentSetting(query) {
				return h.handleCurrentSetting(query)
			}
			//if pgCatalogRegex.MatchString(sql) {
			if isSpecialPgCatalog(query) {
				return h.handlePgCatalog(query)
			}
			return false, nil
		},
	},
	"SHOW": {
		HandledInPlace: func(query ConvertedQuery) (bool, error) {
			switch query.AST.(type) {
			case *tree.ShowVar:
				return true, nil
			}
			return false, nil
		},
		Handler: func(h *ConnectionHandler, query ConvertedQuery) (bool, error) {
			showVar, ok := query.AST.(*tree.ShowVar)
			if !ok {
				return false, fmt.Errorf("error: invalid show_variables query: %v", query.String)
			}
			key := strings.ToLower(showVar.Name)
			if key != "all" {
				setting, err := h.queryPGSetting(key)
				if err != nil {
					return false, err
				}
				return true, h.query(ConvertedQuery{
					String:       fmt.Sprintf(`SELECT '%s' AS "%s";`, fmt.Sprintf("%v", setting), key),
					StatementTag: "SELECT",
				})
			}
			// TODO(sean): Implement SHOW ALL
			_ = h.send(&pgproto3.ErrorResponse{
				Severity: string(ErrorResponseSeverity_Error),
				Code:     "0A000",
				Message:  "Statement 'SHOW ALL' is not supported yet.",
			})
			return true, nil
		},
	},
	"SET": {
		HandledInPlace: func(query ConvertedQuery) (bool, error) {
			switch stmt := query.AST.(type) {
			case *tree.SetVar:
				key := strings.ToLower(stmt.Name)
				if key == "database" {
					// This is the statement of `USE xxx`, which is used for changing the schema.
					// Route it to the engine directly.
					return false, nil
				}
				if !pgconfig.IsValidPostgresConfigParameter(key) {
					// This is a configuration of DuckDB, it should be bypassed to DuckDB
					return false, nil
				}
				if len(stmt.Values) > 1 {
					return false, fmt.Errorf("error: invalid set statement: %v", query.String)
				}
				return true, nil
			}
			return false, nil
		},
		Handler: func(h *ConnectionHandler, query ConvertedQuery) (bool, error) {
			setVar, ok := query.AST.(*tree.SetVar)
			if !ok {
				return false, fmt.Errorf("error: invalid set statement: %v", query.String)
			}
			key := strings.ToLower(setVar.Name)
			value := setVar.Values[0]
			_, isDefault := value.(tree.DefaultVal)

			if key == "database" {
				// This is the statement of `USE xxx`, which is used for changing the schema.
				// Route it to the engine directly.
				return false, nil
			}
			if !pgconfig.IsValidPostgresConfigParameter(key) {
				// This is a configuration of DuckDB, it should be bypassed to DuckDB
				return false, nil
			}

			var v any
			switch val := value.(type) {
			case *tree.UnresolvedName:
				if val.NumParts != 1 {
					return false, fmt.Errorf("error: invalid value in set statement: %v", query.String)
				}
				v = val.Parts[0]
			case *tree.StrVal:
				v = val.RawString()
			default:
				v = val.String()
			}

			return h.setPgSessionVar(key, v, isDefault, "SET")
		},
	},
	"RESET": {
		HandledInPlace: func(query ConvertedQuery) (bool, error) {
			switch stmt := query.AST.(type) {
			case *tree.SetVar:
				if !stmt.Reset && !stmt.ResetAll {
					return false, fmt.Errorf("error: invalid reset statement: %v", stmt)
				}
				key := strings.ToLower(stmt.Name)
				if !pgconfig.IsValidPostgresConfigParameter(key) {
					return false, nil
				}
				return true, nil
			}
			return false, nil
		},
		Handler: func(h *ConnectionHandler, query ConvertedQuery) (bool, error) {
			resetVar, ok := query.AST.(*tree.SetVar)
			if !ok || (!resetVar.Reset && !resetVar.ResetAll) {
				return false, fmt.Errorf("error: invalid reset statement: %v", query.String)
			}
			key := strings.ToLower(resetVar.Name)
			if !pgconfig.IsValidPostgresConfigParameter(key) {
				// This is a configuration of DuckDB, it should be bypassed to DuckDB
				return false, nil
			}
			if !resetVar.ResetAll {
				return h.setPgSessionVar(key, nil, true, "RESET")
			}
			// TODO(sean): Implement RESET ALL
			_ = h.send(&pgproto3.ErrorResponse{
				Severity: string(ErrorResponseSeverity_Error),
				Code:     "0A000",
				Message:  "Statement 'RESET ALL' is not supported yet.",
			})
			return true, nil
		},
	},
}

// checkIsPgCatalogStmtAndHandledInPlace checks whether a query matches any regex in the pgCatalogHandlers
// and the query should be handled in place rather than being passed to the engine.
func checkIsPgCatalogStmtAndHandledInPlace(sql ConvertedQuery) (bool, error) {
	handler, ok := pgCatalogHandlers[sql.StatementTag]
	if !ok {
		return false, nil
	}
	handledInPlace, err := handler.HandledInPlace(sql)
	if err != nil {
		return false, err
	}
	return handledInPlace, nil
}

// TODO(sean): This is a temporary work around for clients that query the views from schema 'pg_catalog'.
// Remove this once we add the views for 'pg_catalog'.
func (h *ConnectionHandler) handlePgCatalogQueries(sql ConvertedQuery) (bool, error) {
	handler, ok := pgCatalogHandlers[sql.StatementTag]
	if !ok {
		return false, nil
	}
	return handler.Handler(h, sql)
}

// shouldQueryBeHandledInPlace determines whether a query should be handled in place, rather than being
// passed to the engine. This is useful for queries that are not supported by the engine, or that require
// special handling.
func shouldQueryBeHandledInPlace(query ConvertedQuery) (bool, error) {
	return checkIsPgCatalogStmtAndHandledInPlace(query)
}
