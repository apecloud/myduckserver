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

type InPlaceHandler struct {
	// ShouldBeHandledInPlace is a function that determines if the query should be
	// handled in place and not passed to the engine.
	ShouldBeHandledInPlace func(*ConnectionHandler, *ConvertedStatement) (bool, error)
	Handler                func(*ConnectionHandler, ConvertedStatement) (bool, error)
}

type SelectionConversion struct {
	needConvert func(*ConvertedStatement) bool
	doConvert   func(*ConnectionHandler, *ConvertedStatement) error
	// Indicate that the query will be converted to a constant query.
	// The data will be fetched internally and used as a constant value for query.
	// e.g. SELECT current_setting('application_name'); -> SELECT 'myDUCK' AS "current_setting";
	// Be careful while handling extended queries, as the SQL statement requested by the client
	// is a prepared statement. If we convert the query to a constant query, the client
	// will not be able to fetch the fresh data from the server.
	isConstQuery bool
}

var selectionConversions = []SelectionConversion{
	{
		needConvert: func(query *ConvertedStatement) bool {
			sql := RemoveComments(query.String)
			// TODO(sean): Evaluate the conditions by iterating over the AST.
			return pgIsInRecoveryRegex.MatchString(sql)
		},
		doConvert: func(h *ConnectionHandler, query *ConvertedStatement) error {
			isInRecovery, err := h.isInRecovery()
			if err != nil {
				return err
			}
			sqlStr := fmt.Sprintf(`SELECT '%s' AS "pg_is_in_recovery";`, isInRecovery)
			query.String = sqlStr
			return nil
		},
	},
	{
		needConvert: func(query *ConvertedStatement) bool {
			sql := RemoveComments(query.String)
			// TODO(sean): Evaluate the conditions by iterating over the AST.
			return pgWALLSNRegex.MatchString(sql)
		},
		doConvert: func(h *ConnectionHandler, query *ConvertedStatement) error {
			lsnStr, err := h.readOneWALPositionStr()
			if err != nil {
				return err
			}
			sqlStr := fmt.Sprintf(`SELECT '%s' AS "%s";`, lsnStr, "pg_current_wal_lsn")
			query.String = sqlStr
			return nil
		},
	},
	{
		needConvert: func(query *ConvertedStatement) bool {
			sql := RemoveComments(query.String)
			// TODO(sean): Evaluate the conditions by iterating over the AST.
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
		},
		doConvert: func(h *ConnectionHandler, query *ConvertedStatement) error {
			sql := RemoveComments(query.String)
			matches := currentSettingRegex.FindStringSubmatch(sql)
			setting, err := h.queryPGSetting(matches[2])
			if err != nil {
				return err
			}
			sqlStr := fmt.Sprintf(`SELECT '%s' AS "current_setting";`, fmt.Sprintf("%v", setting))
			query.String = sqlStr
			return nil
		},
		isConstQuery: true,
	},
	{
		needConvert: func(query *ConvertedStatement) bool {
			sql := RemoveComments(query.String)
			// TODO(sean): Evaluate the conditions by iterating over the AST.
			return getPgCatalogRegex().MatchString(sql)
		},
		doConvert: func(h *ConnectionHandler, query *ConvertedStatement) error {
			sqlStr := ConvertToSys(query.String)
			query.String = sqlStr
			return nil
		},
	},
	{
		needConvert: func(query *ConvertedStatement) bool {
			sql := RemoveComments(query.String)
			// TODO(sean): Evaluate the conditions by iterating over the AST.
			return getRenamePgCatalogFuncRegex().MatchString(sql) || getPgFuncRegex().MatchString(sql)
		},
		doConvert: func(h *ConnectionHandler, query *ConvertedStatement) error {
			var sqlStr string
			if getRenamePgCatalogFuncRegex().MatchString(query.String) {
				sqlStr = ConvertPgCatalogFuncToSys(query.String)
			} else {
				sqlStr = query.String
			}
			sqlStr = ConvertToDuckDBMacro(sqlStr)
			query.String = sqlStr
			return nil
		},
	},
	{
		needConvert: func(query *ConvertedStatement) bool {
			sqlStr := RemoveComments(query.String)
			// TODO(sean): Evaluate the conditions by iterating over the AST.
			return getSimpleStringMatchingRegex().MatchString(sqlStr)
		},
		doConvert: func(h *ConnectionHandler, query *ConvertedStatement) error {
			sqlStr := RemoveComments(query.String)
			sqlStr = SimpleStrReplacement(sqlStr)
			query.String = sqlStr
			return nil
		},
	},
	{
		needConvert: func(query *ConvertedStatement) bool {
			sqlStr := RemoveComments(query.String)
			// TODO: Evaluate the conditions by iterating over the AST.
			return getPgAnyOpRegex().MatchString(sqlStr)
		},
		doConvert: func(h *ConnectionHandler, query *ConvertedStatement) error {
			sqlStr := RemoveComments(query.String)
			sqlStr = ConvertAnyOp(sqlStr)
			query.String = sqlStr
			return nil
		},
	},
}

// The key is the statement tag of the query.
var inPlaceHandlers = map[string]InPlaceHandler{
	"SELECT": {
		ShouldBeHandledInPlace: func(h *ConnectionHandler, query *ConvertedStatement) (bool, error) {
			for _, conv := range selectionConversions {
				if conv.needConvert(query) {
					var err error
					if conv.isConstQuery {
						// Since the query is a constant query, we should not modify the query before
						// it's executed. Instead, we mark it as a query that should be handled in place.
						return true, nil
					}
					// Do not execute this query here. Instead, fallback to the original processing.
					// So we don't have to deal with the dynamic SQL with placeholders here.
					err = conv.doConvert(h, query)
					if err != nil {
						return false, err
					}
				}
			}
			return false, nil
		},
		Handler: func(h *ConnectionHandler, query ConvertedStatement) (bool, error) {
			// This is for simple query
			converted := false
			convertedStatement := query
			for _, conv := range selectionConversions {
				if conv.needConvert(&convertedStatement) {
					var err error
					err = conv.doConvert(h, &convertedStatement)
					if err != nil {
						return false, err
					}
					converted = true
				}
			}
			if converted {
				return true, h.run(convertedStatement)
			}
			return false, nil
		},
	},
	"SHOW": {
		ShouldBeHandledInPlace: func(h *ConnectionHandler, query *ConvertedStatement) (bool, error) {
			switch query.AST.(type) {
			case *tree.ShowVar:
				return true, nil
			}
			return false, nil
		},
		Handler: func(h *ConnectionHandler, query ConvertedStatement) (bool, error) {
			showVar, ok := query.AST.(*tree.ShowVar)
			if !ok {
				return false, nil
			}
			key := strings.ToLower(showVar.Name)
			if key != "all" {
				setting, err := h.queryPGSetting(key)
				if err != nil {
					return false, err
				}
				return true, h.run(ConvertedStatement{
					String: fmt.Sprintf(`SELECT '%s' AS "%s";`, fmt.Sprintf("%v", setting), key),
					Tag:    "SELECT",
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
		ShouldBeHandledInPlace: func(h *ConnectionHandler, query *ConvertedStatement) (bool, error) {
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
			case *tree.SetSessionCharacteristics:
				// This is a statement of `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL xxx`.
				return true, nil
			}
			return false, nil
		},
		Handler: func(h *ConnectionHandler, query ConvertedStatement) (bool, error) {
			var key string
			var value any
			var isDefault bool
			switch stmt := query.AST.(type) {
			case *tree.SetVar:
				key = strings.ToLower(stmt.Name)
				value = stmt.Values[0]
				_, isDefault = value.(tree.DefaultVal)
			case *tree.SetSessionCharacteristics:
				// This is a statement of `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL xxx`.
				key = "default_transaction_isolation"
				value = strings.ReplaceAll(stmt.Modes.Isolation.String(), " ", "-")
				isDefault = false
			default:
				return false, fmt.Errorf("error: invalid set statement: %v", query.String)
			}

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
				v = fmt.Sprintf("%v", val)
			}

			return h.setPgSessionVar(key, v, isDefault, "SET")
		},
	},
	"RESET": {
		ShouldBeHandledInPlace: func(h *ConnectionHandler, query *ConvertedStatement) (bool, error) {
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
		Handler: func(h *ConnectionHandler, query ConvertedStatement) (bool, error) {
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

// shouldQueryBeHandledInPlace determines whether a query should be handled in place, rather than being
// passed to the engine. This is useful for queries that are not supported by the engine, or that require
// special handling.
func shouldQueryBeHandledInPlace(h *ConnectionHandler, sql *ConvertedStatement) (bool, error) {
	handler, ok := inPlaceHandlers[sql.Tag]
	if !ok {
		return false, nil
	}
	handledInPlace, err := handler.ShouldBeHandledInPlace(h, sql)
	if err != nil {
		return false, err
	}
	return handledInPlace, nil
}

// TODO(sean): This is a temporary work around for clients that query the views from schema 'pg_catalog'.
// Remove this once we add the views for 'pg_catalog'.
func (h *ConnectionHandler) handleInPlaceQueries(sql ConvertedStatement) (bool, error) {
	handler, ok := inPlaceHandlers[sql.Tag]
	if !ok {
		return false, nil
	}
	return handler.Handler(h, sql)
}
