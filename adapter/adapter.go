package adapter

import (
	"context"
	stdsql "database/sql"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

type ConnectionHolder interface {
	GetConn(ctx context.Context) (*stdsql.Conn, error)
	GetTxn(ctx context.Context, options *stdsql.TxOptions) (*stdsql.Tx, error)
	GetCatalogConn(ctx context.Context) (*stdsql.Conn, error)
	GetCatalogTxn(ctx context.Context, options *stdsql.TxOptions) (*stdsql.Tx, error)
	TryGetTxn() *stdsql.Tx
	GetCurrentCatalog() string
	GetCurrentSchema() string
	CloseTxn()
	CloseConn()
}

// SQLExecutor is the common database/sql surface used by statement execution.
// Both *sql.Conn and *sql.Tx satisfy it. Keeping the interface here lets query
// paths bind to an already active session transaction without giving up the
// connection-specific setup needed by callers that acquire a *sql.Conn.
type SQLExecutor interface {
	ExecContext(context.Context, string, ...any) (stdsql.Result, error)
	QueryContext(context.Context, string, ...any) (*stdsql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *stdsql.Row
	PrepareContext(context.Context, string) (*stdsql.Stmt, error)
}

func GetConn(ctx *sql.Context) (*stdsql.Conn, error) {
	return ctx.Session.(ConnectionHolder).GetConn(ctx)
}

func GetCatalogConn(ctx *sql.Context) (*stdsql.Conn, error) {
	return ctx.Session.(ConnectionHolder).GetCatalogConn(ctx)
}

func CloseConn(ctx *sql.Context) {
	ctx.Session.(ConnectionHolder).CloseConn()
}

func GetTxn(ctx *sql.Context, options *stdsql.TxOptions) (*stdsql.Tx, error) {
	return ctx.Session.(ConnectionHolder).GetTxn(ctx, options)
}

func GetCatalogTxn(ctx *sql.Context, options *stdsql.TxOptions) (*stdsql.Tx, error) {
	return ctx.Session.(ConnectionHolder).GetCatalogTxn(ctx, options)
}

func TryGetTxn(ctx *sql.Context) *stdsql.Tx {
	return ctx.Session.(ConnectionHolder).TryGetTxn()
}

func GetCurrentCatalog(ctx *sql.Context) string {
	return ctx.Session.(ConnectionHolder).GetCurrentCatalog()
}

func GetCurrentSchema(ctx *sql.Context) string {
	return ctx.Session.(ConnectionHolder).GetCurrentSchema()
}

func CloseTxn(ctx *sql.Context) {
	ctx.Session.(ConnectionHolder).CloseTxn()
}

// SQLExecutorForConn returns the active session transaction when one exists;
// otherwise it returns the already acquired connection. Callers that need
// driver-connection operations (for example, DuckLake ATTACH) should continue
// using the original *sql.Conn directly.
func SQLExecutorForConn(ctx *sql.Context, conn *stdsql.Conn) SQLExecutor {
	if tx := TryGetTxn(ctx); tx != nil {
		return tx
	}
	return conn
}

// GetSQLExecutor acquires the normal session connection only when no active
// transaction owns it. It is intended for statement helpers whose work must
// participate in the session transaction when present.
func GetSQLExecutor(ctx *sql.Context) (SQLExecutor, error) {
	if tx := TryGetTxn(ctx); tx != nil {
		return tx, nil
	}
	return GetConn(ctx)
}

// GetCatalogExecutor is the catalog equivalent of GetSQLExecutor. Metadata
// reads and writes therefore observe the same transaction as object-table DML.
func GetCatalogExecutor(ctx *sql.Context) (SQLExecutor, error) {
	if tx := TryGetTxn(ctx); tx != nil {
		return tx, nil
	}
	return GetCatalogConn(ctx)
}

func Query(ctx *sql.Context, query string, args ...any) (*stdsql.Rows, error) {
	conn, err := GetConn(ctx)
	if err != nil {
		return nil, err
	}
	return conn.QueryContext(ctx, query, args...)
}

func QueryRow(ctx *sql.Context, query string, args ...any) *stdsql.Row {
	conn, err := GetConn(ctx)
	if err != nil {
		return nil
	}
	return conn.QueryRowContext(ctx, query, args...)
}

// QueryCatalog is a helper function to query the catalog, such as information_schema.
// Unlike QueryContext, this function does not require a schema name to be set on the connection,
// and the current schema of the connection does not matter.
func QueryCatalog(ctx *sql.Context, query string, args ...any) (*stdsql.Rows, error) {
	conn, err := ctx.Session.(ConnectionHolder).GetCatalogConn(ctx)
	if err != nil {
		return nil, err
	}
	return conn.QueryContext(ctx, query, args...)
}

func QueryRowCatalog(ctx *sql.Context, query string, args ...any) *stdsql.Row {
	conn, err := ctx.Session.(ConnectionHolder).GetCatalogConn(ctx)
	if err != nil {
		return nil
	}
	return conn.QueryRowContext(ctx, query, args...)
}

func Exec(ctx *sql.Context, query string, args ...any) (stdsql.Result, error) {
	conn, err := GetConn(ctx)
	if err != nil {
		return nil, err
	}
	return conn.ExecContext(ctx, query, args...)
}

// ExecCatalog is a helper function to execute a catalog modification query, such as creating a database.
// Unlike ExecContext, this function does not require a schema name to be set on the connection,
// and the current schema of the connection does not matter.
func ExecCatalog(ctx *sql.Context, query string, args ...any) (stdsql.Result, error) {
	conn, err := ctx.Session.(ConnectionHolder).GetCatalogConn(ctx)
	if err != nil {
		return nil, err
	}
	return conn.ExecContext(ctx, query, args...)
}

func ExecCatalogInTxn(ctx *sql.Context, query string, args ...any) (stdsql.Result, error) {
	tx, err := ctx.Session.(ConnectionHolder).GetCatalogTxn(ctx, nil)
	if err != nil {
		return nil, err
	}
	return tx.ExecContext(ctx, query, args...)
}

func ExecInTxn(ctx *sql.Context, query string, args ...any) (stdsql.Result, error) {
	tx, err := GetTxn(ctx, nil)
	if err != nil {
		return nil, err
	}
	return tx.ExecContext(ctx, query, args...)
}

func CommitAndCloseTxn(sqlCtx *sql.Context) error {
	tx := TryGetTxn(sqlCtx)
	if tx != nil {
		defer CloseTxn(sqlCtx)
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
	}
	return nil
}
