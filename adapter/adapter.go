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
	CloseTxn()
	CloseBackendConn()
}

func GetConn(ctx *sql.Context) (*stdsql.Conn, error) {
	return ctx.Session.(ConnectionHolder).GetConn(ctx)
}

func GetCatalogConn(ctx *sql.Context) (*stdsql.Conn, error) {
	return ctx.Session.(ConnectionHolder).GetCatalogConn(ctx)
}

func CloseBackendConn(ctx *sql.Context) {
	ctx.Session.(ConnectionHolder).CloseBackendConn()
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

func CloseTxn(ctx *sql.Context) {
	ctx.Session.(ConnectionHolder).CloseTxn()
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
