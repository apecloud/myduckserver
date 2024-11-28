package logrepl

import (
	"context"
	stdsql "database/sql"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/sql"
)

func WriteSubscription(ctx *sql.Context, name, conn, pub string) error {
	_, err := adapter.ExecCatalogInTxn(ctx, catalog.InternalTables.PgSubscription.UpsertStmt(), name, conn, pub)
	return err
}

func FindReplication(db *stdsql.DB) (name, conn, pub string, ok bool, err error) {
	var rows *stdsql.Rows
	rows, err = db.QueryContext(context.Background(), catalog.InternalTables.PgSubscription.SelectAllStmt())
	if err != nil {
		return
	}
	defer rows.Close()

	if !rows.Next() {
		return
	}

	if err = rows.Scan(&name, &conn, &pub); err != nil {
		return
	}

	ok = true
	return
}
