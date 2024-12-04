package logrepl

import (
	"fmt"
	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/sql"
	"sync"
)

type Subscription struct {
	Name        string
	Conn        string
	Publication string
	Replicator  *LogicalReplicator
}

var subscriptionMap = sync.Map{}

func GetAllSubscriptions(ctx *sql.Context) ([]*Subscription, error) {
	if err := loadAllSubscriptions(ctx); err != nil {
		return nil, err
	}

	var replicators []*Subscription
	subscriptionMap.Range(func(key, value interface{}) bool {
		replicators = append(replicators, value.(*Subscription))
		return true
	})

	return replicators, nil
}

func GetSubscription(ctx *sql.Context, name string) (*Subscription, error) {
	value, ok := subscriptionMap.Load(name)
	if !ok {
		err := loadAllSubscriptions(ctx)
		if err != nil {
			return nil, err
		}
		value, ok = subscriptionMap.Load(name)
		if !ok {
			return nil, err
		}
	}

	return value.(*Subscription), nil
}

func CreateSubscription(ctx *sql.Context, name string, conn string, publication string) (*Subscription, error) {

	subscription, err := GetSubscription(ctx, name)
	if err != nil {
		return nil, err
	}

	if subscription != nil {
		return nil, fmt.Errorf("subscription %s already exists", name)
	}

	replicator, err := NewLogicalReplicator(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to create logical replicator: %v", err)
	}

	subscription = &Subscription{
		Name:        name,
		Conn:        conn,
		Publication: publication,
		Replicator:  replicator,
	}

	subscriptionMap.Store(name, subscription)

	return subscription, nil
}

func loadAllSubscriptions(ctx *sql.Context) error {
	rows, err := adapter.QueryCatalog(ctx, catalog.InternalTables.PgSubscription.SelectAllStmt())
	if err != nil {
		return err
	}
	defer rows.Close()

	if rows != nil {
		for rows.Next() {
			var name, conn, pub string
			err = rows.Scan(&name, &conn, &pub)
			_, ok := subscriptionMap.Load(name)
			if !ok {
				replicator, err := NewLogicalReplicator(conn)
				if err != nil {
					return err
				}
				subscriptionMap.Store(name, &Subscription{
					Name:        name,
					Conn:        conn,
					Publication: pub,
					Replicator:  replicator,
				})
			}
		}
	}

	return err
}

func WriteSubscription(ctx *sql.Context, name, conn, pub string) error {
	_, err := adapter.ExecCatalogInTxn(ctx, catalog.InternalTables.PgSubscription.UpsertStmt(), name, conn, pub)
	return err
}
