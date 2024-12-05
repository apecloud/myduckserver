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
var createMutex sync.Mutex

func GetAllSubscriptions(ctx *sql.Context) ([]*Subscription, error) {
	if err := loadAllSubscriptions(ctx); err != nil {
		return nil, err
	}

	var subscriptions []*Subscription
	subscriptionMap.Range(func(key, value interface{}) bool {
		if sub, ok := value.(*Subscription); ok {
			subscriptions = append(subscriptions, sub)
		}
		return true
	})

	return subscriptions, nil
}

func GetSubscription(ctx *sql.Context, name string) (*Subscription, error) {
	if value, ok := subscriptionMap.Load(name); ok {
		if sub, ok := value.(*Subscription); ok {
			return sub, nil
		}
	}

	// Attempt to reload all subscriptions if not found
	if err := loadAllSubscriptions(ctx); err != nil {
		return nil, err
	}

	if value, ok := subscriptionMap.Load(name); ok {
		if sub, ok := value.(*Subscription); ok {
			return sub, nil
		}
	}

	return nil, nil
}

func CreateSubscription(ctx *sql.Context, name string, conn string, publication string) (*Subscription, error) {
	createMutex.Lock()
	defer createMutex.Unlock()

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

	for rows.Next() {
		var name, conn, pub string
		if err := rows.Scan(&name, &conn, &pub); err != nil {
			return err
		}
		if _, loaded := subscriptionMap.LoadOrStore(name, &Subscription{
			Name:        name,
			Conn:        conn,
			Publication: pub,
			Replicator:  nil,
		}); !loaded {
			replicator, err := NewLogicalReplicator(conn)
			if err != nil {
				return err
			}
			if sub, ok := subscriptionMap.Load(name); ok {
				if subscription, ok := sub.(*Subscription); ok {
					subscription.Replicator = replicator
				}
			}
		}
	}

	return nil
}

func WriteSubscription(ctx *sql.Context, name, conn, pub string) error {
	_, err := adapter.ExecCatalogInTxn(ctx, catalog.InternalTables.PgSubscription.UpsertStmt(), name, conn, pub)
	return err
}
