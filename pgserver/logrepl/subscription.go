package logrepl

import (
	stdsql "database/sql"
	"errors"
	"fmt"
	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pglogrepl"
	"sync"
)

type Subscription struct {
	Subscription string
	Conn         string
	Publication  string
	Lsn          pglogrepl.LSN
	Enabled      bool
	Replicator   *LogicalReplicator
}

var keyColumns = []string{"subname"}
var statusValueColumns = []string{"substatus"}
var lsnValueColumns = []string{"subskiplsn"}

var subscriptionMap = sync.Map{}
var mu sync.Mutex

func GetSubscription(ctx *sql.Context, name string) (*Subscription, error) {
	if value, ok := subscriptionMap.Load(name); ok {
		if sub, ok := value.(*Subscription); ok {
			return sub, nil
		}
	}

	// Attempt to reload all subscriptions if not found
	if err := UpdateSubscriptions(ctx); err != nil {
		return nil, err
	}

	if value, ok := subscriptionMap.Load(name); ok {
		if sub, ok := value.(*Subscription); ok {
			return sub, nil
		}
	}

	return nil, nil
}

func UpdateSubscriptions(ctx *sql.Context) error {
	mu.Lock()
	defer mu.Unlock()
	rows, err := adapter.QueryCatalog(ctx, catalog.InternalTables.PgSubscription.SelectAllStmt())
	if err != nil {
		return err
	}
	defer rows.Close()

	var tempMap = make(map[string]*Subscription)
	for rows.Next() {
		var name, conn, pub string
		var enabled bool
		if err := rows.Scan(&name, &conn, &pub, &enabled); err != nil {
			return err
		}
		tempMap[name] = &Subscription{
			Subscription: name,
			Conn:         conn,
			Publication:  pub,
			Enabled:      enabled,
			Replicator:   nil,
		}
	}

	for tempName, tempSub := range tempMap {
		if _, loaded := subscriptionMap.LoadOrStore(tempName, tempSub); !loaded {
			replicator, err := NewLogicalReplicator(tempSub.Conn)
			if err != nil {
				return fmt.Errorf("failed to create logical replicator: %v", err)
			}

			if sub, ok := subscriptionMap.Load(tempName); ok {
				if subscription, ok := sub.(*Subscription); ok {
					subscription.Replicator = replicator
				}
			}

			err = replicator.CreateReplicationSlotIfNotExists(tempSub.Publication)
			if err != nil {
				return fmt.Errorf("failed to create replication slot: %v", err)
			}
		} else {
			if sub, ok := subscriptionMap.Load(tempSub); ok {
				if subscription, ok := sub.(*Subscription); ok {
					if tempSub.Enabled != subscription.Enabled {
						subscription.Enabled = tempSub.Enabled
						if subscription.Enabled {
							go subscription.Replicator.StartReplication(ctx, subscription.Publication)
						} else {
							subscription.Replicator.Stop()
						}
					}
				}
			}
		}
	}

	subscriptionMap.Range(func(key, value interface{}) bool {
		name, _ := key.(string)
		subscription, _ := value.(*Subscription)
		if _, ok := tempMap[name]; !ok {
			subscription.Replicator.Stop()
			subscriptionMap.Delete(name)
		}
		return true
	})

	return nil
}

func CreateSubscription(ctx *sql.Context, name, conn, pub, lsn string, enabled bool) error {
	_, err := adapter.ExecCatalogInTxn(ctx, catalog.InternalTables.PgSubscription.UpsertStmt(), name, conn, pub, lsn, enabled)
	return err
}

func UpdateSubscriptionStatus(ctx *sql.Context, name string, enabled bool) error {
	_, err := adapter.ExecCatalogInTxn(ctx, catalog.InternalTables.PgSubscription.UpdateStmt(keyColumns, statusValueColumns), name, enabled)
	return err
}

func DeleteSubscription(ctx *sql.Context, name string) error {
	_, err := adapter.ExecCatalogInTxn(ctx, catalog.InternalTables.PgSubscription.DeleteStmt(), name)
	return err
}

func UpdateSubscriptionLsn(ctx *sql.Context, name, lsn string) error {
	_, err := adapter.ExecCatalogInTxn(ctx, catalog.InternalTables.PgSubscription.UpdateStmt(keyColumns, lsnValueColumns), name, lsn)
	return err
}

func SelectSubscriptionLsn(ctx *sql.Context, subscription string) (pglogrepl.LSN, error) {
	var lsn string
	if err := adapter.QueryRowCatalog(ctx, catalog.InternalTables.PgSubscription.SelectColumnsStmt(lsnValueColumns), subscription).Scan(&lsn); err != nil {
		if errors.Is(err, stdsql.ErrNoRows) {
			// if the LSN doesn't exist, consider this a cold start and return 0
			return pglogrepl.LSN(0), nil
		}
		return 0, err
	}

	return pglogrepl.ParseLSN(lsn)
}
