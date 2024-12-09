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
	LsnStr       string
	Enabled      bool
	Replicator   *LogicalReplicator
}

var keyColumns = []string{"subname"}
var statusValueColumns = []string{"subenabled"}
var lsnValueColumns = []string{"subskiplsn"}

var subscriptionMap = sync.Map{}

func UpdateSubscriptions(ctx *sql.Context) error {
	rows, err := adapter.QueryCatalog(ctx, catalog.InternalTables.PgSubscription.SelectAllStmt())
	if err != nil {
		return err
	}
	defer rows.Close()

	var subMap = make(map[string]*Subscription)
	for rows.Next() {
		var name, conn, pub, lsn string
		var enabled bool
		if err := rows.Scan(&name, &conn, &pub, &lsn, &enabled); err != nil {
			return err
		}
		subMap[name] = &Subscription{
			Subscription: name,
			Conn:         conn,
			Publication:  pub,
			LsnStr:       lsn,
			Enabled:      enabled,
			Replicator:   nil,
		}
	}

	if err = rows.Err(); err != nil {
		return err
	}

	for tempName, tempSub := range subMap {
		if _, loaded := subscriptionMap.LoadOrStore(tempName, tempSub); !loaded {
			replicator, err := NewLogicalReplicator(tempName, tempSub.Conn)
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
			if tempSub.Enabled {
				go replicator.StartReplication(ctx, tempSub.Publication)
			}
		} else {
			if sub, ok := subscriptionMap.Load(tempName); ok {
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
		if _, ok := subMap[name]; !ok {
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

func UpdateSubscriptionStatus(ctx *sql.Context, enabled bool, name string) error {
	_, err := adapter.ExecCatalogInTxn(ctx, catalog.InternalTables.PgSubscription.UpdateStmt(keyColumns, statusValueColumns), enabled, name)
	return err
}

func DeleteSubscription(ctx *sql.Context, name string) error {
	_, err := adapter.ExecCatalogInTxn(ctx, catalog.InternalTables.PgSubscription.DeleteStmt(), name)
	return err
}

func UpdateSubscriptionLsn(ctx *sql.Context, lsn, name string) error {
	_, err := adapter.ExecCatalogInTxn(ctx, catalog.InternalTables.PgSubscription.UpdateStmt(keyColumns, lsnValueColumns), lsn, name)
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
