// Copyright 2023 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package binlogreplication

import (
	stdsql "database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"
	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"vitess.io/vitess/go/mysql/replication"
)

const binlogPositionDirectory = ".replica"
const defaultChannelName = ""

// binlogPositionStore manages loading and saving data to the binlog position metadata table. This provides
// durable storage for the set of GTIDs that have been successfully executed on the replica, so that the replica
// server can be restarted and resume binlog event messages at the correct point.
type binlogPositionStore struct {
	mu sync.Mutex
}

// Load loads a mysql.Position instance from the database. The returned mysql.Position
// represents the set of GTIDs that have been successfully executed and applied on this replica.
// Currently only the default binlog channel ("") is supported.
// If no position is stored, this method returns a zero mysql.Position and a nil error.
// If any errors are encountered, a nil mysql.Position and an error are returned.
func (store *binlogPositionStore) Load(flavor string, ctx *sql.Context, engine *gms.Engine) (pos replication.Position, err error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	var positionString string
	err = adapter.QueryRowCatalog(ctx, catalog.InternalTables.BinlogPosition.SelectStmt(), defaultChannelName).Scan(&positionString)
	if err == stdsql.ErrNoRows {
		return replication.Position{}, nil
	} else if err != nil {
		return replication.Position{}, fmt.Errorf("unable to load binlog position: %w", err)
	}

	// Strip off the "MySQL56/" prefix
	positionString = strings.TrimPrefix(positionString, "MySQL56/")

	return replication.ParsePosition(flavor, positionString)
}

// Save persists the specified |position| to disk.
// The |position| represents the set of GTIDs that have been successfully executed and applied on this replica.
// Currently only the default binlog channel ("") is supported.
// If any errors are encountered persisting the position to disk, an error is returned.
func (store *binlogPositionStore) Save(ctx *sql.Context, engine *gms.Engine, position replication.Position) error {
	if position.IsZero() {
		return fmt.Errorf("unable to save binlog position: empty position passed")
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	if _, err := adapter.ExecCatalogInTxn(
		ctx,
		catalog.InternalTables.BinlogPosition.UpsertStmt(),
		defaultChannelName, position.String(),
	); err != nil {
		return fmt.Errorf("unable to save binlog position: %w", err)
	}
	return nil
}

// Delete deletes the stored mysql.Position information stored in .replica/binlog-position in the root of the provider's
// filesystem. This is useful for the "RESET REPLICA" command, since it clears out the current replication state. If
// any errors are encountered removing the position file, an error is returned.
func (store *binlogPositionStore) Delete(ctx *sql.Context, engine *gms.Engine) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	_, err := adapter.ExecCatalogInTxn(ctx, catalog.InternalTables.BinlogPosition.DeleteStmt(), defaultChannelName)
	return err
}

// createReplicaDir creates the .replica directory if it doesn't already exist.
func createReplicaDir(engine *gms.Engine) (string, error) {
	dir := filepath.Join(getDataDir(engine), binlogPositionDirectory)
	stat, err := os.Stat(dir)
	if err != nil && errors.Is(err, fs.ErrNotExist) {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return "", fmt.Errorf("unable to save binlog metadata: %s", err)
		}
	} else if err != nil {
		return "", err
	} else if !stat.IsDir() {
		return "", fmt.Errorf("unable to save binlog metadata: %s exists as a file, not a dir", binlogPositionDirectory)
	}

	return dir, nil
}
