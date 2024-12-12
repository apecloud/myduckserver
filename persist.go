// Copyright 2024-2025 ApeCloud, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"context"
	"os"
	"path"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
)

const persistFile = "mysql.bin"

// MySQLPersister is an example struct which handles the persistence of the data in the "mysql" database.
type MySQLPersister struct {
	FilePath string
}

var _ mysql_db.MySQLDbPersistence = (*MySQLPersister)(nil)

// Persist implements the interface mysql_db.MySQLDbPersistence.
func (m *MySQLPersister) Persist(ctx *sql.Context, data []byte) error {
	return os.WriteFile(m.FilePath, data, 0644)
}

// https://github.com/dolthub/go-mysql-server/blob/main/_example/users_example.go
func setPersister(provider sql.DatabaseProvider, engine *sqle.Engine) error {
	session := memory.NewSession(sql.NewBaseSession(), provider)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))
	ctx.SetCurrentDatabase("mysql")

	mysqlDb := engine.Analyzer.Catalog.MySQLDb

	// The functions "AddRootAccount" and "LoadData" both automatically enable the "mysql" database, but this is just
	// to explicitly show how one can manually enable (or disable) the database.
	mysqlDb.SetEnabled(true)

	// The persister here simply stands-in for your provided persistence function. The database calls this whenever it
	// needs to save any changes to any of the "mysql" database's tables. The memory session persists in memory,
	// but can be swapped out with the lines below
	persister := &MySQLPersister{FilePath: path.Join(dataDirectory, persistFile)}
	mysqlDb.SetPersister(persister)

	if _, err := os.Stat(persister.FilePath); err == nil {
		data, err := os.ReadFile(persister.FilePath)
		if err != nil {
			return err
		}

		if err := mysqlDb.LoadData(ctx, data); err != nil {
			return err
		}
	}

	addAccount := func(account string, address string) {
		ed := mysqlDb.Editor()
		defer ed.Close()
		mysqlDb.AddSuperUser(ed, account, address, "")
	}

	// Modify it to "%" to allow accepting connections outside when myduckserver runs in Docker
	// TODO should add a config to decide this or some better way to support this
	// addAccount("root", "localhost")
	addAccount("root", "%")

	return nil
}
