// Copyright 2024-2025 ApeCloud, Ltd.
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

package main

import (
	"context"
	"fmt"

	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/environment"
	"github.com/apecloud/myduckserver/myfunc"
	"github.com/apecloud/myduckserver/pgserver"
	"github.com/apecloud/myduckserver/pgserver/config"
	"github.com/apecloud/myduckserver/pgserver/logrepl"
	"github.com/apecloud/myduckserver/plugin"
	"github.com/apecloud/myduckserver/replica"
	"github.com/apecloud/myduckserver/transpiler"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/sirupsen/logrus"
)

func ensureSQLTranslate() {
	_, err := transpiler.TranslateWithSQLGlot("SELECT 1")
	if err != nil {
		panic(err)
	}
}

func main() {
	environment.Init()

	logrus.SetLevel(logrus.Level(environment.GetLogLevel()))

	ensureSQLTranslate()

	if environment.GetInitMode() {
		provider := catalog.NewInMemoryDBProvider()
		provider.Close()
		return
	}

	provider, err := catalog.NewDBProvider(environment.GetDataDirectory(), environment.GetDbFileName())
	if err != nil {
		logrus.Fatalln("Failed to open the database:", err)
	}
	defer provider.Close()

	pool := backend.NewConnectionPool(provider.CatalogName(), provider.Connector(), provider.Storage())

	if _, err := pool.ExecContext(context.Background(), "PRAGMA enable_checkpoint_on_shutdown"); err != nil {
		logrus.WithError(err).Fatalln("Failed to enable checkpoint on shutdown")
	}

	if environment.GetDefaultTimeZone() != "" {
		_, err := pool.ExecContext(context.Background(), fmt.Sprintf(`SET TimeZone = '%s'`, environment.GetDefaultTimeZone()))
		if err != nil {
			logrus.WithError(err).Fatalln("Failed to set the default time zone")
		}
	}

	// Clear the pipes directory on startup.
	backend.RemoveAllPipes(environment.GetDataDirectory())

	engine := sqle.NewDefault(provider)

	builder := backend.NewDuckBuilder(engine.Analyzer.ExecBuilder, pool, provider)
	engine.Analyzer.ExecBuilder = builder
	engine.Analyzer.Catalog.RegisterFunction(sql.NewContext(context.Background()), myfunc.ExtraBuiltIns...)
	engine.Analyzer.Catalog.MySQLDb.SetPlugins(plugin.AuthPlugins)

	if err := setPersister(provider, engine); err != nil {
		logrus.Fatalln("Failed to set the persister:", err)
	}

	replica.RegisterReplicaOptions(environment.GetReplicaOptions())
	replica.RegisterReplicaController(provider, engine, pool, builder)

	serverConfig := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%d", environment.GetAddress(), environment.GetPort()),
		Socket:   environment.GetSocket(),
	}
	myServer, err := server.NewServerWithHandler(serverConfig, engine, backend.NewSessionBuilder(provider, pool), nil, backend.WrapHandler(pool))
	if err != nil {
		logrus.WithError(err).Fatalln("Failed to create MySQL-protocol server")
	}

	if environment.GetPostgresPort() > 0 {
		// Postgres tables are created in the `public` schema by default.
		// Create the `public` schema if it doesn't exist.
		_, err := pool.ExecContext(context.Background(), "CREATE SCHEMA IF NOT EXISTS public")
		if err != nil {
			logrus.WithError(err).Fatalln("Failed to create the `public` schema")
		}

		pgServer, err := pgserver.NewServer(
			environment.GetAddress(), environment.GetPostgresPort(),
			func() *sql.Context {
				session := backend.NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider, pool)
				return sql.NewContext(context.Background(), sql.WithSession(session))
			},
			pgserver.WithEngine(myServer.Engine),
			pgserver.WithSessionManager(myServer.SessionManager()),
			pgserver.WithConnID(&myServer.Listener.(*mysql.Listener).ConnectionID), // Shared connection ID counter
		)
		if err != nil {
			logrus.WithError(err).Fatalln("Failed to create Postgres-protocol server")
		}

		// Check if there is a replication subscription and start replication if there is.
		err = logrepl.UpdateSubscriptions(pgServer.NewInternalCtx())
		if err != nil {
			logrus.WithError(err).Warnln("Failed to update subscriptions")
		}

		// Load the configuration for the Postgres server.
		config.Init()
		go pgServer.Start()
	}

	if err = myServer.Start(); err != nil {
		logrus.WithError(err).Fatalln("Failed to start MySQL-protocol server")
	}
}
