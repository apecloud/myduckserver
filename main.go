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
	"flag"
	"fmt"

	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/catalog"
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

// After running the executable, you may connect to it using the following:
//
// > mysql --host=localhost --port=3306 --user=root
//
// The included MySQL client is used in this example, however any MySQL-compatible client will work.

var (
	initMode = false

	address       = "0.0.0.0"
	port          = 3306
	socket        string
	dataDirectory = "."
	dbFileName    = "mysql.db"
	logLevel      = int(logrus.InfoLevel)

	replicaOptions replica.ReplicaOptions

	postgresPort = 5432

	defaultTimeZone = ""
)

func init() {
	flag.BoolVar(&initMode, "init", initMode, "Initialize the program and exit. The necessary extensions will be installed.")

	flag.StringVar(&address, "address", address, "The address to bind to.")
	flag.IntVar(&port, "port", port, "The port to bind to.")
	flag.StringVar(&socket, "socket", socket, "The Unix domain socket to bind to.")
	flag.StringVar(&dataDirectory, "datadir", dataDirectory, "The directory to store the database.")
	flag.IntVar(&logLevel, "loglevel", logLevel, "The log level to use.")

	// The following options need to be set for MySQL Shell's utilities to work properly.

	// https://dev.mysql.com/doc/refman/8.4/en/replication-options-replica.html#sysvar_report_host
	flag.StringVar(&replicaOptions.ReportHost, "report-host", replicaOptions.ReportHost, "The host name or IP address of the replica to be reported to the source during replica registration.")
	// https://dev.mysql.com/doc/refman/8.4/en/replication-options-replica.html#sysvar_report_port
	flag.IntVar(&replicaOptions.ReportPort, "report-port", replicaOptions.ReportPort, "The TCP/IP port number for connecting to the replica, to be reported to the source during replica registration.")
	// https://dev.mysql.com/doc/refman/8.4/en/replication-options-replica.html#sysvar_report_user
	flag.StringVar(&replicaOptions.ReportUser, "report-user", replicaOptions.ReportUser, "The account user name of the replica to be reported to the source during replica registration.")
	// https://dev.mysql.com/doc/refman/8.4/en/replication-options-replica.html#sysvar_report_password
	flag.StringVar(&replicaOptions.ReportPassword, "report-password", replicaOptions.ReportPassword, "The account password of the replica to be reported to the source during replica registration.")

	// The following options are used to configure the Postgres server.

	flag.IntVar(&postgresPort, "pg-port", postgresPort, "The port to bind to for PostgreSQL wire protocol.")

	// The following options configure default DuckDB settings.

	flag.StringVar(&defaultTimeZone, "default-time-zone", defaultTimeZone, "The default time zone to use.")
}

func ensureSQLTranslate() {
	_, err := transpiler.TranslateWithSQLGlot("SELECT 1")
	if err != nil {
		panic(err)
	}
}

func main() {
	flag.Parse()

	if replicaOptions.ReportPort == 0 {
		replicaOptions.ReportPort = port
	}

	logrus.SetLevel(logrus.Level(logLevel))

	ensureSQLTranslate()

	if initMode {
		provider := catalog.NewInMemoryDBProvider()
		provider.Close()
		return
	}

	provider, err := catalog.NewDBProvider(dataDirectory, dbFileName)
	if err != nil {
		logrus.Fatalln("Failed to open the database:", err)
	}
	defer provider.Close()

	pool := backend.NewConnectionPool(provider.CatalogName(), provider.Connector(), provider.Storage())

	if _, err := pool.ExecContext(context.Background(), "PRAGMA enable_checkpoint_on_shutdown"); err != nil {
		logrus.WithError(err).Fatalln("Failed to enable checkpoint on shutdown")
	}

	if defaultTimeZone != "" {
		_, err := pool.ExecContext(context.Background(), fmt.Sprintf(`SET TimeZone = '%s'`, defaultTimeZone))
		if err != nil {
			logrus.WithError(err).Fatalln("Failed to set the default time zone")
		}
	}

	// Clear the pipes directory on startup.
	backend.RemoveAllPipes(dataDirectory)

	engine := sqle.NewDefault(provider)

	builder := backend.NewDuckBuilder(engine.Analyzer.ExecBuilder, pool, provider)
	engine.Analyzer.ExecBuilder = builder
	engine.Analyzer.Catalog.RegisterFunction(sql.NewContext(context.Background()), myfunc.ExtraBuiltIns...)
	engine.Analyzer.Catalog.MySQLDb.SetPlugins(plugin.AuthPlugins)

	if err := setPersister(provider, engine); err != nil {
		logrus.Fatalln("Failed to set the persister:", err)
	}

	replica.RegisterReplicaOptions(&replicaOptions)
	replica.RegisterReplicaController(provider, engine, pool, builder)

	serverConfig := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%d", address, port),
		Socket:   socket,
	}
	myServer, err := server.NewServerWithHandler(serverConfig, engine, backend.NewSessionBuilder(provider, pool), nil, backend.WrapHandler(pool))
	if err != nil {
		logrus.WithError(err).Fatalln("Failed to create MySQL-protocol server")
	}

	if postgresPort > 0 {
		// Postgres tables are created in the `public` schema by default.
		// Create the `public` schema if it doesn't exist.
		_, err := pool.ExecContext(context.Background(), "CREATE SCHEMA IF NOT EXISTS public")
		if err != nil {
			logrus.WithError(err).Fatalln("Failed to create the `public` schema")
		}

		pgServer, err := pgserver.NewServer(
			address, postgresPort,
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
		subscriptions, err := logrepl.GetAllSubscriptions(pgServer.NewInternalCtx())
		if err != nil {
			logrus.WithError(err).Warnln("Failed to find replication")
		} else {
			for _, subscription := range subscriptions {
				go subscription.Replicator.StartReplication(pgServer.NewInternalCtx(), subscription.Publication)
			}
		}

		// Load the configuration for the Postgres server.
		config.Init()
		go pgServer.Start()
	}

	if err = myServer.Start(); err != nil {
		logrus.WithError(err).Fatalln("Failed to start MySQL-protocol server")
	}
}
