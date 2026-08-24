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
	"log"
	"net"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/flightsqlserver"
	"github.com/apecloud/myduckserver/myfunc"
	"github.com/apecloud/myduckserver/pgserver"
	"github.com/apecloud/myduckserver/pgserver/logrepl"
	"github.com/apecloud/myduckserver/pgserver/pgconfig"
	"github.com/apecloud/myduckserver/plugin"
	"github.com/apecloud/myduckserver/replica"
	"github.com/apecloud/myduckserver/transpiler"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/sirupsen/logrus"
)

var (
	initMode    = false
	versionMode = false

	address       = "0.0.0.0"
	port          = 3306
	socket        string
	defaultDb     = "myduck"
	dataDirectory = "."
	logLevel      = int(logrus.InfoLevel)
	readOnly      = false

	replicaOptions replica.ReplicaOptions

	postgresPort = 5432

	// Shared between the MySQL and Postgres servers.
	superuserPassword = ""

	defaultTimeZone = ""
	queryRowLimit   uint64

	// for Restore
	restoreFile            = ""
	restoreEndpoint        = ""
	restoreAccessKeyId     = ""
	restoreSecretAccessKey = ""

	flightsqlHost = "localhost"
	flightsqlPort = -1 // Disabled by default
)

func init() {
	flag.BoolVar(&initMode, "init", initMode, "Initialize the program and exit. The necessary extensions will be installed.")
	flag.BoolVar(&versionMode, "version", versionMode, "Print version information and exit.")

	flag.StringVar(&address, "address", address, "The address to bind to.")
	flag.IntVar(&port, "port", port, "The port to bind to.")
	flag.StringVar(&socket, "socket", socket, "The Unix domain socket to bind to.")
	flag.StringVar(&dataDirectory, "datadir", dataDirectory, "The directory to store the database.")
	flag.StringVar(&defaultDb, "default-db", defaultDb, "The default database name to use.")
	flag.IntVar(&logLevel, "loglevel", logLevel, "The log level to use.")
	flag.BoolVar(&readOnly, "read-only", readOnly, "Start ordinary MySQL/Postgres client sessions in read-only mode.")

	flag.StringVar(&superuserPassword, "superuser-password", superuserPassword, "The password for the superuser account.")

	flag.StringVar(&replicaOptions.ReportHost, "report-host", replicaOptions.ReportHost, "The host name or IP address of the replica to be reported to the source during replica registration.")
	flag.IntVar(&replicaOptions.ReportPort, "report-port", replicaOptions.ReportPort, "The TCP/IP port number for connecting to the replica, to be reported to the source during replica registration.")
	flag.StringVar(&replicaOptions.ReportUser, "report-user", replicaOptions.ReportUser, "The account user name of the replica to be reported to the source during replica registration.")
	flag.StringVar(&replicaOptions.ReportPassword, "report-password", replicaOptions.ReportPassword, "The account password of the replica to be reported to the source during replica registration.")

	flag.IntVar(&postgresPort, "pg-port", postgresPort, "The port to bind to for PostgreSQL wire protocol.")
	flag.StringVar(&defaultTimeZone, "default-time-zone", defaultTimeZone, "The default time zone to use.")
	flag.Uint64Var(&queryRowLimit, "query-row-limit", queryRowLimit, "Maximum rows returned by a query in ordinary MySQL and PostgreSQL sessions; 0 means unlimited.")

	flag.StringVar(&restoreFile, "restore-file", restoreFile, "The file to restore from.")
	flag.StringVar(&restoreEndpoint, "restore-endpoint", restoreEndpoint, "The endpoint of object storage service to restore from.")
	flag.StringVar(&restoreAccessKeyId, "restore-access-key-id", restoreAccessKeyId, "The access key ID to restore from.")
	flag.StringVar(&restoreSecretAccessKey, "restore-secret-access-key", restoreSecretAccessKey, "The secret access key to restore from.")

	flag.StringVar(&flightsqlHost, "flightsql-host", flightsqlHost, "hostname for the Flight SQL service")
	flag.IntVar(&flightsqlPort, "flightsql-port", flightsqlPort, "port number for the Flight SQL service")
}

func ensureSQLTranslate() {
	_, err := transpiler.TranslateWithSQLGlot("SELECT 1")
	if err != nil {
		panic(err)
	}
}

func main() {
	flag.Parse() // Parse all flags
	if versionMode {
		fmt.Println(versionInfo())
		return
	}

	if replicaOptions.ReportPort == 0 {
		replicaOptions.ReportPort = port
	}

	logrus.SetLevel(logrus.Level(logLevel))

	ensureSQLTranslate()

	executeRestoreIfNeeded()

	if initMode {
		provider := catalog.NewInMemoryDBProvider()
		provider.Close()
		return
	}

	provider, err := catalog.NewDBProvider(defaultTimeZone, dataDirectory, defaultDb)
	if err != nil {
		logrus.Fatalln("Failed to open the database:", err)
	}
	defer provider.Close()

	// Clear the pipes directory on startup.
	backend.RemoveAllPipes(dataDirectory)

	engine, builder := backend.NewEngine(provider)
	engine.Analyzer.Catalog.RegisterFunction(sql.NewContext(context.Background()), myfunc.ExtraBuiltIns...)
	engine.Analyzer.Catalog.MySQLDb.SetPlugins(plugin.AuthPlugins)

	if err := setPersister(provider, engine, "root", superuserPassword); err != nil {
		logrus.Fatalln("Failed to set the persister:", err)
	}

	replica.RegisterReplicaOptions(&replicaOptions)
	replica.RegisterReplicaController(provider, engine, builder)

	serverConfig := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%d", address, port),
		Socket:   socket,
	}
	var myServer *server.Server
	myServer, err = server.NewServerWithHandler(
		serverConfig,
		engine,
		sql.NewContext,
		backend.NewSessionBuilder(provider, backend.WithQueryRowLimit(queryRowLimit)),
		nil,
		backend.WrapHandler(provider, engine, func(ctx context.Context, conn *mysql.Conn, query string) (*sql.Context, error) {
			return myServer.SessionManager().NewContextWithQuery(ctx, conn, query)
		}, readOnly),
	)
	if err != nil {
		logrus.WithError(err).Fatalln("Failed to create MySQL-protocol server")
	}

	if postgresPort > 0 {
		var postgresConnID atomic.Uint32
		postgresConnID.Store(1 << 31)
		pgServer, err := pgserver.NewServer(
			provider,
			address, postgresPort,
			superuserPassword,
			func() *sql.Context {
				session := backend.NewSession(memory.NewSession(sql.NewBaseSession(), provider), provider)
				return sql.NewContext(context.Background(), sql.WithSession(session))
			},
			pgserver.WithEngine(myServer.Engine),
			pgserver.WithSessionManager(myServer.SessionManager()),
			pgserver.WithConnID(&postgresConnID),
			pgserver.WithReadOnly(readOnly),
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
		pgconfig.Init()
		go pgServer.Start()
	}

	if flightsqlPort > 0 {

		db := provider.Storage()
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		srv, err := flightsqlserver.NewSQLiteFlightSQLServer(db)
		if err != nil {
			log.Fatal(err)
		}

		server := flight.NewServerWithMiddleware(nil)
		server.RegisterFlightService(flightsql.NewFlightServer(srv))
		server.Init(net.JoinHostPort(*&flightsqlHost, strconv.Itoa(*&flightsqlPort)))
		server.SetShutdownOnSignals(os.Interrupt, os.Kill)

		fmt.Println("Starting SQLite Flight SQL Server on", server.Addr(), "...")

		go server.Serve()
	}

	if err = myServer.Start(); err != nil {
		logrus.WithError(err).Fatalln("Failed to start MySQL-protocol server")
	}
}

func executeRestoreIfNeeded() {
	// If none of the restore parameters are set, return early.
	if restoreFile == "" && restoreEndpoint == "" && restoreAccessKeyId == "" && restoreSecretAccessKey == "" {
		return
	}

	// Map of required parameters to their names for validation.
	required := map[string]string{
		restoreFile:            "restore file",
		restoreEndpoint:        "restore endpoint",
		restoreAccessKeyId:     "restore access key ID",
		restoreSecretAccessKey: "restore secret access key",
	}

	// Validate that all required parameters are set.
	for val, name := range required {
		if val == "" {
			logrus.Fatalf("The %s is required.", name)
		}
	}

	msg, err := pgserver.ExecuteRestore(
		defaultDb,
		dataDirectory,
		defaultDb+".db",
		restoreFile,
		restoreEndpoint,
		restoreAccessKeyId,
		restoreSecretAccessKey,
	)
	if err != nil {
		logrus.WithError(err).Fatalln("Failed to execute restore:", msg)
	}

	logrus.Infoln("Restore completed successfully:", msg)
}
