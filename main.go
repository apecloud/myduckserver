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
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/configuration"
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
	"google.golang.org/grpc"
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

var errServeLoopStopped = errors.New("serve loop stopped unexpectedly")

type serveLoop struct {
	name  string
	serve func() error
}

type serveResult struct {
	name string
	err  error
}

func serveResultError(result serveResult, expected bool) error {
	if result.err != nil {
		return fmt.Errorf("%s serve loop: %w", result.name, result.err)
	}
	if !expected {
		return fmt.Errorf("%w: %s", errServeLoopStopped, result.name)
	}
	return nil
}

func normalizeFlightServeError(err error) error {
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

func runServerLifecycle(
	shutdownSignals <-chan os.Signal,
	loops []serveLoop,
	stopProtocols func() error,
	finalize func() error,
) error {
	if len(loops) == 0 {
		return errors.Join(stopProtocols(), finalize())
	}

	results := make(chan serveResult, len(loops))
	for _, loop := range loops {
		loop := loop
		go func() {
			results <- serveResult{name: loop.name, err: loop.serve()}
		}()
	}

	remaining := len(loops)
	var lifecycleErr error
	select {
	case receivedSignal := <-shutdownSignals:
		logrus.WithField("signal", receivedSignal).Infoln("Shutdown signal received")
	case result := <-results:
		remaining--
		lifecycleErr = errors.Join(lifecycleErr, serveResultError(result, false))
	}

	lifecycleErr = errors.Join(lifecycleErr, stopProtocols())
	for range remaining {
		result := <-results
		lifecycleErr = errors.Join(lifecycleErr, serveResultError(result, true))
	}
	logrus.Infoln("Protocol serve loops stopped")

	return errors.Join(lifecycleErr, finalize())
}

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
	var shutdownSignals chan os.Signal
	if !initMode {
		shutdownSignals = make(chan os.Signal, 1)
		signal.Notify(shutdownSignals, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
		defer signal.Stop(shutdownSignals)
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

	duckLakeConfig, err := configuration.LoadDuckLakeConfig()
	if err != nil {
		// Configuration errors fail closed before any provider or protocol
		// listener is created. LoadDuckLakeConfig never includes credential
		// values in its error text.
		logrus.Fatalln("Failed to load DuckLake service configuration:", err)
	}
	provider, err := catalog.NewDBProvider(defaultTimeZone, dataDirectory, defaultDb, catalog.WithDuckLakeConfig(duckLakeConfig))
	if err != nil {
		logrus.Fatalln("Failed to open the database:", err)
	}
	closeProvider := sync.OnceValue(func() error {
		logrus.Infoln("Closing database provider")
		closeErr := provider.Close()
		if closeErr != nil {
			logrus.WithError(closeErr).Errorln("Failed to close database provider")
			return closeErr
		}
		logrus.Infoln("Database provider closed")
		return nil
	})
	defer func() { _ = closeProvider() }()

	// Clear the pipes directory on startup.
	backend.RemoveAllPipes(dataDirectory)

	engine, builder := backend.NewEngine(provider)
	finalize := sync.OnceValue(func() error {
		logrus.Infoln("Closing SQL engine")
		engineErr := engine.Close()
		if engineErr != nil {
			logrus.WithError(engineErr).Errorln("Failed to close SQL engine")
		} else {
			logrus.Infoln("SQL engine closed")
		}
		return errors.Join(engineErr, closeProvider())
	})
	defer func() { _ = finalize() }()
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

	var postgresServer *pgserver.Server
	if postgresPort > 0 {
		var postgresConnID atomic.Uint32
		postgresConnID.Store(1 << 31)
		postgresServer, err = pgserver.NewServer(
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
		err = logrepl.UpdateSubscriptions(postgresServer.NewInternalCtx())
		if err != nil {
			logrus.WithError(err).Warnln("Failed to update subscriptions")
		}

		// Load the configuration for the Postgres server.
		pgconfig.Init()
	}

	var flightServer flight.Server
	if flightsqlPort > 0 {
		db := provider.Storage()

		srv, err := flightsqlserver.NewSQLiteFlightSQLServer(db, provider.InitializeConnection)
		if err != nil {
			log.Fatal(err)
		}

		flightServer = flight.NewServerWithMiddleware(nil)
		flightServer.RegisterFlightService(flightsql.NewFlightServer(srv))
		flightServer.Init(net.JoinHostPort(*&flightsqlHost, strconv.Itoa(*&flightsqlPort)))

		fmt.Println("Starting SQLite Flight SQL Server on", flightServer.Addr(), "...")
	}

	loops := []serveLoop{{name: "mysql", serve: myServer.Start}}
	if postgresServer != nil {
		loops = append(loops, serveLoop{
			name: "postgres",
			serve: func() error {
				postgresServer.Start()
				return nil
			},
		})
	}
	if flightServer != nil {
		loops = append(loops, serveLoop{
			name: "flight",
			serve: func() error {
				return normalizeFlightServeError(flightServer.Serve())
			},
		})
	}

	stopProtocols := sync.OnceValue(func() error {
		logrus.Infoln("Stopping protocol servers")
		var stopErr error
		if postgresServer != nil {
			postgresServer.Close()
		}
		if closeErr := myServer.Close(); closeErr != nil {
			stopErr = errors.Join(stopErr, fmt.Errorf("close MySQL server: %w", closeErr))
		}
		if flightServer != nil {
			flightServer.Shutdown()
		}
		return stopErr
	})

	if err = runServerLifecycle(shutdownSignals, loops, stopProtocols, finalize); err != nil {
		logrus.WithError(err).Fatalln("Server stopped with an error")
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
