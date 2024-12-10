package environment

import (
	"flag"
	"github.com/apecloud/myduckserver/replica"
	"github.com/sirupsen/logrus"
)

// unexported global variables
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

func Init() {
	flag.BoolVar(&initMode, "init", initMode, "Initialize the program and exit. The necessary extensions will be installed.")

	flag.StringVar(&address, "address", address, "The address to bind to.")
	flag.IntVar(&port, "port", port, "The port to bind to.")
	flag.StringVar(&socket, "socket", socket, "The Unix domain socket to bind to.")
	flag.StringVar(&dataDirectory, "datadir", dataDirectory, "The directory to store the database.")
	flag.IntVar(&logLevel, "loglevel", logLevel, "The log level to use.")

	flag.StringVar(&replicaOptions.ReportHost, "report-host", replicaOptions.ReportHost, "The host name or IP address of the replica to be reported to the source during replica registration.")
	flag.IntVar(&replicaOptions.ReportPort, "report-port", replicaOptions.ReportPort, "The TCP/IP port number for connecting to the replica, to be reported to the source during replica registration.")
	flag.StringVar(&replicaOptions.ReportUser, "report-user", replicaOptions.ReportUser, "The account user name of the replica to be reported to the source during replica registration.")
	flag.StringVar(&replicaOptions.ReportPassword, "report-password", replicaOptions.ReportPassword, "The account password of the replica to be reported to the source during replica registration.")

	flag.IntVar(&postgresPort, "pg-port", postgresPort, "The port to bind to for PostgreSQL wire protocol.")
	flag.StringVar(&defaultTimeZone, "default-time-zone", defaultTimeZone, "The default time zone to use.")

	flag.Parse() // Parse all flags

	if replicaOptions.ReportPort == 0 {
		replicaOptions.ReportPort = port
	}
}

func GetInitMode() bool {
	return initMode
}

func GetAddress() string {
	return address
}

func GetPort() int {
	return port
}

func GetSocket() string {
	return socket
}

func GetDataDirectory() string {
	return dataDirectory
}

func GetLogLevel() int {
	return logLevel
}

func GetReplicaOptions() *replica.ReplicaOptions {
	return &replicaOptions
}

func GetPostgresPort() int {
	return postgresPort
}

func GetDefaultTimeZone() string {
	return defaultTimeZone
}

func GetDbFileName() string {
	return dbFileName
}
