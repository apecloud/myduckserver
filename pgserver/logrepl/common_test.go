package logrepl_test

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os/exec"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// findFreePort returns an available port that can be used for a server. If any errors are
// encountered, this function will panic and fail the current test.
func findFreePort() int {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(fmt.Sprintf("unable to find available TCP port: %v", err.Error()))
	}
	freePort := listener.Addr().(*net.TCPAddr).Port
	err = listener.Close()
	if err != nil {
		panic(fmt.Sprintf("unable to find available TCP port: %v", err.Error()))
	}

	if freePort < 0 {
		panic(fmt.Sprintf("unable to find available TCP port; found port %v", freePort))
	}

	return freePort
}

// StartPostgresServer configures a starts a fresh Postgres server instance in a Docker container
// and returns the port it is running on. If unable to start up the server, an error is returned.
func StartPostgresServer() (containerName string, dsn string, port int, err error) {
	port = findFreePort()

	// Use a random name for the container to avoid conflicts
	containerName = "postgres-test-" + strconv.Itoa(rand.Int())

	// Build the Docker command to start the Postgres container
	// NOTE: wal_level must be set to logical for logical replication to work.
	//   Otherwise: ERROR: logical decoding requires "wal_level" >= "logical" (SQLSTATE 55000)
	cmd := exec.Command("docker", "run",
		"--rm",                             // Remove the container when it stops
		"-d",                               // Run in detached mode
		"-p", fmt.Sprintf("%d:5432", port), // Map the container's port 5432 to the host's port
		"-e", "POSTGRES_PASSWORD=password", // Set the root password
		"--name", containerName, // Give the container a name
		"postgres:latest",         // Use the latest Postgres image
		"-c", "wal_level=logical", // Enable logical replication
		"-c", "max_wal_senders=30", // Set the maximum number of WAL senders
		"-c", "wal_sender_timeout=10", // Set the WAL sender timeout
	)

	// Execute the Docker command
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", "", -1, fmt.Errorf("unable to start MySQL container: %v - %s", err, output)
	}

	// Wait for the MySQL server to be ready
	dsn = fmt.Sprintf("postgres://postgres:password@localhost:%v/postgres", port)
	err = waitForSqlServerToStart(dsn)
	if err != nil {
		return "", "", -1, err
	}

	fmt.Printf("Postgres server started in container %s on port %v \n", containerName, port)

	return
}

// waitForSqlServerToStart polls the specified database to wait for it to become available, pausing
// between retry attempts, and returning an error if it is not able to verify that the database is
// available.
func waitForSqlServerToStart(dsn string) error {
	fmt.Printf("Waiting for server to start...\n")
	ctx := context.Background()
	for counter := 0; counter < 30; counter++ {
		conn, err := pgx.Connect(ctx, dsn)
		if err == nil {
			err = conn.Ping(ctx)
			conn.Close(ctx)
			if err == nil {
				return nil
			}
		}
		fmt.Printf("not up yet; waiting...\n")
		time.Sleep(500 * time.Millisecond)
	}

	return nil
}
