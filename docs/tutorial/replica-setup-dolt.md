# Connecting to DoltDB

[Dolt](https://www.dolthub.com) is a "Git for data" MySQL-compatible database that brings version control capabilities directly into database operations. Unlike other databases, DoltDB requires additional pre-configuration to support replication setups as detailed in [replica-setup-rds.md](replica-setup-rds.md). This document provides guidance for setting up DoltDB configurations using Docker and the Dolt CLI.

## Pre-configuring DoltDB with Docker

To simplify the configuration, you can use a `config.json` file that specifies necessary system variables. Follow these steps:

```shell
# Create and navigate to a new configuration directory
mkdir doltcfg && cd doltcfg

# Write required configurations to config.json
cat <<EOF > config.json
{
  "sqlserver.global.enforce_gtid_consistency": "ON",
  "sqlserver.global.gtid_mode": "ON",
  "sqlserver.global.log_bin": "1"
}
EOF

# Start DoltDB with Docker, mounting the configuration file
docker run -p 3307:3306 -v "$(pwd)":/etc/dolt/doltcfg.d/ dolthub/dolt-sql-server:latest
```

This configuration file enables GTID mode and binary logging, essential for replication.

## Pre-configuring DoltDB with the Dolt CLI

Alternatively, you can use the Dolt CLI to initialize and configure the database manually. For more on Dolt’s MySQL compatibility and replication setup, refer to [Dolt to MySQL Replication](https://www.dolthub.com/blog/2024-07-05-binlog-source-preview/).

```shell
# Create a new directory for the Dolt database and initialize it
mkdir doltPrimary && cd doltPrimary
dolt init --fun

# Persist system variables for replication
dolt sql -q "SET @@PERSIST.log_bin=1;"
dolt sql -q "SET @@PERSIST.gtid_mode=ON;"
dolt sql -q "SET @@PERSIST.enforce_gtid_consistency=ON;"

# Start the Dolt SQL server
dolt sql-server --loglevel DEBUG --port 11229
```

## Next Steps

Once the DoltDB configuration is complete, refer to [replica-setup-rds.md](replica-setup-rds.md) for instructions on setting up replication with DoltDB as a source. This setup allows you to harness DoltDB's version-controlled database features in replication workflows.