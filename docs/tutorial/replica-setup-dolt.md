# Connecting to Dolt

[Dolt](https://www.dolthub.com) is a MySQL-compatible database with integrated Git-like version control, designed to manage database state over time. 
Unlike traditional databases, Dolt requires some initial setup to support replication. 
Follow this guide for configuring Dolt via Docker or the Dolt CLI.

## Pre-configuring Dolt

### Option 1: Docker

Using Docker is a convenient way to configure Dolt with a `config.json` file for replication setup.

```shell
# Create a directory for configuration and navigate to it
mkdir doltcfg && cd doltcfg

# Write configuration settings to config.json
cat <<EOF > config.json
{
  "sqlserver.global.enforce_gtid_consistency": "ON",
  "sqlserver.global.gtid_mode": "ON",
  "sqlserver.global.log_bin": "1"
}
EOF

# Run Dolt in Docker with the configuration file
docker run -p 11229:3306 -v "$(pwd)":/etc/dolt/doltcfg.d/ dolthub/dolt-sql-server:latest
```

This configuration enables `GTID mode` and binary logging, which are essential for replication.

### Option 2: Dolt CLI

You can also configure Dolt directly with the Dolt CLI, allowing for the manual setup and additional control.

```shell
# Create a directory for the Dolt database and initialize it
mkdir doltPrimary && cd doltPrimary
dolt init --fun

# Configure replication settings
dolt sql -q "SET @@PERSIST.log_bin=1;"
dolt sql -q "SET @@PERSIST.gtid_mode=ON;"
dolt sql -q "SET @@PERSIST.enforce_gtid_consistency=ON;"

# Start Dolt SQL server on port 11229
dolt sql-server --loglevel DEBUG --port 11229
```

### Verify the Connection

After setup, you can verify the connection to Dolt:

```shell
mysql -h127.0.0.1 -uroot -P11229
```

## Next Steps

With Dolt configured, refer to [replica-setup-rds.md](replica-setup-rds.md) to complete the setup process for replication.
Only data from the `main` branch can be replicated to MyDuckServer. 
To replicate data from other branches, please refer to the [Dolt to MySQL Replication guide](https://www.dolthub.com/blog/2024-07-05-binlog-source-preview/), which offers detailed instructions and additional options for working with different branches in Dolt.