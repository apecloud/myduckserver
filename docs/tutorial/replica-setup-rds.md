# Connecting to Cloud MySQL

MyDuck Server supports setting up replication from common cloud-based MySQL offerings, including AWS RDS for MySQL, AWS Aurora for MySQL, Alibaba Cloud ApsaraDB RDS for MySQL, Azure Database for MySQL, [ApeCloud WeSQL](https://wesql.io/), and self-hosted MySQL Community Edition.

## Step 1: Verify BINLOG and GTID Configuration

Ensure the necessary parameters are properly set. Run the following query to check the configuration:

```sql
SHOW VARIABLES WHERE variable_name IN ('log_bin', 'gtid_mode', 'enforce_gtid_consistency', 'gtid_strict_mode', 'binlog_format');
```

**Expected Output (for MySQL):**

```plaintext
+--------------------------+-------+
| Variable_name            | Value |
+--------------------------+-------+
| binlog_format            | ROW   |
| enforce_gtid_consistency | ON    |
| gtid_mode                | ON    |
| log_bin                  | ON    |
+--------------------------+-------+
```

**Expected Output (for MariaDB):**

```plaintext
+------------------+-------+
| Variable_name    | Value |
+------------------+-------+
| binlog_format    | ROW   |
| gtid_strict_mode | ON    |
| log_bin          | ON    |
+------------------+-------+
```

These parameters are usually enabled by default or easily configurable with most cloud providers like Azure, Aliyun, ApeCloud, and MySQL Community Edition. However, for **AWS RDS** or **AWS Aurora for MySQL**, you may need to follow the specific instructions to enable them. Refer to the official documentation:
- [AWS RDS for MySQL](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_LogAccess.MySQL.BinaryFormat.html)
- [AWS Aurora for MySQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_LogAccess.MySQL.BinaryFormat.html)

## Step 2: Create a User for Replica Setup

Create a user with the necessary privileges for replication using the following SQL:

```sql
CREATE USER '<mysql_user>'@'%' IDENTIFIED BY '<mysql_password>';
GRANT SELECT, RELOAD, REPLICATION CLIENT, REPLICATION SLAVE, SHOW VIEW, EVENT ON *.* TO '<mysql_user>'@'%';
```

## Step 3: Setup Replica Using Docker

Run the following command to set up the replica with MyDuckServer:

```bash
docker run \
  --network=host \
  --privileged \
  --workdir=/home/admin \
  --env=SETUP_MODE=REPLICA \
  --env=MYSQL_HOST=<mysql_host> \
  --env=MYSQL_PORT=<mysql_port> \
  --env=MYSQL_USER=<mysql_user> \
  --env=MYSQL_PASSWORD=<mysql_password> \
  --detach=true \
  apecloud/myduckserver:latest
```

## Step 4: Check Replica Status

To verify the replica setup, connect to MyDuckServer:

```bash
mysql -h127.0.0.1 -P3306 -uroot
```

Check if the source MySQL databases and tables have been replicated to MyDuckServer:

```sql
SHOW DATABASES;
```
