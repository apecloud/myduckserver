# MyDuck Server Backup and Restore Guide

## Introduction

MyDuck Server leverages DuckDB's high-performance analytical capabilities to manage and analyze your data efficiently. This guide provides comprehensive instructions on how to perform backups and restores using the enhanced mechanisms introduced in MyDuck Server, ensuring quick and reliable data management.

## Backup

### Motivation

DuckDB natively supports [`EXPORT/IMPORT DATABASE TO/FROM OBJECT STORAGE`](https://duckdb.org/docs/sql/statements/export), where the schema is stored in `schema.sql` and data in `.csv` or `.parquet` files. However, the restore process requires executing the SQL from `schema.sql` and then importing all corresponding data files, which can be time-consuming.

To optimize backup and restore times, MyDuck Server now allows you to backup the entire database into a single file (`mysql.db`). This approach simplifies the backup and restore process, enabling users to download and directly attach the backup file during a cold start, significantly reducing restoration time.

**Note:** During the backup process, the server switches to read-only mode, rejecting all DML (Data Manipulation Language) and DDL (Data Definition Language) operations. Once the backup completes, the server automatically reverts to read-write mode.

### Backup Syntax

```sql
BACKUP DATABASE my_database TO '<uri>'
  ENDPOINT = '<endpoint>'
  ACCESS_KEY_ID = '<access_key>'
  SECRET_ACCESS_KEY = '<secret_key>'
```

**Example:**

```sql
BACKUP DATABASE my_database TO 's3://my_bucket/my_database/'
  ENDPOINT = 's3.cn-northwest-1.amazonaws.com.cn'
  ACCESS_KEY_ID = 'xxxxxxxxxxxxx'
  SECRET_ACCESS_KEY = 'xxxxxxxxxxxx'
```

**Notes:**
- `s3` refers to AWS S3.
- `s3c` refers to other S3-compatible object storage services, such as [MinIO](https://min.io/) or [Alibaba Cloud OSS](https://www.alibabacloud.com/help/en/oss/).

## Restore

### Restore Process

To restore a database, download the backup file (`mysql.db`) and attach it.

**Note:** Backup files created by either MyDuck Server or DuckDB can be used to restore MyDuck Server.

### Restore at Startup

#### Docker Usage

Use the following Docker command to run MyDuck Server with restore parameters:

```bash
docker run \
  -p 13306:3306 \
  -p 15432:5432 \
  --env=SETUP_MODE=SERVER \
  --env=RESTORE_FILE=<uri> \
  --env=RESTORE_ENDPOINT=<endpoint> \
  --env=RESTORE_ACCESS_KEY_ID=<access_key> \
  --env=RESTORE_SECRET_ACCESS_KEY=<secret_key> \
  apecloud/myduckserver:latest
```

**Parameters:**
- `RESTORE_FILE`: URI to the backup file (e.g., `s3c://your/path/to/mysql.db`).
- `RESTORE_ENDPOINT`: Endpoint for object storage (e.g., ``).
- `RESTORE_ACCESS_KEY_ID`: Access key ID for object storage.
- `RESTORE_SECRET_ACCESS_KEY`: Secret access key for object storage.

**Example:**
```bash
docker run \
  -p 13306:3306 \
  -p 15432:5432 \
  --env=SETUP_MODE=SERVER \
  --env=RESTORE_FILE=s3://your/path/to/mysql.db \
  --env=RESTORE_ENDPOINT=s3.ap-northwest-1.amazonaws.com \
  --env=RESTORE_ACCESS_KEY_ID=xxxxxxxxxxxxxxxx \
  --env=RESTORE_SECRET_ACCESS_KEY=xxxxxxxxxxxxxxxx \
  apecloud/myduckserver:latest
```

#### Command Line Usage

Run MyDuck Server with the following command-line arguments to perform a restore:

```bash
./myduckserver \
  --restore-file=<uri> \
  --restore-endpoint=<endpoint> \
  --restore-access-key-id=<access_key> \
  --restore-secret-access-key=<secret_key>
```

**Example:**

```bash
./myduckserver \
  --restore-file=s3://your/path/to/mysql.db \
  --restore-endpoint=s3.ap-northwest-1.amazonaws.com \
  --restore-access-key-id=xxxxxxxxxxxxxx \
  --restore-secret-access-key=xxxxxxxxxxxxxx
```

### Restore Syntax (Restore at Runtime)

```sql
RESTORE DATABASE my_database FROM '<uri>'
  ENDPOINT = '<endpoint>'
  ACCESS_KEY_ID = '<access_key>'
  SECRET_ACCESS_KEY = '<secret_key>'
```

**Example**

```sql
RESTORE DATABASE my_database FROM 's3://my_bucket/my_database/'
  ENDPOINT = 's3.cn-northwest-1.amazonaws.com.cn'
  ACCESS_KEY_ID = 'xxxxxxxxxxxxx'
  SECRET_ACCESS_KEY = 'xxxxxxxxxxxx'
```
