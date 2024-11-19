# PostgreSQL to MyDuck Replication Setup

This guide walks you through configuring MyDuck Server as a replica of a running PostgreSQL instance.

## Prerequisites

Before you begin, ensure that:

- **MyDuck Server** is installed and running on your server.
- You have a **PostgreSQL dump** file that you want to replicate.

## Getting Started

To let MyDuck Server replicate data from an existing PostgreSQL instance, run the provided `replica_setup.sh` script. You will need to supply the PostgreSQL connection details as parameters.

### Usage

```bash
bash replica_setup.sh --pg_dump /path/to/pg_dump
```

### Parameters

- **`--pg_dump`**: The path to the PostgreSQL dump file.
- **`--myduck_port`**: The port on which MyDuck Server is listening connections.

## Example

```bash
bash replica_setup.sh --pg_dump ../../backup.sql
```

This command sets up MyDuck Server as a replica of the PostgreSQL instance running at `192.168.1.100` on port `3306` with the user `root` and password `mypassword`.