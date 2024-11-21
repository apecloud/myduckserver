# Loading Data from PostgreSQL to MyDuck using pg_dump

This guide walks you through the process of loading data from an existing PostgreSQL instance to MyDuck Server.

## Prerequisites

Before you begin, ensure that:

- **MyDuck Server** is installed and running on your server.
- A **pg_dump** file is available.

## Getting Started

To let MyDuck Server replicate data from an existing PostgreSQL instance, run the provided `setup.sh` script.

### Usage

```bash
bash setup.sh --pg_dump /path/to/pg_dump
```

### Parameters

- **`--pg_dump`**: The path to the PostgreSQL dump file.
- **`--myduck_port`**: The port on which MyDuck Server is listening connections.

### Example

```bash
bash setup.sh --pg_dump pg_dump.sql
```

This command loads data from the PostgreSQL dump file `pg_dump.sql` into MyDuck Server.
