#!/bin/bash

# Check if PG_DUMP is set
if [[ -z "$PG_DUMP" ]]; then
    echo "Error: PG_DUMP variable is not set."
    exit 1
fi

# Check if the file exists
if [[ ! -f "$PG_DUMP" ]]; then
    echo "Error: File $PG_DUMP does not exist."
    exit 1
fi

# Read the file and match CREATE SCHEMA, CREATE TABLE and COPY statements
SQLS=$(grep -Pzo 'CREATE SCHEMA [^;]+;\n|CREATE TABLE [^;]+;\n|COPY [^\\]+\\\.\n' "$PG_DUMP")

# Execute the matched SQL statements using psql
psql -h $MYDUCK_HOST -p $MYDUCK_PORT -U $MYDUCK_USER -d $MYDUCK_DB -v ON_ERROR_STOP=1 << EOF
CREATE SCHEMA IF NOT EXISTS public;
$SQLS
EOF