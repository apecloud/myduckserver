#!/bin/bash

# Start a PostgreSQL container and mount the current directory
CONTAINER_ID=$(docker run --rm -d -e POSTGRES_PASSWORD=postgres -v "$(pwd):/data" postgres)
sleep 5

# Set file paths within the container
PG_CLASS_FILE="/data/pg_class.csv"
PG_PROC_FILE="/data/pg_proc.csv"
PG_TYPE_FILE="/data/pg_type.csv"

# Define SQL queries
PG_CLASS_QUERY="SELECT oid, relname, relnamespace, reltype, reloftype, relowner, relam, relfilenode, reltablespace, relpages, reltuples, relallvisible, reltoastrelid, relhasindex, relisshared, relpersistence, relkind, relnatts, relchecks, relhasrules, relhastriggers, relhassubclass, relrowsecurity, relforcerowsecurity, relispopulated, relreplident, relispartition, relrewrite, relfrozenxid, relminmxid, relacl, reloptions, relpartbound FROM pg_class"
PG_PROC_QUERY="SELECT oid, proname, pronamespace, proowner, prolang, procost, prorows, provariadic, prosupport::regproc::oid, prokind, prosecdef, proleakproof, proisstrict, proretset, provolatile, proparallel, pronargs, pronargdefaults, prorettype, proargtypes, proallargtypes, proargmodes, proargnames, proargdefaults, protrftypes, prosrc, probin, prosqlbody, proconfig, proacl FROM pg_proc"
PG_TYPE_QUERY="SELECT oid, typname, typnamespace, typowner, typlen, typbyval, typtype, typcategory, typispreferred, typisdefined, typdelim, typrelid, typsubscript::regproc::oid, typelem, typarray, typinput::regproc::oid, typoutput::regproc::oid, typreceive::regproc::oid, typsend::regproc::oid, typmodin::regproc::oid, typmodout::regproc::oid, typanalyze::regproc::oid, typalign, typstorage, typnotnull, typbasetype, typtypmod, typndims, typcollation, typdefaultbin, typdefault, typacl FROM pg_type"

# Execute queries and export data to mounted files
docker exec -i $CONTAINER_ID psql -U postgres -c "\COPY ($PG_CLASS_QUERY) TO '$PG_CLASS_FILE' WITH CSV HEADER"
docker exec -i $CONTAINER_ID psql -U postgres -c "\COPY ($PG_PROC_QUERY) TO '$PG_PROC_FILE' WITH CSV HEADER"
docker exec -i $CONTAINER_ID psql -U postgres -c "\COPY ($PG_TYPE_QUERY) TO '$PG_TYPE_FILE' WITH CSV HEADER"

# Stop the container
docker kill $CONTAINER_ID