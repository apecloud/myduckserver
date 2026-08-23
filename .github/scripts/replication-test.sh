#!/usr/bin/env bash

set -Eeuo pipefail

SOURCE=${1:?usage: replication-test.sh postgres|mysql|mariadb|dolt}
MYDUCK_IMAGE=${MYDUCK_IMAGE:-myduckserver:replication-test}
DOLT_IMAGE='dolthub/dolt-sql-server@sha256:dbd13efe2e19e02c079efd2152bca5032071a3cd6f79a9b2a7b404d19ff3ee3f'
RESOURCE_SUFFIX="${GITHUB_RUN_ID:-local}-${SOURCE}-$$"
NETWORK="myduck-replication-${RESOURCE_SUFFIX}"
DATA_VOLUME="myduck-replication-data-${RESOURCE_SUFFIX}"
DOLT_CONFIG_DIR=""
SCHEMA='test'
SOURCE_DSN=""

cleanup() {
  docker rm -f myduck source-db >/dev/null 2>&1 || true
  docker volume rm "$DATA_VOLUME" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
  if [[ -n "$DOLT_CONFIG_DIR" && -d "$DOLT_CONFIG_DIR" ]]; then
    rm -rf "$DOLT_CONFIG_DIR"
  fi
}
trap cleanup EXIT

docker network create "$NETWORK" >/dev/null
docker volume create "$DATA_VOLUME" >/dev/null
docker run --rm --user root \
  --volume "$DATA_VOLUME:/home/admin/data" \
  --entrypoint /bin/sh "$MYDUCK_IMAGE" \
  -c 'chown -R admin:admin /home/admin/data'

wait_for_source() {
  local attempt
  for attempt in {1..60}; do
    if ! docker inspect source-db --format '{{.State.Running}}' 2>/dev/null | grep -q true; then
      docker logs source-db || true
      return 1
    fi

    case "$SOURCE" in
      postgres)
        docker exec source-db pg_isready -U postgres >/dev/null 2>&1 && return 0
        ;;
      mysql)
        docker exec source-db mysql -uroot -proot -e "SELECT 1" >/dev/null 2>&1 && return 0
        ;;
      mariadb)
        docker exec source-db mariadb -uroot -proot -e "SELECT 1" >/dev/null 2>&1 && return 0
        ;;
      dolt)
        docker exec source-db dolt sql -q "SELECT 1" >/dev/null 2>&1 && return 0
        ;;
    esac
    sleep 2
  done

  docker logs source-db || true
  return 1
}

source_sql() {
  local statement=$1
  case "$SOURCE" in
    postgres)
      docker exec source-db psql -v ON_ERROR_STOP=1 -U postgres -d test -c "$statement"
      ;;
    mysql)
      docker exec source-db mysql -uroot -proot -e "$statement"
      ;;
    mariadb)
      docker exec source-db mariadb -uroot -proot -e "$statement"
      ;;
    dolt)
      docker exec source-db dolt sql -q "$statement"
      ;;
  esac
}

create_source_data() {
  if [[ "$SOURCE" == "postgres" ]]; then
    source_sql "
      CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(50));
      INSERT INTO items VALUES (1, 'test1'), (2, 'test2');"
  else
    source_sql "
      CREATE DATABASE IF NOT EXISTS test;
      CREATE TABLE test.items (id INT PRIMARY KEY, name VARCHAR(50));
      INSERT INTO test.items VALUES (1, 'test1'), (2, 'test2');
      CREATE TABLE test.skip (id INT PRIMARY KEY, name VARCHAR(50));
      INSERT INTO test.skip VALUES (1, 'abc'), (2, 'def');"
  fi
}

case "$SOURCE" in
  postgres)
    SCHEMA=public
    SOURCE_DSN='postgres://postgres:postgres@source:5432/test'
    docker run -d --name source-db --network "$NETWORK" --network-alias source \
      -e POSTGRES_PASSWORD=postgres \
      -e POSTGRES_DB=test \
      postgres:latest \
      -c wal_level=logical >/dev/null
    ;;
  mysql)
    SOURCE_DSN='mysql://root:root@source-db:3306/test?skip-tables=test.skip'
    docker run -d --name source-db --network "$NETWORK" \
      -e MYSQL_ROOT_PASSWORD=root \
      -e MYSQL_ROOT_HOST=% \
      -e MYSQL_DATABASE=test \
      mysql:8.4 \
      --gtid-mode=ON \
      --enforce-gtid-consistency=ON \
      --binlog-format=ROW >/dev/null
    ;;
  mariadb)
    SOURCE_DSN='mysql://root:root@source-db:3306/test?skip-tables=test.skip'
    docker run -d --name source-db --network "$NETWORK" \
      -e MARIADB_ROOT_PASSWORD=root \
      -e MARIADB_ROOT_HOST=% \
      -e MARIADB_DATABASE=test \
      mariadb:11.4 \
      --gtid-strict-mode=1 \
      --log-bin=mybinlog \
      --binlog-format=ROW >/dev/null
    ;;
  dolt)
    SOURCE_DSN='mysql://replicator:replicator@source-db:3306/test?skip-tables=test.skip'
    DOLT_CONFIG_DIR=$(mktemp -d)
    cat >"$DOLT_CONFIG_DIR/config.json" <<'JSON'
{
  "sqlserver.global.enforce_gtid_consistency": "ON",
  "sqlserver.global.gtid_mode": "ON",
  "sqlserver.global.log_bin": "1"
}
JSON
    docker run -d --name source-db --network "$NETWORK" \
      --volume "$DOLT_CONFIG_DIR/config.json:/etc/dolt/doltcfg.d/config.json:ro" \
      "$DOLT_IMAGE" >/dev/null
    ;;
  *)
    echo "unsupported source: $SOURCE" >&2
    exit 2
    ;;
esac

wait_for_source

if [[ "$SOURCE" == "dolt" ]]; then
  source_sql "
    CREATE DATABASE test;
    CREATE USER 'replicator'@'%' IDENTIFIED BY 'replicator';
    GRANT SELECT, RELOAD, REPLICATION CLIENT, REPLICATION SLAVE, SHOW VIEW, EVENT
      ON *.* TO 'replicator'@'%';"
else
  create_source_data
fi

start_myduck() {
  local mode=$1
  local args=(
    -d --name myduck --network "$NETWORK"
    --volume "$DATA_VOLUME:/home/admin/data"
    --env "SETUP_MODE=$mode"
  )
  if [[ "$mode" == "REPLICA" ]]; then
    args+=(--env "SOURCE_DSN=$SOURCE_DSN")
  fi
  docker run "${args[@]}" "$MYDUCK_IMAGE" >/dev/null
}

wait_for_myduck_setup() {
  local attempt
  for attempt in {1..90}; do
    if ! docker inspect myduck --format '{{.State.Running}}' 2>/dev/null | grep -q true; then
      docker logs myduck || true
      return 1
    fi
    if docker logs myduck 2>&1 | grep -q 'Replica setup completed.'; then
      return 0
    fi
    sleep 2
  done
  docker logs myduck || true
  return 1
}

wait_for_myduck_ready() {
  local attempt
  for attempt in {1..60}; do
    if docker exec myduck psql -XAt -h 127.0.0.1 -U postgres -c 'SELECT 1' 2>/dev/null | grep -qx 1; then
      return 0
    fi
    if ! docker inspect myduck --format '{{.State.Running}}' 2>/dev/null | grep -q true; then
      docker logs myduck || true
      return 1
    fi
    sleep 2
  done
  docker logs myduck || true
  return 1
}

myduck_scalar() {
  local statement=$1
  docker exec myduck psql -XAt -h 127.0.0.1 -U postgres -c "$statement" | tr -d '[:space:]'
}

myduck_mysql() {
  local statement=$1
  docker exec myduck mysqlsh --sql --host 127.0.0.1 --port 3306 \
    --user root --no-password --result-format=tabbed --execute "$statement"
}

myduck_mysql_scalar() {
  local statement=$1
  myduck_mysql "$statement" | tail -n 1 | tr -d '[:space:]'
}

source_rows() {
  case "$SOURCE" in
    postgres)
      docker exec source-db psql -XAt -U postgres -d test \
        -c "SELECT id || ':' || name FROM items ORDER BY id"
      ;;
    mysql)
      docker exec source-db mysql --batch --skip-column-names -uroot -proot test \
        -e "SELECT CONCAT(id, ':', name) FROM items ORDER BY id"
      ;;
    mariadb)
      docker exec source-db mariadb --batch --skip-column-names -uroot -proot test \
        -e "SELECT CONCAT(id, ':', name) FROM items ORDER BY id"
      ;;
    dolt)
      docker exec source-db dolt sql --result-format csv -q \
        "SELECT CONCAT(id, ':', name) AS row_value FROM test.items ORDER BY id" \
        | tail -n +2 | tr -d '\r'
      ;;
  esac
}

myduck_rows() {
  docker exec myduck psql -XAt -h 127.0.0.1 -U postgres \
    -c "SELECT id || ':' || name FROM ${SCHEMA}.items ORDER BY id"
}

wait_for_count() {
  local expected=$1
  local attempt got
  # The attempt value is only used to bound retries.
  # shellcheck disable=SC2034
  for attempt in {1..60}; do
    got=$(myduck_scalar "SELECT COUNT(*) FROM ${SCHEMA}.items" 2>/dev/null || true)
    if [[ "$got" == "$expected" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "expected ${SCHEMA}.items count=$expected, got=${got:-query failed}" >&2
  docker logs myduck || true
  return 1
}

record_phase() {
  local phase=$1
  echo "===== $SOURCE / $phase ====="
  myduck_mysql "
    SELECT @@GLOBAL.gtid_executed AS gtid_executed;
    SELECT * FROM __sys__.binlog_position;
    SELECT COUNT(*) AS total_rows, COUNT(DISTINCT id) AS unique_rows FROM ${SCHEMA}.items;
    SHOW REPLICA STATUS\G"
}

assert_row_count() {
  local expected=$1
  local counts
  counts=$(myduck_scalar "SELECT CONCAT(COUNT(*), ':', COUNT(DISTINCT id)) FROM ${SCHEMA}.items")
  [[ "$counts" == "$expected:$expected" ]]
}

assert_rows_match_source() {
  local expected=$1 source_values replica_values actual_count
  source_values=$(source_rows | tr -d '\r')
  replica_values=$(myduck_rows | tr -d '\r')
  if [[ "$replica_values" != "$source_values" ]]; then
    echo "source rows: ${source_values@Q}" >&2
    echo "replica rows: ${replica_values@Q}" >&2
    return 1
  fi
  actual_count=$(printf '%s\n' "$replica_values" | awk 'NF { count++ } END { print count + 0 }')
  [[ "$actual_count" == "$expected" ]]
}

assert_skip_table_absent() {
  local skipped_tables skipped_rows
  skipped_tables=$(myduck_scalar \
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='test' AND table_name='skip'")
  if [[ "$skipped_tables" == 0 ]]; then
    return 0
  fi
  skipped_rows=$(myduck_scalar "SELECT COUNT(*) FROM test.skip")
  [[ "$skipped_rows" == 0 ]]
}

start_myduck REPLICA
wait_for_myduck_setup

if [[ "$SOURCE" == "dolt" ]]; then
  create_source_data
fi

wait_for_count 2
assert_rows_match_source 2
if [[ "$SOURCE" != "postgres" ]]; then
  assert_skip_table_absent
fi
record_phase initial

source_sql "INSERT INTO ${SCHEMA}.items VALUES (3, 'test3');"
wait_for_count 3
assert_rows_match_source 3
record_phase incremental

if [[ "$SOURCE" == "dolt" ]]; then
  position_before_stop=$(myduck_mysql_scalar 'SELECT @@GLOBAL.gtid_executed')
  myduck_mysql 'STOP REPLICA;'
  source_sql "INSERT INTO test.items VALUES (4, 'test4');"
  sleep 3
  assert_row_count 3
  source_position_after_stop=$(docker exec source-db dolt sql --result-format csv -q \
    'SELECT @@GLOBAL.gtid_executed' | tail -n 1 | tr -d '[:space:]')

  myduck_mysql 'START REPLICA;'
  wait_for_count 4
  assert_rows_match_source 4
  position_after_start=$(myduck_mysql_scalar 'SELECT @@GLOBAL.gtid_executed')
  [[ "$position_after_start" == "$source_position_after_stop" ]]
  [[ "$position_after_start" != "$position_before_stop" ]]
  record_phase stop-start

  docker rm -f myduck >/dev/null
  source_sql "INSERT INTO test.items VALUES (5, 'test5');"
  source_position_before_restart=$(docker exec source-db dolt sql --result-format csv -q \
    'SELECT @@GLOBAL.gtid_executed' | tail -n 1 | tr -d '[:space:]')
  start_myduck SERVER
  wait_for_myduck_ready
  wait_for_count 5
  assert_rows_match_source 5
  [[ "$(myduck_mysql_scalar 'SELECT @@GLOBAL.gtid_executed')" == "$source_position_before_restart" ]]
  record_phase process-restart
fi

expected_rows=3
if [[ "$SOURCE" == "dolt" ]]; then
  expected_rows=5
fi

assert_rows_match_source "$expected_rows"
[[ "$(myduck_mysql_scalar "SELECT COUNT(*) FROM ${SCHEMA}.items")" == "$expected_rows" ]]

if [[ "$SOURCE" != "postgres" ]]; then
  assert_skip_table_absent
fi

if [[ "$SOURCE" != "postgres" ]]; then
  status=$(myduck_mysql 'SHOW REPLICA STATUS\G')
  grep -Eq 'Replica_IO_Running:[[:space:]]+Yes' <<<"$status"
  grep -Eq 'Replica_SQL_Running:[[:space:]]+Yes' <<<"$status"
  if [[ "$SOURCE" != "mariadb" ]]; then
    grep -Eq 'Last_IO_Errno:[[:space:]]+0' <<<"$status"
    grep -Eq 'Last_SQL_Errno:[[:space:]]+0' <<<"$status"
  fi
fi

echo "$SOURCE replication regression passed"
