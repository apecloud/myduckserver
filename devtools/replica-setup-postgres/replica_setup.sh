#!/bin/bash

usage() {
    echo "Usage: $0 --postgres_host <host> --postgres_port <port> --postgres_user <user> --postgres_password <password> [--myduck_host <host>] [--myduck_port <port>] [--myduck_user <user>] [--myduck_password <password>] [--myduck_in_docker <true|false>]"
    exit 1
}

MYDUCK_HOST=${MYDUCK_HOST:-127.0.0.1}
MYDUCK_PORT=${MYDUCK_PORT:-5432}
MYDUCK_USER=${MYDUCK_USER:-mysql}
MYDUCK_PASSWORD=${MYDUCK_PASSWORD:-}
MYDUCK_SERVER_ID=${MYDUCK_SERVER_ID:-2}
MYDUCK_IN_DOCKER=${MYDUCK_IN_DOCKER:-false}

while [[ $# -gt 0 ]]; do
    case $1 in
        --postgres_host)
            SOURCE_HOST="$2"
            shift 2
            ;;
        --postgres_port)
            SOURCE_PORT="$2"
            shift 2
            ;;
        --postgres_user)
            SOURCE_USER="$2"
            shift 2
            ;;
        --postgres_password)
            SOURCE_PASSWORD="$2"
            shift 2
            ;;
        --myduck_host)
            MYDUCK_HOST="$2"
            shift 2
            ;;
        --myduck_port)
            MYDUCK_PORT="$2"
            shift 2
            ;;
        --myduck_user)
            MYDUCK_USER="$2"
            shift 2
            ;;
        --myduck_password)
            MYDUCK_PASSWORD="$2"
            shift 2
            ;;
        --myduck_server_id)
            MYDUCK_SERVER_ID="$2"
            shift 2
            ;;
        --myduck_in_docker)
            MYDUCK_IN_DOCKER="$2"
            shift 2
            ;;
        *)
            echo "Unknown parameter: $1"
            usage
            ;;
    esac
done

# Check if all parameters are set
if [[ -z "$SOURCE_HOST" || -z "$SOURCE_PORT" || -z "$SOURCE_USER" ]]; then
    echo "Error: Missing required Postgres connection variables: SOURCE_HOST, SOURCE_PORT, SOURCE_USER."
    usage
fi

# Step 1: Check Postgres configuration
echo "Checking Postgres configuration..."
# TODO(neo.zty): add check for Postgres configuration

# Step 2: Establish replication
echo "Starting replication..."
export PUBLICATION_NAME="myduck_publication"
export SUBSCRIPTION_NAME="myduck_subscription"

CREATE_SUBSCRIPTION_SQL="CREATE SUBSCRIPTION ${SUBSCRIPTION_NAME} \
    CONNECTION 'dbname=${SOURCE_DATABASE} host=${SOURCE_HOST} port=${SOURCE_PORT} user=${SOURCE_USER} password=${SOURCE_PASSWORD}' \
    PUBLICATION ${PUBLICATION_NAME};"

psql -h $MYDUCK_HOST -p $MYDUCK_PORT -U $MYDUCK_USER <<EOF
${CREATE_SUBSCRIPTION_SQL}
EOF

if [[ -n "$?" ]]; then
    echo "SQL executed successfully."
else
    echo "SQL execution failed. Check the error message above."
    exit 1
fi