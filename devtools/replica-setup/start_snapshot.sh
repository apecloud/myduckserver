#!/bin/bash

# 1st step: Run MySQL commands to create admin user for replication and set local_infile to 1
if ! mysqlsh --sql --host=${MYDUCK_HOST} --port=${MYDUCK_PORT}  --user=root --password='' -e "SELECT 1 FROM mysql.user WHERE user = '${MYDUCK_USER}'" | grep -q 1; then
    echo "Creating admin user for replication and setting local_infile to 1..."
    mysqlsh --sql --host=${MYDUCK_HOST} --port=${MYDUCK_PORT}  --user=root --password='' <<EOF
CREATE USER '${MYDUCK_USER}'@'%' IDENTIFIED BY '${MYDUCK_PASSWORD}';
GRANT ALL PRIVILEGES ON *.* TO '${MYDUCK_USER}'@'%';
SET GLOBAL local_infile = 1;
EOF
fi

if [[ $? -ne 0 ]]; then
    echo "Failed to create '${MYDUCK_USER}' user or set local_infile. Exiting."
    exit 1
fi

# 2nd step: Get the core count based on cgroup information

# Function to extract core count from cgroup v1 and v2
get_core_count() {
    if [[ -f /sys/fs/cgroup/cpu/cpu.cfs_quota_us && -f /sys/fs/cgroup/cpu/cpu.cfs_period_us ]]; then
        # CGroup v1
        local quota=$(cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us)
        local period=$(cat /sys/fs/cgroup/cpu/cpu.cfs_period_us)
        if [[ $quota -gt 0 && $period -gt 0 ]]; then
            echo $(( quota / period ))
        else
            # Use available CPU count as a fallback
            nproc
        fi
    elif [[ -f /sys/fs/cgroup/cpu.max ]]; then
        # CGroup v2
        local max=$(cat /sys/fs/cgroup/cpu.max | cut -d' ' -f1)
        local period=$(cat /sys/fs/cgroup/cpu.max | cut -d' ' -f2)
        if [[ $max != "max" && $period -gt 0 ]]; then
            echo $(( max / period ))
        else
            # Use available CPU count as a fallback
            nproc
        fi
    else
        # Use available CPU count if cgroup info is unavailable
        nproc
    fi
}

CORE_COUNT=$(get_core_count)
THREAD_COUNT=$(( 2 * CORE_COUNT ))

echo "Detected core count: $CORE_COUNT"
echo "Thread count set to: $THREAD_COUNT"

# 3rd step: Execute mysqlsh command
echo "Starting snapshot copy with mysqlsh..."
# Run mysqlsh command and capture the output
output=$(mysqlsh -h${MYSQL_HOST} -P${MYSQL_PORT} -u${MYSQL_USER} -p${MYSQL_PASSWORD} -- util copy-instance "mysql://${MYDUCK_USER}:${MYDUCK_PASSWORD}@${MYDUCK_HOST}:${MYDUCK_PORT}" --users false --consistent false --ignore-existing-objects true --handle-grant-errors ignore --threads $THREAD_COUNT --bytesPerChunk 256M)

# Extract the EXECUTED_GTID_SET using grep and awk
EXECUTED_GTID_SET=$(echo "$output" | grep -i "EXECUTED_GTID_SET" | awk '{print $2}')

# Check if EXECUTED_GTID_SET is empty
if [ -z "$EXECUTED_GTID_SET" ]; then
  echo "EXECUTED_GTID_SET is empty, exiting."
  exit 1
fi

# If not empty, print the extracted GTID set
echo "EXECUTED_GTID_SET: $EXECUTED_GTID_SET"

echo "Snapshot completed successfully."