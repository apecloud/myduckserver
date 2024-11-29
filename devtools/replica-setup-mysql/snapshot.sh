#!/bin/bash

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

echo "Copying data from MySQL to MyDuck..."
# Run mysqlsh command and capture the output
output=$(mysqlsh --uri "$SOURCE_DSN" $NO_PASSWORD_OPTION -- util copy-instance "mysql://${MYDUCK_USER}:${MYDUCK_PASSWORD}@${MYDUCK_HOST}:${MYDUCK_PORT}" --users false --consistent false --ignore-existing-objects true --handle-grant-errors ignore --threads $THREAD_COUNT --bytesPerChunk 256M --ignore-version true)

if [[ $GTID_MODE == "ON" ]]; then
    # Extract the EXECUTED_GTID_SET from this output:
    #   Executed_GTID_set: 369107a6-a0a5-11ef-a255-0242ac110008:1-10
    EXECUTED_GTID_SET=$(echo "$output" | grep -i "EXECUTED_GTID_SET" | awk '{print $2}')

    # Check if EXECUTED_GTID_SET is empty
    if [ -z "$EXECUTED_GTID_SET" ]; then
        echo "EXECUTED_GTID_SET is empty, exiting."
        exit 1
    fi

    # If not empty, print the extracted GTID set
    echo "EXECUTED_GTID_SET: $EXECUTED_GTID_SET"
else
    # Extract the BINLOG_FILE and BINLOG_POS from this output:
    #   Binlog_file: binlog.000002
    #   Binlog_position: 3763
    #   Executed_GTID_set: ''
    BINLOG_FILE=$(echo "$output" | grep -i "Binlog_file" | awk '{print $2}')
    BINLOG_POS=$(echo "$output" | grep -i "Binlog_position" | awk '{print $2}')

    # Check if BINLOG_FILE and BINLOG_POS are empty
    if [ -z "$BINLOG_FILE" ] || [ -z "$BINLOG_POS" ]; then
        echo "BINLOG_FILE or BINLOG_POS is empty, exiting."
        exit 1
    fi

    # If not empty, print the extracted BINLOG_FILE and BINLOG_POS
    echo "BINLOG_FILE: $BINLOG_FILE"
    echo "BINLOG_POS: $BINLOG_POS"
fi


echo "Snapshot completed successfully."