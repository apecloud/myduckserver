#!/bin/bash

# Function to check if a command was successful
check_command() {
    if [[ $? -ne 0 ]]; then
        echo "Error: $1 failed."
        exit 1
    fi
}
