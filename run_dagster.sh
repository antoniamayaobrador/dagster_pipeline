#!/bin/bash

# Load environment variables
echo "Loading environment variables..."
set -a
source .env
set +a

# Verify environment variables are set
for var in SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_PASSWORD SNOWFLAKE_DATABASE SNOWFLAKE_WAREHOUSE; do
    if [ -z "${!var}" ]; then
        echo "Error: $var is not set in .env file"
        exit 1
    fi
    # Mask the value for security
    echo "$var=${!var:0:2}...${!var: -2}"
done

# Go to the weather project directory and compile dbt
echo "Compiling dbt project..."
cd weather_project

# Ensure target directory exists
mkdir -p target

# Generate the manifest
dbt deps
dbt compile

# Verify manifest was created
if [ ! -f "target/manifest.json" ]; then
    echo "Error: Failed to generate manifest.json"
    exit 1
fi

# Go back to the project root
cd ..

# Run dagster
echo "Starting Dagster..."
dagster dev
