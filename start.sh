#!/bin/bash

# This script builds and runs the Docker containers for the Spark pipeline.
# It ensures a clean environment by removing previous output and setting correct permissions.

set -e

# add processed date with default value
export PROCESS_DATE=${1:-2025-08-16}

# Define the output directory
OUTPUT_DIR="./data/output"

echo "--- Preparing environment ---"

# Remove the previous output directory if it exists
if [ -d "$OUTPUT_DIR" ]; then
  echo "Removing previous output from $OUTPUT_DIR (using sudo)..."
  sudo rm -rf "$OUTPUT_DIR"
fi

# Set the ownership of the data directory to the user ID (1001) that the sparkuser
# inside the Bitnami container uses. This ensures the container can write to the volume.
echo "Setting permissions on ./data directory..."
sudo chown -R 1001:1001 ./data

echo "Environment is clean and permissions are set."
echo ""
echo "--- Starting the Spark pipeline via Docker Compose ---"

docker-compose up --build --remove-orphans

echo ""
echo "--- Spark pipeline finished successfully! ---"
echo "Output data is available in the '$OUTPUT_DIR' directory."
