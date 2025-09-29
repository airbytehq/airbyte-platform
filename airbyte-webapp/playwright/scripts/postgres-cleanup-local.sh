#!/bin/bash
# Cleanup script for Postgres test environment

set -e

CONTAINER_NAME="postgres-test"

echo "🧹 Cleaning up Postgres test environment..."

# Stop container if running
if docker ps --filter "name=$CONTAINER_NAME" --filter "status=running" --quiet | grep -q .; then
    echo "🛑 Stopping postgres-test container..."
    docker stop $CONTAINER_NAME
fi

# Remove container if exists
if docker ps -a --filter "name=$CONTAINER_NAME" --quiet | grep -q .; then
    echo "🗑️  Removing postgres-test container..."
    docker rm $CONTAINER_NAME
fi

echo "✅ Cleanup complete!"
