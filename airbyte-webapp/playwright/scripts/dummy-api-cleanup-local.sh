#!/bin/bash
set -e

# Local script for cleaning up Dummy API test environment with Docker

echo "🧹 Cleaning up Dummy API test environment with Docker..."

# Stop and remove container
if docker ps -a --format '{{.Names}}' | grep -q '^dummy_api$'; then
    echo "🗑️  Stopping and removing dummy_api container..."
    docker stop dummy_api 2>/dev/null || true
    docker rm dummy_api 2>/dev/null || true
    echo "✅ Dummy API cleanup complete!"
else
    echo "ℹ️  No dummy_api container found, nothing to clean up"
fi

