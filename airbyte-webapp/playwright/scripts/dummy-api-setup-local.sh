#!/bin/bash
set -e

# Local script for setting up Dummy API test environment with Docker
# This script deploys the dummy API server used by connector builder tests

echo "🚀 Setting up Dummy API test environment with Docker..."

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUMMY_API_SCRIPT="$SCRIPT_DIR/../../scripts/dummy_api.js"

# Check if dummy_api.js exists
if [ ! -f "$DUMMY_API_SCRIPT" ]; then
    echo "❌ Error: dummy_api.js not found at $DUMMY_API_SCRIPT"
    exit 1
fi

# Wait for HTTP server to actually accept connections
wait_for_http_server() {
    echo "⏳ Waiting for HTTP server to accept connections..."
    for i in {1..30}; do
        if curl -s -f http://localhost:6767/health >/dev/null 2>&1; then
            echo "✅ Dummy API HTTP server is ready and serving requests!"
            return 0
        fi
        echo "   Attempt $i/30: HTTP server not ready yet, waiting..."
        sleep 1
    done
    echo "❌ HTTP server failed to start within 30 seconds"
    echo "📋 Container logs:"
    docker logs dummy_api --tail=50 || true
    exit 1
}

# 1. Clean up existing container
echo "🔄 Cleaning up any existing dummy_api container..."
docker stop dummy_api 2>/dev/null || true
docker rm dummy_api 2>/dev/null || true

# 2. Start the dummy API container
echo "🚀 Starting dummy API container..."
docker run --rm -d \
    -p 6767:6767 \
    --mount type=bind,source="$DUMMY_API_SCRIPT",target=/index.js \
    --name=dummy_api \
    node:20-alpine \
    node /index.js

# 3. Wait for HTTP server to actually accept connections
wait_for_http_server

# 4. Verify setup with a test request
echo "✅ Verifying setup with test request..."
HEALTH_CHECK=$(curl -s http://localhost:6767/health)
if [ "$HEALTH_CHECK" = "OK" ]; then
    echo "✅ Setup complete! Dummy API verified and responding"
else
    echo "❌ Setup verification failed! Health check returned: $HEALTH_CHECK"
    exit 1
fi

echo "🎉 Dummy API test environment ready!"
echo "   Endpoint: http://localhost:6767"
echo "   Health endpoint: http://localhost:6767/health"
echo "   Items endpoint: http://localhost:6767/items/"
echo ""
echo "💡 Container access:"
echo "   docker logs dummy_api           # View logs"
echo "   docker exec -it dummy_api sh    # Access container shell"
echo ""
echo "🛑 To cleanup when done:"
echo "   pnpm run cleanup"

