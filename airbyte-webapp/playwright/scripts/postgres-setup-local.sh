#!/bin/bash
# Automated Postgres test setup for Playwright E2E tests
# Run this from the airbyte-platform-internal root directory

set -e

CONTAINER_NAME="postgres-test"
POSTGRES_PORT="5433"

echo "🚀 Setting up Postgres test environment..."

# Function to check if container is running
check_container() {
    docker ps --filter "name=$CONTAINER_NAME" --filter "status=running" --quiet
}

# Function to wait for Postgres to be ready
wait_for_postgres() {
    echo "⏳ Waiting for Postgres to be ready..."
    for i in {1..30}; do
        if docker exec $CONTAINER_NAME pg_isready -U postgres >/dev/null 2>&1; then
            echo "✅ Postgres is ready!"
            return 0
        fi
        echo "   Attempt $i/30: Postgres not ready yet, waiting..."
        sleep 2
    done
    echo "❌ Postgres failed to start within 60 seconds"
    exit 1
}

# Clean up any existing container
if [ "$(check_container)" ]; then
    echo "🔄 Stopping existing postgres-test container..."
    docker stop $CONTAINER_NAME >/dev/null 2>&1 || true
fi

if docker ps -a --filter "name=$CONTAINER_NAME" --quiet | grep -q .; then
    echo "🗑️  Removing existing postgres-test container..."
    docker rm $CONTAINER_NAME >/dev/null 2>&1 || true
fi

# Start Postgres container
echo "🐘 Starting Postgres container..."
docker run -d \
    --name $CONTAINER_NAME \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=secret_password \
    -e POSTGRES_DB=postgres \
    -p $POSTGRES_PORT:5432 \
    postgres:13

# Wait for Postgres to be ready
wait_for_postgres

# Create test databases
echo "📊 Creating test databases..."
docker exec $CONTAINER_NAME createdb -U postgres airbyte_ci_source 2>/dev/null || echo "   Source DB already exists"
docker exec $CONTAINER_NAME createdb -U postgres airbyte_ci_destination 2>/dev/null || echo "   Destination DB already exists"

# Set up test data
echo "📝 Setting up test data..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
docker exec -i $CONTAINER_NAME psql -U postgres -d airbyte_ci_source -q < "$SCRIPT_DIR/postgres-test-data.sql"

# Verify setup
echo "🔍 Verifying setup..."
USER_COUNT=$(docker exec $CONTAINER_NAME psql -U postgres -d airbyte_ci_source -t -q -c "SELECT count(*) FROM users;" | xargs)
CITIES_COUNT=$(docker exec $CONTAINER_NAME psql -U postgres -d airbyte_ci_source -t -q -c "SELECT count(*) FROM cities;" | xargs)
CARS_COUNT=$(docker exec $CONTAINER_NAME psql -U postgres -d airbyte_ci_source -t -q -c "SELECT count(*) FROM cars;" | xargs)

if [ "$USER_COUNT" = "5" ] && [ "$CITIES_COUNT" = "5" ] && [ "$CARS_COUNT" = "5" ]; then
    echo "✅ Setup complete! Test data verified:"
    echo "   📊 Users: $USER_COUNT records"
    echo "   🏙️  Cities: $CITIES_COUNT records" 
    echo "   🚗 Cars: $CARS_COUNT records"
echo ""
echo "🧪 Ready to run tests! Use these commands:"
echo "   cd oss/airbyte-webapp"
echo "   pnpm test -- --serverHost='https://local.airbyte.dev' <<optional_testPath>>"
echo ""
echo "💡 Tip: Use these convenient pnpm commands (work in both local and CI):"
echo "   pnpm run setup                       # Setup postgres + dummy API"
echo ""
echo "🛑 To cleanup when done:"
echo "   pnpm run cleanup"
else
    echo "❌ Setup verification failed!"
    echo "   Expected 5 records in each table, got: users=$USER_COUNT, cities=$CITIES_COUNT, cars=$CARS_COUNT"
    exit 1
fi
