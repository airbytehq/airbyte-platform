#!/bin/bash
set -e

# CI script for setting up Postgres test environment in Kubernetes
# This script mirrors the functionality of setup-postgres-test.sh but uses kubectl instead of docker

if [ -z "$NAMESPACE" ]; then
    echo "❌ Error: NAMESPACE environment variable is required"
    exit 1
fi

echo "🚀 Setting up Postgres test environment in Kubernetes namespace: $NAMESPACE"

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"


# Wait for Postgres to be ready
wait_for_postgres() {
    echo "⏳ Waiting for Postgres to actually accept connections..."
    for i in {1..30}; do
        if kubectl exec postgres-test --namespace="$NAMESPACE" -- pg_isready -U postgres >/dev/null 2>&1; then
            echo "✅ Postgres is ready!"
            return 0
        fi
        echo "   Attempt $i/30: Postgres not ready yet, waiting..."
        sleep 2
    done
    echo "❌ Postgres failed to start within 60 seconds"
    exit 1
}

# Retries on kubectl commands
retry_kubectl() {
    local max_attempts=3
    local delay=2
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if "$@"; then
            return 0
        else
            echo "   Attempt $attempt/$max_attempts failed, retrying in ${delay}s..."
            sleep $delay
            attempt=$((attempt + 1))
        fi
    done
    echo "❌ Command failed after $max_attempts attempts: $*"
    return 1
}

# 1. Clean up existing resources
echo "🔄 Cleaning up any existing postgres-test resources..."
kubectl delete pod postgres-test --namespace="$NAMESPACE" --ignore-not-found=true
kubectl wait --for=delete pod/postgres-test --namespace="$NAMESPACE" --timeout=30s || true
kubectl delete service postgres-test-svc --namespace="$NAMESPACE" --ignore-not-found=true

# 2. Deploy Postgres pod
echo "🐘 Deploying Postgres pod..."
retry_kubectl kubectl run postgres-test \
    --image=postgres:13 \
    --restart=Never \
    --namespace="$NAMESPACE" \
    --env="POSTGRES_USER=postgres" \
    --env="POSTGRES_PASSWORD=secret_password" \
    --env="POSTGRES_DB=postgres" \
    --port=5432

# 3. Expose Postgres as a service
echo "🌐 Exposing Postgres service..."
retry_kubectl kubectl expose pod postgres-test \
    --port=5432 \
    --name=postgres-test-svc \
    --namespace="$NAMESPACE"

# 4. Wait for pod to be ready
echo "⏳ Waiting for pod to be ready..."
kubectl wait --for=condition=Ready pod/postgres-test \
    --namespace="$NAMESPACE" \
    --timeout=60s

# 5. Wait for Postgres to actually accept connections
wait_for_postgres

# 6. Create test databases with retry and error handling
echo "📊 Creating test databases..."
retry_kubectl kubectl exec postgres-test --namespace="$NAMESPACE" -- createdb -U postgres airbyte_ci_source 2>/dev/null || echo "   Source DB already exists or created successfully"
retry_kubectl kubectl exec postgres-test --namespace="$NAMESPACE" -- createdb -U postgres airbyte_ci_destination 2>/dev/null || echo "   Destination DB already exists or created successfully"

# 7. Set up test data with retry
echo "📝 Setting up test data..."
if ! retry_kubectl kubectl exec -i postgres-test --namespace="$NAMESPACE" -- psql -U postgres -d airbyte_ci_source -q < "$SCRIPT_DIR/postgres-test-data.sql"; then
    echo "❌ Failed to load test data"
    exit 1
fi

# 8. Verify setup with proper error handling
echo "✅ Verifying setup..."
USER_COUNT=$(kubectl exec postgres-test --namespace="$NAMESPACE" -- psql -U postgres -d airbyte_ci_source -t -c "SELECT count(*) FROM users;" 2>/dev/null | tr -d ' ' | head -1)
CITY_COUNT=$(kubectl exec postgres-test --namespace="$NAMESPACE" -- psql -U postgres -d airbyte_ci_source -t -c "SELECT count(*) FROM cities;" 2>/dev/null | tr -d ' ' | head -1)
CAR_COUNT=$(kubectl exec postgres-test --namespace="$NAMESPACE" -- psql -U postgres -d airbyte_ci_source -t -c "SELECT count(*) FROM cars;" 2>/dev/null | tr -d ' ' | head -1)

# Verify counts are numeric and correct
if [[ "$USER_COUNT" =~ ^[0-9]+$ ]] && [[ "$CITY_COUNT" =~ ^[0-9]+$ ]] && [[ "$CAR_COUNT" =~ ^[0-9]+$ ]]; then
    if [ "$USER_COUNT" = "5" ] && [ "$CITY_COUNT" = "5" ] && [ "$CAR_COUNT" = "5" ]; then
        echo "✅ Setup complete! Test data verified:"
        echo "   📊 Users: $USER_COUNT records"
        echo "   🏙️  Cities: $CITY_COUNT records"
        echo "   🚗 Cars: $CAR_COUNT records"
    else
        echo "❌ Setup verification failed! Expected 5 records in each table"
        echo "   Got: users=$USER_COUNT, cities=$CITY_COUNT, cars=$CAR_COUNT"
        exit 1
    fi
else
    echo "❌ Setup verification failed! Could not get valid counts from database"
    echo "   Raw counts: users='$USER_COUNT', cities='$CITY_COUNT', cars='$CAR_COUNT'"
    exit 1
fi

echo "🎉 Postgres test environment ready!"
echo "   Service: postgres-test-svc:5432"
echo "   Databases: airbyte_ci_source, airbyte_ci_destination"
