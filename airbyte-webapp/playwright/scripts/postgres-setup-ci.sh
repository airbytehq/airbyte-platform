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

# 1. Prepare SQL data (no ConfigMap needed for this approach)
echo "📊 Preparing SQL initialization data..."

# 2. Deploy Postgres pod
echo "🐘 Deploying Postgres pod..."
kubectl run postgres-test \
    --image=postgres:13 \
    --restart=Never \
    --namespace="$NAMESPACE" \
    --env="POSTGRES_USER=postgres" \
    --env="POSTGRES_PASSWORD=secret_password" \
    --env="POSTGRES_DB=postgres" \
    --port=5432

# 3. Expose Postgres as a service
echo "🌐 Exposing Postgres service..."
kubectl expose pod postgres-test \
    --port=5432 \
    --name=postgres-test-svc \
    --namespace="$NAMESPACE"

# 4. Wait for pod to be ready
echo "⏳ Waiting for Postgres to be ready..."
kubectl wait --for=condition=Ready pod/postgres-test \
    --namespace="$NAMESPACE" \
    --timeout=60s

# 5. Create test databases
echo "📊 Creating test databases..."
kubectl exec postgres-test --namespace="$NAMESPACE" -- createdb -U postgres airbyte_ci_source
kubectl exec postgres-test --namespace="$NAMESPACE" -- createdb -U postgres airbyte_ci_destination

# 6. Set up test data
echo "📝 Setting up test data..."
kubectl exec -i postgres-test --namespace="$NAMESPACE" -- psql -U postgres -d airbyte_ci_source < "$SCRIPT_DIR/postgres-test-data.sql"

# 7. Verify setup
echo "✅ Verifying setup..."
USER_COUNT=$(kubectl exec postgres-test --namespace="$NAMESPACE" -- psql -U postgres -d airbyte_ci_source -t -c "SELECT count(*) FROM users;" | tr -d ' ')
CITY_COUNT=$(kubectl exec postgres-test --namespace="$NAMESPACE" -- psql -U postgres -d airbyte_ci_source -t -c "SELECT count(*) FROM cities;" | tr -d ' ')
CAR_COUNT=$(kubectl exec postgres-test --namespace="$NAMESPACE" -- psql -U postgres -d airbyte_ci_source -t -c "SELECT count(*) FROM cars;" | tr -d ' ')

echo "✅ Setup complete! Test data verified:"
echo "   📊 Users: $USER_COUNT records"
echo "   🏙️  Cities: $CITY_COUNT records"
echo "   🚗 Cars: $CAR_COUNT records"

echo "🎉 Postgres test environment ready!"
echo "   Service: postgres-test-svc:5432"
echo "   Databases: airbyte_ci_source, airbyte_ci_destination"
