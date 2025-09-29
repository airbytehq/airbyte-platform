#!/bin/bash
set -e

# CI script for cleaning up Postgres test environment in Kubernetes

if [ -z "$NAMESPACE" ]; then
    echo "❌ Error: NAMESPACE environment variable is required"
    exit 1
fi

echo "🧹 Cleaning up Postgres test environment in namespace: $NAMESPACE"

# Cleanup Postgres resources
kubectl delete pod postgres-test --namespace="$NAMESPACE" --ignore-not-found=true
kubectl delete service postgres-test-svc --namespace="$NAMESPACE" --ignore-not-found=true

echo "✅ Postgres cleanup complete"
