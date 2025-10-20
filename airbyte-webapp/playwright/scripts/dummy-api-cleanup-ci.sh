#!/bin/bash
set -e

# CI script for cleaning up Dummy API test environment in Kubernetes

if [ -z "$NAMESPACE" ]; then
    echo "❌ Error: NAMESPACE environment variable is required"
    exit 1
fi

echo "🧹 Cleaning up Dummy API test environment in Kubernetes namespace: $NAMESPACE"

# Delete resources
echo "🗑️  Deleting dummy API resources..."
kubectl delete pod dummy-api --namespace="$NAMESPACE" --ignore-not-found=true
kubectl delete service dummy-api-svc --namespace="$NAMESPACE" --ignore-not-found=true
kubectl delete configmap dummy-api-script --namespace="$NAMESPACE" --ignore-not-found=true

# Wait for pod to be deleted
kubectl wait --for=delete pod/dummy-api --namespace="$NAMESPACE" --timeout=30s 2>/dev/null || true

echo "✅ Dummy API cleanup complete!"

