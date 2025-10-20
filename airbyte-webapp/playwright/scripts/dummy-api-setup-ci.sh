#!/bin/bash
set -e

# CI script for setting up Dummy API test environment in Kubernetes
# This script deploys the dummy API server used by connector builder tests

if [ -z "$NAMESPACE" ]; then
    echo "❌ Error: NAMESPACE environment variable is required"
    exit 1
fi

echo "🚀 Setting up Dummy API test environment in Kubernetes namespace: $NAMESPACE"

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Wait for HTTP server to actually accept connections
wait_for_http_server() {
    echo "⏳ Waiting for HTTP server to accept connections..."
    for i in {1..30}; do
        if kubectl exec -n "$NAMESPACE" dummy-api -- wget -q -O- http://localhost:6767/health 2>/dev/null | grep -q "OK"; then
            echo "✅ Dummy API HTTP server is ready and serving requests!"
            return 0
        fi
        echo "   Attempt $i/30: HTTP server not ready yet, waiting..."
        sleep 1
    done
    echo "❌ HTTP server failed to start within 30 seconds"
    echo "📋 Pod logs:"
    kubectl logs dummy-api --namespace="$NAMESPACE" --tail=50 || true
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
echo "🔄 Cleaning up any existing dummy-api resources..."
kubectl delete pod dummy-api --namespace="$NAMESPACE" --ignore-not-found=true
kubectl wait --for=delete pod/dummy-api --namespace="$NAMESPACE" --timeout=30s || true
kubectl delete service dummy-api-svc --namespace="$NAMESPACE" --ignore-not-found=true
kubectl delete configmap dummy-api-script --namespace="$NAMESPACE" --ignore-not-found=true

# 2. Create ConfigMap with the dummy API script
echo "📝 Creating ConfigMap with dummy API script..."
retry_kubectl kubectl create configmap dummy-api-script \
    --from-file=dummy_api.js="$SCRIPT_DIR/../../scripts/dummy_api.js" \
    --namespace="$NAMESPACE"

# 3. Deploy dummy API pod
echo "🚀 Deploying dummy API pod..."
retry_kubectl kubectl run dummy-api \
    --image=node:20-alpine \
    --restart=Never \
    --namespace="$NAMESPACE" \
    --overrides='{
      "spec": {
        "containers": [{
          "name": "dummy-api",
          "image": "node:20-alpine", 
          "command": ["node", "/script/dummy_api.js"],
          "ports": [{"containerPort": 6767}],
          "volumeMounts": [{
            "name": "script",
            "mountPath": "/script"
          }]
        }],
        "volumes": [{
          "name": "script",
          "configMap": {"name": "dummy-api-script"}
        }]
      }
    }'

# 4. Expose dummy API as a service
echo "🌐 Exposing dummy API service..."
retry_kubectl kubectl expose pod dummy-api \
    --port=6767 \
    --name=dummy-api-svc \
    --namespace="$NAMESPACE"

# 5. Wait for pod to be ready
echo "⏳ Waiting for pod to be ready..."
kubectl wait --for=condition=Ready pod/dummy-api \
    --namespace="$NAMESPACE" \
    --timeout=60s

# 6. Wait for HTTP server to actually accept connections
wait_for_http_server

# 7. Verify setup with a test request
echo "✅ Verifying setup with test request..."
HEALTH_CHECK=$(kubectl exec dummy-api --namespace="$NAMESPACE" -- wget -q -O- http://localhost:6767/health 2>/dev/null)
if [ "$HEALTH_CHECK" = "OK" ]; then
    echo "✅ Setup complete! Dummy API verified and responding"
else
    echo "❌ Setup verification failed! Health check returned: $HEALTH_CHECK"
    exit 1
fi

echo "🎉 Dummy API test environment ready!"
echo "   Service: dummy-api-svc:6767"
echo "   Health endpoint: http://dummy-api-svc:6767/health"
echo "   Items endpoint: http://dummy-api-svc:6767/items/"

