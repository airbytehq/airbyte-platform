#!/bin/bash
# Environment-aware Dummy API test cleanup script
# Automatically detects CI vs local environment and uses appropriate cleanup method

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Environment detection logic
detect_environment() {
    if [ -n "$NAMESPACE" ] && command -v kubectl >/dev/null 2>&1; then
        echo "ci"
    elif command -v docker >/dev/null 2>&1; then
        echo "local"
    else
        echo "unknown"
    fi
}

# Main execution
ENVIRONMENT=$(detect_environment)

echo "🔍 Detected environment: $ENVIRONMENT"

case $ENVIRONMENT in
    "ci")
        echo "🚀 Running CI cleanup with Kubernetes..."
        exec "$SCRIPT_DIR/dummy-api-cleanup-ci.sh"
        ;;
    "local")
        echo "🚀 Running local cleanup with Docker..."
        exec "$SCRIPT_DIR/dummy-api-cleanup-local.sh"
        ;;
    "unknown")
        echo "❌ Error: Cannot determine environment or missing required tools"
        echo "   For CI: Ensure NAMESPACE is set and kubectl is available"
        echo "   For local: Ensure docker is available"
        exit 1
        ;;
esac

