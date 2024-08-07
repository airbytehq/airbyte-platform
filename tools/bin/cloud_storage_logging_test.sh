#!/usr/bin/env bash

set -e

# GCS resources for the following tests are located in the dataline-integration-testing GCP project.
# GCS testing creds can be found in the "google cloud storage ( gcs ) test creds" secret in the `Shared-integration-tests`
# folder in Lastpass.

# AWS resources for the following tests are located in the dev account.
# S3 testing creds can be found in the `AWS_S3_INTEGRATION_TEST_CREDS` secret in the `Shared-integration-tests`
# folder in Lastpass.

echo "Writing cloud storage credentials.."

# Storage shared
export STORAGE_BUCKET_ACTIVITY_PAYLOAD=unused
export STORAGE_BUCKET_LOG=airbyte-kube-integration-logging-test
export STORAGE_BUCKET_STATE=unused
export STORAGE_BUCKET_WORKLOAD_OUTPUT=unused

# S3
export AWS_ACCESS_KEY_ID="$(echo "$AWS_S3_INTEGRATION_TEST_CREDS" | jq -r .aws_access_key_id)"
export AWS_SECRET_ACCESS_KEY="$(echo "$AWS_S3_INTEGRATION_TEST_CREDS" | jq -r .aws_secret_access_key)"
export AWS_DEFAULT_REGION=us-west-2

# GCS
echo "$GOOGLE_CLOUD_STORAGE_TEST_CREDS" > "/tmp/gcs.json"
export GOOGLE_APPLICATION_CREDENTIALS="/tmp/gcs.json"

# Run the logging test first since the same client is used in the log4j2 integration test.
echo "Running log client tests.."
./gradlew :oss:airbyte-config:config-models:logClientsIntegrationTest  --scan

echo "Running cloud storage tests.."
./gradlew :oss:airbyte-workers:cloudStorageIntegrationTest  --scan

# Reset existing configurations and run this for each possible configuration
# These configurations mirror the configurations documented in https://docs.airbyte.io/deploying-airbyte/on-kubernetes#configure-logs.
# Some duplication here for clarity.
export WORKER_ENVIRONMENT=kubernetes

echo "Setting S3 configuration.."
export AWS_ACCESS_KEY_ID="$(echo "$AWS_S3_INTEGRATION_TEST_CREDS" | jq -r .aws_access_key_id)"
export AWS_SECRET_ACCESS_KEY="$(echo "$AWS_S3_INTEGRATION_TEST_CREDS" | jq -r .aws_secret_access_key)"
export MINIO_ENDPOINT=
export S3_PATH_STYLE_ACCESS=

export GOOGLE_APPLICATION_CREDENTIALS=
export STORAGE_BUCKET_LOG=

echo "Running logging to S3 test.."
./gradlew  :oss:airbyte-config:models:log4j2IntegrationTest --scan --rerun-tasks -i

echo "Setting GCS configuration.."
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export AWS_DEFAULT_REGION=
export MINIO_ENDPOINT=
export S3_PATH_STYLE_ACCESS=

export GOOGLE_APPLICATION_CREDENTIALS="/tmp/gcs.json"

echo "Running logging to GCS test.."
./gradlew :oss:airbyte-config:models:log4j2IntegrationTest --scan --rerun-tasks -i

echo "Starting Minio service.."
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio

export MINIO_ROOT_USER=minio
export MINIO_ROOT_PASSWORD=miniostorage
./minio server /tmp/desktop &

echo "Setting Minio configuration.."
export AWS_ACCESS_KEY_ID=minio
export AWS_SECRET_ACCESS_KEY=miniostorage
export AWS_DEFAULT_REGION=
export MINIO_ENDPOINT=http://localhost:9000
export S3_PATH_STYLE_ACCESS=true

export GOOGLE_APPLICATION_CREDENTIALS=

echo "Running logging to Minio test.."
./gradlew :oss:airbyte-config:models:log4j2IntegrationTest --scan --rerun-tasks -i
