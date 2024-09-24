# Connector Rollout Client

Manually control progressive connector rollouts.

## Local Development
The Airbyte server, connector rollout worker, and a Temporal server are required for local development and are all deployed to our local kubernetes cluster.
The Temporal UI server is optional but is useful for debugging. It's deployed to our kubernetes cluster in debug mode.

To run the airbyte cluster in kubernetes with the Temporal UI enabled:
```sh
make deploy.oss.debug
```
This will start the connector rollout worker, the Temporal server, and Airbyte server.

Once the pods are running, get a shell into the `ab-temporal` pod and run:
```sh
cd /tmp
curl -sOL https://github.com/andrew-d/static-binaries/raw/master/binaries/linux/x86_64/socat
chmod +x socat
./socat TCP-LISTEN:7777,reuseaddr,fork TCP:$(netstat -tuln | awk '/LISTEN/ {print $4}' | cut -d':' -f1 | head -n 1):7233 &
```

We'll need to forward traffic from localhost to this port to allow CLI requests to hit the Temporal server in kubernetes:
```sh
kubectl port-forward -n ab <ab-temporal POD-NAME> 7233:7777
```

And forward traffic from localhost to 8080 to allow us to access the Temporal UI:
```sh
kubectl port-forward -n ab <ab-temporal-ui POD-NAME> 8080:8080
```
Navigate to the Temporal UI at http://localhost:8080 to verify that Temporal UI is running and communicating with `ab-temporal` server.


## Running connector rollouts

### Start a connector rollout

Execute commands using the CLI:
```sh
# Start a rollout for the source-faker connector with version 6.2.11
./gradlew :oss:airbyte-connector-rollout-client:runConnectorRolloutCLI --args="start -d airbyte/source-faker -i 6.2.11 -a dfd88b22-b603-4c3d-aad7-3701784586b1"

# Find information about all rollouts for source-faker version 6.2.11
# This can be used to get the rollout ID
./gradlew :oss:airbyte-connector-rollout-client:runConnectorRolloutCLI --args="find -d airbyte/source-faker -i 6.2.11 -a dfd88b22-b603-4c3d-aad7-3701784586b1"

# Get the rollout status
./gradlew :oss:airbyte-connector-rollout-client:runConnectorRolloutCLI --args="get -d airbyte/source-faker -i 6.2.11 -a dfd88b22-b603-4c3d-aad7-3701784586b1 -r d584520a-1127-4524-a357-52903c58477c"

# Rollout the connector to new actors
./gradlew :oss:airbyte-connector-rollout-client:runConnectorRolloutCLI --args="rollout -d airbyte/source-faker -i 6.2.10 -a dfd88b22-b603-4c3d-aad7-3701784586b1 -r e74f0ae2-1e1f-438f-95e7-9a40ae9ea63d -c a1d07f1a-5dc0-43c9-b938-decc4511e522,b2e1802b-9df1-33b8-6a44-1122adf1e413"

# Promote the rollout
./gradlew :oss:airbyte-connector-rollout-client:runConnectorRolloutCLI --args="promote -d airbyte/source-faker -i 6.2.11 -a dfd88b22-b603-4c3d-aad7-3701784586b1 -r d584520a-1127-4524-a357-52903c58477c"

# Fail the rollout
./gradlew :oss:airbyte-connector-rollout-client:runConnectorRolloutCLI --args="fail -d airbyte/source-faker -i 6.2.11 -a dfd88b22-b603-4c3d-aad7-3701784586b1 -r d584520a-1127-4524-a357-52903c58477c"

# Cancel the rollout
./gradlew :oss:airbyte-connector-rollout-client:runConnectorRolloutCLI --args="cancel -d airbyte/source-faker -i 6.2.11 -a dfd88b22-b603-4c3d-aad7-3701784586b1 -r d584520a-1127-4524-a357-52903c58477c"


...
```

```sh
psql -U airbyte
\c db-airbyte
truncate connector_rollout;
```

## Production usage

TODO
