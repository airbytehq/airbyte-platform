# For local dev

The consumer in the launcher has been mocked to read an input message every 30 seconds. This can be adjusted by changing the sleep value in mocks.MessageConsumer. Once a message is read the launcher will start a sync using the mocked configs.

The stubbed input messages reference database ids, so the database must be hydrated from the `mock-dump.sql.txt` file provided. (NOTE: this will blow away any data in your kube database)

To do that run the following commands:

```SQL
echo "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = 'db-airbyte'; drop database \"db-airbyte\";" | kubectl exec -it -n ab airbyte-db-0 -- psql -U airbyte

cat {adjust path as necessary}/oss/airbyte-workload-launcher/src/main/resources/mock-dump.sql.txt | kubectl exec -it -n ab airbyte-db-0 -- psql -U airbyte
```

Additionally you will need to enable the server in helm.

To do that run:

Edit `oss/charts/airbyte-workload-launcher/values.yaml` setting at the top level `enabled: true`

Edit `oss/charts/airbyte/values.yaml` setting `enabled: true` under `workload-launcher:`

Edit `oss/charts/airbyte/Chart.yaml.test` adding the following block:
```yaml
  - condition: airbyte-workload-launcher.enabled
    name: workload-launcher
    repository: "file://../airbyte-workload-launcher"
    version: "*"
```

### Dev creds
The syncs and mocked data are run under the following workspace:

Workspace: `6dab9cb2-b78e-46ac-b643-db908869a8bc`

email: airbyte@airbyte.com

password: airbyte