# airbyte-commons-storage

This module contains the logic for storage of objects from Airbyte jobs (logs, state, etc).

## Configuration

The library automatically selects the local or cloud logging client based on whether
the Airbyte platform is deployed to Kubernetes or not.  If deployed in Kubernetes, additional
configuration must be provided to further configure the log client between Google Cloud Storage
or S3/Minio.

```yaml
airbyte:
  cloud:
    storage:
      type: <one of gcs|s3|minio|local>
```

### Cloud Configuration

If configured to run in cloud/Kubernetes, additional configuration is required:

| Storage Type | Configuration Property                            | Description                                     |
|:-------------|:--------------------------------------------------|:------------------------------------------------|
| GCS          | airbyte.cloud.storage.gcs.application-credentials | Location of Google access credentials JSON file |
| Minio        | airbyte.cloud.storage.minio.access-key            | AWS access key                                  |
| Minio        | airbyte.cloud.storage.minio.secret-access-key     | AWS secret access key                           |
| Minio        | airbyte.cloud.storage.minio.endpoint              | Minio instance URI                              |
| S3           | airbyte.cloud.storage.s3.access-key               | AWS access key                                  |
| S3           | airbyte.cloud.storage.s3.secret-access-key        | AWS secret access key                           |
| S3           | airbyte.cloud.storage.s3.region                   | AWS region used to store the log files          |

The cloud log file prefix can be controlled by setting the `airbyte.logging.client.cloud.log-prefix` configuration property (default value is `job-logging` if not explicitly set).

### Local Configuration

If configured to run locally/not in Kubernetes, the additional configuration may be set to change the eahvior of the log client:

| Configuration Property                            | Description                                     | Default Value                                               |
|:--------------------------------------------------|:------------------------------------------------|:------------------------------------------------------------|
| airbyte.cloud.storage.local.root | The root directory used to store log files | /tmp/local-storage |

### General Configuration

Regardless of runtime location, the following configuration may be set to change the behavior of the log client:

| Configuration Property                            | Description                                     | Default Value                                               |
|:--------------------------------------------------|:------------------------------------------------|:------------------------------------------------------------|
 | airbyte.logging.client.log-tail-size | The maximum number of log lines to retrieve | 1000000 |

## Usage

To retrieve log files and/or control the logging MDC for job log files, simply inject the `LogClientManager` singleton:

```kotlin
@Inject
val logClientManager: LogClientManager
```

### Retrieving Jog Logs

```kotlin
val logLines = logClientManager.getJobLogFile(logPath = jobRoot)
```

## Metrics

When used in the cloud/in Kubernetes, the logging client emits the following metrics for log retrieval:


| Metric Name                               | Type  | Description                                                         |
|:------------------------------------------|:------|:--------------------------------------------------------------------|
| airbyte.log_client_file_byte_count        | Count | The count in bytes of the retrieved log lines                       |
| airbyte.log_client_file_line_count        | Count | The count of the number of lines retrieved from the log file        |
| airbyte.log_client_files_retrieved        | Gauge | The number of log files read in order to retrieve the log lines.    |
| airbyte.log_client_file_retrieval_time_ms | Timer | The amount of time in milliseconds spent to retrieve the log lines. |

Each of these metrics are automatically tagged with the log client type (e.g. `gcs`, `s3`, `minio`, etc). 