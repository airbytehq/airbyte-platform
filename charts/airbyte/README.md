# airbyte

![Version: 0.50.14](https://img.shields.io/badge/Version-0.50.14-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: dev](https://img.shields.io/badge/AppVersion-dev-informational?style=flat-square)

Helm chart to deploy airbyte

## Requirements

| Repository | Name | Version |
|------------|------|---------|
| https://airbytehq.github.io/helm-charts/ | airbyte-api-server | 0.50.14 |
| https://airbytehq.github.io/helm-charts/ | airbyte-bootloader | 0.50.14 |
| https://airbytehq.github.io/helm-charts/ | connector-builder-server | 0.50.14 |
| https://airbytehq.github.io/helm-charts/ | cron | 0.50.14 |
| https://airbytehq.github.io/helm-charts/ | keycloak | 0.50.14 |
| https://airbytehq.github.io/helm-charts/ | keycloak-setup | 0.50.14 |
| https://airbytehq.github.io/helm-charts/ | metrics | 0.50.14 |
| https://airbytehq.github.io/helm-charts/ | pod-sweeper | 0.50.14 |
| https://airbytehq.github.io/helm-charts/ | server | 0.50.14 |
| https://airbytehq.github.io/helm-charts/ | temporal | 0.50.14 |
| https://airbytehq.github.io/helm-charts/ | webapp | 0.50.14 |
| https://airbytehq.github.io/helm-charts/ | worker | 0.50.14 |
| https://airbytehq.github.io/helm-charts/ | workload-api-server | 0.50.14 |
| https://airbytehq.github.io/helm-charts/ | workload-launcher | 0.50.14 |
| https://charts.bitnami.com/bitnami | common | 1.x.x |

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| airbyte-api-server.enabled | bool | `true` |  |
| airbyte-api-server.env_vars | object | `{}` |  |
| airbyte-api-server.image.pullPolicy | string | `"IfNotPresent"` | The pull policy to use for the airbyte airbyte-api-server image |
| airbyte-api-server.image.repository | string | `"airbyte/airbyte-api-server"` | The repository to use for the airbyte airbyte-api-server image. |
| airbyte-api-server.ingress.annotations | object | `{}` | Ingress annotations done as key:value pairs |
| airbyte-api-server.ingress.className | string | `""` | Specifies ingressClassName for clusters >= 1.18+ |
| airbyte-api-server.ingress.enabled | bool | `false` | Set to true to enable ingress record generation |
| airbyte-api-server.ingress.hosts | list | `[]` | The list of hostnames to be covered with this ingress record. |
| airbyte-api-server.ingress.tls | list | `[]` | Custom ingress TLS configuration |
| airbyte-api-server.livenessProbe.enabled | bool | `true` | Enable livenessProbe on the server |
| airbyte-api-server.livenessProbe.failureThreshold | int | `3` | Failure threshold for livenessProbe |
| airbyte-api-server.livenessProbe.initialDelaySeconds | int | `30` | Initial delay seconds for livenessProbe |
| airbyte-api-server.livenessProbe.periodSeconds | int | `10` | Period seconds for livenessProbe |
| airbyte-api-server.livenessProbe.successThreshold | int | `1` | Success threshold for livenessProbe |
| airbyte-api-server.livenessProbe.timeoutSeconds | int | `10` | Timeout seconds for livenessProbe |
| airbyte-api-server.log.level | string | `"INFO"` | The log level to log at. |
| airbyte-api-server.readinessProbe.enabled | bool | `true` | Enable readinessProbe on the server |
| airbyte-api-server.readinessProbe.failureThreshold | int | `3` | Failure threshold for readinessProbe |
| airbyte-api-server.readinessProbe.initialDelaySeconds | int | `10` | Initial delay seconds for readinessProbe |
| airbyte-api-server.readinessProbe.periodSeconds | int | `10` | Period seconds for readinessProbe |
| airbyte-api-server.readinessProbe.successThreshold | int | `1` | Success threshold for readinessProbe |
| airbyte-api-server.readinessProbe.timeoutSeconds | int | `10` | Timeout seconds for readinessProbe |
| airbyte-api-server.replicaCount | int | `1` | Number of airbyte-api-server replicas |
| airbyte-api-server.resources.limits | object | `{}` | The resources limits for the airbyte-api-server container |
| airbyte-api-server.resources.requests | object | `{}` |  |
| airbyte-api-server.service.port | int | `80` |  |
| airbyte-bootloader.affinity | object | `{}` | Affinity and anti-affinity for bootloader pod assignment, see https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity |
| airbyte-bootloader.enabled | bool | `true` |  |
| airbyte-bootloader.env_vars | object | `{}` | Supply extra env variables to main container using simplified notation |
| airbyte-bootloader.extraContainers | list | `[]` | Additional container for server pod(s) |
| airbyte-bootloader.extraEnv | list | `[]` | Supply extra env variables to main container using full notation |
| airbyte-bootloader.extraInitContainers | list | `[]` | Additional init containers for server pods |
| airbyte-bootloader.extraVolumeMounts | list | `[]` | Additional volumeMounts for server containers |
| airbyte-bootloader.extraVolumes | list | `[]` | Additional volumes for server pods |
| airbyte-bootloader.image.pullPolicy | string | `"IfNotPresent"` | The pull policy to use for the airbyte bootloader image |
| airbyte-bootloader.image.repository | string | `"airbyte/bootloader"` | The repository to use for the airbyte bootloader image. |
| airbyte-bootloader.nodeSelector | object | `{}` | Node labels for pod assignment, see https://kubernetes.io/docs/user-guide/node-selection/ |
| airbyte-bootloader.podAnnotations | object | `{}` | Add extra annotations to the bootloader pod |
| airbyte-bootloader.podLabels | object | `{}` | Add extra labels to the bootloader pod |
| airbyte-bootloader.resources.limits | object | `{}` | The resources limits for the airbyte bootloader image |
| airbyte-bootloader.resources.requests | object | `{}` | The requested resources for the airbyte bootloader image |
| airbyte-bootloader.secrets | object | `{}` | Supply additional secrets to container |
| airbyte-bootloader.tolerations | list | `[]` | Tolerations for worker pod assignment, see https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/ |
| connector-builder-server.enabled | bool | `true` |  |
| connector-builder-server.env_vars | object | `{}` |  |
| connector-builder-server.image.pullPolicy | string | `"IfNotPresent"` | The pull policy to use for the airbyte connector-builder-server image |
| connector-builder-server.image.repository | string | `"airbyte/connector-builder-server"` | The repository to use for the airbyte connector-builder-server image. |
| connector-builder-server.livenessProbe.enabled | bool | `true` | Enable livenessProbe on the server |
| connector-builder-server.livenessProbe.failureThreshold | int | `3` | Failure threshold for livenessProbe |
| connector-builder-server.livenessProbe.initialDelaySeconds | int | `30` | Initial delay seconds for livenessProbe |
| connector-builder-server.livenessProbe.periodSeconds | int | `10` | Period seconds for livenessProbe |
| connector-builder-server.livenessProbe.successThreshold | int | `1` | Success threshold for livenessProbe |
| connector-builder-server.livenessProbe.timeoutSeconds | int | `10` | Timeout seconds for livenessProbe |
| connector-builder-server.log.level | string | `"INFO"` | The log level to log at. |
| connector-builder-server.readinessProbe.enabled | bool | `true` | Enable readinessProbe on the server |
| connector-builder-server.readinessProbe.failureThreshold | int | `3` | Failure threshold for readinessProbe |
| connector-builder-server.readinessProbe.initialDelaySeconds | int | `10` | Initial delay seconds for readinessProbe |
| connector-builder-server.readinessProbe.periodSeconds | int | `10` | Period seconds for readinessProbe |
| connector-builder-server.readinessProbe.successThreshold | int | `1` | Success threshold for readinessProbe |
| connector-builder-server.readinessProbe.timeoutSeconds | int | `10` | Timeout seconds for readinessProbe |
| connector-builder-server.replicaCount | int | `1` | Number of connector-builder-server replicas |
| connector-builder-server.resources.limits | object | `{}` | The resources limits for the connector-builder-server container |
| connector-builder-server.resources.requests | object | `{}` | The requested resources for the connector-builder-server container |
| connector-builder-server.service.port | int | `80` |  |
| cron.affinity | object | `{}` | Affinity and anti-affinity for cron pod assignment, see https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity |
| cron.containerSecurityContext | object | `{}` | Security context for the container |
| cron.enabled | bool | `true` |  |
| cron.env_vars | object | `{}` | Supply extra env variables to main container using simplified notation |
| cron.extraContainers | list | `[]` | Additional container for cron pods |
| cron.extraEnv | list | `[]` | Supply extra env variables to main container using full notation |
| cron.extraInitContainers | list | `[]` | Additional init containers for cron pods |
| cron.extraVolumeMounts | list | `[]` | Additional volumeMounts for cron containers |
| cron.extraVolumes | list | `[]` | Additional volumes for cron pods |
| cron.image.pullPolicy | string | `"IfNotPresent"` | The pull policy to use for the airbyte cron image |
| cron.image.repository | string | `"airbyte/cron"` | The repository to use for the airbyte cron image. |
| cron.livenessProbe.enabled | bool | `true` | Enable livenessProbe on the cron |
| cron.livenessProbe.failureThreshold | int | `3` | Failure threshold for livenessProbe |
| cron.livenessProbe.initialDelaySeconds | int | `30` | Initial delay seconds for livenessProbe |
| cron.livenessProbe.periodSeconds | int | `10` | Period seconds for livenessProbe |
| cron.livenessProbe.successThreshold | int | `1` | Success threshold for livenessProbe |
| cron.livenessProbe.timeoutSeconds | int | `1` | Timeout seconds for livenessProbe |
| cron.log.level | string | `"INFO"` | The log level to log at. |
| cron.nodeSelector | object | `{}` | Node labels for pod assignment, see https://kubernetes.io/docs/user-guide/node-selection/ |
| cron.podAnnotations | object | `{}` | Add extra annotations to the cron pods |
| cron.podLabels | object | `{}` | Add extra labels to the cron pods |
| cron.readinessProbe.enabled | bool | `true` | Enable readinessProbe on the cron |
| cron.readinessProbe.failureThreshold | int | `3` | Failure threshold for readinessProbe |
| cron.readinessProbe.initialDelaySeconds | int | `10` | Initial delay seconds for readinessProbe |
| cron.readinessProbe.periodSeconds | int | `10` | Period seconds for readinessProbe |
| cron.readinessProbe.successThreshold | int | `1` | Success threshold for readinessProbe |
| cron.readinessProbe.timeoutSeconds | int | `1` | Timeout seconds for readinessProbe |
| cron.replicaCount | int | `1` | Number of cron replicas |
| cron.resources.limits | object | `{}` | The resources limits for the cron container |
| cron.resources.requests | object | `{}` | The requested resources for the cron container |
| cron.secrets | object | `{}` | Supply additional secrets to container |
| cron.tolerations | list | `[]` | Tolerations for cron pod assignment, see https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/ |
| externalDatabase.database | string | `"db-airbyte"` | Database name |
| externalDatabase.existingSecret | string | `""` | Name of an existing secret resource containing the DB password |
| externalDatabase.existingSecretPasswordKey | string | `""` | Name of an existing secret key containing the DB password |
| externalDatabase.host | string | `"localhost"` | Database host |
| externalDatabase.jdbcUrl | string | `""` | Database full JDBL URL (ex: jdbc:postgresql://host:port/db?parameters) |
| externalDatabase.password | string | `""` | Database password |
| externalDatabase.port | int | `5432` | Database port number |
| externalDatabase.user | string | `"airbyte"` | non-root Username for Airbyte Database |
| fullnameOverride | string | `""` | String to fully override airbyte.fullname template with a string |
| global.database.secretName | string | `""` | Secret name where database credentials are stored |
| global.database.secretValue | string | `""` | Secret value for database password |
| global.deploymentMode | string | `"oss"` | Deployment mode, whether or not render the default env vars and volumes in deployment spec |
| global.edition | string | `"community"` | Edition; "community" or "pro" |
| global.env_vars | object | `{}` | Environment variables |
| global.jobs.kube.annotations | object | `{}` | key/value annotations applied to kube jobs |
| global.jobs.kube.images.busybox | string | `""` | busybox image used by the job pod |
| global.jobs.kube.images.curl | string | `""` | curl image used by the job pod |
| global.jobs.kube.images.socat | string | `""` | socat image used by the job pod |
| global.jobs.kube.labels | object | `{}` | key/value labels applied to kube jobs |
| global.jobs.kube.main_container_image_pull_secret | string | `""` | image pull secret to use for job pod |
| global.jobs.kube.nodeSelector | object | `{}` | Node labels for pod assignment |
| global.jobs.kube.tolerations | list | `[]` | Node tolerations for pod assignment  Any boolean values should be quoted to ensure the value is passed through as a string. |
| global.jobs.resources.limits | object | `{}` | Job resource limits |
| global.jobs.resources.requests | object | `{}` | Job resource requests |
| global.logs.accessKey.existingSecret | string | `""` | Existing secret |
| global.logs.accessKey.existingSecretKey | string | `""` | Existing secret key |
| global.logs.accessKey.password | string | `""` | Logs access key |
| global.logs.externalMinio.enabled | bool | `false` | Enable or disable an external Minio instance |
| global.logs.externalMinio.host | string | `"localhost"` | External Minio host |
| global.logs.externalMinio.port | int | `9000` | External Minio port |
| global.logs.gcs.bucket | string | `""` | GCS Bucket Name |
| global.logs.gcs.credentials | string | `""` | The path the GCS creds are written to. If you are mounting an existing secret to extraVolumes on scheduler, server and worker deployments, then set credentials to the path of the mounted JSON file. |
| global.logs.gcs.credentialsJson | string | `""` | Base64 encoded json GCP credentials file contents. If credentialsJson is set then credentials auto resolves (to /secrets/gcs-log-creds/gcp.json). |
| global.logs.minio.affinity | object | `{}` | Node affinity and anti-affinity for pod assignment |
| global.logs.minio.enabled | bool | `true` | Enable or disable the Minio helm chart |
| global.logs.minio.nodeSelector | object | `{}` | Node labels for pod assignment |
| global.logs.minio.tolerations | list | `[]` | Node tolerations for pod assignment |
| global.logs.s3 | object | `{"bucket":"airbyte-dev-logs","bucketRegion":"","enabled":false}` | S3 configuration, used if logs.storage.type is "S3" |
| global.logs.s3.bucket | string | `"airbyte-dev-logs"` | Bucket name where logs should be stored |
| global.logs.s3.bucketRegion | string | `""` | Region where bucket is located (must be empty if using Minio) |
| global.logs.s3.enabled | bool | `false` | Enable or disable custom S3 log location |
| global.logs.secretKey.existingSecret | string | `""` | Existing secret |
| global.logs.secretKey.existingSecretKey | string | `""` | Existing secret key |
| global.logs.secretKey.password | string | `""` | Logs secret key |
| global.logs.storage.type | string | `"MINIO"` | Determines which log storage will be utilized; "MINIO", "S3", or "GCS". Used in conjunction with logs.minio.*, logs.s3.* or logs.gcs.* |
| global.metrics.metricClient | string | `""` | The metric client to configure globally. Supports "otel" |
| global.metrics.otelCollectorEndpoint | string | `""` | The open-telemetry-collector endpoint that metrics will be sent to |
| global.serviceAccountName | string | `"airbyte-admin"` | Service Account name override |
| global.state.storage | object | `{"type":"MINIO"}` | Determines which state storage will be utilized; "MINIO", "S3", or "GCS" |
| keycloak-setup.enabled | bool | `false` |  |
| keycloak.auth.adminPassword | string | `"keycloak123"` |  |
| keycloak.auth.adminUsername | string | `"airbyteAdmin"` |  |
| keycloak.enabled | bool | `false` |  |
| metrics.affinity | object | `{}` | Affinity and anti-affinity for metrics-reporter pod assignment, see https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity |
| metrics.containerSecurityContext | object | `{}` | Security context for the container |
| metrics.enabled | bool | `false` |  |
| metrics.env_vars | object | `{}` |  |
| metrics.extraContainers | list | `[]` |  |
| metrics.extraEnv | list | `[]` | Additional env vars for metrics-reporter pods |
| metrics.extraVolumeMounts | list | `[]` | Additional volumeMounts for metrics-reporter containers |
| metrics.extraVolumes | list | `[]` | Additional volumes for metrics-reporter pods |
| metrics.image.pullPolicy | string | `"IfNotPresent"` | The pull policy to use for the airbyte metrics-reporter image |
| metrics.image.repository | string | `"airbyte/metrics-reporter"` | The repository to use for the airbyte metrics-reporter image. |
| metrics.nodeSelector | object | `{}` | Node labels for pod assignment, see https://kubernetes.io/docs/user-guide/node-selection/ |
| metrics.podAnnotations | object | `{}` | Add extra annotations to the metrics-reporter pod |
| metrics.podLabels | object | `{}` | Add extra labels to the metrics-reporter pod |
| metrics.replicaCount | int | `1` | Number of metrics-reporter replicas |
| metrics.resources.limits | object | `{}` | The resources limits for the metrics-reporter container |
| metrics.resources.requests | object | `{}` | The requested resources for the metrics-reporter container |
| metrics.secrets | object | `{}` |  |
| metrics.tolerations | list | `[]` | Tolerations for metrics-reporter pod assignment, see https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/ |
| minio.affinity | object | `{}` | Affinity and anti-affinity for minio pod assignment, see https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity |
| minio.auth.rootPassword | string | `"minio123"` |  |
| minio.auth.rootUser | string | `"minio"` |  |
| minio.enabled | bool | `true` |  |
| minio.endpoint | string | `"http://airbyte-minio-svc:9000"` |  |
| minio.image.repository | string | `"minio/minio"` | Minio image used by Minio helm chart |
| minio.image.tag | string | `"RELEASE.2023-11-20T22-40-07Z"` | Minio tag image |
| minio.nodeSelector | object | `{}` | Node labels for pod assignment, see https://kubernetes.io/docs/user-guide/node-selection/ # |
| minio.storage.volumeClaimValue | string | `"500Mi"` |  |
| minio.tolerations | list | `[]` | Tolerations for minio pod assignment, see https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/ # |
| nameOverride | string | `""` | String to partially override airbyte.fullname template with a string (will prepend the release name) |
| pod-sweeper.affinity | object | `{}` | Affinity and anti-affinity for pod assignment, see https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity |
| pod-sweeper.containerSecurityContext | object | `{}` | Security context for the container |
| pod-sweeper.enabled | bool | `true` |  |
| pod-sweeper.extraVolumeMounts | list | `[]` | Additional volumeMounts for podSweeper container(s). |
| pod-sweeper.extraVolumes | list | `[]` | Additional volumes for podSweeper pod(s). |
| pod-sweeper.image.pullPolicy | string | `"IfNotPresent"` | The pull policy for the pod sweeper image |
| pod-sweeper.image.repository | string | `"bitnami/kubectl"` | The image repository to use for the pod sweeper |
| pod-sweeper.image.tag | string | `"1.28.4"` | The pod sweeper image tag to use |
| pod-sweeper.livenessProbe.enabled | bool | `true` | Enable livenessProbe on the podSweeper |
| pod-sweeper.livenessProbe.failureThreshold | int | `3` | Failure threshold for livenessProbe |
| pod-sweeper.livenessProbe.initialDelaySeconds | int | `5` | Initial delay seconds for livenessProbe |
| pod-sweeper.livenessProbe.periodSeconds | int | `30` | Period seconds for livenessProbe |
| pod-sweeper.livenessProbe.successThreshold | int | `1` | Success threshold for livenessProbe |
| pod-sweeper.livenessProbe.timeoutSeconds | int | `1` | Timeout seconds for livenessProbe |
| pod-sweeper.nodeSelector | object | `{}` | Node labels for pod assignment, see https://kubernetes.io/docs/user-guide/node-selection/ |
| pod-sweeper.podAnnotations | object | `{}` | Add extra annotations to the podSweeper pod |
| pod-sweeper.podLabels | object | `{}` | Add extra labels to the podSweeper pod |
| pod-sweeper.readinessProbe.enabled | bool | `true` | Enable readinessProbe on the podSweeper |
| pod-sweeper.readinessProbe.failureThreshold | int | `3` | Failure threshold for readinessProbe |
| pod-sweeper.readinessProbe.initialDelaySeconds | int | `5` | Initial delay seconds for readinessProbe |
| pod-sweeper.readinessProbe.periodSeconds | int | `30` | Period seconds for readinessProbe |
| pod-sweeper.readinessProbe.successThreshold | int | `1` | Success threshold for readinessProbe |
| pod-sweeper.readinessProbe.timeoutSeconds | int | `1` | Timeout seconds for readinessProbe |
| pod-sweeper.resources.limits | object | `{}` | The resources limits for the podSweeper container |
| pod-sweeper.resources.requests | object | `{}` | The requested resources for the podSweeper container |
| pod-sweeper.tolerations | list | `[]` | Tolerations for pod assignment, see https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/ |
| postgresql.affinity | object | `{}` | Affinity and anti-affinity for postgresql pod assignment, see https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity |
| postgresql.commonAnnotations."helm.sh/hook" | string | `"pre-install"` | It will determine when the hook should be rendered |
| postgresql.commonAnnotations."helm.sh/hook-weight" | string | `"-1"` | The order in which the hooks are executed. If weight is lower, it has higher priority |
| postgresql.containerSecurityContext.runAsNonRoot | bool | `true` | Ensures the container will run with a non-root user |
| postgresql.enabled | bool | `true` | Switch to enable or disable the PostgreSQL helm chart |
| postgresql.existingSecret | string | `""` | Name of an existing secret containing the PostgreSQL password ('postgresql-password' key) |
| postgresql.image.repository | string | `"airbyte/db"` |  |
| postgresql.nodeSelector | object | `{}` | Node labels for pod assignment, see https://kubernetes.io/docs/user-guide/node-selection/ |
| postgresql.postgresqlDatabase | string | `"db-airbyte"` | Airbyte Postgresql database |
| postgresql.postgresqlPassword | string | `"airbyte"` | Airbyte Postgresql password |
| postgresql.postgresqlUsername | string | `"airbyte"` | Airbyte Postgresql username |
| postgresql.tolerations | list | `[]` | Tolerations for postgresql pod assignment, see https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/ |
| server.affinity | object | `{}` | Affinity and anti-affinity for server pod assignment, see https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity |
| server.containerSecurityContext | object | `{}` | Security context for the container |
| server.enabled | bool | `true` |  |
| server.env_vars | object | `{}` | Supply extra env variables to main container using simplified notation |
| server.extraContainers | list | `[]` | Additional container for server pods |
| server.extraEnv | list | `[]` | Supply extra env variables to main container using full notation |
| server.extraInitContainers | list | `[]` | Additional init containers for server pods |
| server.extraVolumeMounts | list | `[]` | Additional volumeMounts for server containers |
| server.extraVolumes | list | `[]` | Additional volumes for server pods |
| server.image.pullPolicy | string | `"IfNotPresent"` | the pull policy to use for the airbyte server image |
| server.image.repository | string | `"airbyte/server"` | The repository to use for the airbyte server image. |
| server.livenessProbe.enabled | bool | `true` | Enable livenessProbe on the server |
| server.livenessProbe.failureThreshold | int | `3` | Failure threshold for livenessProbe |
| server.livenessProbe.initialDelaySeconds | int | `30` | Initial delay seconds for livenessProbe |
| server.livenessProbe.periodSeconds | int | `10` | Period seconds for livenessProbe |
| server.livenessProbe.successThreshold | int | `1` | Success threshold for livenessProbe |
| server.livenessProbe.timeoutSeconds | int | `10` | Timeout seconds for livenessProbe |
| server.log.level | string | `"INFO"` | The log level to log at |
| server.nodeSelector | object | `{}` | Node labels for pod assignment, see https://kubernetes.io/docs/user-guide/node-selection/ |
| server.podAnnotations | object | `{}` | Add extra annotations to the server pods |
| server.podLabels | object | `{}` | Add extra labels to the server pods |
| server.readinessProbe.enabled | bool | `true` | Enable readinessProbe on the server |
| server.readinessProbe.failureThreshold | int | `3` | Failure threshold for readinessProbe |
| server.readinessProbe.initialDelaySeconds | int | `10` | Initial delay seconds for readinessProbe |
| server.readinessProbe.periodSeconds | int | `10` | Period seconds for readinessProbe |
| server.readinessProbe.successThreshold | int | `1` | Success threshold for readinessProbe |
| server.readinessProbe.timeoutSeconds | int | `10` | Timeout seconds for readinessProbe |
| server.replicaCount | int | `1` | Number of server replicas |
| server.resources.limits | object | `{}` | The resources limits for the server container |
| server.resources.requests | object | `{}` | The requested resources for the server container |
| server.secrets | object | `{}` | Supply additional secrets to container |
| server.tolerations | list | `[]` | Tolerations for server pod assignment, see https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/ |
| serviceAccount.annotations | object | `{}` | Annotations for service account. Evaluated as a template. Only used if `create` is `true`. |
| serviceAccount.create | bool | `true` | Specifies whether a ServiceAccount should be created |
| serviceAccount.name | string | `"airbyte-admin"` | Name of the service account to use. If not set and create is true, a name is generated using the fullname template. |
| temporal.affinity | object | `{}` | Affinity and anti-affinity for temporal pod assignment, see https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity |
| temporal.containerSecurityContext | object | `{}` | Security context for the container |
| temporal.enabled | bool | `true` |  |
| temporal.extraContainers | list | `[]` |  |
| temporal.extraEnv | list | `[]` | Additional env vars for temporal pod(s). |
| temporal.extraInitContainers | list | `[]` | Additional InitContainers to initialize the pod |
| temporal.extraVolumeMounts | list | `[]` | Additional volumeMounts for temporal containers |
| temporal.extraVolumes | list | `[]` | Additional volumes for temporal pods |
| temporal.image.pullPolicy | string | `"IfNotPresent"` | The pull policy for the temporal image |
| temporal.image.repository | string | `"temporalio/auto-setup"` | The temporal image repository to use |
| temporal.image.tag | string | `"1.20.1"` | The temporal image tag to use |
| temporal.livenessProbe.enabled | bool | `false` | Enable livenessProbe on the temporal |
| temporal.nodeSelector | object | `{}` | Node labels for temporal pod assignment, see https://kubernetes.io/docs/user-guide/node-selection/ |
| temporal.podAnnotations | object | `{}` | Add extra annotations to the temporal pod |
| temporal.podLabels | object | `{}` | Add extra labels to the temporal pod |
| temporal.readinessProbe.enabled | bool | `false` | Enable readinessProbe on the temporal |
| temporal.replicaCount | int | `1` | The number of temporal replicas to deploy |
| temporal.resources.limits | object | `{}` | The resources limits for temporal pods |
| temporal.resources.requests | object | `{}` | The requested resources for temporal pods |
| temporal.service.port | int | `7233` | The temporal port and exposed kubernetes port |
| temporal.service.type | string | `"ClusterIP"` | The Kubernetes Service Type |
| temporal.tolerations | list | `[]` | Tolerations for temporal pod assignment, see https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/ |
| version | string | `""` | Sets the AIRBYTE_VERSION environment variable. Defaults to Chart.AppVersion. # If changing the image tags below, you should probably also update this. |
| webapp.affinity | object | `{}` | Affinity and anti-affinity for webapp pod assignment, see https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity |
| webapp.connector-builder-server.url | string | `"/connector-builder-api"` |  |
| webapp.containerSecurityContext | object | `{}` | Security context for the container # Examples: # containerSecurityContext: #    runAsNonRoot: true #    runAsUser: 1000 #    readOnlyRootFilesystem: true |
| webapp.enabled | bool | `true` |  |
| webapp.env_vars | object | `{}` | Supply extra env variables to main container using simplified notation |
| webapp.extraContainers | list | `[]` | Additional container for server pods |
| webapp.extraEnv | list | `[]` | Supply extra env variables to main container using full notation |
| webapp.extraInitContainers | list | `[]` | Additional init containers for server pods |
| webapp.extraVolumeMounts | list | `[]` | Additional volumeMounts for webapp containers |
| webapp.extraVolumes | list | `[]` | Additional volumes for webapp pods |
| webapp.fullstory.enabled | bool | `false` | Whether or not to enable fullstory |
| webapp.image.pullPolicy | string | `"IfNotPresent"` | The pull policy to use for the airbyte webapp image |
| webapp.image.repository | string | `"airbyte/webapp"` | The repository to use for the airbyte webapp image |
| webapp.ingress.annotations | object | `{}` | Ingress annotations done as key:value pairs |
| webapp.ingress.api | string | `nil` |  |
| webapp.ingress.className | string | `""` | Specifies ingressClassName for clusters >= 1.18+ |
| webapp.ingress.enabled | bool | `false` | Set to true to enable ingress record generation |
| webapp.ingress.hosts | list | `[]` | The list of hostnames to be covered with this ingress record. |
| webapp.ingress.tls | list | `[]` | Custom ingress TLS configuration |
| webapp.ingress.url | string | `"/api/v1/"` | The webapp API url |
| webapp.livenessProbe.enabled | bool | `true` | Enable livenessProbe on the webapp |
| webapp.livenessProbe.failureThreshold | int | `3` | Failure threshold for livenessProbe |
| webapp.livenessProbe.initialDelaySeconds | int | `30` | Initial delay seconds for livenessProbe |
| webapp.livenessProbe.periodSeconds | int | `10` | Period seconds for livenessProbe |
| webapp.livenessProbe.successThreshold | int | `1` | Success threshold for livenessProbe |
| webapp.livenessProbe.timeoutSeconds | int | `1` | Timeout seconds for livenessProbe |
| webapp.nodeSelector | object | `{}` | Node labels for pod assignment, see https://kubernetes.io/docs/user-guide/node-selection/ |
| webapp.podAnnotations | object | `{}` | Add extra annotations to the webapp pods |
| webapp.podLabels | object | `{}` | webapp.podLabels [object] Add extra labels to the webapp pods |
| webapp.readinessProbe.enabled | bool | `true` | Enable readinessProbe on the webapp |
| webapp.readinessProbe.failureThreshold | int | `3` | Failure threshold for readinessProbe |
| webapp.readinessProbe.initialDelaySeconds | int | `10` | Initial delay seconds for readinessProbe |
| webapp.readinessProbe.periodSeconds | int | `10` | Period seconds for readinessProbe |
| webapp.readinessProbe.successThreshold | int | `1` | Success threshold for readinessProbe |
| webapp.readinessProbe.timeoutSeconds | int | `1` | Timeout seconds for readinessProbe |
| webapp.replicaCount | int | `1` | Number of webapp replicas |
| webapp.resources.limits | object | `{}` | The resources limits for the Web container |
| webapp.resources.requests | object | `{}` | The requested resources for the Web container |
| webapp.secrets | object | `{}` | Supply additional secrets to container |
| webapp.service.annotations | object | `{}` | Annotations for the webapp service resource |
| webapp.service.port | int | `80` | The service port to expose the webapp on |
| webapp.service.type | string | `"ClusterIP"` | The service type to use for the webapp service |
| webapp.tolerations | list | `[]` | Tolerations for webapp pod assignment, see https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/ |
| worker.activityInitialDelayBetweenAttemptsSeconds | string | `""` |  |
| worker.activityMaxAttempt | string | `""` |  |
| worker.activityMaxDelayBetweenAttemptsSeconds | string | `""` |  |
| worker.affinity | object | `{}` | Affinity and anti-affinity for worker pod assignment, see https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity |
| worker.containerOrchestrator.image | string | `""` | Orchestrator image |
| worker.containerSecurityContext | object | `{}` | Security context for the container |
| worker.debug.enabled | bool | `false` |  |
| worker.enabled | bool | `true` |  |
| worker.extraContainers | list | `[]` | Additional container for worker pods |
| worker.extraEnv | list | `[]` | Additional env vars for worker pods |
| worker.extraVolumeMounts | list | `[]` | Additional volumeMounts for worker containers |
| worker.extraVolumes | list | `[]` | Additional volumes for worker pods |
| worker.hpa.enabled | bool | `false` |  |
| worker.image.pullPolicy | string | `"IfNotPresent"` | the pull policy to use for the airbyte worker image |
| worker.image.repository | string | `"airbyte/worker"` | The repository to use for the airbyte worker image. |
| worker.livenessProbe.enabled | bool | `true` | Enable livenessProbe on the worker |
| worker.livenessProbe.failureThreshold | int | `3` | Failure threshold for livenessProbe |
| worker.livenessProbe.initialDelaySeconds | int | `30` | Initial delay seconds for livenessProbe |
| worker.livenessProbe.periodSeconds | int | `10` | Period seconds for livenessProbe |
| worker.livenessProbe.successThreshold | int | `1` | Success threshold for livenessProbe |
| worker.livenessProbe.timeoutSeconds | int | `1` | Timeout seconds for livenessProbe |
| worker.log.level | string | `"INFO"` |  |
| worker.maxNotifyWorkers | int | `5` |  |
| worker.nodeSelector | object | `{}` | Node labels for pod assignment, see https://kubernetes.io/docs/user-guide/node-selection/ |
| worker.podAnnotations | object | `{}` | Add extra annotations to the worker pods |
| worker.podLabels | object | `{}` | Add extra labels to the worker pods |
| worker.readinessProbe.enabled | bool | `true` | Enable readinessProbe on the worker |
| worker.readinessProbe.failureThreshold | int | `3` | Failure threshold for readinessProbe |
| worker.readinessProbe.initialDelaySeconds | int | `10` | Initial delay seconds for readinessProbe |
| worker.readinessProbe.periodSeconds | int | `10` | Period seconds for readinessProbe |
| worker.readinessProbe.successThreshold | int | `1` | Success threshold for readinessProbe |
| worker.readinessProbe.timeoutSeconds | int | `1` | Timeout seconds for readinessProbe |
| worker.replicaCount | int | `1` | Number of worker replicas |
| worker.resources.limits | object | `{}` |  |
| worker.resources.requests | object | `{}` | The requested resources for the worker container |
| worker.tolerations | list | `[]` | Tolerations for worker pod assignment, see https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/ |
| workload-api-server.bearerToken | string | `"token"` |  |
| workload-api-server.enabled | bool | `false` |  |
| workload-api-server.env_vars | object | `{}` |  |
| workload-api-server.image.pullPolicy | string | `"IfNotPresent"` | The pull policy to use for the airbyte-workload-api-server image |
| workload-api-server.image.repository | string | `"airbyte/workload-api-server"` | The repository to use for the airbyte-workload-api-server image. |
| workload-api-server.ingress.annotations | object | `{}` | Ingress annotations done as key:value pairs |
| workload-api-server.ingress.className | string | `""` | Specifies ingressClassName for clusters >= 1.18+ |
| workload-api-server.ingress.enabled | bool | `false` | Set to true to enable ingress record generation |
| workload-api-server.ingress.hosts | list | `[]` | The list of hostnames to be covered with this ingress record |
| workload-api-server.ingress.tls | list | `[]` | Custom ingress TLS configuration |
| workload-api-server.livenessProbe.enabled | bool | `true` | Enable livenessProbe on the server |
| workload-api-server.livenessProbe.failureThreshold | int | `3` | Failure threshold for livenessProbe |
| workload-api-server.livenessProbe.initialDelaySeconds | int | `30` | Initial delay seconds for livenessProbe |
| workload-api-server.livenessProbe.periodSeconds | int | `10` | Period seconds for livenessProbe |
| workload-api-server.livenessProbe.successThreshold | int | `1` | Success threshold for livenessProbe |
| workload-api-server.livenessProbe.timeoutSeconds | int | `10` | Timeout seconds for livenessProbe |
| workload-api-server.log.level | string | `"INFO"` | The log level at which to log |
| workload-api-server.nodeSelector | object | `{}` |  |
| workload-api-server.readinessProbe.enabled | bool | `true` | Enable readinessProbe on the server |
| workload-api-server.readinessProbe.failureThreshold | int | `3` | Failure threshold for readinessProbe |
| workload-api-server.readinessProbe.initialDelaySeconds | int | `10` | Initial delay seconds for readinessProbe |
| workload-api-server.readinessProbe.periodSeconds | int | `10` | Period seconds for readinessProbe |
| workload-api-server.readinessProbe.successThreshold | int | `1` | Success threshold for readinessProbe |
| workload-api-server.readinessProbe.timeoutSeconds | int | `10` | Timeout seconds for readinessProbe |
| workload-api-server.replicaCount | int | `1` | airbyte-api-server replicas |
| workload-api-server.resources.limits | object | `{}` | The resources limits for the airbyte-workload-api-server container |
| workload-api-server.resources.requests | object | `{}` | The requested resources for the airbyte-workload-api-server container |
| workload-api-server.service.port | int | `8007` |  |
| workload-launcher.activityInitialDelayBetweenAttemptsSeconds | string | `""` |  |
| workload-launcher.activityMaxAttempt | string | `""` |  |
| workload-launcher.activityMaxDelayBetweenAttemptsSeconds | string | `""` |  |
| workload-launcher.affinity | object | `{}` |  |
| workload-launcher.containerOrchestrator.enabled | bool | `true` | Enable or disable Orchestrator |
| workload-launcher.containerOrchestrator.image | string | `""` | Orchestrator image |
| workload-launcher.containerSecurityContext | object | `{}` | Security context for the container |
| workload-launcher.debug.enabled | bool | `false` |  |
| workload-launcher.enabled | bool | `false` |  |
| workload-launcher.extraContainers | list | `[]` |  |
| workload-launcher.extraEnv | list | `[]` | Additional env vars for workload launcher pods |
| workload-launcher.extraVolumeMounts | list | `[]` | Additional volumeMounts for workload launcher containers |
| workload-launcher.extraVolumes | list | `[]` | Additional volumes for workload launcher pods |
| workload-launcher.hpa.enabled | bool | `false` |  |
| workload-launcher.image.pullPolicy | string | `"IfNotPresent"` | The pull policy to use for the airbyte workload launcher image |
| workload-launcher.image.repository | string | `"airbyte/workload-launcher"` | The repository to use for the airbyte workload launcher image. |
| workload-launcher.livenessProbe.enabled | bool | `true` | Enable livenessProbe on the workload launcher |
| workload-launcher.livenessProbe.failureThreshold | int | `3` | Failure threshold for livenessProbe |
| workload-launcher.livenessProbe.initialDelaySeconds | int | `30` | Initial delay seconds for livenessProbe |
| workload-launcher.livenessProbe.periodSeconds | int | `10` | Period seconds for livenessProbe |
| workload-launcher.livenessProbe.successThreshold | int | `1` | Success threshold for livenessProbe |
| workload-launcher.livenessProbe.timeoutSeconds | int | `1` | Timeout seconds for livenessProbe |
| workload-launcher.log.level | string | `"INFO"` | The log level to log at |
| workload-launcher.maxNotifyWorkers | int | `5` |  |
| workload-launcher.nodeSelector | object | `{}` | Node labels for pod assignment, see https://kubernetes.io/docs/user-guide/node-selection/ |
| workload-launcher.podAnnotations | object | `{}` | Add extra annotations to the workload launcher pods |
| workload-launcher.podLabels | object | `{}` | Add extra labels to the workload launcher pods |
| workload-launcher.readinessProbe.enabled | bool | `true` | Enable readinessProbe on the workload launcher |
| workload-launcher.readinessProbe.failureThreshold | int | `3` | Failure threshold for readinessProbe |
| workload-launcher.readinessProbe.initialDelaySeconds | int | `10` | Initial delay seconds for readinessProbe |
| workload-launcher.readinessProbe.periodSeconds | int | `10` | Period seconds for readinessProbe |
| workload-launcher.readinessProbe.successThreshold | int | `1` | Success threshold for readinessProbe |
| workload-launcher.readinessProbe.timeoutSeconds | int | `1` | Timeout seconds for readinessProbe |
| workload-launcher.replicaCount | int | `1` | Number of workload launcher replicas |
| workload-launcher.resources.limits | object | `{}` | The resources limits for the workload launcher container |
| workload-launcher.resources.requests | object | `{}` | The requested resources for the workload launcher container |
| workload-launcher.tolerations | list | `[]` | Tolerations for workload launcher pod assignment, see https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/ |

