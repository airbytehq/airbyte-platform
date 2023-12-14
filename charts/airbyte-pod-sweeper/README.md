# pod-sweeper

![Version: 0.50.14](https://img.shields.io/badge/Version-0.50.14-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: dev](https://img.shields.io/badge/AppVersion-dev-informational?style=flat-square)

Helm chart to deploy airbyte-pod-sweeper

## Requirements

| Repository | Name | Version |
|------------|------|---------|
| https://charts.bitnami.com/bitnami | common | 1.x.x |

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` |  |
| containerSecurityContext | object | `{}` |  |
| enabled | bool | `true` |  |
| extraVolumeMounts | list | `[]` |  |
| extraVolumes | list | `[]` |  |
| global.database.secretName | string | `""` |  |
| global.database.secretValue | string | `""` |  |
| global.jobs.kube.annotations | object | `{}` |  |
| global.jobs.kube.main_container_image_pull_secret | string | `""` |  |
| global.jobs.kube.nodeSelector | object | `{}` |  |
| global.jobs.kube.tolerations | list | `[]` |  |
| global.jobs.resources.limits | object | `{}` |  |
| global.jobs.resources.requests | object | `{}` |  |
| global.logs.accessKey.existingSecret | string | `""` |  |
| global.logs.accessKey.existingSecretKey | string | `""` |  |
| global.logs.accessKey.password | string | `"minio"` |  |
| global.logs.externalMinio.enabled | bool | `false` |  |
| global.logs.externalMinio.host | string | `"localhost"` |  |
| global.logs.externalMinio.port | int | `9000` |  |
| global.logs.gcs.bucket | string | `""` |  |
| global.logs.gcs.credentials | string | `""` |  |
| global.logs.gcs.credentialsJson | string | `""` |  |
| global.logs.minio.enabled | bool | `true` |  |
| global.logs.s3.bucket | string | `"airbyte-dev-logs"` |  |
| global.logs.s3.bucketRegion | string | `""` |  |
| global.logs.s3.enabled | bool | `false` |  |
| global.logs.secretKey.existingSecret | string | `""` |  |
| global.logs.secretKey.existingSecretKey | string | `""` |  |
| global.logs.secretKey.password | string | `"minio123"` |  |
| global.secretName | string | `""` |  |
| global.serviceAccountName | string | `"airbyte-admin"` |  |
| image.pullPolicy | string | `"IfNotPresent"` |  |
| image.repository | string | `"bitnami/kubectl"` |  |
| image.tag | string | `"latest"` |  |
| livenessProbe.enabled | bool | `true` |  |
| livenessProbe.failureThreshold | int | `3` |  |
| livenessProbe.initialDelaySeconds | int | `5` |  |
| livenessProbe.periodSeconds | int | `30` |  |
| livenessProbe.successThreshold | int | `1` |  |
| livenessProbe.timeoutSeconds | int | `1` |  |
| namespace | string | `""` |  |
| nodeSelector | object | `{}` |  |
| podAnnotations | object | `{}` |  |
| podLabels | object | `{}` |  |
| readinessProbe.enabled | bool | `true` |  |
| readinessProbe.failureThreshold | int | `3` |  |
| readinessProbe.initialDelaySeconds | int | `5` |  |
| readinessProbe.periodSeconds | int | `30` |  |
| readinessProbe.successThreshold | int | `1` |  |
| readinessProbe.timeoutSeconds | int | `1` |  |
| replicaCount | int | `1` |  |
| resources.limits | object | `{}` |  |
| resources.requests | object | `{}` |  |
| timeToDeletePods.running | string | `""` |  |
| timeToDeletePods.succeeded | int | `120` |  |
| timeToDeletePods.unsuccessful | int | `1440` |  |
| tolerations | list | `[]` |  |

