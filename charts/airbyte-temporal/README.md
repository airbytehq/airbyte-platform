# temporal

![Version: 0.50.14](https://img.shields.io/badge/Version-0.50.14-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: dev](https://img.shields.io/badge/AppVersion-dev-informational?style=flat-square)

Helm chart to deploy airbyte-temporal

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
| env_vars | object | `{}` |  |
| extraContainers | list | `[]` |  |
| extraEnv | list | `[]` |  |
| extraInitContainers | list | `[]` |  |
| extraLabels | object | `{}` |  |
| extraSelectorLabels | object | `{}` |  |
| extraVolumeMounts | list | `[]` |  |
| extraVolumes | list | `[]` |  |
| global.configMapName | string | `""` |  |
| global.database.secretName | string | `""` |  |
| global.database.secretValue | string | `""` |  |
| global.deploymentMode | string | `"oss"` |  |
| global.extraContainers | list | `[]` |  |
| global.extraLabels | object | `{}` |  |
| global.extraSelectorLabels | object | `{}` |  |
| global.secretName | string | `""` |  |
| global.serviceAccountName | string | `"placeholderServiceAccount"` |  |
| image.pullPolicy | string | `"IfNotPresent"` |  |
| image.repository | string | `"temporalio/auto-setup"` |  |
| image.tag | string | `"1.20.1"` |  |
| livenessProbe.enabled | bool | `true` |  |
| livenessProbe.failureThreshold | int | `3` |  |
| livenessProbe.initialDelaySeconds | int | `5` |  |
| livenessProbe.periodSeconds | int | `30` |  |
| livenessProbe.successThreshold | int | `1` |  |
| livenessProbe.timeoutSeconds | int | `1` |  |
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
| secrets | object | `{}` |  |
| service.port | int | `7233` |  |
| service.type | string | `"ClusterIP"` |  |
| tolerations | list | `[]` |  |

