# connector-rollout-worker

![Version: 0.67.17](https://img.shields.io/badge/Version-0.67.17-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: dev](https://img.shields.io/badge/AppVersion-dev-informational?style=flat-square)

Helm chart to deploy airbyte-connector-rollout-worker

## Requirements

| Repository | Name | Version |
|------------|------|---------|
| https://charts.bitnami.com/bitnami | common | 1.x.x |

## Values

| Key                                               | Type | Default                              | Description |
|---------------------------------------------------|------|--------------------------------------|-------------|
| affinity                                          | object | `{}`                                 |  |
| containerSecurityContext                          | object | `{}`                                 |  |
| debug.enabled                                     | bool | `false`                              |  |
| debug.remoteDebugPort                             | int | `5005`                               |  |
| enabled                                           | bool | `true`                               |  |
| env_vars                                          | object | `{}`                                 |  |
| extraContainers                                   | list | `[]`                                 |  |
| extraEnv                                          | list | `[]`                                 |  |
| extraInitContainers                               | list | `[]`                                 |  |
| extraLabels                                       | object | `{}`                                 |  |
| extraSelectorLabels                               | object | `{}`                                 |  |
| extraVolumeMounts                                 | list | `[]`                                 |  |
| extraVolumes                                      | list | `[]`                                 |  |
| global.credVolumeOverride                         | string | `""`                                 |  |
| global.database.secretName                        | string | `""`                                 |  |
| global.database.secretValue                       | string | `""`                                 |  |
| global.extraContainers                            | list | `[]`                                 |  |
| global.extraLabels                                | object | `{}`                                 |  |
| global.extraSelectorLabels                        | object | `{}`                                 |  |
| global.secretName                                 | string | `""`                                 |  |
| global.serviceAccountName                         | string | `"airbyte-admin"`                    |  |
| image.pullPolicy                                  | string | `"IfNotPresent"`                     |  |
| image.repository                                  | string | `"airbyte/connector-rollout-worker"` |  |
| livenessProbe.enabled                             | bool | `true`                               |  |
| livenessProbe.failureThreshold                    | int | `3`                                  |  |
| livenessProbe.initialDelaySeconds                 | int | `50`                                 |  |
| livenessProbe.periodSeconds                       | int | `10`                                 |  |
| livenessProbe.successThreshold                    | int | `1`                                  |  |
| livenessProbe.timeoutSeconds                      | int | `1`                                  |  |
| log.level                                         | string | `"INFO"`                             |  |
| nodeSelector                                      | object | `{}`                                 |  |
| podAnnotations                                    | object | `{}`                                 |  |
| podLabels                                         | object | `{}`                                 |  |
| readinessProbe.enabled                            | bool | `true`                               |  |
| readinessProbe.failureThreshold                   | int | `3`                                  |  |
| readinessProbe.initialDelaySeconds                | int | `10`                                 |  |
| readinessProbe.periodSeconds                      | int | `10`                                 |  |
| readinessProbe.successThreshold                   | int | `1`                                  |  |
| readinessProbe.timeoutSeconds                     | int | `1`                                  |  |
| replicaCount                                      | int | `1`                                  |  |
| resources.limits                                  | object | `{}`                                 |  |
| resources.requests                                | object | `{}`                                 |  |
| secrets                                           | object | `{}`                                 |  |
| tolerations                                       | list | `[]`                                 |  |
