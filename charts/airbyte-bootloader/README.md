# airbyte-bootloader

![Version: 0.67.17](https://img.shields.io/badge/Version-0.67.17-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: dev](https://img.shields.io/badge/AppVersion-dev-informational?style=flat-square)

Helm chart to deploy airbyte-bootloader

## Requirements

| Repository | Name | Version |
|------------|------|---------|
| https://charts.bitnami.com/bitnami | common | 1.x.x |

## Values

| Key                         | Type | Default                       | Description |
|-----------------------------|------|-------------------------------|-------------|
| affinity                    | object | `{}`                          |  |
| containerSecurityContext    | object | `{}`                          |  |
| enabled                     | bool | `true`                        |  |
| env_vars                    | object | `{}`                          |  |
| extraContainers             | list | `[]`                          |  |
| extraEnv                    | list | `[]`                          |  |
| extraInitContainers         | list | `[]`                          |  |
| extraLabels                 | object | `{}`                          |  |
| extraSelectorLabels         | object | `{}`                          |  |
| extraVolumeMounts           | list | `[]`                          |  |
| extraVolumes                | list | `[]`                          |  |
| global.database.secretName  | string | `""`                          |  |
| global.database.secretValue | string | `""`                          |  |
| global.edition       | string | `"community"`                 |  |
| global.env_vars             | object | `{}`                          |  |
| global.extraContainers      | list | `[]`                          |  |
| global.extraLabels          | object | `{}`                          |  |
| global.extraSelectorLabels  | object | `{}`                          |  |
| global.secretName           | string | `""`                          |  |
| global.secrets              | object | `{}`                          |  |
| global.serviceAccountName   | string | `"placeholderServiceAccount"` |  |
| image.pullPolicy            | string | `"IfNotPresent"`              |  |
| image.repository            | string | `"airbyte/bootloader"`        |  |
| nodeSelector                | object | `{}`                          |  |
| podAnnotations              | object | `{}`                          |  |
| podLabels                   | object | `{}`                          |  |
| resources.limits            | object | `{}`                          |  |
| resources.requests          | object | `{}`                          |  |
| secrets                     | object | `{}`                          |  |
| tolerations                 | list | `[]`                          |  |

