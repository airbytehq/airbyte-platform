{{/*
Expand the name of the chart.
*/}}
{{- define "airbyte.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "airbyte.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Renders a value that contains template perhaps with scope if the scope is present.
Usage:
{{ include "airbyte.tplvalues.render" ( dict "value" .Values.path.to.the.Value "context" $ ) }}
{{ include "airbyte.tplvalues.render" ( dict "value" .Values.path.to.the.Value "context" $ "scope" $app ) }}
*/}}
{{- define "airbyte.tplvalues.render" -}}
{{- $value := typeIs "string" .value | ternary .value (.value | toYaml) }}
{{- if contains "{{" (toJson .value) }}
  {{- if .scope }}
      {{- tpl (cat "{{- with $.RelativeScope -}}" $value "{{- end }}") (merge (dict "RelativeScope" .scope) .context) }}
  {{- else }}
    {{- tpl $value .context }}
  {{- end }}
{{- else }}
    {{- $value }}
{{- end }}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "airbyte.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "airbyte.labels" -}}
helm.sh/chart: {{ include "airbyte.chart" . }}
{{ include "airbyte.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "airbyte.selectorLabels" -}}
app.kubernetes.io/name: {{ include "airbyte.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Define db secret
*/}}

{{- define "database.secret.name" -}}
{{- printf "%s-postgresql" .Release.Name }}
{{- end }}

## DEFAULT HELM VALUES
# Secret Manager Defaults
{{/*
Define secret persistence
*/}}
{{- define "airbyte.secretPersistence" -}}
{{- if (((.Values.global).secretsManager).type) }}
    {{- printf "%s" (snakecase .Values.global.secretsManager.type) }}
{{- else }}
    {{- printf "" }}
{{- end }}
{{- end }}

{{/*
Get secret store name or default
*/}}
{{- define "airbyte.secretStoreName" -}}
{{- $secretStoreName := . -}}
{{- if $secretStoreName -}}
  {{- printf "%s" $secretStoreName -}}
{{- else -}}
  {{- printf "airbyte-config-secrets" -}}
{{- end -}}
{{- end -}}

{{/*
Get awsSecretManager access key id secret key or default
*/}}
{{- define "airbyte.awsSecretManagerAccessKeyIdSecretKey" -}}
{{- $awsSecretManagerAccessKeyIdSecretKey := . -}}
{{- if $awsSecretManagerAccessKeyIdSecretKey -}}
  {{- printf "%s" $awsSecretManagerAccessKeyIdSecretKey -}}
{{- else -}}
  {{- printf "aws-secret-manager-access-key-id" -}}
{{- end -}}
{{- end -}}

{{/*
Get awsSecretManager secret access key secret key or default
*/}}
{{- define "airbyte.awsSecretManagerSecretAccessKeySecretKey" -}}
{{- $awsSecretManagerSecretAccessKeySecretKey := . -}}
{{- if $awsSecretManagerSecretAccessKeySecretKey -}}
  {{- printf "%s" $awsSecretManagerSecretAccessKeySecretKey -}}
{{- else -}}
  {{- printf "aws-secret-manager-secret-access-key" -}}
{{- end -}}
{{- end -}}

{{/*
Get googleSecretManager credentials secret key or default
*/}}
{{- define "airbyte.googleSecretManagerCredentialsSecretKey" -}}
{{- $googleSecretManagerCredentialsSecretKey := . -}}
{{- if $googleSecretManagerCredentialsSecretKey -}}
  {{- printf "%s" $googleSecretManagerCredentialsSecretKey -}}
{{- else -}}
  {{- printf "google-secret-manager-credentials" -}}
{{- end -}}
{{- end -}}

{{/*
Get vault auth token secret key or default
*/}}
{{- define "airbyte.vaultAuthTokenSecretKey" -}}
{{- $vaultAuthTokenSecretKey := . -}}
{{- if $vaultAuthTokenSecretKey -}}
  {{- printf "%s" $vaultAuthTokenSecretKey -}}
{{- else -}}
  {{- printf "vault-auth-token" -}}
{{- end -}}
{{- end -}}


# Storage Defaults
{{/*
Get storage type or default
*/}}
{{- define "airbyte.storageType" -}}
{{- $storageType := . -}}
{{- if $storageType -}}
  {{- printf "%s" $storageType -}}
{{- else -}}
  {{- printf "local" -}}
{{- end -}}
{{- end -}}

{{/*
Get storage bucket log or default
*/}}
{{- define "airbyte.storageBucketLog" -}}
{{- $storageBucketLog := . -}}
{{- if $storageBucketLog -}}
  {{- printf "%s" $storageBucketLog -}}
{{- else -}}
  {{- printf "airbyte-storage" -}}
{{- end -}}
{{- end -}}

{{/*
Get storage bucket state or default
*/}}
{{- define "airbyte.storageBucketState" -}}
{{- $storageBucketState := . -}}
{{- if $storageBucketState -}}
  {{- printf "%s" $storageBucketState -}}
{{- else -}}
  {{- printf "airbyte-storage" -}}
{{- end -}}
{{- end -}}

{{/*
Get storage bucket workload output or default
*/}}
{{- define "airbyte.storageBucketWorkloadOutput" -}}
{{- $storageBucketWorkloadOutput := . -}}
{{- if $storageBucketWorkloadOutput -}}
  {{- printf "%s" $storageBucketWorkloadOutput -}}
{{- else -}}
  {{- printf "airbyte-storage" -}}
{{- end -}}
{{- end -}}

{{/*
Get s3 access key id secret key or default
*/}}
{{- define "airbyte.s3AccessKeyIdSecretKey" -}}
{{- $s3AccessKeyIdSecretKey := . -}}
{{- if $s3AccessKeyIdSecretKey -}}
  {{- printf "%s" $s3AccessKeyIdSecretKey -}}
{{- else -}}
  {{- printf "s3-access-key-id" -}}
{{- end -}}
{{- end -}}

{{/*
Get s3 secret access key secret key or default
*/}}
{{- define "airbyte.s3SecretAccessKeySecretKey" -}}
{{- $s3SecretAccessKeySecretKey := . -}}
{{- if $s3SecretAccessKeySecretKey -}}
  {{- printf "%s" $s3SecretAccessKeySecretKey -}}
{{- else -}}
  {{- printf "s3-secret-access-key" -}}
{{- end -}}
{{- end -}}

{{/*
Get minio access key id secret key or default
*/}}
{{- define "airbyte.minioAccessKeyIdSecretKey" -}}
{{- $minioAccessKeyIdSecretKey := . -}}
{{- if $minioAccessKeyIdSecretKey -}}
  {{- printf "%s" $minioAccessKeyIdSecretKey -}}
{{- else -}}
  {{- printf "minio-access-key-id" -}}
{{- end -}}
{{- end -}}

{{/*
Get minio secret access key secret key or default
*/}}
{{- define "airbyte.minioSecretAccessKeySecretKey" -}}
{{- $minioSecretAccessKeySecretKey := . -}}
{{- if $minioSecretAccessKeySecretKey -}}
  {{- printf "%s" $minioSecretAccessKeySecretKey -}}
{{- else -}}
  {{- printf "minio-secret-access-key" -}}
{{- end -}}
{{- end -}}

{{/*
Get gcs credentials secret key or default
*/}}
{{- define "airbyte.gcsCredentialsSecretKey" -}}
{{- $gcsCredentialsSecretKey := . -}}
{{- if $gcsCredentialsSecretKey -}}
  {{- printf "%s" $gcsCredentialsSecretKey -}}
{{- else -}}
  {{- printf "gcs-credentials" -}}
{{- end -}}
{{- end -}}
