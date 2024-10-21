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
Common DB labels
*/}}
{{- define "airbyte.databaseLabels" -}}
helm.sh/chart: {{ include "airbyte.chart" . }}
{{ include "airbyte.databaseSelectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector DB labels
*/}}
{{- define "airbyte.databaseSelectorLabels" -}}
app.kubernetes.io/name: {{ printf "%s-db" .Release.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Common DB labels
*/}}
{{- define "airbyte.minioLabels" -}}
helm.sh/chart: {{ include "airbyte.chart" . }}
{{ include "airbyte.minioSelectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector DB labels
*/}}
{{- define "airbyte.minioSelectorLabels" -}}
app.kubernetes.io/name: {{ printf "%s-minio" .Release.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "airbyte.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "airbyte.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Add environment variables to configure minio
*/}}
{{- define "airbyte.storage.minio.endpoint" -}}
{{- if ((((.Values.global).storage).minio).endpoint) }}
    {{- .Values.global.storage.minio.endpoint -}}
{{- else -}}
    {{- printf "http://airbyte-minio-svc:9000" -}}
{{- end -}}
{{- end -}}

{{- define "airbyte.s3PathStyleAccess" -}}
{{- ternary "true" "" (eq (lower (default "" .Values.global.storage.type)) "minio") -}}
{{- end -}}

{{/*
Returns the GCP credentials path
*/}}
{{- define "airbyte.gcpLogCredentialsPath" -}}
{{- if ((((.Values.global).storage).gcs).credentialsPath) }}
    {{- printf "%s" .Values.global.storage.gcs.credentialsPath -}}
{{- else -}}
    {{- printf "%s" "/secrets/gcs-log-creds/gcp.json" -}}
{{- end -}}
{{- end -}}

{{/*
Construct comma separated list of key/value pairs from object (useful for ENV var values)
*/}}
{{- define "airbyte.flattenMap" -}}
{{- $kvList := list -}}
{{- range $key, $value := . -}}
{{- $kvList = printf "%s=%s" $key $value | mustAppend $kvList -}}
{{- end -}}
{{ join "," $kvList }}
{{- end -}}

{{/*
Construct semi-colon delimited list of comma separated key/value pairs from array of objects (useful for ENV var values)
*/}}
{{- define "airbyte.flattenArrayMap" -}}
{{- $mapList := list -}}
{{- range $element := . -}}
{{- $mapList = include "airbyte.flattenMap" $element | mustAppend $mapList -}}
{{- end -}}
{{ join ";" $mapList }}
{{- end -}}

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
{{- if  $s3AccessKeyIdSecretKey -}}
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

{{/*
Convert tags to a comma-separated list of key=value pairs.
*/}}
{{- define "airbyte.tagsToString" -}}
{{- $result := list -}}
{{- range . -}}
  {{- $key := .key -}}
  {{- $value := .value -}}
  {{- if eq (typeOf $value) "bool" -}}
    {{- $value = ternary "true" "false" $value -}}
  {{- end -}}
  {{- $result = append $result (printf "%s=%s" $key $value) -}}
{{- end -}}
{{- join "," $result -}}
{{- end -}}
