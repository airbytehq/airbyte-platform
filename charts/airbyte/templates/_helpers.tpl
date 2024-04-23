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
Create a default fully qualified postgresql name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "airbyte.postgresql.fullname" -}}
{{- $name := default "postgresql" .Values.postgresql.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Get the Postgresql credentials secret name.
*/}}
{{- define "airbyte.database.secret.name" -}}
{{- if .Values.postgresql.enabled -}}
    {{ template "postgresql.secretName" .Subcharts.postgresql }}
{{- else }}
    {{- if .Values.externalDatabase.existingSecret -}}
        {{- printf "%s" .Values.externalDatabase.existingSecret -}}
    {{- else -}}
        {{ printf "%s-%s" (include "common.names.fullname" .) "secrets" }}
    {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Add environment variables to configure database values
*/}}
{{- define "airbyte.database.host" -}}
{{- ternary "airbyte-db-svc" .Values.externalDatabase.host .Values.postgresql.enabled -}}
{{- end -}}

{{/*
Add environment variables to configure database values
*/}}
{{- define "airbyte.database.user" -}}
{{- ternary .Values.postgresql.postgresqlUsername .Values.externalDatabase.user .Values.postgresql.enabled -}}
{{- end -}}

{{/*
Add environment variables to configure database values
*/}}
{{- define "airbyte.database.name" -}}
{{- ternary .Values.postgresql.postgresqlDatabase .Values.externalDatabase.database .Values.postgresql.enabled -}}
{{- end -}}

{{/*
Get the Postgresql credentials secret password key
*/}}
{{- define "airbyte.database.secret.passwordKey" -}}
{{- if .Values.postgresql.enabled -}}
    {{- printf "%s" "postgresql-password" -}}
{{- else -}}
    {{- if .Values.externalDatabase.existingSecret -}}
        {{- if .Values.externalDatabase.existingSecretPasswordKey -}}
            {{- printf "%s" .Values.externalDatabase.existingSecretPasswordKey -}}
        {{- else -}}
            {{- printf "%s" "postgresql-password" -}}
        {{- end -}}
    {{- else -}}
        {{- printf "%s" "DATABASE_PASSWORD" -}}
    {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Add environment variables to configure database values
*/}}
{{- define "airbyte.database.port" -}}
{{- ternary "5432" .Values.externalDatabase.port .Values.postgresql.enabled -}}
{{- end -}}

{{/*
Add environment variables to configure database values
*/}}
{{- define "airbyte.database.url" -}}
{{- if .Values.externalDatabase.jdbcUrl -}}
{{- .Values.externalDatabase.jdbcUrl -}}
{{- else -}}
{{- $host := (include "airbyte.database.host" .) -}}
{{- $dbName := (include "airbyte.database.name" .) -}}
{{- $port := (include "airbyte.database.port" . ) -}}
{{- printf "jdbc:postgresql://%s:%s/%s" $host $port $dbName -}}
{{- end -}}
{{- end -}}

{{/*
Create JDBC URL specifically for Keycloak with a custom schema
*/}}
{{- define "keycloak.database.url" -}}
{{- if .Values.externalDatabase.jdbcUrl -}}
{{- printf "%s?currentSchema=keycloak" .Values.externalDatabase.jdbcUrl -}}
{{- else -}}
{{- $baseURL := include "airbyte.database.url" . -}}
{{- printf "%s?currentSchema=keycloak" $baseURL -}}
{{- end -}}
{{- end -}}


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
Returns the Airbyte Scheduler Image
*/}}
{{- define "airbyte.schedulerImage" -}}
{{- include "common.images.image" (dict "imageRoot" .Values.scheduler.image "global" .Values.global) -}}
{{- end -}}

{{/*
Returns the Airbyte Server Image
*/}}
{{- define "airbyte.serverImage" -}}
{{- include "common.images.image" (dict "imageRoot" .Values.server.image "global" .Values.global) -}}
{{- end -}}

{{/*
Returns the Airbyte Webapp Image
*/}}
{{- define "airbyte.webappImage" -}}
{{- include "common.images.image" (dict "imageRoot" .Values.webapp.image "global" .Values.global) -}}
{{- end -}}

{{/*
Returns the Airbyte PodSweeper Image
*/}}
{{- define "airbyte.podSweeperImage" -}}
{{- include "common.images.image" (dict "imageRoot" .Values.podSweeper.image "global" .Values.global) -}}
{{- end -}}

{{/*
Returns the Airbyte Worker Image
*/}}
{{- define "airbyte.workerImage" -}}
{{- include "common.images.image" (dict "imageRoot" .Values.worker.image "global" .Values.global) -}}
{{- end -}}

{{/*
Returns the Airbyte Workload Launcher Image
*/}}
{{- define "airbyte.workloadLauncherImage" -}}
{{- include "common.images.image" (dict "imageRoot" .Values.worker.image "global" .Values.global) -}}
{{- end -}}

{{/*
Returns the Airbyte Bootloader Image
*/}}
{{- define "airbyte.bootloaderImage" -}}
{{- include "common.images.image" (dict "imageRoot" .Values.bootloader.image "global" .Values.global) -}}
{{- end -}}

{{/*
Returns the Temporal Image. TODO: This will probably be replaced if we move to using temporal as a dependency, like minio and postgres.
*/}}
{{- define "airbyte.temporalImage" -}}
{{- include "common.images.image" (dict "imageRoot" .Values.temporal.image "global" .Values.global) -}}
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

{{/*
Determine the correct log4j configuration file to load based on the defined storage type.
*/}}
{{- define "airbyte.log4jConfig" -}}
{{- if eq (lower (default "" .Values.global.storage.type)) "minio" }}
    {{- printf "log4j2-minio.xml" -}}
{{- else if eq (lower (default "" .Values.global.storage.type)) "gcs" -}}
    {{- printf "log4j2-gcs.xml" -}}
{{- else if eq (lower (default "" .Values.global.storage.type)) "s3" -}}
    {{- printf "log4j2-s3.xml" -}}
{{- else -}}
    {{- printf "log4j2.xml" -}}
{{- end -}}
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
  {{- $result = append $result (printf "%s=%s" .key .value) -}}
{{- end -}}
{{- join "," $result -}}
{{- end -}}

