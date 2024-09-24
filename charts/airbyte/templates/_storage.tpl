{{/*
Storage configs
*/}}

{{/*
Returns the provider name
*/}}
{{- define "airbyte.storage.provider" }}
{{- if .Values.global.storage.type }}
  {{- printf "%s" (lower .Values.global.storage.type) }}
{{- else }}
  {{- printf "minio" }}
{{- end }}
{{- end }}

{{/*
Returns the storage provider secret name
*/}}
{{- define "airbyte.storage.secretName" }}
{{- if .Values.global.storage.secretName }}
  {{- printf "%s" .Values.global.storage.secretName }}
{{- else if .Values.global.storage.storageSecretName }}
  {{/*
  NOTE: `storageSecretName` is the legacy name of this key, but we want to standardize on all configs
  providing the name of the secret as `secretName`, under the respective section of `values.yaml`.
  We continue to support this here for backwards compatibility.
  */}}
  {{- printf "%s" .Values.global.storage.storageSecretName }}
{{- else -}}
  {{/* GCS has its own default secret we create */}}
  {{- if eq (include "airbyte.storage.provider" .) "gcs" }}
    {{- printf "%s-gcs-log-creds" .Release.Name }}
  {{- else }}
    {{- printf "%s-airbyte-secrets" .Release.Name }}
  {{- end }}
{{- end }}
{{- end }}

{{/*
Returns azure environment variables.
*/}}
{{- define "airbyte.storage.azure.envs" }}
{{- if .Values.global.storage.azure.connectionString }}
- name: AZURE_STORAGE_CONNECTION_STRING
  value: {{ .Values.global.storage.azure.connectionString }}
{{- end }}
{{- if .Values.global.storage.azure.connectionStringSecretKey }}
- name: AZURE_STORAGE_CONNECTION_STRING
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.storage.secretName" . }}
      key: {{ .Values.global.storage.azure.connectionStringSecretKey }}
{{- end }}
{{- end }}

{{/*
Returns azure secrets
*/}}
{{- define "airbyte.storage.azure.secrets" }}
{{- if .Values.global.storage.azure }}
AZURE_STORAGE_CONNECTION_STRING: {{ .Values.global.storage.azure.connectionString | default "" }}
{{- end }}
{{- end }}

{{/*
Returns S3 environment variables.
*/}}
{{- define "airbyte.storage.s3.envs" }}
{{- if eq .Values.global.storage.s3.authenticationType "credentials" }}
- name: AWS_ACCESS_KEY_ID 
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.storage.secretName" . }}
      key: {{ .Values.global.storage.s3.accessKeyIdSecretKey | default "s3-access-key-id" }}
- name: AWS_SECRET_ACCESS_KEY 
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.storage.secretName" . }}
      key: {{ .Values.global.storage.s3.secretAccessKeySecretKey | default "s3-secret-access-key" }}
{{- end }}
{{- if .Values.global.storage.s3.region }}
- name: AWS_DEFAULT_REGION 
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: AWS_DEFAULT_REGION
{{- end }}
{{- end}}

{{/*
Returns S3 secrets
*/}}
{{- define "airbyte.storage.s3.secrets" }}
{{- if and (.Values.global.storage.s3) (eq .Values.global.storage.s3.authenticationType "credentials") }}
AWS_ACCESS_KEY_ID: {{ .Values.global.storage.s3.accessKeyId | default "" }}
AWS_SECRET_ACCESS_KEY: {{ .Values.global.storage.s3.secretAccessKey | default "" }}
{{- end }}
{{- end }}

{{/*
Returns GCS environment variables.
*/}}
{{- define "airbyte.storage.gcs.envs" }}
- name: GOOGLE_APPLICATION_CREDENTIALS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: GOOGLE_APPLICATION_CREDENTIALS 
{{- end}}

{{/*
Returns GCS secrets.
*/}}
{{- define "airbyte.storage.gcs.secrets" }}
{{- if .Values.global.storage.gcs }}
gcp.json: {{ .Values.global.storage.gcs.credentialsJson }}
{{- end }}
{{- end}}

{{/*
Returns Minio environment variables.
*/}}
{{- define "airbyte.storage.minio.envs" }}
{{- if .Values.global.storage.minio }}
- name: AWS_ACCESS_KEY_ID 
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.storage.secretName" . }}
      key: {{ .Values.global.storage.minio.accessKeyIdSecretKey | default "MINIO_ACCESS_KEY_ID" }}
- name: AWS_SECRET_ACCESS_KEY 
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.storage.secretName" . }}
      key: {{ .Values.global.storage.minio.secretAccessKeySecretKey | default "MINIO_SECRET_ACCESS_KEY" }}
{{- else }}
- name: AWS_ACCESS_KEY_ID 
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.storage.secretName" . }}
      key: "MINIO_ACCESS_KEY_ID" 
- name: AWS_SECRET_ACCESS_KEY 
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.storage.secretName" . }}
      key: "MINIO_SECRET_ACCESS_KEY" 
{{- end }}
- name: MINIO_ENDPOINT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: MINIO_ENDPOINT
{{- end }}

{{- define "airbyte.storage.minio.secrets" }}
{{- if .Values.global.storage.minio }}
MINIO_ACCESS_KEY_ID: {{ .Values.global.storage.minio.accessKeyId | default "minio" | quote }}
MINIO_SECRET_ACCESS_KEY: {{ .Values.global.storage.minio.secretAccessKey | default "minio123" | quote }}
{{- else }}
MINIO_ACCESS_KEY_ID: "minio"
MINIO_SECRET_ACCESS_KEY: "minio123"
{{- end }}
{{- end }}

{{/*
Returns storage config environment variables.
*/}}
{{- define "airbyte.storage.envs" }}
{{- $storageProvider := (include "airbyte.storage.provider" .) }}
- name: S3_PATH_STYLE_ACCESS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: S3_PATH_STYLE_ACCESS
- name: STORAGE_TYPE
  value: {{ upper $storageProvider }}
- name: STORAGE_BUCKET_ACTIVITY_PAYLOAD
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: STORAGE_BUCKET_ACTIVITY_PAYLOAD
- name: STORAGE_BUCKET_LOG
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: STORAGE_BUCKET_LOG
- name: STORAGE_BUCKET_STATE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: STORAGE_BUCKET_STATE
- name: STORAGE_BUCKET_WORKLOAD_OUTPUT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: STORAGE_BUCKET_WORKLOAD_OUTPUT
{{/* S3 */}}
{{- if eq $storageProvider "s3" }}
{{- include "airbyte.storage.s3.envs" . }}
{{- end }}

{{/* GCS */}}
{{- if eq $storageProvider "gcs" }}
{{- include "airbyte.storage.gcs.envs" . }}
{{- end }}

{{/* MINIO */}}
{{- if eq $storageProvider "minio" }}
{{- include "airbyte.storage.minio.envs" . }}
{{- end }}

{{/* AZURE */}}
{{- if eq $storageProvider "azure" }}
{{- include "airbyte.storage.azure.envs" . }}
{{- end }}

{{/* LOCAl */}}
{{- if eq $storageProvider "local" }}
- name: LOCAL_ROOT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: LOCAL_ROOT
{{- end }}
{{- end }}

{{/*
Returns storage config variables.
*/}}
{{- define "airbyte.storage.configVars"  }}
{{- end }}

{{/*
Returns storage config secrets.
*/}}
{{- define "airbyte.storage.secrets"  }}
{{- $storageProvider := (include "airbyte.storage.provider" .) }}
{{/* S3 */}}
{{- if eq $storageProvider "s3" }}
{{- include "airbyte.storage.s3.secrets" . }}
{{- end }}

{{/* GCS */}}
{{- if eq $storageProvider "gcs" }}
{{- include "airbyte.storage.gcs.secrets" . }}
{{- end }}

{{/* MINIO */}}
{{- if eq $storageProvider "minio" }}
{{- include "airbyte.storage.minio.secrets" . }}
{{- end }}

{{/* AZURE */}}
{{- if eq $storageProvider "azure" }}
{{- include "airbyte.storage.azure.secrets" . }}
{{- end }}
{{- end }}
