
{{/* DO NOT EDIT: This file was autogenerated. */}}

{{/*
    Storage Configuration
*/}}

{{/*
Renders the storage secret name
*/}}
{{- define "airbyte.storage.secretName" }}
{{- if .Values.global.storage.secretName }}
    {{- .Values.global.storage.secretName }}
{{- else }}
    {{- .Values.global.secretName | default (printf "%s-airbyte-secrets" .Release.Name) }}
{{- end }}
{{- end }}

{{/*
Renders the global.storage.type value
*/}}
{{- define "airbyte.storage.type" }}
    {{- .Values.global.storage.type | default "minio" }}
{{- end }}

{{/*
Renders the storage.type environment variable
*/}}
{{- define "airbyte.storage.type.env" }}
- name: STORAGE_TYPE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: STORAGE_TYPE
{{- end }}

{{/*
Renders the global.storage.bucket.activityPayload value
*/}}
{{- define "airbyte.storage.bucket.activityPayload" }}
    {{- .Values.global.storage.bucket.activityPayload | default "airbyte-storage" }}
{{- end }}

{{/*
Renders the storage.bucket.activityPayload environment variable
*/}}
{{- define "airbyte.storage.bucket.activityPayload.env" }}
- name: STORAGE_BUCKET_ACTIVITY_PAYLOAD
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: STORAGE_BUCKET_ACTIVITY_PAYLOAD
{{- end }}

{{/*
Renders the global.storage.bucket.auditLogging value
*/}}
{{- define "airbyte.storage.bucket.auditLogging" }}
    {{- .Values.global.storage.bucket.auditLogging | default "airbyte-storage" }}
{{- end }}

{{/*
Renders the storage.bucket.auditLogging environment variable
*/}}
{{- define "airbyte.storage.bucket.auditLogging.env" }}
- name: STORAGE_BUCKET_AUDIT_LOGGING
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: STORAGE_BUCKET_AUDIT_LOGGING
{{- end }}

{{/*
Renders the global.storage.bucket.log value
*/}}
{{- define "airbyte.storage.bucket.log" }}
    {{- .Values.global.storage.bucket.log | default "airbyte-storage" }}
{{- end }}

{{/*
Renders the storage.bucket.log environment variable
*/}}
{{- define "airbyte.storage.bucket.log.env" }}
- name: STORAGE_BUCKET_LOG
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: STORAGE_BUCKET_LOG
{{- end }}

{{/*
Renders the global.storage.bucket.state value
*/}}
{{- define "airbyte.storage.bucket.state" }}
    {{- .Values.global.storage.bucket.state | default "airbyte-storage" }}
{{- end }}

{{/*
Renders the storage.bucket.state environment variable
*/}}
{{- define "airbyte.storage.bucket.state.env" }}
- name: STORAGE_BUCKET_STATE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: STORAGE_BUCKET_STATE
{{- end }}

{{/*
Renders the global.storage.bucket.workloadOutput value
*/}}
{{- define "airbyte.storage.bucket.workloadOutput" }}
    {{- .Values.global.storage.bucket.workloadOutput | default "airbyte-storage" }}
{{- end }}

{{/*
Renders the storage.bucket.workloadOutput environment variable
*/}}
{{- define "airbyte.storage.bucket.workloadOutput.env" }}
- name: STORAGE_BUCKET_WORKLOAD_OUTPUT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: STORAGE_BUCKET_WORKLOAD_OUTPUT
{{- end }}

{{/*
Renders the global.storage.s3.region value
*/}}
{{- define "airbyte.storage.s3.region" }}
    {{- .Values.global.storage.s3.region }}
{{- end }}

{{/*
Renders the storage.s3.region environment variable
*/}}
{{- define "airbyte.storage.s3.region.env" }}
- name: AWS_DEFAULT_REGION
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: AWS_DEFAULT_REGION
{{- end }}

{{/*
Renders the global.storage.s3.authenticationType value
*/}}
{{- define "airbyte.storage.s3.authenticationType" }}
    {{- .Values.global.storage.s3.authenticationType | default "credentials" }}
{{- end }}

{{/*
Renders the storage.s3.authenticationType environment variable
*/}}
{{- define "airbyte.storage.s3.authenticationType.env" }}
- name: AWS_AUTHENTICATION_TYPE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: AWS_AUTHENTICATION_TYPE
{{- end }}

{{/*
Renders the global.storage.s3.accessKeyId value
*/}}
{{- define "airbyte.storage.s3.accessKeyId" }}
    {{- .Values.global.storage.s3.accessKeyId }}
{{- end }}

{{/*
Renders the storage.s3.accessKeyId secret key
*/}}
{{- define "airbyte.storage.s3.accessKeyId.secretKey" }}
	{{- .Values.global.storage.s3.accessKeyIdSecretKey | default "AWS_ACCESS_KEY_ID" }}
{{- end }}

{{/*
Renders the storage.s3.accessKeyId environment variable
*/}}
{{- define "airbyte.storage.s3.accessKeyId.env" }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.storage.secretName" . }}
      key: {{ include "airbyte.storage.s3.accessKeyId.secretKey" . }}
{{- end }}

{{/*
Renders the global.storage.s3.secretAccessKey value
*/}}
{{- define "airbyte.storage.s3.secretAccessKey" }}
    {{- .Values.global.storage.s3.secretAccessKey }}
{{- end }}

{{/*
Renders the storage.s3.secretAccessKey secret key
*/}}
{{- define "airbyte.storage.s3.secretAccessKey.secretKey" }}
	{{- .Values.global.storage.s3.secretAccessKeySecretKey | default "AWS_SECRET_ACCESS_KEY" }}
{{- end }}

{{/*
Renders the storage.s3.secretAccessKey environment variable
*/}}
{{- define "airbyte.storage.s3.secretAccessKey.env" }}
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.storage.secretName" . }}
      key: {{ include "airbyte.storage.s3.secretAccessKey.secretKey" . }}
{{- end }}

{{/*
Renders the global.storage.azure.connectionString value
*/}}
{{- define "airbyte.storage.azure.connectionString" }}
    {{- .Values.global.storage.azure.connectionString }}
{{- end }}

{{/*
Renders the storage.azure.connectionString secret key
*/}}
{{- define "airbyte.storage.azure.connectionString.secretKey" }}
	{{- .Values.global.storage.azure.connectionStringSecretKey | default "AZURE_STORAGE_CONNECTION_STRING" }}
{{- end }}

{{/*
Renders the storage.azure.connectionString environment variable
*/}}
{{- define "airbyte.storage.azure.connectionString.env" }}
- name: AZURE_STORAGE_CONNECTION_STRING
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.storage.secretName" . }}
      key: {{ include "airbyte.storage.azure.connectionString.secretKey" . }}
{{- end }}

{{/*
Renders the global.storage.gcs.credentialsJson value
*/}}
{{- define "airbyte.storage.gcs.credentialsJson" }}
    {{- .Values.global.storage.gcs.credentialsJson }}
{{- end }}

{{/*
Renders the storage.gcs.credentialsJson secret key
*/}}
{{- define "airbyte.storage.gcs.credentialsJson.secretKey" }}
	{{- .Values.global.storage.gcs.credentialsJsonSecretKey | default "GOOGLE_APPLICATION_CREDENTIALS_JSON" }}
{{- end }}

{{/*
Renders the storage.gcs.credentialsJson environment variable
*/}}
{{- define "airbyte.storage.gcs.credentialsJson.env" }}
- name: GOOGLE_APPLICATION_CREDENTIALS_JSON
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.storage.secretName" . }}
      key: {{ include "airbyte.storage.gcs.credentialsJson.secretKey" . }}
{{- end }}

{{/*
Renders the global.storage.gcs.credentialsJsonPath value
*/}}
{{- define "airbyte.storage.gcs.credentialsJsonPath" }}
    {{- .Values.global.storage.gcs.credentialsJsonPath | default "/secrets/gcp-creds/gcp.json" }}
{{- end }}

{{/*
Renders the storage.gcs.credentialsJsonPath environment variable
*/}}
{{- define "airbyte.storage.gcs.credentialsJsonPath.env" }}
- name: GOOGLE_APPLICATION_CREDENTIALS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: GOOGLE_APPLICATION_CREDENTIALS
{{- end }}

{{/*
Renders the global.storage.minio.accessKeyId value
*/}}
{{- define "airbyte.storage.minio.accessKeyId" }}
    {{- .Values.global.storage.minio.accessKeyId | default "minio" }}
{{- end }}

{{/*
Renders the storage.minio.accessKeyId secret key
*/}}
{{- define "airbyte.storage.minio.accessKeyId.secretKey" }}
	{{- .Values.global.storage.minio.accessKeyIdSecretKey | default "AWS_ACCESS_KEY_ID" }}
{{- end }}

{{/*
Renders the storage.minio.accessKeyId environment variable
*/}}
{{- define "airbyte.storage.minio.accessKeyId.env" }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.storage.secretName" . }}
      key: {{ include "airbyte.storage.minio.accessKeyId.secretKey" . }}
{{- end }}

{{/*
Renders the global.storage.minio.secretAccessKey value
*/}}
{{- define "airbyte.storage.minio.secretAccessKey" }}
    {{- .Values.global.storage.minio.secretAccessKey | default "minio123" }}
{{- end }}

{{/*
Renders the storage.minio.secretAccessKey secret key
*/}}
{{- define "airbyte.storage.minio.secretAccessKey.secretKey" }}
	{{- .Values.global.storage.minio.secretAccessKeySecretKey | default "AWS_SECRET_ACCESS_KEY" }}
{{- end }}

{{/*
Renders the storage.minio.secretAccessKey environment variable
*/}}
{{- define "airbyte.storage.minio.secretAccessKey.env" }}
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.storage.secretName" . }}
      key: {{ include "airbyte.storage.minio.secretAccessKey.secretKey" . }}
{{- end }}

{{/*
Renders the global.storage.minio.endpoint value
*/}}
{{- define "airbyte.storage.minio.endpoint" }}
    {{- .Values.global.storage.minio.endpoint | default (printf "http://airbyte-minio-svc.%s:9000" .Release.Namespace) }}
{{- end }}

{{/*
Renders the storage.minio.endpoint environment variable
*/}}
{{- define "airbyte.storage.minio.endpoint.env" }}
- name: MINIO_ENDPOINT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: MINIO_ENDPOINT
{{- end }}

{{/*
Renders the global.storage.minio.s3PathStyleAccess value
*/}}
{{- define "airbyte.storage.minio.s3PathStyleAccess" }}
	{{- if eq .Values.global.storage.minio.s3PathStyleAccess nil }}
    	{{- true }}
	{{- else }}
    	{{- .Values.global.storage.minio.s3PathStyleAccess }}
	{{- end }}
{{- end }}

{{/*
Renders the storage.minio.s3PathStyleAccess environment variable
*/}}
{{- define "airbyte.storage.minio.s3PathStyleAccess.env" }}
- name: S3_PATH_STYLE_ACCESS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: S3_PATH_STYLE_ACCESS
{{- end }}

{{/*
Renders the set of all storage environment variables
*/}}
{{- define "airbyte.storage.envs" }}
{{- include "airbyte.storage.type.env" . }}
{{- include "airbyte.storage.bucket.activityPayload.env" . }}
{{- include "airbyte.storage.bucket.auditLogging.env" . }}
{{- include "airbyte.storage.bucket.log.env" . }}
{{- include "airbyte.storage.bucket.state.env" . }}
{{- include "airbyte.storage.bucket.workloadOutput.env" . }}
{{- $opt := (include "airbyte.storage.type" .) }}

{{- if eq $opt "azure" }}
{{- include "airbyte.storage.azure.connectionString.env" . }}
{{- end }}

{{- if eq $opt "gcs" }}
{{- include "airbyte.storage.gcs.credentialsJson.env" . }}
{{- include "airbyte.storage.gcs.credentialsJsonPath.env" . }}
{{- end }}

{{- if eq $opt "minio" }}
{{- include "airbyte.storage.minio.accessKeyId.env" . }}
{{- include "airbyte.storage.minio.secretAccessKey.env" . }}
{{- include "airbyte.storage.minio.endpoint.env" . }}
{{- include "airbyte.storage.minio.s3PathStyleAccess.env" . }}
{{- end }}

{{- if eq $opt "s3" }}
{{- include "airbyte.storage.s3.region.env" . }}
{{- include "airbyte.storage.s3.authenticationType.env" . }}
{{- include "airbyte.storage.s3.accessKeyId.env" . }}
{{- include "airbyte.storage.s3.secretAccessKey.env" . }}
{{- end }}

{{- if eq $opt "local" }}
{{- end }}

{{- end }}

{{/*
Renders the set of all storage config map variables
*/}}
{{- define "airbyte.storage.configVars" }}
STORAGE_TYPE: {{ include "airbyte.storage.type" . | quote }}
STORAGE_BUCKET_ACTIVITY_PAYLOAD: {{ include "airbyte.storage.bucket.activityPayload" . | quote }}
STORAGE_BUCKET_AUDIT_LOGGING: {{ include "airbyte.storage.bucket.auditLogging" . | quote }}
STORAGE_BUCKET_LOG: {{ include "airbyte.storage.bucket.log" . | quote }}
STORAGE_BUCKET_STATE: {{ include "airbyte.storage.bucket.state" . | quote }}
STORAGE_BUCKET_WORKLOAD_OUTPUT: {{ include "airbyte.storage.bucket.workloadOutput" . | quote }}
{{- $opt := (include "airbyte.storage.type" .) }}

{{- if eq $opt "azure" }}
{{- end }}

{{- if eq $opt "gcs" }}
GOOGLE_APPLICATION_CREDENTIALS: {{ include "airbyte.storage.gcs.credentialsJsonPath" . | quote }}
{{- end }}

{{- if eq $opt "minio" }}
MINIO_ENDPOINT: {{ include "airbyte.storage.minio.endpoint" . | quote }}
S3_PATH_STYLE_ACCESS: {{ include "airbyte.storage.minio.s3PathStyleAccess" . | quote }}
{{- end }}

{{- if eq $opt "s3" }}
AWS_DEFAULT_REGION: {{ include "airbyte.storage.s3.region" . | quote }}
AWS_AUTHENTICATION_TYPE: {{ include "airbyte.storage.s3.authenticationType" . | quote }}
{{- end }}

{{- if eq $opt "local" }}
{{- end }}

{{- end }}

{{/*
Renders the set of all storage secret variables
*/}}
{{- define "airbyte.storage.secrets" }}
{{- $opt := (include "airbyte.storage.type" .) }}

{{- if eq $opt "azure" }}
AZURE_STORAGE_CONNECTION_STRING: {{ include "airbyte.storage.azure.connectionString" . | quote }}
{{- end }}

{{- if eq $opt "gcs" }}
GOOGLE_APPLICATION_CREDENTIALS_JSON: {{ include "airbyte.storage.gcs.credentialsJson" . | quote }}
{{- end }}

{{- if eq $opt "minio" }}
AWS_ACCESS_KEY_ID: {{ include "airbyte.storage.minio.accessKeyId" . | quote }}
AWS_SECRET_ACCESS_KEY: {{ include "airbyte.storage.minio.secretAccessKey" . | quote }}
{{- end }}

{{- if eq $opt "s3" }}
AWS_ACCESS_KEY_ID: {{ include "airbyte.storage.s3.accessKeyId" . | quote }}
AWS_SECRET_ACCESS_KEY: {{ include "airbyte.storage.s3.secretAccessKey" . | quote }}
{{- end }}

{{- if eq $opt "local" }}
{{- end }}

{{- end }}
