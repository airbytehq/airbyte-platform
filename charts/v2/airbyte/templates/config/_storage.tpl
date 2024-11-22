{{/*
Storage Configuration
*/}}

{{/*
Renders the global.storage secret name
*/}}
{{- define "airbyte.storage.secretName" }}
{{- if .Values.global.storage.secretName }}
  {{- .Values.global.storage.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}
{{/*
Renders the global.storage.type value
*/}}
{{- define "airbyte.storage.type" }}
{{- .Values.global.storage.type | default "minio" }}
{{- end }}

{{/*
Renders the STORAGE_TYPE environment variable
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
Renders the STORAGE_BUCKET_ACTIVITY_PAYLOAD environment variable
*/}}
{{- define "airbyte.storage.bucket.activityPayload.env" }}
- name: STORAGE_BUCKET_ACTIVITY_PAYLOAD
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: STORAGE_BUCKET_ACTIVITY_PAYLOAD
{{- end }}
{{/*
Renders the global.storage.bucket.log value
*/}}
{{- define "airbyte.storage.bucket.log" }}
{{- .Values.global.storage.bucket.log | default "airbyte-storage" }}
{{- end }}

{{/*
Renders the STORAGE_BUCKET_LOG environment variable
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
Renders the STORAGE_BUCKET_STATE environment variable
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
Renders the STORAGE_BUCKET_WORKLOAD_OUTPUT environment variable
*/}}
{{- define "airbyte.storage.bucket.workloadOutput.env" }}
- name: STORAGE_BUCKET_WORKLOAD_OUTPUT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: STORAGE_BUCKET_WORKLOAD_OUTPUT
{{- end }}

{{/*
Renders the global.storage.azure.connectionString value
*/}}
{{- define "airbyte.storage.azure.connectionString" }}
{{- .Values.global.storage.azure.connectionString  }}
{{- end }}

{{/*
Renders the AZURE_STORAGE_CONNECTION_STRING environment variable
*/}}
{{- define "airbyte.storage.azure.connectionString.env" }}
- name: AZURE_STORAGE_CONNECTION_STRING
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.storage.secretName" . }}
      key: {{ .Values.global.storage.azure.connectionStringSecretKey | default "AZURE_STORAGE_CONNECTION_STRING" }}
{{- end }}

{{/*
Renders the set of all storage.azure environment variables
*/}}
{{- define "airbyte.storage.azure.envs" }}
{{- include "airbyte.storage.azure.connectionString.env" . }}
{{- end }}

{{/*
Renders the set of all storage.azure secrets
*/}}
{{- define "airbyte.storage.azure.secrets" }}
{{- if not (empty (include "airbyte.storage.azure.connectionString" .)) }}
AZURE_STORAGE_CONNECTION_STRING: {{ include "airbyte.storage.azure.connectionString" . | quote }}
{{- end }}
{{- end }}

{{/*
Renders the global.storage.gcs.credentialsJson value
*/}}
{{- define "airbyte.storage.gcs.credentialsJson" }}
{{- .Values.global.storage.gcs.credentialsJson  }}
{{- end }}

{{/*
Renders the GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable
*/}}
{{- define "airbyte.storage.gcs.credentialsJson.env" }}
- name: GOOGLE_APPLICATION_CREDENTIALS_JSON
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.storage.secretName" . }}
      key: {{ .Values.global.storage.gcs.credentialsJsonSecretKey | default "GOOGLE_APPLICATION_CREDENTIALS_JSON" }}
{{- end }}

{{/*
Renders the global.storage.gcs.credentialsJsonPath value
*/}}
{{- define "airbyte.storage.gcs.credentialsJsonPath" }}
{{- .Values.global.storage.gcs.credentialsJsonPath  }}
{{- end }}

{{/*
Renders the GOOGLE_APPLICATION_CREDENTIALS environment variable
*/}}
{{- define "airbyte.storage.gcs.credentialsJsonPath.env" }}
- name: GOOGLE_APPLICATION_CREDENTIALS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: GOOGLE_APPLICATION_CREDENTIALS
{{- end }}

{{/*
Renders the global.storage.containerOrchestrator.secretMountPath value
*/}}
{{- define "airbyte.storage.containerOrchestrator.secretMountPath" }}
{{- "/secrets/gcs-log-creds" }}
{{- end }}

{{/*
Renders the CONTAINER_ORCHESTRATOR_SECRET_MOUNT_PATH environment variable
*/}}
{{- define "airbyte.storage.containerOrchestrator.secretMountPath.env" }}
- name: CONTAINER_ORCHESTRATOR_SECRET_MOUNT_PATH
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CONTAINER_ORCHESTRATOR_SECRET_MOUNT_PATH
{{- end }}

{{/*
Renders the set of all storage.gcs environment variables
*/}}
{{- define "airbyte.storage.gcs.envs" }}
{{- include "airbyte.storage.gcs.credentialsJson.env" . }}
{{- include "airbyte.storage.gcs.credentialsJsonPath.env" . }}
{{- include "airbyte.storage.containerOrchestrator.secretMountPath.env" . }}
{{- end }}

{{/*
Renders the set of all storage.gcs secrets
*/}}
{{- define "airbyte.storage.gcs.secrets" }}
{{- if not (empty (include "airbyte.storage.gcs.credentialsJson" .)) }}
GOOGLE_APPLICATION_CREDENTIALS_JSON: {{ include "airbyte.storage.gcs.credentialsJson" . | quote }}
{{- end }}
{{- end }}

{{/*
Renders the global.storage.minio.accessKeyId value
*/}}
{{- define "airbyte.storage.minio.accessKeyId" }}
{{- .Values.global.storage.minio.accessKeyId | default "minio" }}
{{- end }}

{{/*
Renders the AWS_ACCESS_KEY_ID environment variable
*/}}
{{- define "airbyte.storage.minio.accessKeyId.env" }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.storage.secretName" . }}
      key: {{ .Values.global.storage.minio.accessKeyIdSecretKey | default "AWS_ACCESS_KEY_ID" }}
{{- end }}

{{/*
Renders the global.storage.minio.secretAccessKey value
*/}}
{{- define "airbyte.storage.minio.secretAccessKey" }}
{{- .Values.global.storage.minio.secretAccessKey | default "minio123" }}
{{- end }}

{{/*
Renders the AWS_SECRET_ACCESS_KEY environment variable
*/}}
{{- define "airbyte.storage.minio.secretAccessKey.env" }}
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.storage.secretName" . }}
      key: {{ .Values.global.storage.minio.secretAccessKeySecretKey | default "AWS_SECRET_ACCESS_KEY" }}
{{- end }}

{{/*
Renders the global.storage.minio.endpoint value
*/}}
{{- define "airbyte.storage.minio.endpoint" }}
{{- .Values.global.storage.minio.endpoint | default "http://airbyte-minio-svc:9000" }}
{{- end }}

{{/*
Renders the MINIO_ENDPOINT environment variable
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
{{- .Values.global.storage.minio.s3PathStyleAccess | default true }}
{{- end }}

{{/*
Renders the S3_PATH_STYLE environment variable
*/}}
{{- define "airbyte.storage.minio.s3PathStyleAccess.env" }}
- name: S3_PATH_STYLE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: S3_PATH_STYLE
{{- end }}

{{/*
Renders the set of all storage.minio environment variables
*/}}
{{- define "airbyte.storage.minio.envs" }}
{{- include "airbyte.storage.minio.accessKeyId.env" . }}
{{- include "airbyte.storage.minio.secretAccessKey.env" . }}
{{- include "airbyte.storage.minio.endpoint.env" . }}
{{- include "airbyte.storage.minio.s3PathStyleAccess.env" . }}
{{- end }}

{{/*
Renders the set of all storage.minio secrets
*/}}
{{- define "airbyte.storage.minio.secrets" }}
{{- if not (empty (include "airbyte.storage.minio.accessKeyId" .)) }}
AWS_ACCESS_KEY_ID: {{ include "airbyte.storage.minio.accessKeyId" . | quote }}
{{- end }}
{{- if not (empty (include "airbyte.storage.minio.secretAccessKey" .)) }}
AWS_SECRET_ACCESS_KEY: {{ include "airbyte.storage.minio.secretAccessKey" . | quote }}
{{- end }}
{{- end }}

{{/*
Renders the global.storage.s3.region value
*/}}
{{- define "airbyte.storage.s3.region" }}
{{- .Values.global.storage.s3.region  }}
{{- end }}

{{/*
Renders the AWS_DEFAULT_REGION environment variable
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
Renders the AWS_AUTHENTICATION_TYPE environment variable
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
{{- .Values.global.storage.s3.accessKeyId  }}
{{- end }}

{{/*
Renders the AWS_ACCESS_KEY_ID environment variable
*/}}
{{- define "airbyte.storage.s3.accessKeyId.env" }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.storage.secretName" . }}
      key: {{ .Values.global.storage.s3.accessKeyIdSecretKey | default "AWS_ACCESS_KEY_ID" }}
{{- end }}

{{/*
Renders the global.storage.s3.secretAccesskey value
*/}}
{{- define "airbyte.storage.s3.secretAccesskey" }}
{{- .Values.global.storage.s3.secretAccesskey  }}
{{- end }}

{{/*
Renders the AWS_SECRET_ACCESS_KEY environment variable
*/}}
{{- define "airbyte.storage.s3.secretAccesskey.env" }}
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.storage.secretName" . }}
      key: {{ .Values.global.storage.s3.secretAccesskeySecretKey | default "AWS_SECRET_ACCESS_KEY" }}
{{- end }}

{{/*
Renders the set of all storage.s3 environment variables
*/}}
{{- define "airbyte.storage.s3.envs" }}
{{- include "airbyte.storage.s3.region.env" . }}
{{- include "airbyte.storage.s3.authenticationType.env" . }}
{{- include "airbyte.storage.s3.accessKeyId.env" . }}
{{- include "airbyte.storage.s3.secretAccesskey.env" . }}
{{- end }}

{{/*
Renders the set of all storage.s3 secrets
*/}}
{{- define "airbyte.storage.s3.secrets" }}
{{- if not (empty (include "airbyte.storage.s3.accessKeyId" .)) }}
AWS_ACCESS_KEY_ID: {{ include "airbyte.storage.s3.accessKeyId" . | quote }}
{{- end }}
{{- if not (empty (include "airbyte.storage.s3.secretAccesskey" .)) }}
AWS_SECRET_ACCESS_KEY: {{ include "airbyte.storage.s3.secretAccesskey" . | quote }}
{{- end }}
{{- end }}

{{/*
Renders the set of all storage environment variables
*/}}
{{- define "airbyte.storage.envs" }}
{{- include "airbyte.storage.type.env" . }}
{{- include "airbyte.storage.bucket.activityPayload.env" . }}
{{- include "airbyte.storage.bucket.log.env" . }}
{{- include "airbyte.storage.bucket.state.env" . }}
{{- include "airbyte.storage.bucket.workloadOutput.env" . }}
{{- $v := ( include "airbyte.storage.type" . ) }}
{{- if eq $v "azure" }}
{{- include "airbyte.storage.azure.envs" . }}
{{- end }}

{{- if eq $v "gcs" }}
{{- include "airbyte.storage.gcs.envs" . }}
{{- end }}

{{- if eq $v "minio" }}
{{- include "airbyte.storage.minio.envs" . }}
{{- end }}

{{- if eq $v "s3" }}
{{- include "airbyte.storage.s3.envs" . }}
{{- end }}
{{- end }}

{{/*
Renders the set of all storage config map variables
*/}}
{{- define "airbyte.storage.configVars" }}
STORAGE_TYPE: {{ include "airbyte.storage.type" . | quote }}
STORAGE_BUCKET_ACTIVITY_PAYLOAD: {{ include "airbyte.storage.bucket.activityPayload" . | quote }}
STORAGE_BUCKET_LOG: {{ include "airbyte.storage.bucket.log" . | quote }}
STORAGE_BUCKET_STATE: {{ include "airbyte.storage.bucket.state" . | quote }}
STORAGE_BUCKET_WORKLOAD_OUTPUT: {{ include "airbyte.storage.bucket.workloadOutput" . | quote }}
{{- $v := ( include "airbyte.storage.type" . ) }}
{{- if eq $v "azure" }}
AZURE_STORAGE_CONNECTION_STRING: {{ include "airbyte.storage.azure.connectionString" . | quote }}
{{- end }}

{{- if eq $v "gcs" }}
GOOGLE_APPLICATION_CREDENTIALS_JSON: {{ include "airbyte.storage.gcs.credentialsJson" . | quote }}
GOOGLE_APPLICATION_CREDENTIALS: {{ include "airbyte.storage.gcs.credentialsJsonPath" . | quote }}
CONTAINER_ORCHESTRATOR_SECRET_MOUNT_PATH: {{ include "airbyte.storage.containerOrchestrator.secretMountPath" . | quote }}
{{- end }}

{{- if eq $v "minio" }}
AWS_ACCESS_KEY_ID: {{ include "airbyte.storage.minio.accessKeyId" . | quote }}
AWS_SECRET_ACCESS_KEY: {{ include "airbyte.storage.minio.secretAccessKey" . | quote }}
MINIO_ENDPOINT: {{ include "airbyte.storage.minio.endpoint" . | quote }}
S3_PATH_STYLE: {{ include "airbyte.storage.minio.s3PathStyleAccess" . | quote }}
{{- end }}

{{- if eq $v "s3" }}
AWS_DEFAULT_REGION: {{ include "airbyte.storage.s3.region" . | quote }}
AWS_AUTHENTICATION_TYPE: {{ include "airbyte.storage.s3.authenticationType" . | quote }}
AWS_ACCESS_KEY_ID: {{ include "airbyte.storage.s3.accessKeyId" . | quote }}
AWS_SECRET_ACCESS_KEY: {{ include "airbyte.storage.s3.secretAccesskey" . | quote }}
{{- end }}
{{- end }}

{{/*
Renders the set of all storage secrets
*/}}
{{- define "airbyte.storage.secrets" }}
{{- $v := ( include "airbyte.storage.type" . ) }}
{{- if eq $v "azure" }}
{{- include "airbyte.storage.azure.secrets" . }}
{{- end }}

{{- if eq $v "gcs" }}
{{- include "airbyte.storage.gcs.secrets" . }}
{{- end }}

{{- if eq $v "minio" }}
{{- include "airbyte.storage.minio.secrets" . }}
{{- end }}

{{- if eq $v "s3" }}
{{- include "airbyte.storage.s3.secrets" . }}
{{- end }}
{{- end }}

