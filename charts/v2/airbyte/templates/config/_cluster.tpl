
{{/* DO NOT EDIT: This file was autogenerated. */}}

{{/*
    Cluster Configuration
*/}}

{{/*
Renders the cluster secret name
*/}}
{{- define "airbyte.cluster.secretName" }}
{{- if .Values.global.cluster.secretName }}
    {{- .Values.global.cluster.secretName }}
{{- else }}
    {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.cluster.type value
*/}}
{{- define "airbyte.cluster.type" }}
    {{- .Values.global.cluster.type | default "hybrid" }}
{{- end }}

{{/*
Renders the cluster.type environment variable
*/}}
{{- define "airbyte.cluster.type.env" }}
- name: CLUSTER_TYPE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CLUSTER_TYPE
{{- end }}

{{/*
Renders the global.cluster.controlPlane.serviceAccounts value
*/}}
{{- define "airbyte.cluster.controlPlane.serviceAccounts" }}
    {{- .Values.global.cluster.controlPlane.serviceAccounts }}
{{- end }}

{{/*
Renders the cluster.controlPlane.serviceAccounts environment variable
*/}}
{{- define "airbyte.cluster.controlPlane.serviceAccounts.env" }}
- name: REMOTE_DATAPLANE_SERVICEACCOUNTS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: REMOTE_DATAPLANE_SERVICEACCOUNTS
{{- end }}

{{/*
Renders the global.cluster.dataPlane.id value
*/}}
{{- define "airbyte.cluster.dataPlane.id" }}
    {{- .Values.global.cluster.dataPlane.id | default "local" }}
{{- end }}

{{/*
Renders the cluster.dataPlane.id environment variable
*/}}
{{- define "airbyte.cluster.dataPlane.id.env" }}
- name: DATA_PLANE_ID
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DATA_PLANE_ID
{{- end }}

{{/*
Renders the global.cluster.dataPlane.controlPlaneAuthEndpoint value
*/}}
{{- define "airbyte.cluster.dataPlane.controlPlaneAuthEndpoint" }}
    {{- .Values.global.cluster.dataPlane.controlPlaneAuthEndpoint | default .Values.global.airbyteUrl }}
{{- end }}

{{/*
Renders the cluster.dataPlane.controlPlaneAuthEndpoint environment variable
*/}}
{{- define "airbyte.cluster.dataPlane.controlPlaneAuthEndpoint.env" }}
- name: CONTROL_PLANE_AUTH_ENDPOINT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CONTROL_PLANE_AUTH_ENDPOINT
{{- end }}

{{/*
Renders the global.cluster.dataPlane.serviceAccountEmail value
*/}}
{{- define "airbyte.cluster.dataPlane.serviceAccountEmail" }}
    {{- .Values.global.cluster.dataPlane.serviceAccountEmail }}
{{- end }}

{{/*
Renders the cluster.dataPlane.serviceAccountEmail environment variable
*/}}
{{- define "airbyte.cluster.dataPlane.serviceAccountEmail.env" }}
- name: DATA_PLANE_SERVICE_ACCOUNT_EMAIL
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DATA_PLANE_SERVICE_ACCOUNT_EMAIL
{{- end }}

{{/*
Renders the global.cluster.dataPlane.serviceAccountCredentialsPath value
*/}}
{{- define "airbyte.cluster.dataPlane.serviceAccountCredentialsPath" }}
    {{- .Values.global.cluster.dataPlane.serviceAccountCredentialsPath | default "/secrets/dataplane-creds/sa.json" }}
{{- end }}

{{/*
Renders the cluster.dataPlane.serviceAccountCredentialsPath environment variable
*/}}
{{- define "airbyte.cluster.dataPlane.serviceAccountCredentialsPath.env" }}
- name: DATA_PLANE_SERVICE_ACCOUNT_CREDENTIALS_PATH
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DATA_PLANE_SERVICE_ACCOUNT_CREDENTIALS_PATH
{{- end }}

{{/*
Renders the set of all cluster environment variables
*/}}
{{- define "airbyte.cluster.envs" }}
{{- include "airbyte.cluster.type.env" . }}
{{- $opt := (include "airbyte.cluster.type" .) }}

{{- if eq $opt "control-plane" }}
{{- include "airbyte.cluster.controlPlane.serviceAccounts.env" . }}
{{- end }}

{{- if eq $opt "data-plane" }}
{{- include "airbyte.cluster.dataPlane.id.env" . }}
{{- include "airbyte.cluster.dataPlane.controlPlaneAuthEndpoint.env" . }}
{{- include "airbyte.cluster.dataPlane.serviceAccountEmail.env" . }}
{{- include "airbyte.cluster.dataPlane.serviceAccountCredentialsPath.env" . }}
{{- end }}

{{- if eq $opt "hybrid" }}
{{- include "airbyte.cluster.controlPlane.serviceAccounts.env" . }}
{{- include "airbyte.cluster.dataPlane.id.env" . }}
{{- include "airbyte.cluster.dataPlane.controlPlaneAuthEndpoint.env" . }}
{{- include "airbyte.cluster.dataPlane.serviceAccountEmail.env" . }}
{{- include "airbyte.cluster.dataPlane.serviceAccountCredentialsPath.env" . }}
{{- end }}

{{- end }}

{{/*
Renders the set of all cluster config map variables
*/}}
{{- define "airbyte.cluster.configVars" }}
CLUSTER_TYPE: {{ include "airbyte.cluster.type" . | quote }}
{{- $opt := (include "airbyte.cluster.type" .) }}

{{- if eq $opt "control-plane" }}
REMOTE_DATAPLANE_SERVICEACCOUNTS: {{ include "airbyte.cluster.controlPlane.serviceAccounts" . | quote }}
{{- end }}

{{- if eq $opt "data-plane" }}
DATA_PLANE_ID: {{ include "airbyte.cluster.dataPlane.id" . | quote }}
CONTROL_PLANE_AUTH_ENDPOINT: {{ include "airbyte.cluster.dataPlane.controlPlaneAuthEndpoint" . | quote }}
DATA_PLANE_SERVICE_ACCOUNT_EMAIL: {{ include "airbyte.cluster.dataPlane.serviceAccountEmail" . | quote }}
DATA_PLANE_SERVICE_ACCOUNT_CREDENTIALS_PATH: {{ include "airbyte.cluster.dataPlane.serviceAccountCredentialsPath" . | quote }}
{{- end }}

{{- if eq $opt "hybrid" }}
REMOTE_DATAPLANE_SERVICEACCOUNTS: {{ include "airbyte.cluster.controlPlane.serviceAccounts" . | quote }}
DATA_PLANE_ID: {{ include "airbyte.cluster.dataPlane.id" . | quote }}
CONTROL_PLANE_AUTH_ENDPOINT: {{ include "airbyte.cluster.dataPlane.controlPlaneAuthEndpoint" . | quote }}
DATA_PLANE_SERVICE_ACCOUNT_EMAIL: {{ include "airbyte.cluster.dataPlane.serviceAccountEmail" . | quote }}
DATA_PLANE_SERVICE_ACCOUNT_CREDENTIALS_PATH: {{ include "airbyte.cluster.dataPlane.serviceAccountCredentialsPath" . | quote }}
{{- end }}

{{- end }}