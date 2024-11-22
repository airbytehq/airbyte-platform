{{/*
WorkloadApiServer Configuration
*/}}

{{/*
Renders the workloadApiServer secret name
*/}}
{{- define "airbyte.workloadApiServer.secretName" }}
{{- if .Values.workloadApiServer.secretName }}
  {{- .Values.workloadApiServer.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the workloadApiServer.enabled value
*/}}
{{- define "airbyte.workloadApiServer.enabled" }}
{{- .Values.workloadApiServer.enabled | default true }}
{{- end }}

{{/*
Renders the WORKLOAD_API_SERVER_ENABLED environment variable
*/}}
{{- define "airbyte.workloadApiServer.enabled.env" }}
- name: WORKLOAD_API_SERVER_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WORKLOAD_API_SERVER_ENABLED
{{- end }}

{{/*
Renders the workloadApiServer.host value
*/}}
{{- define "airbyte.workloadApiServer.host" }}
{{- (printf "http://%s-workload-api-server-svc:%d" .Release.Name (int .Values.workloadApiServer.service.port)) }}
{{- end }}

{{/*
Renders the WORKLOAD_API_HOST environment variable
*/}}
{{- define "airbyte.workloadApiServer.host.env" }}
- name: WORKLOAD_API_HOST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WORKLOAD_API_HOST
{{- end }}

{{/*
Renders the workloadApiServer.bearerToken value
*/}}
{{- define "airbyte.workloadApiServer.bearerToken" }}
{{- .Values.workloadApiServer.bearerToken | default "token" }}
{{- end }}

{{/*
Renders the WORKLOAD_API_BEARER_TOKEN environment variable
*/}}
{{- define "airbyte.workloadApiServer.bearerToken.env" }}
- name: WORKLOAD_API_BEARER_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.workloadApiServer.secretName" . }}
      key: {{ .Values.workloadApiServer.bearerTokenSecretKey | default "WORKLOAD_API_BEARER_TOKEN" }}
{{- end }}

{{/*
Renders the workloadApiServer.bearerTokenSecretName value
*/}}
{{- define "airbyte.workloadApiServer.bearerTokenSecretName" }}
{{- .Values.workloadApiServer.bearerTokenSecretName | default (include "airbyte.workloads.secretName" .) }}
{{- end }}

{{/*
Renders the WORKLOAD_API_BEARER_TOKEN_SECRET_NAME environment variable
*/}}
{{- define "airbyte.workloadApiServer.bearerTokenSecretName.env" }}
- name: WORKLOAD_API_BEARER_TOKEN_SECRET_NAME
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WORKLOAD_API_BEARER_TOKEN_SECRET_NAME
{{- end }}

{{/*
Renders the workloadApiServer.bearerTokenSecretKey value
*/}}
{{- define "airbyte.workloadApiServer.bearerTokenSecretKey" }}
{{- .Values.workloadApiServer.bearerTokenSecretKey | default "WORKLOAD_API_BEARER_TOKEN" }}
{{- end }}

{{/*
Renders the WORKLOAD_API_BEARER_TOKEN_SECRET_KEY environment variable
*/}}
{{- define "airbyte.workloadApiServer.bearerTokenSecretKey.env" }}
- name: WORKLOAD_API_BEARER_TOKEN_SECRET_KEY
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WORKLOAD_API_BEARER_TOKEN_SECRET_KEY
{{- end }}

{{/*
Renders the set of all workloadApiServer environment variables
*/}}
{{- define "airbyte.workloadApiServer.envs" }}
{{- include "airbyte.workloadApiServer.enabled.env" . }}
{{- include "airbyte.workloadApiServer.host.env" . }}
{{- include "airbyte.workloadApiServer.bearerToken.env" . }}
{{- include "airbyte.workloadApiServer.bearerTokenSecretName.env" . }}
{{- include "airbyte.workloadApiServer.bearerTokenSecretKey.env" . }}
{{- end }}

{{/*
Renders the set of all workloadApiServer config map variables
*/}}
{{- define "airbyte.workloadApiServer.configVars" }}
WORKLOAD_API_SERVER_ENABLED: {{ include "airbyte.workloadApiServer.enabled" . | quote }}
WORKLOAD_API_HOST: {{ include "airbyte.workloadApiServer.host" . | quote }}
WORKLOAD_API_BEARER_TOKEN_SECRET_NAME: {{ include "airbyte.workloadApiServer.bearerTokenSecretName" . | quote }}
WORKLOAD_API_BEARER_TOKEN_SECRET_KEY: {{ include "airbyte.workloadApiServer.bearerTokenSecretKey" . | quote }}
{{- end }}

{{/*
Renders the set of all workloadApiServer secrets
*/}}
{{- define "airbyte.workloadApiServer.secrets" }}
{{- if not (empty (include "airbyte.workloadApiServer.bearerToken" .)) }}
WORKLOAD_API_BEARER_TOKEN: {{ include "airbyte.workloadApiServer.bearerToken" . | quote }}
{{- end }}
{{- end }}

