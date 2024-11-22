{{/*
Datadog Configuration
*/}}

{{/*
Renders the global.datadog secret name
*/}}
{{- define "airbyte.datadog.secretName" }}
{{- if .Values.global.datadog.secretName }}
  {{- .Values.global.datadog.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the DD_AGENT_HOST environment variable
*/}}
{{- define "airbyte.datadog.agentHost.env" }}
- name: DD_AGENT_HOST
  valueFrom:
    fieldRef:
      fieldPath: status.hostIP
{{- end }}

{{/*
Renders the global.datadog.enabled value
*/}}
{{- define "airbyte.datadog.enabled" }}
{{- .Values.global.datadog.enabled  }}
{{- end }}

{{/*
Renders the DD_ENABLED environment variable
*/}}
{{- define "airbyte.datadog.enabled.env" }}
- name: DD_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DD_ENABLED
{{- end }}

{{/*
Renders the global.datadog.env value
*/}}
{{- define "airbyte.datadog.env" }}
{{- .Values.global.datadog.env  }}
{{- end }}

{{/*
Renders the DD_ENV environment variable
*/}}
{{- define "airbyte.datadog.env.env" }}
- name: DD_ENV
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DD_ENV
{{- end }}

{{/*
Renders the global.datadog.service value
*/}}
{{- define "airbyte.datadog.service" }}
{{- "airbyte-{{ .Chart.Name }}" }}
{{- end }}

{{/*
Renders the DD_SERVICE environment variable
*/}}
{{- define "airbyte.datadog.service.env" }}
- name: DD_SERVICE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DD_SERVICE
{{- end }}

{{/*
Renders the global.datadog.statsd.port value
*/}}
{{- define "airbyte.datadog.statsd.port" }}
{{- .Values.global.datadog.statsd.port  }}
{{- end }}

{{/*
Renders the DD_DOGSTATSD_PORT environment variable
*/}}
{{- define "airbyte.datadog.statsd.port.env" }}
- name: DD_DOGSTATSD_PORT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DD_DOGSTATSD_PORT
{{- end }}

{{/*
Renders the global.datadog.traceAgentPort value
*/}}
{{- define "airbyte.datadog.traceAgentPort" }}
{{- .Values.global.datadog.traceAgentPort  }}
{{- end }}

{{/*
Renders the DD_TRACEAGENT_PORT environment variable
*/}}
{{- define "airbyte.datadog.traceAgentPort.env" }}
- name: DD_TRACEAGENT_PORT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DD_TRACEAGENT_PORT
{{- end }}

{{/*
Renders the global.datadog.integrations.dbm.enabled value
*/}}
{{- define "airbyte.datadog.integrations.dbm.enabled" }}
{{- .Values.global.datadog.integrations.dbm.enabled  }}
{{- end }}

{{/*
Renders the DD_INTEGRATION_DBM_ENABLED environment variable
*/}}
{{- define "airbyte.datadog.integrations.dbm.enabled.env" }}
- name: DD_INTEGRATION_DBM_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DD_INTEGRATION_DBM_ENABLED
{{- end }}

{{/*
Renders the global.datadog.integrations.dbm.propagationMode value
*/}}
{{- define "airbyte.datadog.integrations.dbm.propagationMode" }}
{{- .Values.global.datadog.integrations.dbm.propagationMode | default "full" }}
{{- end }}

{{/*
Renders the DD_INTEGRATION_DBM_PROPAGATION_MODE environment variable
*/}}
{{- define "airbyte.datadog.integrations.dbm.propagationMode.env" }}
- name: DD_INTEGRATION_DBM_PROPAGATION_MODE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DD_INTEGRATION_DBM_PROPAGATION_MODE
{{- end }}

{{/*
Renders the global.datadog.integrations.googleHttpClient.enabled value
*/}}
{{- define "airbyte.datadog.integrations.googleHttpClient.enabled" }}
{{- .Values.global.datadog.integrations.googleHttpClient.enabled  }}
{{- end }}

{{/*
Renders the DD_INTEGRATION_GOOGLE_HTTP_CLIENT_ENABLED environment variable
*/}}
{{- define "airbyte.datadog.integrations.googleHttpClient.enabled.env" }}
- name: DD_INTEGRATION_GOOGLE_HTTP_CLIENT_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DD_INTEGRATION_GOOGLE_HTTP_CLIENT_ENABLED
{{- end }}

{{/*
Renders the global.datadog.integrations.grpc.enabled value
*/}}
{{- define "airbyte.datadog.integrations.grpc.enabled" }}
{{- .Values.global.datadog.integrations.grpc.enabled  }}
{{- end }}

{{/*
Renders the DD_INTEGRATION_GRPC_ENABLED environment variable
*/}}
{{- define "airbyte.datadog.integrations.grpc.enabled.env" }}
- name: DD_INTEGRATION_GRPC_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DD_INTEGRATION_GRPC_ENABLED
{{- end }}

{{/*
Renders the global.datadog.integrations.grpc.clientEnabled value
*/}}
{{- define "airbyte.datadog.integrations.grpc.clientEnabled" }}
{{- .Values.global.datadog.integrations.grpc.clientEnabled  }}
{{- end }}

{{/*
Renders the DD_INTEGRATION_GRPC_CLIENT_ENABLED environment variable
*/}}
{{- define "airbyte.datadog.integrations.grpc.clientEnabled.env" }}
- name: DD_INTEGRATION_GRPC_CLIENT_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DD_INTEGRATION_GRPC_CLIENT_ENABLED
{{- end }}

{{/*
Renders the global.datadog.integrations.grpc.serverEnabled value
*/}}
{{- define "airbyte.datadog.integrations.grpc.serverEnabled" }}
{{- .Values.global.datadog.integrations.grpc.serverEnabled  }}
{{- end }}

{{/*
Renders the DD_INTEGRATION_GRPC_SERVER_ENABLED environment variable
*/}}
{{- define "airbyte.datadog.integrations.grpc.serverEnabled.env" }}
- name: DD_INTEGRATION_GRPC_SERVER_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DD_INTEGRATION_GRPC_SERVER_ENABLED
{{- end }}

{{/*
Renders the global.datadog.integrations.httpUrlConnection.enabled value
*/}}
{{- define "airbyte.datadog.integrations.httpUrlConnection.enabled" }}
{{- .Values.global.datadog.integrations.httpUrlConnection.enabled  }}
{{- end }}

{{/*
Renders the DD_INTEGRATION_HTTPURLCONNECTION_ENABLED environment variable
*/}}
{{- define "airbyte.datadog.integrations.httpUrlConnection.enabled.env" }}
- name: DD_INTEGRATION_HTTPURLCONNECTION_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DD_INTEGRATION_HTTPURLCONNECTION_ENABLED
{{- end }}

{{/*
Renders the global.datadog.integrations.netty.enabled value
*/}}
{{- define "airbyte.datadog.integrations.netty.enabled" }}
{{- .Values.global.datadog.integrations.netty.enabled  }}
{{- end }}

{{/*
Renders the DD_INTEGRATION_NETTY_ENABLED environment variable
*/}}
{{- define "airbyte.datadog.integrations.netty.enabled.env" }}
- name: DD_INTEGRATION_NETTY_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DD_INTEGRATION_NETTY_ENABLED
{{- end }}

{{/*
Renders the global.datadog.integrations.netty41.enabled value
*/}}
{{- define "airbyte.datadog.integrations.netty41.enabled" }}
{{- .Values.global.datadog.integrations.netty41.enabled  }}
{{- end }}

{{/*
Renders the DD_INTEGRATION_NETTY_4_1_ENABLED environment variable
*/}}
{{- define "airbyte.datadog.integrations.netty41.enabled.env" }}
- name: DD_INTEGRATION_NETTY_4_1_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DD_INTEGRATION_NETTY_4_1_ENABLED
{{- end }}

{{/*
Renders the global.datadog.integrations.urlConnection.enabled value
*/}}
{{- define "airbyte.datadog.integrations.urlConnection.enabled" }}
{{- .Values.global.datadog.integrations.urlConnection.enabled  }}
{{- end }}

{{/*
Renders the DD_INTEGRATION_URLCONNECTION_ENABLED environment variable
*/}}
{{- define "airbyte.datadog.integrations.urlConnection.enabled.env" }}
- name: DD_INTEGRATION_URLCONNECTION_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DD_INTEGRATION_URLCONNECTION_ENABLED
{{- end }}

{{/*
Renders the set of all datadog environment variables
*/}}
{{- define "airbyte.datadog.envs" }}
{{- include "airbyte.datadog.agentHost.env" . }}
{{- include "airbyte.datadog.enabled.env" . }}
{{- include "airbyte.datadog.env.env" . }}
{{- include "airbyte.datadog.service.env" . }}
{{- include "airbyte.datadog.statsd.port.env" . }}
{{- include "airbyte.datadog.traceAgentPort.env" . }}
{{- include "airbyte.datadog.integrations.dbm.enabled.env" . }}
{{- include "airbyte.datadog.integrations.dbm.propagationMode.env" . }}
{{- include "airbyte.datadog.integrations.googleHttpClient.enabled.env" . }}
{{- include "airbyte.datadog.integrations.grpc.enabled.env" . }}
{{- include "airbyte.datadog.integrations.grpc.clientEnabled.env" . }}
{{- include "airbyte.datadog.integrations.grpc.serverEnabled.env" . }}
{{- include "airbyte.datadog.integrations.httpUrlConnection.enabled.env" . }}
{{- include "airbyte.datadog.integrations.netty.enabled.env" . }}
{{- include "airbyte.datadog.integrations.netty41.enabled.env" . }}
{{- include "airbyte.datadog.integrations.urlConnection.enabled.env" . }}
{{- end }}

{{/*
Renders the set of all datadog config map variables
*/}}
{{- define "airbyte.datadog.configVars" }}
DD_ENABLED: {{ include "airbyte.datadog.enabled" . | quote }}
DD_ENV: {{ include "airbyte.datadog.env" . | quote }}
DD_SERVICE: {{ include "airbyte.datadog.service" . | quote }}
DD_DOGSTATSD_PORT: {{ include "airbyte.datadog.statsd.port" . | quote }}
DD_TRACEAGENT_PORT: {{ include "airbyte.datadog.traceAgentPort" . | quote }}
DD_INTEGRATION_DBM_ENABLED: {{ include "airbyte.datadog.integrations.dbm.enabled" . | quote }}
DD_INTEGRATION_DBM_PROPAGATION_MODE: {{ include "airbyte.datadog.integrations.dbm.propagationMode" . | quote }}
DD_INTEGRATION_GOOGLE_HTTP_CLIENT_ENABLED: {{ include "airbyte.datadog.integrations.googleHttpClient.enabled" . | quote }}
DD_INTEGRATION_GRPC_ENABLED: {{ include "airbyte.datadog.integrations.grpc.enabled" . | quote }}
DD_INTEGRATION_GRPC_CLIENT_ENABLED: {{ include "airbyte.datadog.integrations.grpc.clientEnabled" . | quote }}
DD_INTEGRATION_GRPC_SERVER_ENABLED: {{ include "airbyte.datadog.integrations.grpc.serverEnabled" . | quote }}
DD_INTEGRATION_HTTPURLCONNECTION_ENABLED: {{ include "airbyte.datadog.integrations.httpUrlConnection.enabled" . | quote }}
DD_INTEGRATION_NETTY_ENABLED: {{ include "airbyte.datadog.integrations.netty.enabled" . | quote }}
DD_INTEGRATION_NETTY_4_1_ENABLED: {{ include "airbyte.datadog.integrations.netty41.enabled" . | quote }}
DD_INTEGRATION_URLCONNECTION_ENABLED: {{ include "airbyte.datadog.integrations.urlConnection.enabled" . | quote }}
{{- end }}

{{/*
Renders the set of all datadog secrets
*/}}
{{- define "airbyte.datadog.secrets" }}
{{- end }}

