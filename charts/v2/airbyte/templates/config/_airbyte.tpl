{{/*
Airbyte Configuration
*/}}

{{/*
Renders the global secret name
*/}}
{{- define "airbyte.airbyte.secretName" }}
{{- if .Values.global.secretName }}
  {{- .Values.global.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.edition value
*/}}
{{- define "airbyte.airbyte.edition" }}
{{- ternary "pro" "community" (or (eq .Values.global.edition "pro") (eq .Values.global.edition "enterprise")) }}
{{- end }}

{{/*
Renders the AIRBYTE_EDITION environment variable
*/}}
{{- define "airbyte.airbyte.edition.env" }}
- name: AIRBYTE_EDITION
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: AIRBYTE_EDITION
{{- end }}

{{/*
Renders the global.version value
*/}}
{{- define "airbyte.airbyte.version" }}
{{- .Values.global.version | default .Chart.AppVersion }}
{{- end }}

{{/*
Renders the AIRBYTE_VERSION environment variable
*/}}
{{- define "airbyte.airbyte.version.env" }}
- name: AIRBYTE_VERSION
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: AIRBYTE_VERSION
{{- end }}

{{/*
Renders the global.cluster.type value
*/}}
{{- define "airbyte.airbyte.cluster.type" }}
{{- .Values.global.cluster.type | default "control-plane" }}
{{- end }}

{{/*
Renders the AIRBYTE_CLUSTER_TYPE environment variable
*/}}
{{- define "airbyte.airbyte.cluster.type.env" }}
- name: AIRBYTE_CLUSTER_TYPE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: AIRBYTE_CLUSTER_TYPE
{{- end }}

{{/*
Renders the global.cluster.name value
*/}}
{{- define "airbyte.airbyte.cluster.name" }}
{{- .Values.global.cluster.name  }}
{{- end }}

{{/*
Renders the AIRBYTE_CLUSTER_NAME environment variable
*/}}
{{- define "airbyte.airbyte.cluster.name.env" }}
- name: AIRBYTE_CLUSTER_NAME
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: AIRBYTE_CLUSTER_NAME
{{- end }}

{{/*
Renders the global.airbyteUrl value
*/}}
{{- define "airbyte.airbyte.airbyteUrl" }}
{{- .Values.global.airbyteUrl  }}
{{- end }}

{{/*
Renders the AIRBYTE_URL environment variable
*/}}
{{- define "airbyte.airbyte.airbyteUrl.env" }}
- name: AIRBYTE_URL
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: AIRBYTE_URL
{{- end }}

{{/*
Renders the global.api.host value
*/}}
{{- define "airbyte.airbyte.api.host" }}
{{- ternary (printf "http://localhost:%d/api/public" (int .Values.server.service.port)) (printf "%s/api/public" .Values.global.airbyteUrl) (eq .Values.global.edition "community") }}
{{- end }}

{{/*
Renders the AIRBYTE_API_HOST environment variable
*/}}
{{- define "airbyte.airbyte.api.host.env" }}
- name: AIRBYTE_API_HOST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: AIRBYTE_API_HOST
{{- end }}

{{/*
Renders the global.api.authHeaderName value
*/}}
{{- define "airbyte.airbyte.api.authHeaderName" }}
{{- "X-Airbyte-Auth" }}
{{- end }}

{{/*
Renders the AIRBYTE_API_AUTH_HEADER_NAME environment variable
*/}}
{{- define "airbyte.airbyte.api.authHeaderName.env" }}
- name: AIRBYTE_API_AUTH_HEADER_NAME
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: AIRBYTE_API_AUTH_HEADER_NAME
{{- end }}

{{/*
Renders the global.server.host value
*/}}
{{- define "airbyte.airbyte.server.host" }}
{{- (printf "%s-airbyte-server-svc:%d" .Release.Name (int .Values.server.service.port)) }}
{{- end }}

{{/*
Renders the AIRBYTE_SERVER_HOST environment variable
*/}}
{{- define "airbyte.airbyte.server.host.env" }}
- name: AIRBYTE_SERVER_HOST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: AIRBYTE_SERVER_HOST
{{- end }}

{{/*
Renders the global.api.authEnabled value
*/}}
{{- define "airbyte.airbyte.api.authEnabled" }}
{{- "true" }}
{{- end }}

{{/*
Renders the API_AUTHORIZATION_ENABLED environment variable
*/}}
{{- define "airbyte.airbyte.api.authEnabled.env" }}
- name: API_AUTHORIZATION_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: API_AUTHORIZATION_ENABLED
{{- end }}

{{/*
Renders the global.api.internalHost value
*/}}
{{- define "airbyte.airbyte.api.internalHost" }}
{{- (printf "http://%s-airbyte-server-svc:%d" .Release.Name (int .Values.server.service.port)) }}
{{- end }}

{{/*
Renders the INTERNAL_API_HOST environment variable
*/}}
{{- define "airbyte.airbyte.api.internalHost.env" }}
- name: INTERNAL_API_HOST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: INTERNAL_API_HOST
{{- end }}

{{/*
Renders the global.connectorBuilderServer.apiHost value
*/}}
{{- define "airbyte.airbyte.connectorBuilderServer.apiHost" }}
{{- (printf "http://%s-airbyte-connector-builder-server-svc:%d" .Release.Name (int .Values.connectorBuilderServer.service.port)) }}
{{- end }}

{{/*
Renders the CONNECTOR_BUILDER_SERVER_API_HOST environment variable
*/}}
{{- define "airbyte.airbyte.connectorBuilderServer.apiHost.env" }}
- name: CONNECTOR_BUILDER_SERVER_API_HOST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CONNECTOR_BUILDER_SERVER_API_HOST
{{- end }}

{{/*
Renders the global.webapp.url value
*/}}
{{- define "airbyte.airbyte.webapp.url" }}
{{- (printf "http://%s-airbyte-webapp-svc:%d" .Release.Name (int .Values.webapp.service.port)) }}
{{- end }}

{{/*
Renders the WEBAPP_URL environment variable
*/}}
{{- define "airbyte.airbyte.webapp.url.env" }}
- name: WEBAPP_URL
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WEBAPP_URL
{{- end }}

{{/*
Renders the set of all airbyte environment variables
*/}}
{{- define "airbyte.airbyte.envs" }}
{{- include "airbyte.airbyte.edition.env" . }}
{{- include "airbyte.airbyte.version.env" . }}
{{- include "airbyte.airbyte.cluster.type.env" . }}
{{- include "airbyte.airbyte.cluster.name.env" . }}
{{- include "airbyte.airbyte.airbyteUrl.env" . }}
{{- include "airbyte.airbyte.api.host.env" . }}
{{- include "airbyte.airbyte.api.authHeaderName.env" . }}
{{- include "airbyte.airbyte.server.host.env" . }}
{{- include "airbyte.airbyte.api.authEnabled.env" . }}
{{- include "airbyte.airbyte.api.internalHost.env" . }}
{{- include "airbyte.airbyte.connectorBuilderServer.apiHost.env" . }}
{{- include "airbyte.airbyte.webapp.url.env" . }}
{{- end }}

{{/*
Renders the set of all airbyte config map variables
*/}}
{{- define "airbyte.airbyte.configVars" }}
AIRBYTE_EDITION: {{ include "airbyte.airbyte.edition" . | quote }}
AIRBYTE_VERSION: {{ include "airbyte.airbyte.version" . | quote }}
AIRBYTE_CLUSTER_TYPE: {{ include "airbyte.airbyte.cluster.type" . | quote }}
AIRBYTE_CLUSTER_NAME: {{ include "airbyte.airbyte.cluster.name" . | quote }}
AIRBYTE_URL: {{ include "airbyte.airbyte.airbyteUrl" . | quote }}
AIRBYTE_API_HOST: {{ include "airbyte.airbyte.api.host" . | quote }}
AIRBYTE_API_AUTH_HEADER_NAME: {{ include "airbyte.airbyte.api.authHeaderName" . | quote }}
AIRBYTE_SERVER_HOST: {{ include "airbyte.airbyte.server.host" . | quote }}
API_AUTHORIZATION_ENABLED: {{ include "airbyte.airbyte.api.authEnabled" . | quote }}
INTERNAL_API_HOST: {{ include "airbyte.airbyte.api.internalHost" . | quote }}
CONNECTOR_BUILDER_SERVER_API_HOST: {{ include "airbyte.airbyte.connectorBuilderServer.apiHost" . | quote }}
WEBAPP_URL: {{ include "airbyte.airbyte.webapp.url" . | quote }}
{{- end }}

{{/*
Renders the set of all airbyte secrets
*/}}
{{- define "airbyte.airbyte.secrets" }}
{{- end }}

{{/*
Airbyte Configuration
*/}}

{{/*
Renders the webapp secret name
*/}}
{{- define "airbyte.webapp.secretName" }}
{{- if .Values.webapp.secretName }}
  {{- .Values.webapp.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the webapp.api.url value
*/}}
{{- define "airbyte.webapp.api.url" }}
{{- "/api/v1" }}
{{- end }}

{{/*
Renders the API_URL environment variable
*/}}
{{- define "airbyte.webapp.api.url.env" }}
- name: API_URL
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: API_URL
{{- end }}

{{/*
Renders the webapp.connectorBuilderServer.host value
*/}}
{{- define "airbyte.webapp.connectorBuilderServer.host" }}
{{- (printf "%s-airbyte-connector-builder-server-svc:%d" .Release.Name (int .Values.connectorBuilderServer.service.port)) }}
{{- end }}

{{/*
Renders the CONNECTOR_BUILDER_API_HOST environment variable
*/}}
{{- define "airbyte.webapp.connectorBuilderServer.host.env" }}
- name: CONNECTOR_BUILDER_API_HOST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CONNECTOR_BUILDER_API_HOST
{{- end }}

{{/*
Renders the webapp.connectorBuilderServer.url value
*/}}
{{- define "airbyte.webapp.connectorBuilderServer.url" }}
{{- "/connector-builder-api" }}
{{- end }}

{{/*
Renders the CONNECTOR_BUILDER_API_URL environment variable
*/}}
{{- define "airbyte.webapp.connectorBuilderServer.url.env" }}
- name: CONNECTOR_BUILDER_API_URL
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CONNECTOR_BUILDER_API_URL
{{- end }}

{{/*
Renders the set of all webapp environment variables
*/}}
{{- define "airbyte.webapp.envs" }}
{{- include "airbyte.webapp.api.url.env" . }}
{{- include "airbyte.webapp.connectorBuilderServer.host.env" . }}
{{- include "airbyte.webapp.connectorBuilderServer.url.env" . }}
{{- end }}

{{/*
Renders the set of all webapp config map variables
*/}}
{{- define "airbyte.webapp.configVars" }}
API_URL: {{ include "airbyte.webapp.api.url" . | quote }}
CONNECTOR_BUILDER_API_HOST: {{ include "airbyte.webapp.connectorBuilderServer.host" . | quote }}
CONNECTOR_BUILDER_API_URL: {{ include "airbyte.webapp.connectorBuilderServer.url" . | quote }}
{{- end }}

{{/*
Renders the set of all webapp secrets
*/}}
{{- define "airbyte.webapp.secrets" }}
{{- end }}

