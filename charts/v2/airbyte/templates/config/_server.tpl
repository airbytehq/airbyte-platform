
{{/* DO NOT EDIT: This file was autogenerated. */}}

{{/*
    Server Configuration
*/}}

{{/*
Renders the server.webapp.datadogApplicationId value
*/}}
{{- define "airbyte.server.webapp.datadogApplicationId" }}
    {{- .Values.server.webapp.datadogApplicationId }}
{{- end }}

{{/*
Renders the server.webapp.datadogApplicationId environment variable
*/}}
{{- define "airbyte.server.webapp.datadogApplicationId.env" }}
- name: WEBAPP_DATADOG_APPLICATION_ID
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WEBAPP_DATADOG_APPLICATION_ID
{{- end }}

{{/*
Renders the server.webapp.datadogClientToken value
*/}}
{{- define "airbyte.server.webapp.datadogClientToken" }}
    {{- .Values.server.webapp.datadogClientToken }}
{{- end }}

{{/*
Renders the server.webapp.datadogClientToken environment variable
*/}}
{{- define "airbyte.server.webapp.datadogClientToken.env" }}
- name: WEBAPP_DATADOG_CLIENT_TOKEN
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WEBAPP_DATADOG_CLIENT_TOKEN
{{- end }}

{{/*
Renders the server.webapp.datadogEnv value
*/}}
{{- define "airbyte.server.webapp.datadogEnv" }}
    {{- .Values.server.webapp.datadogEnv }}
{{- end }}

{{/*
Renders the server.webapp.datadogEnv environment variable
*/}}
{{- define "airbyte.server.webapp.datadogEnv.env" }}
- name: WEBAPP_DATADOG_ENV
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WEBAPP_DATADOG_ENV
{{- end }}

{{/*
Renders the server.webapp.datadogService value
*/}}
{{- define "airbyte.server.webapp.datadogService" }}
    {{- .Values.server.webapp.datadogService | default "airbyte-webapp" }}
{{- end }}

{{/*
Renders the server.webapp.datadogService environment variable
*/}}
{{- define "airbyte.server.webapp.datadogService.env" }}
- name: WEBAPP_DATADOG_SERVICE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WEBAPP_DATADOG_SERVICE
{{- end }}

{{/*
Renders the server.webapp.datadogSite value
*/}}
{{- define "airbyte.server.webapp.datadogSite" }}
    {{- .Values.server.webapp.datadogSite | default "datadoghq.com" }}
{{- end }}

{{/*
Renders the server.webapp.datadogSite environment variable
*/}}
{{- define "airbyte.server.webapp.datadogSite.env" }}
- name: WEBAPP_DATADOG_SITE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WEBAPP_DATADOG_SITE
{{- end }}

{{/*
Renders the server.webapp.hockeystackApiKey value
*/}}
{{- define "airbyte.server.webapp.hockeystackApiKey" }}
    {{- .Values.server.webapp.hockeystackApiKey }}
{{- end }}

{{/*
Renders the server.webapp.hockeystackApiKey environment variable
*/}}
{{- define "airbyte.server.webapp.hockeystackApiKey.env" }}
- name: WEBAPP_HOCKEYSTACK_API_KEY
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WEBAPP_HOCKEYSTACK_API_KEY
{{- end }}

{{/*
Renders the server.webapp.launchdarklyKey value
*/}}
{{- define "airbyte.server.webapp.launchdarklyKey" }}
    {{- .Values.server.webapp.launchdarklyKey }}
{{- end }}

{{/*
Renders the server.webapp.launchdarklyKey environment variable
*/}}
{{- define "airbyte.server.webapp.launchdarklyKey.env" }}
- name: WEBAPP_LAUNCHDARKLY_KEY
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WEBAPP_LAUNCHDARKLY_KEY
{{- end }}

{{/*
Renders the server.webapp.osanoKey value
*/}}
{{- define "airbyte.server.webapp.osanoKey" }}
    {{- .Values.server.webapp.osanoKey }}
{{- end }}

{{/*
Renders the server.webapp.osanoKey environment variable
*/}}
{{- define "airbyte.server.webapp.osanoKey.env" }}
- name: WEBAPP_OSANO_KEY
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WEBAPP_OSANO_KEY
{{- end }}

{{/*
Renders the server.webapp.segmentToken value
*/}}
{{- define "airbyte.server.webapp.segmentToken" }}
    {{- .Values.server.webapp.segmentToken }}
{{- end }}

{{/*
Renders the server.webapp.segmentToken environment variable
*/}}
{{- define "airbyte.server.webapp.segmentToken.env" }}
- name: WEBAPP_SEGMENT_TOKEN
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WEBAPP_SEGMENT_TOKEN
{{- end }}

{{/*
Renders the server.webapp.zendeskKey value
*/}}
{{- define "airbyte.server.webapp.zendeskKey" }}
    {{- .Values.server.webapp.zendeskKey }}
{{- end }}

{{/*
Renders the server.webapp.zendeskKey environment variable
*/}}
{{- define "airbyte.server.webapp.zendeskKey.env" }}
- name: WEBAPP_ZENDESK_KEY
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WEBAPP_ZENDESK_KEY
{{- end }}

{{/*
Renders the set of all server environment variables
*/}}
{{- define "airbyte.server.envs" }}
{{- include "airbyte.server.webapp.datadogApplicationId.env" . }}
{{- include "airbyte.server.webapp.datadogClientToken.env" . }}
{{- include "airbyte.server.webapp.datadogEnv.env" . }}
{{- include "airbyte.server.webapp.datadogService.env" . }}
{{- include "airbyte.server.webapp.datadogSite.env" . }}
{{- include "airbyte.server.webapp.hockeystackApiKey.env" . }}
{{- include "airbyte.server.webapp.launchdarklyKey.env" . }}
{{- include "airbyte.server.webapp.osanoKey.env" . }}
{{- include "airbyte.server.webapp.segmentToken.env" . }}
{{- include "airbyte.server.webapp.zendeskKey.env" . }}
{{- end }}

{{/*
Renders the set of all server config map variables
*/}}
{{- define "airbyte.server.configVars" }}
WEBAPP_DATADOG_APPLICATION_ID: {{ include "airbyte.server.webapp.datadogApplicationId" . | quote }}
WEBAPP_DATADOG_CLIENT_TOKEN: {{ include "airbyte.server.webapp.datadogClientToken" . | quote }}
WEBAPP_DATADOG_ENV: {{ include "airbyte.server.webapp.datadogEnv" . | quote }}
WEBAPP_DATADOG_SERVICE: {{ include "airbyte.server.webapp.datadogService" . | quote }}
WEBAPP_DATADOG_SITE: {{ include "airbyte.server.webapp.datadogSite" . | quote }}
WEBAPP_HOCKEYSTACK_API_KEY: {{ include "airbyte.server.webapp.hockeystackApiKey" . | quote }}
WEBAPP_LAUNCHDARKLY_KEY: {{ include "airbyte.server.webapp.launchdarklyKey" . | quote }}
WEBAPP_OSANO_KEY: {{ include "airbyte.server.webapp.osanoKey" . | quote }}
WEBAPP_SEGMENT_TOKEN: {{ include "airbyte.server.webapp.segmentToken" . | quote }}
WEBAPP_ZENDESK_KEY: {{ include "airbyte.server.webapp.zendeskKey" . | quote }}
{{- end }}
