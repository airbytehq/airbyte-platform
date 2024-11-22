{{/*
Email Configuration
*/}}

{{/*
Renders the global.email secret name
*/}}
{{- define "airbyte.email.secretName" }}
{{- if .Values.global.email.secretName }}
  {{- .Values.global.email.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}
{{/*
Renders the global.email.provider value
*/}}
{{- define "airbyte.email.provider" }}
{{- .Values.global.email.provider | default "SENDGRID_API" }}
{{- end }}

{{/*
Renders the EMAIL_CLIENT environment variable
*/}}
{{- define "airbyte.email.provider.env" }}
- name: EMAIL_CLIENT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: EMAIL_CLIENT
{{- end }}

{{/*
Renders the global.email.sendgrid.apiKey value
*/}}
{{- define "airbyte.email.sendgrid.apiKey" }}
{{- .Values.global.email.sendgrid.apiKey  }}
{{- end }}

{{/*
Renders the SENDGRID_API_KEY environment variable
*/}}
{{- define "airbyte.email.sendgrid.apiKey.env" }}
- name: SENDGRID_API_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.email.secretName" . }}
      key: {{ .Values.global.email.sendgrid.apiKeySecretKey | default "SENDGRID_API_KEY" }}
{{- end }}

{{/*
Renders the set of all email.sendgrid environment variables
*/}}
{{- define "airbyte.email.sendgrid.envs" }}
{{- include "airbyte.email.sendgrid.apiKey.env" . }}
{{- end }}

{{/*
Renders the set of all email.sendgrid secrets
*/}}
{{- define "airbyte.email.sendgrid.secrets" }}
{{- if not (empty (include "airbyte.email.sendgrid.apiKey" .)) }}
SENDGRID_API_KEY: {{ include "airbyte.email.sendgrid.apiKey" . | quote }}
{{- end }}
{{- end }}

{{/*
Renders the set of all email environment variables
*/}}
{{- define "airbyte.email.envs" }}
{{- include "airbyte.email.provider.env" . }}
{{- $v := ( include "airbyte.email.provider" . ) }}
{{- if eq $v "sendgrid" }}
{{- include "airbyte.email.sendgrid.envs" . }}
{{- end }}
{{- end }}

{{/*
Renders the set of all email config map variables
*/}}
{{- define "airbyte.email.configVars" }}
EMAIL_CLIENT: {{ include "airbyte.email.provider" . | quote }}
{{- $v := ( include "airbyte.email.provider" . ) }}
{{- if eq $v "sendgrid" }}
SENDGRID_API_KEY: {{ include "airbyte.email.sendgrid.apiKey" . | quote }}
{{- end }}
{{- end }}

{{/*
Renders the set of all email secrets
*/}}
{{- define "airbyte.email.secrets" }}
{{- $v := ( include "airbyte.email.provider" . ) }}
{{- if eq $v "sendgrid" }}
{{- include "airbyte.email.sendgrid.secrets" . }}
{{- end }}
{{- end }}

