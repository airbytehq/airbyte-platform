{{/*
Cron Configuration
*/}}

{{/*
Renders the cron secret name
*/}}
{{- define "airbyte.cron.secretName" }}
{{- if .Values.cron.secretName }}
  {{- .Values.cron.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the cron.jobs.updateDefinitions.enabled value
*/}}
{{- define "airbyte.cron.jobs.updateDefinitions.enabled" }}
{{- .Values.cron.jobs.updateDefinitions.enabled  }}
{{- end }}

{{/*
Renders the UPDATE_DEFINITIONS_CRON_ENABLED environment variable
*/}}
{{- define "airbyte.cron.jobs.updateDefinitions.enabled.env" }}
- name: UPDATE_DEFINITIONS_CRON_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: UPDATE_DEFINITIONS_CRON_ENABLED
{{- end }}

{{/*
Renders the set of all cron environment variables
*/}}
{{- define "airbyte.cron.envs" }}
{{- include "airbyte.cron.jobs.updateDefinitions.enabled.env" . }}
{{- end }}

{{/*
Renders the set of all cron config map variables
*/}}
{{- define "airbyte.cron.configVars" }}
UPDATE_DEFINITIONS_CRON_ENABLED: {{ include "airbyte.cron.jobs.updateDefinitions.enabled" . | quote }}
{{- end }}

{{/*
Renders the set of all cron secrets
*/}}
{{- define "airbyte.cron.secrets" }}
{{- end }}

