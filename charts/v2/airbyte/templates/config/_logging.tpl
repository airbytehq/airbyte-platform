{{/*
Logging Configuration
*/}}

{{/*
Renders the global.logging secret name
*/}}
{{- define "airbyte.logging.secretName" }}
{{- if .Values.global.logging.secretName }}
  {{- .Values.global.logging.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.logging.level value
*/}}
{{- define "airbyte.logging.level" }}
{{- .Values.global.logging.level | default "INFO" }}
{{- end }}

{{/*
Renders the LOG_LEVEL environment variable
*/}}
{{- define "airbyte.logging.level.env" }}
- name: LOG_LEVEL
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: LOG_LEVEL
{{- end }}

{{/*
Renders the set of all logging environment variables
*/}}
{{- define "airbyte.logging.envs" }}
{{- include "airbyte.logging.level.env" . }}
{{- end }}

{{/*
Renders the set of all logging config map variables
*/}}
{{- define "airbyte.logging.configVars" }}
LOG_LEVEL: {{ include "airbyte.logging.level" . | quote }}
{{- end }}

{{/*
Renders the set of all logging secrets
*/}}
{{- define "airbyte.logging.secrets" }}
{{- end }}

