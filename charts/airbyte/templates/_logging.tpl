{{/*
Common logging configs
*/}}
{{- define "airbyte.logging.envs" }}
- name: LOG_LEVEL
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: LOG_LEVEL
{{- end }}
