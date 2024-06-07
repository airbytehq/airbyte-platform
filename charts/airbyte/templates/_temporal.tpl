{{/*
Temporal Configuration
*/}}

{{- define "airbyte.temporal.database.host.env" }}
- name: POSTGRES_SEEDS
  valueFrom:
    {{- if .Values.global.database.hostSecretKey }}
    secretKeyRef:
      name: {{ include "airbyte.database.secretName" . }}
      key: {{ .Values.global.database.hostSecretKey }}
    {{- else }}
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DATABASE_HOST
    {{- end }}
{{- end }}

{{- define "airbyte.temporal.database.port.env" }}
- name: DB_PORT
  valueFrom:
    {{- if .Values.global.database.portSecretKey }}
    secretKeyRef:
      name: {{ include "airbyte.database.secretName" . }}
      key: {{ .Values.global.database.portSecretKey }}
    {{- else }}
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DATABASE_PORT
    {{- end }}
{{- end }}

{{- define "airbyte.temporal.database.user.env" }}
- name: POSTGRES_USER
  valueFrom:
    secretKeyRef:
    {{- if .Values.global.database.userSecretKey }}
      name: {{ .Values.global.database.secretName }}
    {{- else }}
      name: {{ .Release.Name }}-airbyte-secrets
    {{- end }}
      key: {{ include "airbyte.database.userSecretKey" .}}
{{- end }}

{{- define "airbyte.temporal.database.password.env" }}
- name: POSTGRES_PWD
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.database.secretName" . }}
      key: {{ include "airbyte.database.passwordSecretKey" . }}
{{- end }}

{{- define "airbyte.temporal.database.ssl.env" }}
{{- if include "postgresql.tlsEnabled" . }}
- name: SQL_TLS_ENABLED
  value: "true"
{{- if not (include "postgresql.disableHostVerification" .) }}
- name: SQL_TLS_DISABLE_HOST_VERIFICATION
  value: "true"
{{- end }}
{{- end }}
{{- end }}

{{- define "airbyte.temporal.database.envs" }}
{{ include "airbyte.temporal.database.host.env" . }}
{{ include "airbyte.temporal.database.port.env" . }}
{{ include "airbyte.temporal.database.user.env" . }}
{{ include "airbyte.temporal.database.password.env" . }}
{{- end }}
