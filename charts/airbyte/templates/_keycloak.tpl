{{/*
Keycloak Configuration
*/}}

{{- define "airbyte.keycloak.database.user.env" }}
- name: KEYCLOAK_DATABASE_USERNAME
  valueFrom:
    secretKeyRef:
    {{- if .Values.global.database.userSecretKey }}
      name: {{ .Values.global.database.secretName }}
    {{- else }}
      name: {{ .Release.Name }}-airbyte-secrets
    {{- end }}
      key: {{ include "airbyte.database.userSecretKey" .}}
{{- end }}

{{- define "airbyte.keycloak.database.password.env" }}
- name: KEYCLOAK_DATABASE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.database.secretName" . }}
      key: {{ include "airbyte.database.passwordSecretKey" . }}
{{- end }}

{{- define "airbyte.keycloak.database.url" }}
{{- $host := (include "airbyte.database.host" .) }}
{{- $dbName := (include "airbyte.database.name" .) }}
{{- $port := (include "airbyte.database.port" . ) }}
{{- printf "jdbc:postgresql://%s:%s/%s?currentSchema=keycloak" $host $port $dbName }}
{{- end }}

{{- define "airbyte.keycloak.database.url.env" }}
- name: KEYCLOAK_DATABASE_URL
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: KEYCLOAK_DATABASE_URL
{{- end }}

{{- define "airbyte.keycloak.database.envs" }}
{{ include "airbyte.keycloak.database.user.env" . }}
{{ include "airbyte.keycloak.database.password.env" . }}
{{ include "airbyte.keycloak.database.url.env" . }}
{{- end }}
