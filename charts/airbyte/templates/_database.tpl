{{/*
Database Configuration
*/}}

{{/*
Renders the database host
*/}}
{{- define "airbyte.database.host" }}
    {{- if .Values.postgresql.enabled }}
        {{- printf "%s" "airbyte-db-svc" }}
    {{- else if .Values.global.database.host }}
        {{- .Values.global.database.host }}
    {{- else }}
        {{ $host := .Values.global.database.host | required "You must set `global.database.host` when using an external database" }}
    {{- end }}
{{- end }}

{{/*
Renders an environment variable definition that provides the database host
*/}}
{{- define "airbyte.database.host.env" }}
- name: DATABASE_HOST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DATABASE_HOST
{{- end }}

{{/*
Renders the database port
*/}}
{{- define "airbyte.database.port" }}
    {{- if .Values.postgresql.enabled }}
        {{- printf "%s" "5432" }}
    {{- else if .Values.global.database.port }}
        {{- .Values.global.database.port }}
    {{- else }}
        {{ $port := .Values.global.database.port | required "You must set `global.database.port` when using an external database" }}
    {{- end }}
{{- end }}

{{/*
Renders an environment variable definition that provides the database port
*/}}
{{- define "airbyte.database.port.env" }}
- name: DATABASE_PORT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DATABASE_PORT
{{- end }}

{{/*
Renders the database name
*/}}
{{- define "airbyte.database.name" }}
    {{- if .Values.postgresql.enabled }}
        {{- .Values.postgresql.postgresqlDatabase }}
    {{- else if .Values.global.database.database }}
        {{- .Values.global.database.database }}
    {{- else }}
        {{ $database := .Values.global.database.database | required "You must set `global.database.database` when using an external database" }}
    {{- end }}
{{- end }}

{{/*
Renders an environment variable definition that provides the database name
*/}}
{{- define "airbyte.database.name.env" }}
- name: DATABASE_DB
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DATABASE_DB
{{- end }}

{{/*
Renders the database user
*/}}
{{- define "airbyte.database.user" }}
    {{- if .Values.postgresql.enabled }}
        {{- .Values.postgresql.postgresqlUsername }}
    {{- else if .Values.global.database.user }}
        {{- .Values.global.database.user }}
    {{- else }}
        {{- if .Values.global.database.userSecretKey }}
            {{ $secretName := .Values.global.database.secretName | required "You must set `global.database.secretName` when using an external database" }}
        {{- else }}
            {{ $user := .Values.global.database.user | required "You must set `global.database.user` when using an external database" }}
        {{- end }}
    {{- end }}
{{- end }}

{{/*
Renders the name of the secret where the database user will be referenced
*/}}
{{- define "airbyte.database.userSecretKey" }}
    {{- if .Values.global.database.userSecretKey }}
        {{ $secretName := .Values.global.database.secretName | required "You must set `global.database.secretName` when using an external database" }}
        {{- .Values.global.database.userSecretKey }}
    {{- else }}
        {{- printf "%s" "DATABASE_USER" }}
    {{- end }}
{{- end }}

{{/*
Renders an environment variable definition that provides the database user
*/}}
{{- define "airbyte.database.user.env" }}
- name: DATABASE_USER
  valueFrom:
    secretKeyRef:
    {{- if .Values.global.database.userSecretKey }}
      name: {{ .Values.global.database.secretName }}
    {{- else }}
      name: {{ .Release.Name }}-airbyte-secrets
    {{- end }}
      key: {{ include "airbyte.database.userSecretKey" .}}
{{- end }}

{{/*
Renders the database password
*/}}
{{- define "airbyte.database.password" }}
    {{- if .Values.postgresql.enabled }}
        {{- .Values.postgresql.postgresqlPassword }}
    {{- else if .Values.global.database.password }}
        {{- .Values.global.database.password }}
    {{- else }}
        {{- if .Values.global.database.passwordSecretKey }}
            {{ $secretName := .Values.global.database.secretName | required "You must set `global.database.secretName` when using an external database" }}
        {{- else }}
            {{ $password := .Values.global.database.password }}
        {{- end }}
    {{- end }}
{{- end }}

{{/*
Renders the name of the secret where the database password will be referenced
*/}}
{{- define "airbyte.database.passwordSecretKey" }}
    {{- if .Values.global.database.passwordSecretKey }}
        {{ $secretName := .Values.global.database.secretName | required "You must set `global.database.secretName` when using an external database" }}
        {{- .Values.global.database.passwordSecretKey }}
    {{- else }}
        {{- printf "%s" "DATABASE_PASSWORD" }}
    {{- end }}
{{- end }}

{{/*
Renders an environment variable definition that provides the database password
*/}}
{{- define "airbyte.database.password.env" }}
- name: DATABASE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.database.secretName" . }}
      key: {{ include "airbyte.database.passwordSecretKey" . }}
{{- end }}


{{/*
Renders the database url (JDBC)
*/}}
{{- define "airbyte.database.url" }}
{{- $host := (include "airbyte.database.host" .) }}
{{- $dbName := (include "airbyte.database.name" .) }}
{{- $port := (include "airbyte.database.port" . ) }}
{{- printf "jdbc:postgresql://%s:%s/%s" $host $port $dbName }}
{{- end }}

{{/*
Renders an environment variable definition that provides the database url (JDBC)
*/}}
{{- define "airbyte.database.url.env" }}
- name: DATABASE_URL
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DATABASE_URL
{{- end }}

{{/*
Renders the name of the secret containing database credentials
*/}}
{{- define "airbyte.database.secretName" }}
    {{- if .Values.global.database.secretName }}
        {{- .Values.global.database.secretName | quote }}
    {{- else }}
        {{ .Release.Name }}-airbyte-secrets
    {{- end }}
{{- end }}

{{/*
Renders all of the common environment variables which provide database credentials
*/}}
{{- define "airbyte.database.envs" }}
{{ include "airbyte.database.host.env" . }}
{{ include "airbyte.database.port.env" . }}
{{ include "airbyte.database.name.env" . }}
{{ include "airbyte.database.user.env" . }}
{{ include "airbyte.database.password.env" . }}
{{ include "airbyte.database.url.env" . }}
{{- end }}

{{/*
Renders a set of database secrets to be included in the shared Airbyte secret
*/}}
{{- define "airbyte.database.secrets" }}
{{ $user := (include "airbyte.database.user" .)| trim }}
{{- if not (empty $user) }}
DATABASE_USER: {{ $user }}
{{- end }}
{{ $password := (include "airbyte.database.password" .)| trim }}
{{- if not (empty $password) }}
DATABASE_PASSWORD: {{ $password }}
{{- end}}
{{- end }}

{{/*
Renders a set of database configuration variables to be included in the shared Airbyte config map
*/}}
{{- define "airbyte.database.configVars" }}
DATABASE_HOST: {{ include "airbyte.database.host" . }}
DATABASE_PORT: {{ include "airbyte.database.port" . | quote }}
DATABASE_DB: {{ include "airbyte.database.name" . }}
DATABASE_URL: {{ include "airbyte.database.url" . }}
{{- if .Values.global.database.user }}
DATABASE_USER: {{ include "airbyte.database.user" . }}
{{- end}}
{{- if .Values.global.database.password }}
DATABASE_PASSWORD: {{ include "airbyte.database.password" . }}
{{- end}}
{{- end }}
