
{{/* DO NOT EDIT: This file was autogenerated. */}}

{{/*
    Database Configuration
*/}}

{{/*
Renders the database secret name
*/}}
{{- define "airbyte.database.secretName" }}
{{- if .Values.global.database.secretName }}
    {{- .Values.global.database.secretName }}
{{- else }}
    {{- .Values.global.secretName | default (printf "%s-airbyte-secrets" .Release.Name) }}
{{- end }}
{{- end }}

{{/*
Renders the global.database.host value
*/}}
{{- define "airbyte.database.host" }}
    {{- .Values.global.database.host | default (printf "airbyte-db-svc.%s.svc.cluster.local" .Release.Namespace) }}
{{- end }}

{{/*
Renders the database.host environment variable
*/}}
{{- define "airbyte.database.host.env" }}
- name: DATABASE_HOST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DATABASE_HOST
{{- end }}

{{/*
Renders the global.database.port value
*/}}
{{- define "airbyte.database.port" }}
    {{- .Values.global.database.port | default 5432 }}
{{- end }}

{{/*
Renders the database.port environment variable
*/}}
{{- define "airbyte.database.port.env" }}
- name: DATABASE_PORT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DATABASE_PORT
{{- end }}

{{/*
Renders the global.database.url value
*/}}
{{- define "airbyte.database.url" }}
    {{- (printf "jdbc:postgresql://%s:%d/%s" (include "airbyte.database.host" .) (int (include "airbyte.database.port" .)) (include "airbyte.database.name" .)) }}
{{- end }}

{{/*
Renders the database.url environment variable
*/}}
{{- define "airbyte.database.url.env" }}
- name: DATABASE_URL
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DATABASE_URL
{{- end }}

{{/*
Renders the global.database.user value
*/}}
{{- define "airbyte.database.user" }}
    {{- .Values.global.database.user | default "airbyte" }}
{{- end }}

{{/*
Renders the database.user secret key
*/}}
{{- define "airbyte.database.user.secretKey" }}
	{{- .Values.global.database.userSecretKey | default "DATABASE_USER" }}
{{- end }}

{{/*
Renders the database.user environment variable
*/}}
{{- define "airbyte.database.user.env" }}
- name: DATABASE_USER
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.database.secretName" . }}
      key: {{ include "airbyte.database.user.secretKey" . }}
{{- end }}

{{/*
Renders the global.database.password value
*/}}
{{- define "airbyte.database.password" }}
    {{- .Values.global.database.password | default "airbyte" }}
{{- end }}

{{/*
Renders the database.password secret key
*/}}
{{- define "airbyte.database.password.secretKey" }}
	{{- .Values.global.database.passwordSecretKey | default "DATABASE_PASSWORD" }}
{{- end }}

{{/*
Renders the database.password environment variable
*/}}
{{- define "airbyte.database.password.env" }}
- name: DATABASE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.database.secretName" . }}
      key: {{ include "airbyte.database.password.secretKey" . }}
{{- end }}

{{/*
Renders the global.database.name value
*/}}
{{- define "airbyte.database.name" }}
    {{- .Values.global.database.name | default "db-airbyte" }}
{{- end }}

{{/*
Renders the database.name environment variable
*/}}
{{- define "airbyte.database.name.env" }}
- name: DATABASE_DB
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DATABASE_DB
{{- end }}

{{/*
Renders the set of all database environment variables
*/}}
{{- define "airbyte.database.envs" }}
{{- include "airbyte.database.host.env" . }}
{{- include "airbyte.database.port.env" . }}
{{- include "airbyte.database.url.env" . }}
{{- if (eq (include "airbyte.database.cloudSqlProxy.enabled" .) "false") }}
{{- include "airbyte.database.user.env" . }}
{{- end }}
{{- if (eq (include "airbyte.database.cloudSqlProxy.enabled" .) "false") }}
{{- include "airbyte.database.password.env" . }}
{{- end }}
{{- include "airbyte.database.name.env" . }}
{{- end }}

{{/*
Renders the set of all database config map variables
*/}}
{{- define "airbyte.database.configVars" }}
DATABASE_HOST: {{ include "airbyte.database.host" . | quote }}
DATABASE_PORT: {{ include "airbyte.database.port" . | quote }}
DATABASE_URL: {{ include "airbyte.database.url" . | quote }}
DATABASE_DB: {{ include "airbyte.database.name" . | quote }}
{{- end }}

{{/*
Renders the set of all database secret variables
*/}}
{{- define "airbyte.database.secrets" }}
{{- if (eq (include "airbyte.database.cloudSqlProxy.enabled" .) "false") }}
DATABASE_USER: {{ include "airbyte.database.user" . | quote }}
{{- end }}
{{- if (eq (include "airbyte.database.cloudSqlProxy.enabled" .) "false") }}
DATABASE_PASSWORD: {{ include "airbyte.database.password" . | quote }}
{{- end }}
{{- end }}

{{/*
Renders the global.cloudSqlProxy.enabled value
*/}}
{{- define "airbyte.database.cloudSqlProxy.enabled" }}
	{{- if eq .Values.global.cloudSqlProxy.enabled nil }}
    	{{- false }}
	{{- else }}
    	{{- .Values.global.cloudSqlProxy.enabled }}
	{{- end }}
{{- end }}

{{/*
Renders the database.cloudSqlProxy.enabled environment variable
*/}}
{{- define "airbyte.database.cloudSqlProxy.enabled.env" }}
- name: USE_CLOUD_SQL_PROXY
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: USE_CLOUD_SQL_PROXY
{{- end }}

{{/*
Renders the set of all database.cloudSqlProxy environment variables
*/}}
{{- define "airbyte.database.cloudSqlProxy.envs" }}
{{- include "airbyte.database.cloudSqlProxy.enabled.env" . }}
{{- end }}

{{/*
Renders the set of all database.cloudSqlProxy config map variables
*/}}
{{- define "airbyte.database.cloudSqlProxy.configVars" }}
USE_CLOUD_SQL_PROXY: {{ include "airbyte.database.cloudSqlProxy.enabled" . | quote }}
{{- end }}

{{/*
Renders the global.migrations.runAtStartup value
*/}}
{{- define "airbyte.database.migrations.runAtStartup" }}
	{{- if eq .Values.global.migrations.runAtStartup nil }}
    	{{- true }}
	{{- else }}
    	{{- .Values.global.migrations.runAtStartup }}
	{{- end }}
{{- end }}

{{/*
Renders the database.migrations.runAtStartup environment variable
*/}}
{{- define "airbyte.database.migrations.runAtStartup.env" }}
- name: RUN_DATABASE_MIGRATION_ON_STARTUP
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: RUN_DATABASE_MIGRATION_ON_STARTUP
{{- end }}

{{/*
Renders the global.migrations.configDb.minimumFlywayMigrationVersion value
*/}}
{{- define "airbyte.database.migrations.configDb.minimumFlywayMigrationVersion" }}
    {{- .Values.global.migrations.configDb.minimumFlywayMigrationVersion | default "0.35.15.001" }}
{{- end }}

{{/*
Renders the database.migrations.configDb.minimumFlywayMigrationVersion environment variable
*/}}
{{- define "airbyte.database.migrations.configDb.minimumFlywayMigrationVersion.env" }}
- name: CONFIGS_DATABASE_MINIMUM_FLYWAY_MIGRATION_VERSION
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CONFIGS_DATABASE_MINIMUM_FLYWAY_MIGRATION_VERSION
{{- end }}

{{/*
Renders the global.migrations.jobsDb.minimumFlywayMigrationVersion value
*/}}
{{- define "airbyte.database.migrations.jobsDb.minimumFlywayMigrationVersion" }}
    {{- .Values.global.migrations.jobsDb.minimumFlywayMigrationVersion | default "0.29.15.001" }}
{{- end }}

{{/*
Renders the database.migrations.jobsDb.minimumFlywayMigrationVersion environment variable
*/}}
{{- define "airbyte.database.migrations.jobsDb.minimumFlywayMigrationVersion.env" }}
- name: JOBS_DATABASE_MINIMUM_FLYWAY_MIGRATION_VERSION
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: JOBS_DATABASE_MINIMUM_FLYWAY_MIGRATION_VERSION
{{- end }}

{{/*
Renders the set of all database.migrations environment variables
*/}}
{{- define "airbyte.database.migrations.envs" }}
{{- include "airbyte.database.migrations.runAtStartup.env" . }}
{{- include "airbyte.database.migrations.configDb.minimumFlywayMigrationVersion.env" . }}
{{- include "airbyte.database.migrations.jobsDb.minimumFlywayMigrationVersion.env" . }}
{{- end }}

{{/*
Renders the set of all database.migrations config map variables
*/}}
{{- define "airbyte.database.migrations.configVars" }}
RUN_DATABASE_MIGRATION_ON_STARTUP: {{ include "airbyte.database.migrations.runAtStartup" . | quote }}
CONFIGS_DATABASE_MINIMUM_FLYWAY_MIGRATION_VERSION: {{ include "airbyte.database.migrations.configDb.minimumFlywayMigrationVersion" . | quote }}
JOBS_DATABASE_MINIMUM_FLYWAY_MIGRATION_VERSION: {{ include "airbyte.database.migrations.jobsDb.minimumFlywayMigrationVersion" . | quote }}
{{- end }}
