{{/*
Database Configuration
*/}}

{{/*
Renders the global.migrations secret name
*/}}
{{- define "airbyte.database.migrations.secretName" }}
{{- if .Values.global.migrations.secretName }}
  {{- .Values.global.migrations.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.migrations.runAtStartup value
*/}}
{{- define "airbyte.database.migrations.runAtStartup" }}
{{- .Values.global.migrations.runAtStartup | default true }}
{{- end }}

{{/*
Renders the RUN_DATABASE_MIGRATION_ON_STARTUP environment variable
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
Renders the CONFIGS_DATABASE_MINIMUM_FLYWAY_MIGRATION_VERSION environment variable
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
Renders the JOBS_DATABASE_MINIMUM_FLYWAY_MIGRATION_VERSION environment variable
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

{{/*
Renders the set of all database.migrations secrets
*/}}
{{- define "airbyte.database.migrations.secrets" }}
{{- end }}

{{/*
Database Configuration
*/}}

{{/*
Renders the global.database secret name
*/}}
{{- define "airbyte.database.secretName" }}
{{- if .Values.global.database.secretName }}
  {{- .Values.global.database.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.database.host value
*/}}
{{- define "airbyte.database.host" }}
{{- .Values.global.database.host | default (printf "airbyte-db-svc.%s.svc.cluster.local" .Release.Namespace) }}
{{- end }}

{{/*
Renders the DATABASE_HOST environment variable
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
{{- .Values.global.database.port | default "5432" }}
{{- end }}

{{/*
Renders the DATABASE_PORT environment variable
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
{{- printf "jdbc:postgresql://%s:%d/%s" (include "airbyte.database.host" .) (int (include "airbyte.database.port" .)) (include "airbyte.database.name" .) }}
{{- end }}

{{/*
Renders the DATABASE_URL environment variable
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
Renders the DATABASE_USER environment variable
*/}}
{{- define "airbyte.database.user.env" }}
- name: DATABASE_USER
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.database.secretName" . }}
      key: {{ .Values.global.database.userSecretKey | default "DATABASE_USER" }}
{{- end }}

{{/*
Renders the global.database.password value
*/}}
{{- define "airbyte.database.password" }}
{{- .Values.global.database.password | default "airbyte" }}
{{- end }}

{{/*
Renders the DATABASE_PASSWORD environment variable
*/}}
{{- define "airbyte.database.password.env" }}
- name: DATABASE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.database.secretName" . }}
      key: {{ .Values.global.database.passwordSecretKey | default "DATABASE_PASSWORD" }}
{{- end }}

{{/*
Renders the global.database.name value
*/}}
{{- define "airbyte.database.name" }}
{{- .Values.global.database.name | default "db-airbyte" }}
{{- end }}

{{/*
Renders the DATABASE_DB environment variable
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
{{- include "airbyte.database.user.env" . }}
{{- include "airbyte.database.password.env" . }}
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
Renders the set of all database secrets
*/}}
{{- define "airbyte.database.secrets" }}
{{- if not (empty (include "airbyte.database.user" .)) }}
DATABASE_USER: {{ include "airbyte.database.user" . | quote }}
{{- end }}
{{- if not (empty (include "airbyte.database.password" .)) }}
DATABASE_PASSWORD: {{ include "airbyte.database.password" . | quote }}
{{- end }}
{{- end }}

