{{/*
Keycloak Configuration
*/}}

{{/*
Renders the keycloak secret name
*/}}
{{- define "airbyte.keycloak.secretName" }}
{{- if .Values.keycloak.secretName }}
  {{- .Values.keycloak.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the keycloak.auth.adminUsername value
*/}}
{{- define "airbyte.keycloak.auth.adminUsername" }}
{{- .Values.keycloak.auth.adminUsername  }}
{{- end }}

{{/*
Renders the KEYCLOAK_ADMIN_USER environment variable
*/}}
{{- define "airbyte.keycloak.auth.adminUsername.env" }}
- name: KEYCLOAK_ADMIN_USER
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.keycloak.secretName" . }}
      key: {{ .Values.keycloak.auth.adminUsernameSecretKey | default "KEYCLOAK_ADMIN_USER" }}
{{- end }}

{{/*
Renders the keycloak.auth.adminPassword value
*/}}
{{- define "airbyte.keycloak.auth.adminPassword" }}
{{- .Values.keycloak.auth.adminPassword  }}
{{- end }}

{{/*
Renders the KEYCLOAK_ADMIN_PASSWORD environment variable
*/}}
{{- define "airbyte.keycloak.auth.adminPassword.env" }}
- name: KEYCLOAK_ADMIN_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.keycloak.secretName" . }}
      key: {{ .Values.keycloak.auth.adminPasswordSecretKey | default "KEYCLOAK_ADMIN_PASSWORD" }}
{{- end }}

{{/*
Renders the keycloak.auth.adminRealm value
*/}}
{{- define "airbyte.keycloak.auth.adminRealm" }}
{{- .Values.keycloak.auth.adminRealm  }}
{{- end }}

{{/*
Renders the KEYCLOAK_ADMIN_REALM environment variable
*/}}
{{- define "airbyte.keycloak.auth.adminRealm.env" }}
- name: KEYCLOAK_ADMIN_REALM
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: KEYCLOAK_ADMIN_REALM
{{- end }}

{{/*
Renders the keycloak.auth.adminCliClientId value
*/}}
{{- define "airbyte.keycloak.auth.adminCliClientId" }}
{{- .Values.keycloak.auth.adminCliClientId  }}
{{- end }}

{{/*
Renders the KEYCLOAK_ADMIN_CLI_CLIENT_ID environment variable
*/}}
{{- define "airbyte.keycloak.auth.adminCliClientId.env" }}
- name: KEYCLOAK_ADMIN_CLI_CLIENT_ID
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: KEYCLOAK_ADMIN_CLI_CLIENT_ID
{{- end }}

{{/*
Renders the keycloak.database.name value
*/}}
{{- define "airbyte.keycloak.database.name" }}
{{- .Values.keycloak.database.name | default "db-airbyte" }}
{{- end }}

{{/*
Renders the KEYCLOAK_DATABASE_NAME environment variable
*/}}
{{- define "airbyte.keycloak.database.name.env" }}
- name: KEYCLOAK_DATABASE_NAME
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: KEYCLOAK_DATABASE_NAME
{{- end }}

{{/*
Renders the keycloak.database.host value
*/}}
{{- define "airbyte.keycloak.database.host" }}
{{- .Values.keycloak.database.host | default (printf "airbyte-db-svc.%s.svc.cluster.local" .Release.Namespace) }}
{{- end }}

{{/*
Renders the KEYCLOAK_DATABASE_HOST environment variable
*/}}
{{- define "airbyte.keycloak.database.host.env" }}
- name: KEYCLOAK_DATABASE_HOST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: KEYCLOAK_DATABASE_HOST
{{- end }}

{{/*
Renders the keycloak.database.port value
*/}}
{{- define "airbyte.keycloak.database.port" }}
{{- .Values.keycloak.database.port | default "5432" }}
{{- end }}

{{/*
Renders the KEYCLOAK_DATABASE_PORT environment variable
*/}}
{{- define "airbyte.keycloak.database.port.env" }}
- name: KEYCLOAK_DATABASE_PORT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: KEYCLOAK_DATABASE_PORT
{{- end }}

{{/*
Renders the keycloak.database.user value
*/}}
{{- define "airbyte.keycloak.database.user" }}
{{- .Values.keycloak.database.user | default "airbyte" }}
{{- end }}

{{/*
Renders the KEYCLOAK_DATABASE_USERNAME environment variable
*/}}
{{- define "airbyte.keycloak.database.user.env" }}
- name: KEYCLOAK_DATABASE_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.keycloak.secretName" . }}
      key: {{ .Values.keycloak.database.userSecretKey | default "KEYCLOAK_DATABASE_USERNAME" }}
{{- end }}

{{/*
Renders the keycloak.database.password value
*/}}
{{- define "airbyte.keycloak.database.password" }}
{{- .Values.keycloak.database.password | default "airbyte" }}
{{- end }}

{{/*
Renders the KEYCLOAK_DATABASE_PASSWORD environment variable
*/}}
{{- define "airbyte.keycloak.database.password.env" }}
- name: KEYCLOAK_DATABASE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.keycloak.secretName" . }}
      key: {{ .Values.keycloak.database.passwordSecretKey | default "KEYCLOAK_DATABASE_PASSWORD" }}
{{- end }}

{{/*
Renders the keycloak.database.url value
*/}}
{{- define "airbyte.keycloak.database.url" }}
{{- (printf "jdbc:postgresql://%s:%d/%s?currentSchema=keycloak" (include "airbyte.keycloak.database.host" .) (int (include "airbyte.keycloak.database.port" .)) (include "airbyte.keycloak.database.name" .)) }}
{{- end }}

{{/*
Renders the KEYCLOAK_DATABASE_URL environment variable
*/}}
{{- define "airbyte.keycloak.database.url.env" }}
- name: KEYCLOAK_DATABASE_URL
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: KEYCLOAK_DATABASE_URL
{{- end }}

{{/*
Renders the keycloak.internalHost value
*/}}
{{- define "airbyte.keycloak.internalHost" }}
{{- ternary (printf "%s-airbyte-keycloak-svc:%d" .Release.Name (int .Values.keycloak.service.port)) "localhost" (or (eq .Values.global.edition "enterprise") (eq .Values.global.edition "pro")) }}
{{- end }}

{{/*
Renders the KEYCLOAK_INTERNAL_HOST environment variable
*/}}
{{- define "airbyte.keycloak.internalHost.env" }}
- name: KEYCLOAK_INTERNAL_HOST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: KEYCLOAK_INTERNAL_HOST
{{- end }}

{{/*
Renders the keycloak.internalProtocol value
*/}}
{{- define "airbyte.keycloak.internalProtocol" }}
{{- "http" }}
{{- end }}

{{/*
Renders the KEYCLOAK_INTERNAL_PROTOCOL environment variable
*/}}
{{- define "airbyte.keycloak.internalProtocol.env" }}
- name: KEYCLOAK_INTERNAL_PROTOCOL
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: KEYCLOAK_INTERNAL_PROTOCOL
{{- end }}

{{/*
Renders the keycloak.clientRealm value
*/}}
{{- define "airbyte.keycloak.clientRealm" }}
{{- .Values.keycloak.clientRealm | default "_airbyte-application-clients" }}
{{- end }}

{{/*
Renders the KEYCLOAK_CLIENT_REALM environment variable
*/}}
{{- define "airbyte.keycloak.clientRealm.env" }}
- name: KEYCLOAK_CLIENT_REALM
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: KEYCLOAK_CLIENT_REALM
{{- end }}

{{/*
Renders the keycloak.realmIssuer value
*/}}
{{- define "airbyte.keycloak.realmIssuer" }}
{{- ternary (printf "%s/auth/realms/_airbyte-internal" .Values.global.airbyteUrl) (printf "%s-airbyte-keycloak-svc:8001/auth/realms/_airbyte-internal" .Release.Name) (eq (include "airbyte.airbyte.cluster.type" .) "data-plane") }}
{{- end }}

{{/*
Renders the KEYCLOAK_INTERNAL_REALM_ISSUER environment variable
*/}}
{{- define "airbyte.keycloak.realmIssuer.env" }}
- name: KEYCLOAK_INTERNAL_REALM_ISSUER
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: KEYCLOAK_INTERNAL_REALM_ISSUER
{{- end }}

{{/*
Renders the keycloak.service.port value
*/}}
{{- define "airbyte.keycloak.service.port" }}
{{- .Values.keycloak.service.port | default 8180 }}
{{- end }}

{{/*
Renders the KEYCLOAK_PORT environment variable
*/}}
{{- define "airbyte.keycloak.service.port.env" }}
- name: KEYCLOAK_PORT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: KEYCLOAK_PORT
{{- end }}

{{/*
Renders the set of all keycloak environment variables
*/}}
{{- define "airbyte.keycloak.envs" }}
{{- include "airbyte.keycloak.auth.adminUsername.env" . }}
{{- include "airbyte.keycloak.auth.adminPassword.env" . }}
{{- include "airbyte.keycloak.auth.adminRealm.env" . }}
{{- include "airbyte.keycloak.auth.adminCliClientId.env" . }}
{{- include "airbyte.keycloak.database.name.env" . }}
{{- include "airbyte.keycloak.database.host.env" . }}
{{- include "airbyte.keycloak.database.port.env" . }}
{{- include "airbyte.keycloak.database.user.env" . }}
{{- include "airbyte.keycloak.database.password.env" . }}
{{- include "airbyte.keycloak.database.url.env" . }}
{{- include "airbyte.keycloak.internalHost.env" . }}
{{- include "airbyte.keycloak.internalProtocol.env" . }}
{{- include "airbyte.keycloak.clientRealm.env" . }}
{{- include "airbyte.keycloak.realmIssuer.env" . }}
{{- include "airbyte.keycloak.service.port.env" . }}
{{- end }}

{{/*
Renders the set of all keycloak config map variables
*/}}
{{- define "airbyte.keycloak.configVars" }}
KEYCLOAK_ADMIN_REALM: {{ include "airbyte.keycloak.auth.adminRealm" . | quote }}
KEYCLOAK_ADMIN_CLI_CLIENT_ID: {{ include "airbyte.keycloak.auth.adminCliClientId" . | quote }}
KEYCLOAK_DATABASE_NAME: {{ include "airbyte.keycloak.database.name" . | quote }}
KEYCLOAK_DATABASE_HOST: {{ include "airbyte.keycloak.database.host" . | quote }}
KEYCLOAK_DATABASE_PORT: {{ include "airbyte.keycloak.database.port" . | quote }}
KEYCLOAK_DATABASE_URL: {{ include "airbyte.keycloak.database.url" . | quote }}
KEYCLOAK_INTERNAL_HOST: {{ include "airbyte.keycloak.internalHost" . | quote }}
KEYCLOAK_INTERNAL_PROTOCOL: {{ include "airbyte.keycloak.internalProtocol" . | quote }}
KEYCLOAK_CLIENT_REALM: {{ include "airbyte.keycloak.clientRealm" . | quote }}
KEYCLOAK_INTERNAL_REALM_ISSUER: {{ include "airbyte.keycloak.realmIssuer" . | quote }}
KEYCLOAK_PORT: {{ include "airbyte.keycloak.service.port" . | quote }}
{{- end }}

{{/*
Renders the set of all keycloak secrets
*/}}
{{- define "airbyte.keycloak.secrets" }}
{{- if not (empty (include "airbyte.keycloak.auth.adminUsername" .)) }}
KEYCLOAK_ADMIN_USER: {{ include "airbyte.keycloak.auth.adminUsername" . | quote }}
{{- end }}
{{- if not (empty (include "airbyte.keycloak.auth.adminPassword" .)) }}
KEYCLOAK_ADMIN_PASSWORD: {{ include "airbyte.keycloak.auth.adminPassword" . | quote }}
{{- end }}
{{- if not (empty (include "airbyte.keycloak.database.user" .)) }}
KEYCLOAK_DATABASE_USERNAME: {{ include "airbyte.keycloak.database.user" . | quote }}
{{- end }}
{{- if not (empty (include "airbyte.keycloak.database.password" .)) }}
KEYCLOAK_DATABASE_PASSWORD: {{ include "airbyte.keycloak.database.password" . | quote }}
{{- end }}
{{- end }}

{{/*
Keycloak Configuration
*/}}

{{/*
Renders the global.keycloak.javaOpts secret name
*/}}
{{- define "airbyte.keycloak.javaOpts.secretName" }}
{{- if .Values.global.keycloak.javaOpts.secretName }}
  {{- .Values.global.keycloak.javaOpts.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.keycloak.javaOpts.javaOpts value
*/}}
{{- define "airbyte.keycloak.javaOpts.javaOpts" }}
{{- (printf "-Djgroups.dns.query=%s-airbyte-keycloak-headless-svc" .Release.Name) }}
{{- end }}

{{/*
Renders the JAVA_OPTS_APPEND environment variable
*/}}
{{- define "airbyte.keycloak.javaOpts.javaOpts.env" }}
- name: JAVA_OPTS_APPEND
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: JAVA_OPTS_APPEND
{{- end }}

{{/*
Renders the set of all keycloak.javaOpts environment variables
*/}}
{{- define "airbyte.keycloak.javaOpts.envs" }}
{{- include "airbyte.keycloak.javaOpts.javaOpts.env" . }}
{{- end }}

{{/*
Renders the set of all keycloak.javaOpts config map variables
*/}}
{{- define "airbyte.keycloak.javaOpts.configVars" }}
JAVA_OPTS_APPEND: {{ include "airbyte.keycloak.javaOpts.javaOpts" . | quote }}
{{- end }}

{{/*
Renders the set of all keycloak.javaOpts secrets
*/}}
{{- define "airbyte.keycloak.javaOpts.secrets" }}
{{- end }}

