{{/*
Auth Configuration
*/}}

{{/*
Renders the global.auth.jwt secret name
*/}}
{{- define "airbyte.auth.jwt.secretName" }}
  {{- .Values.global.auth.instanceAdmin.secretName }}
{{- end }}

{{/*
Renders the global.auth.jwt.jwtSignatureSecret value
*/}}
{{- define "airbyte.auth.jwt.jwtSignatureSecret" }}
{{- .Values.global.auth.jwt.jwtSignatureSecret  }}
{{- end }}

{{/*
Renders the AB_JWT_SIGNATURE_SECRET environment variable
*/}}
{{- define "airbyte.auth.jwt.jwtSignatureSecret.env" }}
- name: AB_JWT_SIGNATURE_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.auth.jwt.secretName" . }}
      key: JWT_SIGNATURE_SECRET
{{- end }}

{{/*
Renders the set of all auth.jwt environment variables
*/}}
{{- define "airbyte.auth.jwt.envs" }}
{{- include "airbyte.auth.jwt.jwtSignatureSecret.env" . }}
{{- end }}

{{/*
Renders the set of all auth.jwt config map variables
*/}}
{{- define "airbyte.auth.jwt.configVars" }}
{{- end }}

{{/*
Renders the set of all auth.jwt secrets
*/}}
{{- define "airbyte.auth.jwt.secrets" }}
{{- if not (empty (include "airbyte.auth.jwt.jwtSignatureSecret" .)) }}
AB_JWT_SIGNATURE_SECRET: {{ include "airbyte.auth.jwt.jwtSignatureSecret" . | quote }}
{{- end }}
{{- end }}

{{/*
Auth Configuration
*/}}

{{/*
Renders the global.auth.identityProvider secret name
*/}}
{{- define "airbyte.auth.identityProvider.secretName" }}
{{- if .Values.global.auth.identityProvider.secretName }}
  {{- .Values.global.auth.identityProvider.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.auth.identityProvider.type value
*/}}
{{- define "airbyte.auth.identityProvider.type" }}
{{- .Values.global.auth.identityProvider.type  }}
{{- end }}

{{/*
Renders the IDENTITY_PROVIDER_TYPE environment variable
*/}}
{{- define "airbyte.auth.identityProvider.type.env" }}
- name: IDENTITY_PROVIDER_TYPE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: IDENTITY_PROVIDER_TYPE
{{- end }}

{{/*
Renders the global.auth.identityProvider.oidc.domain value
*/}}
{{- define "airbyte.auth.identityProvider.oidc.domain" }}
{{- .Values.global.auth.identityProvider.oidc.domain  }}
{{- end }}

{{/*
Renders the OIDC_DOMAIN environment variable
*/}}
{{- define "airbyte.auth.identityProvider.oidc.domain.env" }}
- name: OIDC_DOMAIN
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: OIDC_DOMAIN
{{- end }}

{{/*
Renders the global.auth.identityProvider.oidc.appName value
*/}}
{{- define "airbyte.auth.identityProvider.oidc.appName" }}
{{- .Values.global.auth.identityProvider.oidc.appName  }}
{{- end }}

{{/*
Renders the OIDC_APP_NAME environment variable
*/}}
{{- define "airbyte.auth.identityProvider.oidc.appName.env" }}
- name: OIDC_APP_NAME
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: OIDC_APP_NAME
{{- end }}

{{/*
Renders the global.auth.identityProvider.oidc.clientId value
*/}}
{{- define "airbyte.auth.identityProvider.oidc.clientId" }}
{{- .Values.global.auth.identityProvider.oidc.clientId  }}
{{- end }}

{{/*
Renders the OIDC_CLIENT_ID environment variable
*/}}
{{- define "airbyte.auth.identityProvider.oidc.clientId.env" }}
- name: OIDC_CLIENT_ID
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.auth.identityProvider.secretName" . }}
      key: {{ .Values.global.auth.identityProvider.oidc.clientIdSecretKey | default "OIDC_CLIENT_ID" }}
{{- end }}

{{/*
Renders the global.auth.identityProvider.oidc.clientSecret value
*/}}
{{- define "airbyte.auth.identityProvider.oidc.clientSecret" }}
{{- .Values.global.auth.identityProvider.oidc.clientSecret  }}
{{- end }}

{{/*
Renders the OIDC_CLIENT_SECRET environment variable
*/}}
{{- define "airbyte.auth.identityProvider.oidc.clientSecret.env" }}
- name: OIDC_CLIENT_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.auth.identityProvider.secretName" . }}
      key: {{ .Values.global.auth.identityProvider.oidc.clientSecretSecretKey | default "OIDC_CLIENT_SECRET" }}
{{- end }}

{{/*
Renders the set of all auth.identityProvider environment variables
*/}}
{{- define "airbyte.auth.identityProvider.envs" }}
{{- include "airbyte.auth.identityProvider.type.env" . }}
{{- include "airbyte.auth.identityProvider.oidc.domain.env" . }}
{{- include "airbyte.auth.identityProvider.oidc.appName.env" . }}
{{- include "airbyte.auth.identityProvider.oidc.clientId.env" . }}
{{- include "airbyte.auth.identityProvider.oidc.clientSecret.env" . }}
{{- end }}

{{/*
Renders the set of all auth.identityProvider config map variables
*/}}
{{- define "airbyte.auth.identityProvider.configVars" }}
IDENTITY_PROVIDER_TYPE: {{ include "airbyte.auth.identityProvider.type" . | quote }}
OIDC_DOMAIN: {{ include "airbyte.auth.identityProvider.oidc.domain" . | quote }}
OIDC_APP_NAME: {{ include "airbyte.auth.identityProvider.oidc.appName" . | quote }}
{{- end }}

{{/*
Renders the set of all auth.identityProvider secrets
*/}}
{{- define "airbyte.auth.identityProvider.secrets" }}
{{- if not (empty (include "airbyte.auth.identityProvider.oidc.clientId" .)) }}
OIDC_CLIENT_ID: {{ include "airbyte.auth.identityProvider.oidc.clientId" . | quote }}
{{- end }}
{{- if not (empty (include "airbyte.auth.identityProvider.oidc.clientSecret" .)) }}
OIDC_CLIENT_SECRET: {{ include "airbyte.auth.identityProvider.oidc.clientSecret" . | quote }}
{{- end }}
{{- end }}

{{/*
Auth Configuration
*/}}

{{/*
Renders the global.auth secret name
*/}}
{{- define "airbyte.auth.secretName" }}
  {{- .Values.global.auth.instanceAdmin.secretName }}
{{- end }}

{{/*
Renders the global.auth.instanceAdmin.password value
*/}}
{{- define "airbyte.auth.instanceAdmin.password" }}
{{- .Values.global.auth.instanceAdmin.password  }}
{{- end }}

{{/*
Renders the AB_INSTANCE_ADMIN_PASSWORD environment variable
*/}}
{{- define "airbyte.auth.instanceAdmin.password.env" }}
- name: AB_INSTANCE_ADMIN_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.auth.secretName" . }}
      key: INSTANCE_ADMIN_PASSWORD
{{- end }}

{{/*
Renders the set of all auth environment variables
*/}}
{{- define "airbyte.auth.envs" }}
{{- include "airbyte.auth.instanceAdmin.password.env" . }}
{{- end }}

{{/*
Renders the set of all auth config map variables
*/}}
{{- define "airbyte.auth.configVars" }}
{{- end }}

{{/*
Renders the set of all auth secrets
*/}}
{{- define "airbyte.auth.secrets" }}
{{- if not (empty (include "airbyte.auth.instanceAdmin.password" .)) }}
AB_INSTANCE_ADMIN_PASSWORD: {{ include "airbyte.auth.instanceAdmin.password" . | quote }}
{{- end }}
{{- end }}

{{/*
Auth Configuration
*/}}

{{/*
Renders the global.auth.instanceAdmin secret name
*/}}
{{- define "airbyte.auth.instanceAdmin.enterprise.secretName" }}
{{- if .Values.global.auth.instanceAdmin.secretName }}
  {{- .Values.global.auth.instanceAdmin.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.auth.instanceAdmin.firstName value
*/}}
{{- define "airbyte.auth.instanceAdmin.enterprise.firstName" }}
{{- .Values.global.auth.instanceAdmin.firstName  }}
{{- end }}

{{/*
Renders the INITIAL_USER_FIRST_NAME environment variable
*/}}
{{- define "airbyte.auth.instanceAdmin.enterprise.firstName.env" }}
- name: INITIAL_USER_FIRST_NAME
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: INITIAL_USER_FIRST_NAME
{{- end }}

{{/*
Renders the global.auth.instanceAdmin.lastName value
*/}}
{{- define "airbyte.auth.instanceAdmin.enterprise.lastName" }}
{{- .Values.global.auth.instanceAdmin.lastName  }}
{{- end }}

{{/*
Renders the INITIAL_USER_LAST_NAME environment variable
*/}}
{{- define "airbyte.auth.instanceAdmin.enterprise.lastName.env" }}
- name: INITIAL_USER_LAST_NAME
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: INITIAL_USER_LAST_NAME
{{- end }}

{{/*
Renders the global.auth.instanceAdmin.email value
*/}}
{{- define "airbyte.auth.instanceAdmin.enterprise.email" }}
{{- .Values.global.auth.instanceAdmin.email  }}
{{- end }}

{{/*
Renders the INITIAL_USER_EMAIL environment variable
*/}}
{{- define "airbyte.auth.instanceAdmin.enterprise.email.env" }}
- name: INITIAL_USER_EMAIL
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.auth.instanceAdmin.enterprise.secretName" . }}
      key: {{ .Values.global.auth.instanceAdmin.emailSecretKey | default "INITIAL_USER_EMAIL" }}
{{- end }}

{{/*
Renders the global.auth.instanceAdmin.password value
*/}}
{{- define "airbyte.auth.instanceAdmin.enterprise.password" }}
{{- .Values.global.auth.instanceAdmin.password  }}
{{- end }}

{{/*
Renders the INITIAL_USER_PASSWORD environment variable
*/}}
{{- define "airbyte.auth.instanceAdmin.enterprise.password.env" }}
- name: INITIAL_USER_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.auth.instanceAdmin.enterprise.secretName" . }}
      key: {{ .Values.global.auth.instanceAdmin.passwordSecretKey | default "INITIAL_USER_PASSWORD" }}
{{- end }}

{{/*
Renders the set of all auth.instanceAdmin.enterprise environment variables
*/}}
{{- define "airbyte.auth.instanceAdmin.enterprise.envs" }}
{{- include "airbyte.auth.instanceAdmin.enterprise.firstName.env" . }}
{{- include "airbyte.auth.instanceAdmin.enterprise.lastName.env" . }}
{{- include "airbyte.auth.instanceAdmin.enterprise.email.env" . }}
{{- include "airbyte.auth.instanceAdmin.enterprise.password.env" . }}
{{- end }}

{{/*
Renders the set of all auth.instanceAdmin.enterprise config map variables
*/}}
{{- define "airbyte.auth.instanceAdmin.enterprise.configVars" }}
INITIAL_USER_FIRST_NAME: {{ include "airbyte.auth.instanceAdmin.enterprise.firstName" . | quote }}
INITIAL_USER_LAST_NAME: {{ include "airbyte.auth.instanceAdmin.enterprise.lastName" . | quote }}
{{- end }}

{{/*
Renders the set of all auth.instanceAdmin.enterprise secrets
*/}}
{{- define "airbyte.auth.instanceAdmin.enterprise.secrets" }}
{{- if not (empty (include "airbyte.auth.instanceAdmin.enterprise.email" .)) }}
INITIAL_USER_EMAIL: {{ include "airbyte.auth.instanceAdmin.enterprise.email" . | quote }}
{{- end }}
{{- if not (empty (include "airbyte.auth.instanceAdmin.enterprise.password" .)) }}
INITIAL_USER_PASSWORD: {{ include "airbyte.auth.instanceAdmin.enterprise.password" . | quote }}
{{- end }}
{{- end }}

{{/*
Auth Configuration
*/}}

{{/*
Renders the global.auth.security secret name
*/}}
{{- define "airbyte.auth.security.secretName" }}
{{- if .Values.global.auth.security.secretName }}
  {{- .Values.global.auth.security.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.auth.security.cookieSecureSetting value
*/}}
{{- define "airbyte.auth.security.cookieSecureSetting" }}
{{- .Values.global.auth.security.cookieSecureSetting | default true }}
{{- end }}

{{/*
Renders the AB_COOKIE_SECURE environment variable
*/}}
{{- define "airbyte.auth.security.cookieSecureSetting.env" }}
- name: AB_COOKIE_SECURE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: AB_COOKIE_SECURE
{{- end }}

{{/*
Renders the global.auth.security.cookieSameSiteSetting value
*/}}
{{- define "airbyte.auth.security.cookieSameSiteSetting" }}
{{- .Values.global.auth.security.cookieSameSiteSetting | default "strict" }}
{{- end }}

{{/*
Renders the AB_COOKIE_SAME_SITE environment variable
*/}}
{{- define "airbyte.auth.security.cookieSameSiteSetting.env" }}
- name: AB_COOKIE_SAME_SITE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: AB_COOKIE_SAME_SITE
{{- end }}

{{/*
Renders the set of all auth.security environment variables
*/}}
{{- define "airbyte.auth.security.envs" }}
{{- include "airbyte.auth.security.cookieSecureSetting.env" . }}
{{- include "airbyte.auth.security.cookieSameSiteSetting.env" . }}
{{- end }}

{{/*
Renders the set of all auth.security config map variables
*/}}
{{- define "airbyte.auth.security.configVars" }}
AB_COOKIE_SECURE: {{ include "airbyte.auth.security.cookieSecureSetting" . | quote }}
AB_COOKIE_SAME_SITE: {{ include "airbyte.auth.security.cookieSameSiteSetting" . | quote }}
{{- end }}

{{/*
Renders the set of all auth.security secrets
*/}}
{{- define "airbyte.auth.security.secrets" }}
{{- end }}

{{/*
Auth Configuration
*/}}

{{/*
Renders the global.auth secret name
*/}}
{{- define "airbyte.auth.bootstrap.secretName" }}
{{- if .Values.global.auth.secretName }}
  {{- .Values.global.auth.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.auth.secretCreationEnabled value
*/}}
{{- define "airbyte.auth.bootstrap.secretCreationEnabled" }}
{{- .Values.global.auth.secretCreationEnabled | default "true" }}
{{- end }}

{{/*
Renders the AB_AUTH_SECRET_CREATION_ENABLED environment variable
*/}}
{{- define "airbyte.auth.bootstrap.secretCreationEnabled.env" }}
- name: AB_AUTH_SECRET_CREATION_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: AB_AUTH_SECRET_CREATION_ENABLED
{{- end }}

{{/*
Renders the global.auth.managedSecretName value
*/}}
{{- define "airbyte.auth.bootstrap.managedSecretName" }}
{{- .Values.global.auth.managedSecretNamed | default "airbyte-auth-secrets" }}
{{- end }}

{{/*
Renders the AB_KUBERNETES_SECRET_NAME environment variable
*/}}
{{- define "airbyte.auth.bootstrap.managedSecretName.env" }}
- name: AB_KUBERNETES_SECRET_NAME
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: AB_KUBERNETES_SECRET_NAME
{{- end }}

{{/*
Renders the global.auth.instanceAdmin.password value
*/}}
{{- define "airbyte.auth.bootstrap.instanceAdmin.password" }}
{{- .Values.global.auth.instanceAdmin.password  }}
{{- end }}

{{/*
Renders the AB_INSTANCE_ADMIN_PASSWORD environment variable
*/}}
{{- define "airbyte.auth.bootstrap.instanceAdmin.password.env" }}
- name: AB_INSTANCE_ADMIN_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.auth.bootstrap.secretName" . }}
      key: {{ .Values.global.auth.instanceAdmin.passwordSecretKey | default "AB_INSTANCE_ADMIN_PASSWORD" }}
{{- end }}

{{/*
Renders the global.auth.instanceAdmin.passwordSecretKey value
*/}}
{{- define "airbyte.auth.bootstrap.instanceAdmin.passwordSecretKey" }}
{{- .Values.global.auth.instanceAdmin.passwordSecretKey | default "INSTANCE_ADMIN_PASSWORD" }}
{{- end }}

{{/*
Renders the AB_INSTANCE_ADMIN_PASSWORD_SECRET_KEY environment variable
*/}}
{{- define "airbyte.auth.bootstrap.instanceAdmin.passwordSecretKey.env" }}
- name: AB_INSTANCE_ADMIN_PASSWORD_SECRET_KEY
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: AB_INSTANCE_ADMIN_PASSWORD_SECRET_KEY
{{- end }}

{{/*
Renders the global.auth.instanceAdmin.cilentId value
*/}}
{{- define "airbyte.auth.bootstrap.instanceAdmin.cilentId" }}
{{- .Values.global.auth.instanceAdmin.cilentId  }}
{{- end }}

{{/*
Renders the AB_INSTANCE_ADMIN_CLIENT_ID environment variable
*/}}
{{- define "airbyte.auth.bootstrap.instanceAdmin.cilentId.env" }}
- name: AB_INSTANCE_ADMIN_CLIENT_ID
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.auth.bootstrap.secretName" . }}
      key: {{ .Values.global.auth.instanceAdmin.cilentIdSecretKey | default "AB_INSTANCE_ADMIN_CLIENT_ID" }}
{{- end }}

{{/*
Renders the global.auth.instanceAdmin.clientIdSecretKey value
*/}}
{{- define "airbyte.auth.bootstrap.instanceAdmin.clientIdSecretKey" }}
{{- .Values.global.auth.instanceAdmin.clientIdSecretKey | default "INSTANCE_ADMIN_CLIENT_ID" }}
{{- end }}

{{/*
Renders the AB_INSTANCE_ADMIN_CLIENT_ID_SECRET_KEY environment variable
*/}}
{{- define "airbyte.auth.bootstrap.instanceAdmin.clientIdSecretKey.env" }}
- name: AB_INSTANCE_ADMIN_CLIENT_ID_SECRET_KEY
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: AB_INSTANCE_ADMIN_CLIENT_ID_SECRET_KEY
{{- end }}

{{/*
Renders the global.auth.instanceAdmin.clientSecret value
*/}}
{{- define "airbyte.auth.bootstrap.instanceAdmin.clientSecret" }}
{{- .Values.global.auth.instanceAdmin.clientSecret  }}
{{- end }}

{{/*
Renders the AB_INSTANCE_ADMIN_CLIENT_SECRET environment variable
*/}}
{{- define "airbyte.auth.bootstrap.instanceAdmin.clientSecret.env" }}
- name: AB_INSTANCE_ADMIN_CLIENT_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.auth.bootstrap.secretName" . }}
      key: {{ .Values.global.auth.instanceAdmin.clientSecretSecretKey | default "AB_INSTANCE_ADMIN_CLIENT_SECRET" }}
{{- end }}

{{/*
Renders the global.auth.instanceAdmin.clientSecretSecretKey value
*/}}
{{- define "airbyte.auth.bootstrap.instanceAdmin.clientSecretSecretKey" }}
{{- .Values.global.auth.instanceAdmin.clientSecretSecretKey | default "INSTANCE_ADMIN_CLIENT_SECRET" }}
{{- end }}

{{/*
Renders the AB_INSTANCE_ADMIN_CLIENT_SECRET_SECRET_KEY environment variable
*/}}
{{- define "airbyte.auth.bootstrap.instanceAdmin.clientSecretSecretKey.env" }}
- name: AB_INSTANCE_ADMIN_CLIENT_SECRET_SECRET_KEY
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: AB_INSTANCE_ADMIN_CLIENT_SECRET_SECRET_KEY
{{- end }}

{{/*
Renders the global.auth.jwtSignatureSecret value
*/}}
{{- define "airbyte.auth.bootstrap.jwtSignatureSecret" }}
{{- .Values.global.auth.jwtSignatureSecret  }}
{{- end }}

{{/*
Renders the AB_JWT_SIGNATURE_SECRET environment variable
*/}}
{{- define "airbyte.auth.bootstrap.jwtSignatureSecret.env" }}
- name: AB_JWT_SIGNATURE_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.auth.bootstrap.secretName" . }}
      key: {{ .Values.global.auth.jwtSignatureSecretSecretKey | default "AB_JWT_SIGNATURE_SECRET" }}
{{- end }}

{{/*
Renders the global.auth.jwtSignatureSecretKey value
*/}}
{{- define "airbyte.auth.bootstrap.jwtSignatureSecretKey" }}
{{- .Values.global.auth.security.jwtSignatureSecretKey | default "JWT_SIGNATURE_SECRET" }}
{{- end }}

{{/*
Renders the AB_JWT_SIGNATURE_SECRET_KEY environment variable
*/}}
{{- define "airbyte.auth.bootstrap.jwtSignatureSecretKey.env" }}
- name: AB_JWT_SIGNATURE_SECRET_KEY
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: AB_JWT_SIGNATURE_SECRET_KEY
{{- end }}

{{/*
Renders the set of all auth.bootstrap environment variables
*/}}
{{- define "airbyte.auth.bootstrap.envs" }}
{{- include "airbyte.auth.bootstrap.secretCreationEnabled.env" . }}
{{- include "airbyte.auth.bootstrap.managedSecretName.env" . }}
{{- include "airbyte.auth.bootstrap.instanceAdmin.password.env" . }}
{{- include "airbyte.auth.bootstrap.instanceAdmin.passwordSecretKey.env" . }}
{{- include "airbyte.auth.bootstrap.instanceAdmin.cilentId.env" . }}
{{- include "airbyte.auth.bootstrap.instanceAdmin.clientIdSecretKey.env" . }}
{{- include "airbyte.auth.bootstrap.instanceAdmin.clientSecret.env" . }}
{{- include "airbyte.auth.bootstrap.instanceAdmin.clientSecretSecretKey.env" . }}
{{- include "airbyte.auth.bootstrap.jwtSignatureSecret.env" . }}
{{- include "airbyte.auth.bootstrap.jwtSignatureSecretKey.env" . }}
{{- end }}

{{/*
Renders the set of all auth.bootstrap config map variables
*/}}
{{- define "airbyte.auth.bootstrap.configVars" }}
AB_AUTH_SECRET_CREATION_ENABLED: {{ include "airbyte.auth.bootstrap.secretCreationEnabled" . | quote }}
AB_KUBERNETES_SECRET_NAME: {{ include "airbyte.auth.bootstrap.managedSecretName" . | quote }}
AB_INSTANCE_ADMIN_PASSWORD_SECRET_KEY: {{ include "airbyte.auth.bootstrap.instanceAdmin.passwordSecretKey" . | quote }}
AB_INSTANCE_ADMIN_CLIENT_ID_SECRET_KEY: {{ include "airbyte.auth.bootstrap.instanceAdmin.clientIdSecretKey" . | quote }}
AB_INSTANCE_ADMIN_CLIENT_SECRET_SECRET_KEY: {{ include "airbyte.auth.bootstrap.instanceAdmin.clientSecretSecretKey" . | quote }}
AB_JWT_SIGNATURE_SECRET_KEY: {{ include "airbyte.auth.bootstrap.jwtSignatureSecretKey" . | quote }}
{{- end }}

{{/*
Renders the set of all auth.bootstrap secrets
*/}}
{{- define "airbyte.auth.bootstrap.secrets" }}
{{- if not (empty (include "airbyte.auth.bootstrap.instanceAdmin.password" .)) }}
AB_INSTANCE_ADMIN_PASSWORD: {{ include "airbyte.auth.bootstrap.instanceAdmin.password" . | quote }}
{{- end }}
{{- if not (empty (include "airbyte.auth.bootstrap.instanceAdmin.cilentId" .)) }}
AB_INSTANCE_ADMIN_CLIENT_ID: {{ include "airbyte.auth.bootstrap.instanceAdmin.cilentId" . | quote }}
{{- end }}
{{- if not (empty (include "airbyte.auth.bootstrap.instanceAdmin.clientSecret" .)) }}
AB_INSTANCE_ADMIN_CLIENT_SECRET: {{ include "airbyte.auth.bootstrap.instanceAdmin.clientSecret" . | quote }}
{{- end }}
{{- if not (empty (include "airbyte.auth.bootstrap.jwtSignatureSecret" .)) }}
AB_JWT_SIGNATURE_SECRET: {{ include "airbyte.auth.bootstrap.jwtSignatureSecret" . | quote }}
{{- end }}
{{- end }}

