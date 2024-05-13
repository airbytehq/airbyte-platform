{{/*
Enteprise Configuration
*/}}

{{- define "airbyte.enterprise.license" -}}
{{- if and (or (eq .Values.global.edition "pro") (eq .Values.global.edition "enterprise")) (not .Values.global.airbyteYml) }}
{{- $licenseKeySecretName := .Values.global.enterprise.licenseKeySecretName | required "You must set `global.enterprise.licenseKeySecretName` when `global.edition` is 'enterprise" }}
- name: AIRBYTE_LICENSE_KEY
  valueFrom:
    secretKeyRef: 
      name: {{ .Values.global.enterprise.licenseKeySecretName | default (printf "%s-airbyte-secrets" .Release.Name) }}
      key: {{ .Values.global.enterprise.licenseKeySecretKey }}
{{- end }}
{{- end }}

{{- define "airbyte.enterprise.instanceAdmin" -}}
{{- if and (or (eq .Values.global.edition "pro") (eq .Values.global.edition "enterprise")) (not .Values.global.airbyteYml) }}
{{- $auth := .Values.global.auth | required "You must set `global.auth` when `global.edition` is 'enterprise'"}}
{{- $authInstanceAdminFirstName :=  .Values.global.auth.instanceAdmin.firstName | required "You must set `global.auth.instanceAdmin.firstName` when `global.edition` is 'enterprise'" }}
{{- $authInstanceAdminLastName := .Values.global.auth.instanceAdmin.lastName | required "You must set `global.auth.instanceAdmin.lastName` when `global.edition` is 'enterprise'" }}
{{- $authInstanceAdminEmailSecretName := .Values.global.auth.instanceAdmin.emailSecretName | required "You must set `global.auth.instanceAdmin.emailSecretName` when `global.edition` is 'enterprise'" }}
{{- $authInstanceAdminPasswordSecretName := .Values.global.auth.instanceAdmin.passwordSecretName | required "You must set `global.auth.instanceAdmin.passwordSecretName` when `global.edition` is 'enterprise'" }}
- name: INITIAL_USER_FIRST_NAME
  valueFrom:
    configMapKeyRef: 
      name: {{ .Release.Name }}-airbyte-env
      key: INITIAL_USER_FIRST_NAME
- name: INITIAL_USER_LAST_NAME
  valueFrom:
    configMapKeyRef: 
      name: {{ .Release.Name }}-airbyte-env
      key: INITIAL_USER_LAST_NAME
- name: INITIAL_USER_EMAIL
  valueFrom:
    secretKeyRef: 
      name: {{ .Values.global.auth.instanceAdmin.emailSecretName | default (printf "%s-airbyte-secrets" .Release.Name) }}
      key: {{ .Values.global.auth.instanceAdmin.emailSecretKey }}
- name: INITIAL_USER_PASSWORD
  valueFrom:
    secretKeyRef: 
      name: {{ .Values.global.auth.instanceAdmin.passwordSecretName | default (printf "%s-airbyte-secrets" .Release.Name) }}
      key: {{ .Values.global.auth.instanceAdmin.passwordSecretKey }}
{{- end }}
{{- end }}

{{- define "airbyte.enterprise.identityProvider" -}}
{{- if and (or (eq .Values.global.edition "pro") (eq .Values.global.edition "enterprise")) (not .Values.global.airbyteYml) .Values.global.auth.identityProvider }}
{{- $authIdentityProviderType := .Values.global.auth.identityProvider.type | required "You must set `global.auth.identityProvider.type` when enabling SSO "}}
{{- $authIdentityProviderOIDC :=  .Values.global.auth.identityProvider.oidc | required "You must set `global.auth.identityProvider.oidc` when enabling SSO" }}
{{- $authIdentityProviderOIDCDomain :=  .Values.global.auth.identityProvider.oidc.domain | required "You must set `global.auth.identityProvider.oidc.domain` when enabling SSO" }}
{{- $authIdentityProviderOIDCClientIdSecretName :=  .Values.global.auth.identityProvider.oidc.clientIdSecretName | required "You must set `global.auth.identityProvider.oidc.clientIdSecretName` when enabling SSO" }}
{{- $authIdentityProviderOIDCClientSecretSecretName :=  .Values.global.auth.identityProvider.oidc.clientSecretSecretName | required "You must set `global.auth.identityProvider.oidc.clientSecretSecretName` when enabling SSO" }}
- name: IDENTITY_PROVIDER_TYPE
  valueFrom:
    configMapKeyRef: 
      name: {{ .Release.Name }}-airbyte-env
      key: IDENTITY_PROVIDER_TYPE 
- name: OIDC_DOMAIN
  valueFrom:
    configMapKeyRef: 
      name: {{ .Release.Name }}-airbyte-env
      key: OIDC_DOMAIN
- name: OIDC_APP_NAME
  valueFrom:
    configMapKeyRef: 
      name: {{ .Release.Name }}-airbyte-env
      key: OIDC_APP_NAME
- name: OIDC_CLIENT_ID
  valueFrom:
    secretKeyRef: 
      name: {{ .Values.global.auth.identityProvider.oidc.clientIdSecretName | default (printf "%s-airbyte-secrets" .Release.Name) }}
      key: {{ .Values.global.auth.identityProvider.oidc.clientIdSecretKey }}
- name: OIDC_CLIENT_SECRET
  valueFrom:
    secretKeyRef: 
      name: {{ .Values.global.auth.identityProvider.oidc.clientSecretSecretName | default (printf "%s-airbyte-secrets" .Release.Name) }}
      key: {{ .Values.global.auth.identityProvider.oidc.clientSecretSecretKey }}
{{- end }}
{{- end }}
