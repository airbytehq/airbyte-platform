{{/*
Enteprise Configuration
*/}}

{{- define "airbyte.enterprise.license" -}}
{{- if and (or (eq .Values.global.edition "pro") (eq .Values.global.edition "enterprise")) }}
{{- $secretName := .Values.global.enterprise.secretName | required "You must set `global.enterprise.secretName` when `global.edition` is 'enterprise'" }}
{{- $secretKey := .Values.global.enterprise.licenseKeySecretKey | required "You must set `global.enterprise.licenseKeySecretKey` when `global.edition` is 'enterprise'" }}
- name: AIRBYTE_LICENSE_KEY
  valueFrom:
    secretKeyRef: 
      name: {{ .Values.global.enterprise.secretName | default (printf "%s-airbyte-secrets" .Release.Name) }}
      key: {{ .Values.global.enterprise.licenseKeySecretKey }}
{{- end }}
{{- end }}

{{- define "airbyte.enterprise.instanceAdmin" -}}
{{- if and (or (eq .Values.global.edition "pro") (eq .Values.global.edition "enterprise")) }}
{{- $auth := .Values.global.auth | required "You must set `global.auth` when `global.edition` is 'enterprise'"}}
{{- $authInstanceAdminSecretName := .Values.global.auth.instanceAdmin.secretName | required "You must set `global.auth.instanceAdmin.secretName` when `global.edition` is 'enterprise'" }}
{{- $authInstanceAdminFirstName :=  .Values.global.auth.instanceAdmin.firstName | required "You must set `global.auth.instanceAdmin.firstName` when `global.edition` is 'enterprise'" }}
{{- $authInstanceAdminLastName := .Values.global.auth.instanceAdmin.lastName | required "You must set `global.auth.instanceAdmin.lastName` when `global.edition` is 'enterprise'" }}
{{- $authInstanceAdminEmailSecretKey := .Values.global.auth.instanceAdmin.emailSecretKey | required "You must set `global.auth.instanceAdmin.emailSecretKey` when `global.edition` is 'enterprise'" }}
{{- $authInstanceAdminPasswordSecretKey := .Values.global.auth.instanceAdmin.passwordSecretKey | required "You must set `global.auth.instanceAdmin.passwordSecretKey` when `global.edition` is 'enterprise'" }}
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
      name: {{ .Values.global.auth.instanceAdmin.secretName | default (printf "%s-airbyte-secrets" .Release.Name) }}
      key: {{ .Values.global.auth.instanceAdmin.emailSecretKey }}
- name: INITIAL_USER_PASSWORD
  valueFrom:
    secretKeyRef: 
      name: {{ .Values.global.auth.instanceAdmin.secretName | default (printf "%s-airbyte-secrets" .Release.Name) }}
      key: {{ .Values.global.auth.instanceAdmin.passwordSecretKey }}
{{- end }}
{{- end }}

{{- define "airbyte.enterprise.identityProvider" -}}
{{- if and (or (eq .Values.global.edition "pro") (eq .Values.global.edition "enterprise")) .Values.global.auth.identityProvider }}
{{- $authIdentityProviderSecretName := .Values.global.auth.identityProvider.secretName | required "You must set `global.auth.identityProvider.secretName` when enabling SSO" }}
{{- $authIdentityProviderType := .Values.global.auth.identityProvider.type | required "You must set `global.auth.identityProvider.type` when enabling SSO "}}
{{- $authIdentityProviderOIDC :=  .Values.global.auth.identityProvider.oidc | required "You must set `global.auth.identityProvider.oidc` when enabling SSO" }}
{{- $authIdentityProviderOIDCDomain :=  .Values.global.auth.identityProvider.oidc.domain | required "You must set `global.auth.identityProvider.oidc.domain` when enabling SSO" }}
{{- $authIdentityProviderOIDCAppName :=  .Values.global.auth.identityProvider.oidc.appName | required "You must set `global.auth.identityProvider.oidc.appName` when enabling SSO" }}
{{- $authIdentityProviderOIDCClientIdSecretKey :=  .Values.global.auth.identityProvider.oidc.clientIdSecretKey | required "You must set `global.auth.identityProvider.oidc.clientIdSecretKey` when enabling SSO" }}
{{- $authIdentityProviderOIDCClientSecretSecretKey :=  .Values.global.auth.identityProvider.oidc.clientSecretSecretKey | required "You must set `global.auth.identityProvider.oidc.clientSecretSecretKey` when enabling SSO" }}
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
      name: {{ .Values.global.auth.identityProvider.secretName | default (printf "%s-airbyte-secrets" .Release.Name) }}
      key: {{ .Values.global.auth.identityProvider.oidc.clientIdSecretKey }}
- name: OIDC_CLIENT_SECRET
  valueFrom:
    secretKeyRef: 
      name: {{ .Values.global.auth.identityProvider.secretName | default (printf "%s-airbyte-secrets" .Release.Name) }}
      key: {{ .Values.global.auth.identityProvider.oidc.clientSecretSecretKey }}
{{- end }}
{{- end }}
