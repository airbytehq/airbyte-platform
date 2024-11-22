{{/*
FeatureFlags Configuration
*/}}

{{/*
Renders the global.featureFlags secret name
*/}}
{{- define "airbyte.featureFlags.secretName" }}
{{- if .Values.global.featureFlags.secretName }}
  {{- .Values.global.featureFlags.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}
{{/*
Renders the global.featureFlags.client value
*/}}
{{- define "airbyte.featureFlags.client" }}
{{- .Values.global.featureFlags.client | default "configfile" }}
{{- end }}

{{/*
Renders the FEATURE_FLAG_CLIENT environment variable
*/}}
{{- define "airbyte.featureFlags.client.env" }}
- name: FEATURE_FLAG_CLIENT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: FEATURE_FLAG_CLIENT
{{- end }}

{{/*
Renders the global.featureFlags.configfile.path value
*/}}
{{- define "airbyte.featureFlags.configfile.path" }}
{{- .Values.global.featureFlags.configfile.path | default "/etc/launchdarkly/flags.yml" }}
{{- end }}

{{/*
Renders the FEATURE_FLAG_PATH environment variable
*/}}
{{- define "airbyte.featureFlags.configfile.path.env" }}
- name: FEATURE_FLAG_PATH
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: FEATURE_FLAG_PATH
{{- end }}

{{/*
Renders the set of all featureFlags.configfile environment variables
*/}}
{{- define "airbyte.featureFlags.configfile.envs" }}
{{- include "airbyte.featureFlags.configfile.path.env" . }}
{{- end }}

{{/*
Renders the set of all featureFlags.configfile secrets
*/}}
{{- define "airbyte.featureFlags.configfile.secrets" }}
{{- end }}

{{/*
Renders the global.featureFlags.launchdarkly.key value
*/}}
{{- define "airbyte.featureFlags.launchdarkly.key" }}
{{- .Values.global.featureFlags.launchdarkly.key  }}
{{- end }}

{{/*
Renders the LAUNCHDARKLY_KEY environment variable
*/}}
{{- define "airbyte.featureFlags.launchdarkly.key.env" }}
- name: LAUNCHDARKLY_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.featureFlags.secretName" . }}
      key: {{ .Values.global.featureFlags.launchdarkly.keySecretKey | default "LAUNCHDARKLY_KEY" }}
{{- end }}

{{/*
Renders the set of all featureFlags.launchdarkly environment variables
*/}}
{{- define "airbyte.featureFlags.launchdarkly.envs" }}
{{- include "airbyte.featureFlags.launchdarkly.key.env" . }}
{{- end }}

{{/*
Renders the set of all featureFlags.launchdarkly secrets
*/}}
{{- define "airbyte.featureFlags.launchdarkly.secrets" }}
{{- if not (empty (include "airbyte.featureFlags.launchdarkly.key" .)) }}
LAUNCHDARKLY_KEY: {{ include "airbyte.featureFlags.launchdarkly.key" . | quote }}
{{- end }}
{{- end }}

{{/*
Renders the set of all featureFlags environment variables
*/}}
{{- define "airbyte.featureFlags.envs" }}
{{- include "airbyte.featureFlags.client.env" . }}
{{- $v := ( include "airbyte.featureFlags.client" . ) }}
{{- if eq $v "configfile" }}
{{- include "airbyte.featureFlags.configfile.envs" . }}
{{- end }}

{{- if eq $v "launchdarkly" }}
{{- include "airbyte.featureFlags.launchdarkly.envs" . }}
{{- end }}
{{- end }}

{{/*
Renders the set of all featureFlags config map variables
*/}}
{{- define "airbyte.featureFlags.configVars" }}
FEATURE_FLAG_CLIENT: {{ include "airbyte.featureFlags.client" . | quote }}
{{- $v := ( include "airbyte.featureFlags.client" . ) }}
{{- if eq $v "configfile" }}
FEATURE_FLAG_PATH: {{ include "airbyte.featureFlags.configfile.path" . | quote }}
{{- end }}

{{- if eq $v "launchdarkly" }}
LAUNCHDARKLY_KEY: {{ include "airbyte.featureFlags.launchdarkly.key" . | quote }}
{{- end }}
{{- end }}

{{/*
Renders the set of all featureFlags secrets
*/}}
{{- define "airbyte.featureFlags.secrets" }}
{{- $v := ( include "airbyte.featureFlags.client" . ) }}
{{- if eq $v "configfile" }}
{{- include "airbyte.featureFlags.configfile.secrets" . }}
{{- end }}

{{- if eq $v "launchdarkly" }}
{{- include "airbyte.featureFlags.launchdarkly.secrets" . }}
{{- end }}
{{- end }}

