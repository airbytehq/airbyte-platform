{{/*
Customerio Configuration
*/}}

{{/*
Renders the global.customerio secret name
*/}}
{{- define "airbyte.customerio.secretName" }}
{{- if .Values.global.customerio.secretName }}
  {{- .Values.global.customerio.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.customerio.apiKey value
*/}}
{{- define "airbyte.customerio.apiKey" }}
{{- .Values.global.customerio.apiKey  }}
{{- end }}

{{/*
Renders the CUSTOMERIO_API_KEY environment variable
*/}}
{{- define "airbyte.customerio.apiKey.env" }}
- name: CUSTOMERIO_API_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.customerio.secretName" . }}
      key: {{ .Values.global.customerio.apiKeySecretKey | default "CUSTOMERIO_API_KEY" }}
{{- end }}

{{/*
Renders the set of all customerio environment variables
*/}}
{{- define "airbyte.customerio.envs" }}
{{- include "airbyte.customerio.apiKey.env" . }}
{{- end }}

{{/*
Renders the set of all customerio config map variables
*/}}
{{- define "airbyte.customerio.configVars" }}
{{- end }}

{{/*
Renders the set of all customerio secrets
*/}}
{{- define "airbyte.customerio.secrets" }}
{{- if not (empty (include "airbyte.customerio.apiKey" .)) }}
CUSTOMERIO_API_KEY: {{ include "airbyte.customerio.apiKey" . | quote }}
{{- end }}
{{- end }}

