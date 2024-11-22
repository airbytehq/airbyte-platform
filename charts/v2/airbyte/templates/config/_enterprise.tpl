{{/*
Enterprise Configuration
*/}}

{{/*
Renders the global.enterprise secret name
*/}}
{{- define "airbyte.enterprise.secretName" }}
{{- if .Values.global.enterprise.secretName }}
  {{- .Values.global.enterprise.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.enterprise.licenseKey value
*/}}
{{- define "airbyte.enterprise.licenseKey" }}
{{- .Values.global.enterprise.licenseKey  }}
{{- end }}

{{/*
Renders the AIRBYTE_LICENSE_KEY environment variable
*/}}
{{- define "airbyte.enterprise.licenseKey.env" }}
- name: AIRBYTE_LICENSE_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.enterprise.secretName" . }}
      key: {{ .Values.global.enterprise.licenseKeySecretKey | default "AIRBYTE_LICENSE_KEY" }}
{{- end }}

{{/*
Renders the set of all enterprise environment variables
*/}}
{{- define "airbyte.enterprise.envs" }}
{{- include "airbyte.enterprise.licenseKey.env" . }}
{{- end }}

{{/*
Renders the set of all enterprise config map variables
*/}}
{{- define "airbyte.enterprise.configVars" }}
{{- end }}

{{/*
Renders the set of all enterprise secrets
*/}}
{{- define "airbyte.enterprise.secrets" }}
{{- if not (empty (include "airbyte.enterprise.licenseKey" .)) }}
AIRBYTE_LICENSE_KEY: {{ include "airbyte.enterprise.licenseKey" . | quote }}
{{- end }}
{{- end }}

