{{/*
Java Configuration
*/}}

{{/*
Renders the global.java secret name
*/}}
{{- define "airbyte.java.secretName" }}
{{- if .Values.global.java.secretName }}
  {{- .Values.global.java.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.java.opts value
*/}}
{{- define "airbyte.java.opts" }}
{{- join " " .Values.global.java.opts }}
{{- end }}

{{/*
Renders the JAVA_TOOL_OPTIONS environment variable
*/}}
{{- define "airbyte.java.opts.env" }}
- name: JAVA_TOOL_OPTIONS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: JAVA_TOOL_OPTIONS
{{- end }}

{{/*
Renders the set of all java environment variables
*/}}
{{- define "airbyte.java.envs" }}
{{- include "airbyte.java.opts.env" . }}
{{- end }}

{{/*
Renders the set of all java config map variables
*/}}
{{- define "airbyte.java.configVars" }}
JAVA_TOOL_OPTIONS: {{ include "airbyte.java.opts" . | quote }}
{{- end }}

{{/*
Renders the set of all java secrets
*/}}
{{- define "airbyte.java.secrets" }}
{{- end }}

