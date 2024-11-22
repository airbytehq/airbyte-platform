{{/*
Micronaut Configuration
*/}}

{{/*
Renders the global.micronaut secret name
*/}}
{{- define "airbyte.micronaut.secretName" }}
{{- if .Values.global.micronaut.secretName }}
  {{- .Values.global.micronaut.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.micronaut.environments value
*/}}
{{- define "airbyte.micronaut.environments" }}
{{- join "," (append .Values.global.micronaut.environments (include "airbyte.airbyte.cluster.type" .)) }}
{{- end }}

{{/*
Renders the MICRONAUT_ENVIRONMENTS environment variable
*/}}
{{- define "airbyte.micronaut.environments.env" }}
- name: MICRONAUT_ENVIRONMENTS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: MICRONAUT_ENVIRONMENTS
{{- end }}

{{/*
Renders the set of all micronaut environment variables
*/}}
{{- define "airbyte.micronaut.envs" }}
{{- include "airbyte.micronaut.environments.env" . }}
{{- end }}

{{/*
Renders the set of all micronaut config map variables
*/}}
{{- define "airbyte.micronaut.configVars" }}
MICRONAUT_ENVIRONMENTS: {{ include "airbyte.micronaut.environments" . | quote }}
{{- end }}

{{/*
Renders the set of all micronaut secrets
*/}}
{{- define "airbyte.micronaut.secrets" }}
{{- end }}

