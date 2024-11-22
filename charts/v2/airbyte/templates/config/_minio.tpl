{{/*
Minio Configuration
*/}}

{{/*
Renders the minio secret name
*/}}
{{- define "airbyte.minio.secretName" }}
{{- if .Values.minio.secretName }}
  {{- .Values.minio.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the minio.rootUser value
*/}}
{{- define "airbyte.minio.rootUser" }}
{{- .Values.minio.rootUser  }}
{{- end }}

{{/*
Renders the MINIO_ROOT_USER environment variable
*/}}
{{- define "airbyte.minio.rootUser.env" }}
- name: MINIO_ROOT_USER
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.minio.secretName" . }}
      key: AWS_ACCESS_KEY_ID
{{- end }}

{{/*
Renders the minio.rootPassword value
*/}}
{{- define "airbyte.minio.rootPassword" }}
{{- .Values.minio.rootPassword  }}
{{- end }}

{{/*
Renders the MINIO_ROOT_PASSWORD environment variable
*/}}
{{- define "airbyte.minio.rootPassword.env" }}
- name: MINIO_ROOT_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte.minio.secretName" . }}
      key: AWS_SECRET_ACCESS_KEY
{{- end }}

{{/*
Renders the set of all minio environment variables
*/}}
{{- define "airbyte.minio.envs" }}
{{- include "airbyte.minio.rootUser.env" . }}
{{- include "airbyte.minio.rootPassword.env" . }}
{{- end }}

{{/*
Renders the set of all minio config map variables
*/}}
{{- define "airbyte.minio.configVars" }}
{{- end }}

{{/*
Renders the set of all minio secrets
*/}}
{{- define "airbyte.minio.secrets" }}
{{- if not (empty (include "airbyte.minio.rootUser" .)) }}
MINIO_ROOT_USER: {{ include "airbyte.minio.rootUser" . | quote }}
{{- end }}
{{- if not (empty (include "airbyte.minio.rootPassword" .)) }}
MINIO_ROOT_PASSWORD: {{ include "airbyte.minio.rootPassword" . | quote }}
{{- end }}
{{- end }}

