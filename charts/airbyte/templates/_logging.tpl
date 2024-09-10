{{/*
Common logging configs
*/}}

{{/*
Determine the correct log4j configuration file to load based on the defined storage type.
*/}}
{{- define "airbyte.logging.log4jConfig" -}}
{{- if eq (lower (default "" .Values.global.storage.type)) "minio" }}
    {{- printf "log4j2-minio.xml" -}}
{{- else if eq (lower (default "" .Values.global.storage.type)) "gcs" -}}
    {{- printf "log4j2-gcs.xml" -}}
{{- else if eq (lower (default "" .Values.global.storage.type)) "s3" -}}
    {{- printf "log4j2-s3.xml" -}}
{{- else if eq (lower (default "" .Values.global.storage.type)) "azure" -}}
    {{- printf "log4j2-azure.xml" -}}
{{- else -}}
    {{- printf "log4j2.xml" -}}
{{- end -}}
{{- end -}}

{{- define "airbyte.logging.envs" }}
- name: LOG_LEVEL
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: LOG_LEVEL
- name: LOG4J_CONFIGURATION_FILE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: LOG4J_CONFIGURATION_FILE
{{- end }}
