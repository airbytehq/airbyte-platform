{{/*
Aws Configuration. Mirrors `airbyte.aws.envs` from the airbyte chart so the
data-plane infra-worker gets the same `AWS_ASSUME_ROLE_*` env vars the
control-plane one does, sourced from the same `airbyte-secrets` keys.
Used for Pulumi state S3 access; K8s API access is granted via RBAC.
*/}}

{{- define "airbyte-data-plane.aws.secretName" }}
{{- .Values.aws.secretName | default "airbyte-secrets" }}
{{- end }}

{{- define "airbyte-data-plane.aws.assumeRole.accessKeyId.secretKey" }}
{{- .Values.aws.assumeRole.accessKeyIdSecretKey | default "AWS_ACCESS_KEY_ID" }}
{{- end }}

{{- define "airbyte-data-plane.aws.assumeRole.secretAccessKey.secretKey" }}
{{- .Values.aws.assumeRole.secretAccessKeySecretKey | default "AWS_SECRET_ACCESS_KEY" }}
{{- end }}

{{- define "airbyte-data-plane.aws.assumeRole.secretName" }}
{{- .Values.aws.assumeRole.secretName | default (include "airbyte-data-plane.aws.secretName" .) }}
{{- end }}

{{- define "airbyte-data-plane.aws.envs" }}
- name: AWS_ASSUME_ROLE_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte-data-plane.aws.secretName" . }}
      key: {{ include "airbyte-data-plane.aws.assumeRole.accessKeyId.secretKey" . }}
- name: AWS_ASSUME_ROLE_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "airbyte-data-plane.aws.secretName" . }}
      key: {{ include "airbyte-data-plane.aws.assumeRole.secretAccessKey.secretKey" . }}
- name: AWS_ASSUME_ROLE_SECRET_NAME
  value: {{ include "airbyte-data-plane.aws.assumeRole.secretName" . | quote }}
{{- end }}
