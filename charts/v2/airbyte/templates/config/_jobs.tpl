{{/*
Jobs Configuration
*/}}

{{/*
Renders the global.jobs secret name
*/}}
{{- define "airbyte.jobs.secretName" }}
{{- if .Values.global.jobs.secretName }}
  {{- .Values.global.jobs.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.jobs.kube.serviceAccount value
*/}}
{{- define "airbyte.jobs.kube.serviceAccount" }}
{{- .Values.global.serviceAccountName }}
{{- end }}

{{/*
Renders the JOB_KUBE_SERVICEACCOUNT environment variable
*/}}
{{- define "airbyte.jobs.kube.serviceAccount.env" }}
- name: JOB_KUBE_SERVICEACCOUNT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: JOB_KUBE_SERVICEACCOUNT
{{- end }}

{{/*
Renders the JOB_KUBE_NAMESPACE environment variable
*/}}
{{- define "airbyte.jobs.kube.namespace.env" }}
- name: JOB_KUBE_NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
{{- end }}

{{/*
Renders the global.jobs.kube.localVolume.enabled value
*/}}
{{- define "airbyte.jobs.kube.localVolume.enabled" }}
{{- .Values.global.jobs.kube.localVolume.enabled  }}
{{- end }}

{{/*
Renders the JOB_KUBE_LOCAL_VOLUME_ENABLED environment variable
*/}}
{{- define "airbyte.jobs.kube.localVolume.enabled.env" }}
- name: JOB_KUBE_LOCAL_VOLUME_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: JOB_KUBE_LOCAL_VOLUME_ENABLED
{{- end }}

{{/*
Renders the global.jobs.kube.images.busybox value
*/}}
{{- define "airbyte.jobs.kube.images.busybox" }}
{{- include "imageUrl" (list .Values.global.jobs.kube.images.busybox $) }}
{{- end }}

{{/*
Renders the JOB_KUBE_BUSYBOX_IMAGE environment variable
*/}}
{{- define "airbyte.jobs.kube.images.busybox.env" }}
- name: JOB_KUBE_BUSYBOX_IMAGE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: JOB_KUBE_BUSYBOX_IMAGE
{{- end }}

{{/*
Renders the global.jobs.kube.images.socat value
*/}}
{{- define "airbyte.jobs.kube.images.socat" }}
{{- include "imageUrl" (list .Values.global.jobs.kube.images.socat $) }}
{{- end }}

{{/*
Renders the JOB_KUBE_SOCAT_IMAGE environment variable
*/}}
{{- define "airbyte.jobs.kube.images.socat.env" }}
- name: JOB_KUBE_SOCAT_IMAGE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: JOB_KUBE_SOCAT_IMAGE
{{- end }}

{{/*
Renders the global.jobs.kube.images.curl value
*/}}
{{- define "airbyte.jobs.kube.images.curl" }}
{{- include "imageUrl" (list .Values.global.jobs.kube.images.curl $) }}
{{- end }}

{{/*
Renders the JOB_KUBE_CURL_IMAGE environment variable
*/}}
{{- define "airbyte.jobs.kube.images.curl.env" }}
- name: JOB_KUBE_CURL_IMAGE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: JOB_KUBE_CURL_IMAGE
{{- end }}

{{/*
Renders the global.jobs.kube.main_container_image_pull_secret value
*/}}
{{- define "airbyte.jobs.kube.main_container_image_pull_secret" }}
{{- .Values.global.jobs.kube.main_container_image_pull_secret  }}
{{- end }}

{{/*
Renders the JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_SECRET environment variable
*/}}
{{- define "airbyte.jobs.kube.main_container_image_pull_secret.env" }}
- name: JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_SECRET
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_SECRET
{{- end }}

{{/*
Renders the global.jobs.kube.annotations value
*/}}
{{- define "airbyte.jobs.kube.annotations" }}
{{- .Values.global.jobs.kube.annotations | include "airbyte.flattenMap" }}
{{- end }}

{{/*
Renders the JOB_KUBE_ANNOTATIONS environment variable
*/}}
{{- define "airbyte.jobs.kube.annotations.env" }}
- name: JOB_KUBE_ANNOTATIONS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: JOB_KUBE_ANNOTATIONS
{{- end }}

{{/*
Renders the global.jobs.kube.labels value
*/}}
{{- define "airbyte.jobs.kube.labels" }}
{{- .Values.global.jobs.kube.labels | include "airbyte.flattenMap" }}
{{- end }}

{{/*
Renders the JOB_KUBE_LABELS environment variable
*/}}
{{- define "airbyte.jobs.kube.labels.env" }}
- name: JOB_KUBE_LABELS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: JOB_KUBE_LABELS
{{- end }}

{{/*
Renders the global.jobs.kube.nodeSelector value
*/}}
{{- define "airbyte.jobs.kube.nodeSelector" }}
{{- .Values.global.jobs.kube.nodeSelector | include "airbyte.flattenMap" }}
{{- end }}

{{/*
Renders the JOB_KUBE_NODE_SELECTORS environment variable
*/}}
{{- define "airbyte.jobs.kube.nodeSelector.env" }}
- name: JOB_KUBE_NODE_SELECTORS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: JOB_KUBE_NODE_SELECTORS
{{- end }}

{{/*
Renders the global.jobs.kube.tolerations value
*/}}
{{- define "airbyte.jobs.kube.tolerations" }}
{{- .Values.global.jobs.kube.tolerations | include "airbyte.flattenArrayMap" }}
{{- end }}

{{/*
Renders the JOB_KUBE_TOLERATIONS environment variable
*/}}
{{- define "airbyte.jobs.kube.tolerations.env" }}
- name: JOB_KUBE_TOLERATIONS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: JOB_KUBE_TOLERATIONS
{{- end }}

{{/*
Renders the global.jobs.errors.reportingStrategy value
*/}}
{{- define "airbyte.jobs.errors.reportingStrategy" }}
{{- "logging" }}
{{- end }}

{{/*
Renders the JOB_ERROR_REPORTING_STRATEGY environment variable
*/}}
{{- define "airbyte.jobs.errors.reportingStrategy.env" }}
- name: JOB_ERROR_REPORTING_STRATEGY
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: JOB_ERROR_REPORTING_STRATEGY
{{- end }}

{{/*
Renders the set of all jobs environment variables
*/}}
{{- define "airbyte.jobs.envs" }}
{{- include "airbyte.jobs.kube.serviceAccount.env" . }}
{{- include "airbyte.jobs.kube.namespace.env" . }}
{{- include "airbyte.jobs.kube.localVolume.enabled.env" . }}
{{- include "airbyte.jobs.kube.images.busybox.env" . }}
{{- include "airbyte.jobs.kube.images.socat.env" . }}
{{- include "airbyte.jobs.kube.images.curl.env" . }}
{{- include "airbyte.jobs.kube.main_container_image_pull_secret.env" . }}
{{- include "airbyte.jobs.kube.annotations.env" . }}
{{- include "airbyte.jobs.kube.labels.env" . }}
{{- include "airbyte.jobs.kube.nodeSelector.env" . }}
{{- include "airbyte.jobs.kube.tolerations.env" . }}
{{- include "airbyte.jobs.errors.reportingStrategy.env" . }}
{{- end }}

{{/*
Renders the set of all jobs config map variables
*/}}
{{- define "airbyte.jobs.configVars" }}
JOB_KUBE_SERVICEACCOUNT: {{ include "airbyte.jobs.kube.serviceAccount" . | quote }}
JOB_KUBE_LOCAL_VOLUME_ENABLED: {{ include "airbyte.jobs.kube.localVolume.enabled" . | quote }}
JOB_KUBE_BUSYBOX_IMAGE: {{ include "airbyte.jobs.kube.images.busybox" . | quote }}
JOB_KUBE_SOCAT_IMAGE: {{ include "airbyte.jobs.kube.images.socat" . | quote }}
JOB_KUBE_CURL_IMAGE: {{ include "airbyte.jobs.kube.images.curl" . | quote }}
JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_SECRET: {{ include "airbyte.jobs.kube.main_container_image_pull_secret" . | quote }}
JOB_KUBE_ANNOTATIONS: {{ include "airbyte.jobs.kube.annotations" . | quote }}
JOB_KUBE_LABELS: {{ include "airbyte.jobs.kube.labels" . | quote }}
JOB_KUBE_NODE_SELECTORS: {{ include "airbyte.jobs.kube.nodeSelector" . | quote }}
JOB_KUBE_TOLERATIONS: {{ include "airbyte.jobs.kube.tolerations" . | quote }}
JOB_ERROR_REPORTING_STRATEGY: {{ include "airbyte.jobs.errors.reportingStrategy" . | quote }}
{{- end }}

{{/*
Renders the set of all jobs secrets
*/}}
{{- define "airbyte.jobs.secrets" }}
{{- end }}

