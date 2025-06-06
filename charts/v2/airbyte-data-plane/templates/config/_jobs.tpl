
{{/* DO NOT EDIT: This file was autogenerated. */}}

{{/*
    Jobs Configuration
*/}}

{{/*
Renders the jobs.kube.serviceAccount value
*/}}
{{- define "airbyte-data-plane.jobs.kube.serviceAccount" }}
    {{- .Values.jobs.kube.serviceAccount | default (include "airbyte-data-plane.serviceAccountName" .) }}
{{- end }}

{{/*
Renders the jobs.kube.serviceAccount environment variable
*/}}
{{- define "airbyte-data-plane.jobs.kube.serviceAccount.env" }}
- name: JOB_KUBE_SERVICEACCOUNT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-data-plane-env
      key: JOB_KUBE_SERVICEACCOUNT
{{- end }}

{{/*
Renders the jobs.kube.namespace value
*/}}
{{- define "airbyte-data-plane.jobs.kube.namespace" }}
    {{- .Values.jobs.kube.namespace }}
{{- end }}

{{/*
Renders the jobs.kube.namespace environment variable
*/}}
{{- define "airbyte-data-plane.jobs.kube.namespace.env" }}
- name: JOB_KUBE_NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
    
{{- end }}

{{/*
Renders the jobs.kube.localVolume.enabled value
*/}}
{{- define "airbyte-data-plane.jobs.kube.localVolume.enabled" }}
	{{- if eq .Values.jobs.kube.localVolume.enabled nil }}
    	{{- false }}
	{{- else }}
    	{{- .Values.jobs.kube.localVolume.enabled }}
	{{- end }}
{{- end }}

{{/*
Renders the jobs.kube.localVolume.enabled environment variable
*/}}
{{- define "airbyte-data-plane.jobs.kube.localVolume.enabled.env" }}
- name: JOB_KUBE_LOCAL_VOLUME_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-data-plane-env
      key: JOB_KUBE_LOCAL_VOLUME_ENABLED
{{- end }}

{{/*
Renders the jobs.kube.mainContainerImagePullPolicy value
*/}}
{{- define "airbyte-data-plane.jobs.kube.mainContainerImagePullPolicy" }}
    {{- .Values.jobs.kube.mainContainerImagePullPolicy | default "IfNotPresent" }}
{{- end }}

{{/*
Renders the jobs.kube.mainContainerImagePullPolicy environment variable
*/}}
{{- define "airbyte-data-plane.jobs.kube.mainContainerImagePullPolicy.env" }}
- name: JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_POLICY
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-data-plane-env
      key: JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_POLICY
{{- end }}

{{/*
Renders the jobs.kube.mainContainerImagePullSecret value
*/}}
{{- define "airbyte-data-plane.jobs.kube.mainContainerImagePullSecret" }}
    {{- join "," (ternary (concat .Values.imagePullSecrets (list .Values.jobs.kube.mainContainerImagePullSecret)) .Values.imagePullSecrets (empty .Values.jobs.kube.mainContainerImagePullSecret)) }}
{{- end }}

{{/*
Renders the jobs.kube.mainContainerImagePullSecret environment variable
*/}}
{{- define "airbyte-data-plane.jobs.kube.mainContainerImagePullSecret.env" }}
- name: JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_SECRET
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-data-plane-env
      key: JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_SECRET
{{- end }}

{{/*
Renders the jobs.kube.annotations value
*/}}
{{- define "airbyte-data-plane.jobs.kube.annotations" }}
    {{- .Values.jobs.kube.annotations | include "airbyte-data-plane.flattenMap" }}
{{- end }}

{{/*
Renders the jobs.kube.annotations environment variable
*/}}
{{- define "airbyte-data-plane.jobs.kube.annotations.env" }}
- name: JOB_KUBE_ANNOTATIONS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-data-plane-env
      key: JOB_KUBE_ANNOTATIONS
{{- end }}

{{/*
Renders the jobs.kube.labels value
*/}}
{{- define "airbyte-data-plane.jobs.kube.labels" }}
    {{- .Values.jobs.kube.labels | include "airbyte-data-plane.flattenMap" }}
{{- end }}

{{/*
Renders the jobs.kube.labels environment variable
*/}}
{{- define "airbyte-data-plane.jobs.kube.labels.env" }}
- name: JOB_KUBE_LABELS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-data-plane-env
      key: JOB_KUBE_LABELS
{{- end }}

{{/*
Renders the jobs.kube.nodeSelector value
*/}}
{{- define "airbyte-data-plane.jobs.kube.nodeSelector" }}
    {{- .Values.jobs.kube.nodeSelector | include "airbyte-data-plane.flattenMap" }}
{{- end }}

{{/*
Renders the jobs.kube.nodeSelector environment variable
*/}}
{{- define "airbyte-data-plane.jobs.kube.nodeSelector.env" }}
- name: JOB_KUBE_NODE_SELECTORS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-data-plane-env
      key: JOB_KUBE_NODE_SELECTORS
{{- end }}

{{/*
Renders the jobs.kube.tolerations value
*/}}
{{- define "airbyte-data-plane.jobs.kube.tolerations" }}
    {{- .Values.jobs.kube.tolerations | include "airbyte-data-plane.flattenArrayMap" }}
{{- end }}

{{/*
Renders the jobs.kube.tolerations environment variable
*/}}
{{- define "airbyte-data-plane.jobs.kube.tolerations.env" }}
- name: JOB_KUBE_TOLERATIONS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-data-plane-env
      key: JOB_KUBE_TOLERATIONS
{{- end }}

{{/*
Renders the set of all jobs environment variables
*/}}
{{- define "airbyte-data-plane.jobs.envs" }}
{{- include "airbyte-data-plane.jobs.kube.serviceAccount.env" . }}
{{- include "airbyte-data-plane.jobs.kube.namespace.env" . }}
{{- include "airbyte-data-plane.jobs.kube.localVolume.enabled.env" . }}
{{- include "airbyte-data-plane.jobs.kube.mainContainerImagePullPolicy.env" . }}
{{- include "airbyte-data-plane.jobs.kube.mainContainerImagePullSecret.env" . }}
{{- include "airbyte-data-plane.jobs.kube.annotations.env" . }}
{{- include "airbyte-data-plane.jobs.kube.labels.env" . }}
{{- include "airbyte-data-plane.jobs.kube.nodeSelector.env" . }}
{{- include "airbyte-data-plane.jobs.kube.tolerations.env" . }}
{{- end }}

{{/*
Renders the set of all jobs config map variables
*/}}
{{- define "airbyte-data-plane.jobs.configVars" }}
JOB_KUBE_SERVICEACCOUNT: {{ include "airbyte-data-plane.jobs.kube.serviceAccount" . | quote }}
JOB_KUBE_NAMESPACE: {{ include "airbyte-data-plane.jobs.kube.namespace" . | quote }}
JOB_KUBE_LOCAL_VOLUME_ENABLED: {{ include "airbyte-data-plane.jobs.kube.localVolume.enabled" . | quote }}
JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_POLICY: {{ include "airbyte-data-plane.jobs.kube.mainContainerImagePullPolicy" . | quote }}
JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_SECRET: {{ include "airbyte-data-plane.jobs.kube.mainContainerImagePullSecret" . | quote }}
JOB_KUBE_ANNOTATIONS: {{ include "airbyte-data-plane.jobs.kube.annotations" . | quote }}
JOB_KUBE_LABELS: {{ include "airbyte-data-plane.jobs.kube.labels" . | quote }}
JOB_KUBE_NODE_SELECTORS: {{ include "airbyte-data-plane.jobs.kube.nodeSelector" . | quote }}
JOB_KUBE_TOLERATIONS: {{ include "airbyte-data-plane.jobs.kube.tolerations" . | quote }}
{{- end }}

{{/*
Renders the jobs.errors.reportingStrategy value
*/}}
{{- define "airbyte-data-plane.jobs.errors.reportingStrategy" }}
    {{- .Values.jobs.errors.reportingStrategy | default "logging" }}
{{- end }}

{{/*
Renders the jobs.errors.reportingStrategy environment variable
*/}}
{{- define "airbyte-data-plane.jobs.errors.reportingStrategy.env" }}
- name: JOB_ERROR_REPORTING_STRATEGY
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-data-plane-env
      key: JOB_ERROR_REPORTING_STRATEGY
{{- end }}

{{/*
Renders the jobs.errors.sentry.dsn value
*/}}
{{- define "airbyte-data-plane.jobs.errors.sentry.dsn" }}
    {{- .Values.jobs.errors.sentry.dsn }}
{{- end }}

{{/*
Renders the jobs.errors.sentry.dsn environment variable
*/}}
{{- define "airbyte-data-plane.jobs.errors.sentry.dsn.env" }}
- name: JOB_ERROR_REPORTING_SENTRY_DSN
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-data-plane-env
      key: JOB_ERROR_REPORTING_SENTRY_DSN
{{- end }}

{{/*
Renders the set of all jobs.errors environment variables
*/}}
{{- define "airbyte-data-plane.jobs.errors.envs" }}
{{- include "airbyte-data-plane.jobs.errors.reportingStrategy.env" . }}
{{- $opt := (include "airbyte-data-plane.jobs.errors.reportingStrategy" .) }}

{{- if eq $opt "logging" }}
{{- end }}

{{- if eq $opt "sentry" }}
{{- include "airbyte-data-plane.jobs.errors.sentry.dsn.env" . }}
{{- end }}

{{- end }}

{{/*
Renders the set of all jobs.errors config map variables
*/}}
{{- define "airbyte-data-plane.jobs.errors.configVars" }}
JOB_ERROR_REPORTING_STRATEGY: {{ include "airbyte-data-plane.jobs.errors.reportingStrategy" . | quote }}
{{- $opt := (include "airbyte-data-plane.jobs.errors.reportingStrategy" .) }}

{{- if eq $opt "logging" }}
{{- end }}

{{- if eq $opt "sentry" }}
JOB_ERROR_REPORTING_SENTRY_DSN: {{ include "airbyte-data-plane.jobs.errors.sentry.dsn" . | quote }}
{{- end }}

{{- end }}

{{/*
Renders the jobs.kube.scheduling.check.nodeSelectors value
*/}}
{{- define "airbyte-data-plane.jobs.scheduling.check.nodeSelectors" }}
    {{- .Values.check.nodeSelectors | include "airbyte-data-plane.flattenMap" }}
{{- end }}

{{/*
Renders the jobs.scheduling.check.nodeSelectors environment variable
*/}}
{{- define "airbyte-data-plane.jobs.scheduling.check.nodeSelectors.env" }}
- name: CHECK_JOB_KUBE_NODE_SELECTORS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-data-plane-env
      key: CHECK_JOB_KUBE_NODE_SELECTORS
{{- end }}

{{/*
Renders the jobs.kube.scheduling.check.runtimeClassName value
*/}}
{{- define "airbyte-data-plane.jobs.scheduling.check.runtimeClassName" }}
    {{- .Values.jobs.kube.scheduling.check.runtimeClassName }}
{{- end }}

{{/*
Renders the jobs.scheduling.check.runtimeClassName environment variable
*/}}
{{- define "airbyte-data-plane.jobs.scheduling.check.runtimeClassName.env" }}
- name: CHECK_JOB_KUBE_RUNTIME_CLASS_NAME
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-data-plane-env
      key: CHECK_JOB_KUBE_RUNTIME_CLASS_NAME
{{- end }}

{{/*
Renders the jobs.kube.scheduling.discover.nodeSelectors value
*/}}
{{- define "airbyte-data-plane.jobs.scheduling.discover.nodeSelectors" }}
    {{- .Values.discover.nodeSelectors | include "airbyte-data-plane.flattenMap" }}
{{- end }}

{{/*
Renders the jobs.scheduling.discover.nodeSelectors environment variable
*/}}
{{- define "airbyte-data-plane.jobs.scheduling.discover.nodeSelectors.env" }}
- name: DISCOVER_JOB_KUBE_NODE_SELECTORS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-data-plane-env
      key: DISCOVER_JOB_KUBE_NODE_SELECTORS
{{- end }}

{{/*
Renders the jobs.kube.scheduling.discover.runtimeClassName value
*/}}
{{- define "airbyte-data-plane.jobs.scheduling.discover.runtimeClassName" }}
    {{- .Values.jobs.kube.scheduling.discover.runtimeClassName }}
{{- end }}

{{/*
Renders the jobs.scheduling.discover.runtimeClassName environment variable
*/}}
{{- define "airbyte-data-plane.jobs.scheduling.discover.runtimeClassName.env" }}
- name: DISCOVER_JOB_KUBE_RUNTIME_CLASS_NAME
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-data-plane-env
      key: DISCOVER_JOB_KUBE_RUNTIME_CLASS_NAME
{{- end }}

{{/*
Renders the jobs.kube.scheduling.isolated.nodeSelectors value
*/}}
{{- define "airbyte-data-plane.jobs.scheduling.isolated.nodeSelectors" }}
    {{- .Values.isolated.nodeSelectors | include "airbyte-data-plane.flattenMap" }}
{{- end }}

{{/*
Renders the jobs.scheduling.isolated.nodeSelectors environment variable
*/}}
{{- define "airbyte-data-plane.jobs.scheduling.isolated.nodeSelectors.env" }}
- name: JOB_ISOLATED_KUBE_NODE_SELECTORS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-data-plane-env
      key: JOB_ISOLATED_KUBE_NODE_SELECTORS
{{- end }}

{{/*
Renders the jobs.kube.scheduling.isolated.runtimeClassName value
*/}}
{{- define "airbyte-data-plane.jobs.scheduling.isolated.runtimeClassName" }}
    {{- .Values.jobs.kube.scheduling.isolated.runtimeClassName }}
{{- end }}

{{/*
Renders the jobs.scheduling.isolated.runtimeClassName environment variable
*/}}
{{- define "airbyte-data-plane.jobs.scheduling.isolated.runtimeClassName.env" }}
- name: JOB_ISOLATED_KUBE_RUNTIME_CLASS_NAME
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-data-plane-env
      key: JOB_ISOLATED_KUBE_RUNTIME_CLASS_NAME
{{- end }}

{{/*
Renders the set of all jobs.scheduling environment variables
*/}}
{{- define "airbyte-data-plane.jobs.scheduling.envs" }}
{{- include "airbyte-data-plane.jobs.scheduling.check.nodeSelectors.env" . }}
{{- include "airbyte-data-plane.jobs.scheduling.check.runtimeClassName.env" . }}
{{- include "airbyte-data-plane.jobs.scheduling.discover.nodeSelectors.env" . }}
{{- include "airbyte-data-plane.jobs.scheduling.discover.runtimeClassName.env" . }}
{{- include "airbyte-data-plane.jobs.scheduling.isolated.nodeSelectors.env" . }}
{{- include "airbyte-data-plane.jobs.scheduling.isolated.runtimeClassName.env" . }}
{{- end }}

{{/*
Renders the set of all jobs.scheduling config map variables
*/}}
{{- define "airbyte-data-plane.jobs.scheduling.configVars" }}
CHECK_JOB_KUBE_NODE_SELECTORS: {{ include "airbyte-data-plane.jobs.scheduling.check.nodeSelectors" . | quote }}
CHECK_JOB_KUBE_RUNTIME_CLASS_NAME: {{ include "airbyte-data-plane.jobs.scheduling.check.runtimeClassName" . | quote }}
DISCOVER_JOB_KUBE_NODE_SELECTORS: {{ include "airbyte-data-plane.jobs.scheduling.discover.nodeSelectors" . | quote }}
DISCOVER_JOB_KUBE_RUNTIME_CLASS_NAME: {{ include "airbyte-data-plane.jobs.scheduling.discover.runtimeClassName" . | quote }}
JOB_ISOLATED_KUBE_NODE_SELECTORS: {{ include "airbyte-data-plane.jobs.scheduling.isolated.nodeSelectors" . | quote }}
JOB_ISOLATED_KUBE_RUNTIME_CLASS_NAME: {{ include "airbyte-data-plane.jobs.scheduling.isolated.runtimeClassName" . | quote }}
{{- end }}
