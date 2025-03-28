
{{/* DO NOT EDIT: This file was autogenerated. */}}

{{/*
    Workloadlauncher Configuration
*/}}

{{/*
Renders the workloadLauncher.enabled value
*/}}
{{- define "airbyte.workloadLauncher.enabled" }}
	{{- if eq .Values.workloadLauncher.enabled nil }}
    	{{- true }}
	{{- else }}
    	{{- .Values.workloadLauncher.enabled }}
	{{- end }}
{{- end }}

{{/*
Renders the workloadLauncher.enabled environment variable
*/}}
{{- define "airbyte.workloadLauncher.enabled.env" }}
- name: WORKLOAD_LAUNCHER_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WORKLOAD_LAUNCHER_ENABLED
{{- end }}

{{/*
Renders the workloadLauncher.parallelism value
*/}}
{{- define "airbyte.workloadLauncher.parallelism" }}
    {{- .Values.workloadLauncher.parallelism | default 10 }}
{{- end }}

{{/*
Renders the workloadLauncher.parallelism environment variable
*/}}
{{- define "airbyte.workloadLauncher.parallelism.env" }}
- name: WORKLOAD_LAUNCHER_PARALLELISM
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WORKLOAD_LAUNCHER_PARALLELISM
{{- end }}

{{/*
Renders the set of all workloadLauncher environment variables
*/}}
{{- define "airbyte.workloadLauncher.envs" }}
{{- include "airbyte.workloadLauncher.enabled.env" . }}
{{- include "airbyte.workloadLauncher.parallelism.env" . }}
{{- end }}

{{/*
Renders the set of all workloadLauncher config map variables
*/}}
{{- define "airbyte.workloadLauncher.configVars" }}
WORKLOAD_LAUNCHER_ENABLED: {{ include "airbyte.workloadLauncher.enabled" . | quote }}
WORKLOAD_LAUNCHER_PARALLELISM: {{ include "airbyte.workloadLauncher.parallelism" . | quote }}
{{- end }}

{{/*
Renders the workloadLauncher.connectorProfiler.image value
*/}}
{{- define "airbyte.workloadLauncher.images.connectorProfiler.image" }}
    {{- include "imageUrl" (list .Values.workloadLauncher.connectorProfiler.image $) }}
{{- end }}

{{/*
Renders the workloadLauncher.images.connectorProfiler.image environment variable
*/}}
{{- define "airbyte.workloadLauncher.images.connectorProfiler.image.env" }}
- name: CONNECTOR_PROFILER_IMAGE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CONNECTOR_PROFILER_IMAGE
{{- end }}

{{/*
Renders the workloadLauncher.connectorSidecar.image value
*/}}
{{- define "airbyte.workloadLauncher.images.connectorSidecar.image" }}
    {{- include "imageUrl" (list .Values.workloadLauncher.connectorSidecar.image $) }}
{{- end }}

{{/*
Renders the workloadLauncher.images.connectorSidecar.image environment variable
*/}}
{{- define "airbyte.workloadLauncher.images.connectorSidecar.image.env" }}
- name: CONNECTOR_SIDECAR_IMAGE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CONNECTOR_SIDECAR_IMAGE
{{- end }}

{{/*
Renders the workloadLauncher.containerOrchestrator.enabled value
*/}}
{{- define "airbyte.workloadLauncher.images.containerOrchestrator.enabled" }}
    {{- .Values.workloadLauncher.containerOrchestrator.enabled }}
{{- end }}

{{/*
Renders the workloadLauncher.images.containerOrchestrator.enabled environment variable
*/}}
{{- define "airbyte.workloadLauncher.images.containerOrchestrator.enabled.env" }}
- name: CONTAINER_ORCHESTRATOR_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CONTAINER_ORCHESTRATOR_ENABLED
{{- end }}

{{/*
Renders the workloadLauncher.containerOrchestrator.image value
*/}}
{{- define "airbyte.workloadLauncher.images.containerOrchestrator.image" }}
    {{- include "imageUrl" (list .Values.workloadLauncher.containerOrchestrator.image $) }}
{{- end }}

{{/*
Renders the workloadLauncher.images.containerOrchestrator.image environment variable
*/}}
{{- define "airbyte.workloadLauncher.images.containerOrchestrator.image.env" }}
- name: CONTAINER_ORCHESTRATOR_IMAGE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CONTAINER_ORCHESTRATOR_IMAGE
{{- end }}

{{/*
Renders the workloadLauncher.workloadInit.image value
*/}}
{{- define "airbyte.workloadLauncher.images.workloadInit.image" }}
    {{- include "imageUrl" (list .Values.workloadLauncher.workloadInit.image $) }}
{{- end }}

{{/*
Renders the workloadLauncher.images.workloadInit.image environment variable
*/}}
{{- define "airbyte.workloadLauncher.images.workloadInit.image.env" }}
- name: WORKLOAD_INIT_IMAGE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WORKLOAD_INIT_IMAGE
{{- end }}

{{/*
Renders the set of all workloadLauncher.images environment variables
*/}}
{{- define "airbyte.workloadLauncher.images.envs" }}
{{- include "airbyte.workloadLauncher.images.connectorProfiler.image.env" . }}
{{- include "airbyte.workloadLauncher.images.connectorSidecar.image.env" . }}
{{- include "airbyte.workloadLauncher.images.containerOrchestrator.enabled.env" . }}
{{- include "airbyte.workloadLauncher.images.containerOrchestrator.image.env" . }}
{{- include "airbyte.workloadLauncher.images.workloadInit.image.env" . }}
{{- end }}

{{/*
Renders the set of all workloadLauncher.images config map variables
*/}}
{{- define "airbyte.workloadLauncher.images.configVars" }}
CONNECTOR_PROFILER_IMAGE: {{ include "airbyte.workloadLauncher.images.connectorProfiler.image" . | quote }}
CONNECTOR_SIDECAR_IMAGE: {{ include "airbyte.workloadLauncher.images.connectorSidecar.image" . | quote }}
CONTAINER_ORCHESTRATOR_ENABLED: {{ include "airbyte.workloadLauncher.images.containerOrchestrator.enabled" . | quote }}
CONTAINER_ORCHESTRATOR_IMAGE: {{ include "airbyte.workloadLauncher.images.containerOrchestrator.image" . | quote }}
WORKLOAD_INIT_IMAGE: {{ include "airbyte.workloadLauncher.images.workloadInit.image" . | quote }}
{{- end }}
