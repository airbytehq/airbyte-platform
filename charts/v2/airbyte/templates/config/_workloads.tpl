{{/*
Workloads Configuration
*/}}

{{/*
Renders the global.workloads.resources secret name
*/}}
{{- define "airbyte.workloads.resources.secretName" }}
{{- if .Values.global.workloads.resources.secretName }}
  {{- .Values.global.workloads.resources.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.workloads.resources.useConnectorResourceDefaults value
*/}}
{{- define "airbyte.workloads.resources.useConnectorResourceDefaults" }}
{{- .Values.global.workloads.resources.useConnectorResourceDefaults | default true }}
{{- end }}

{{/*
Renders the CONNECTOR_SPECIFIC_RESOURCE_DEFAULTS_ENABLED environment variable
*/}}
{{- define "airbyte.workloads.resources.useConnectorResourceDefaults.env" }}
- name: CONNECTOR_SPECIFIC_RESOURCE_DEFAULTS_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CONNECTOR_SPECIFIC_RESOURCE_DEFAULTS_ENABLED
{{- end }}

{{/*
Renders the global.workloads.resources.mainContainer.cpu.limit value
*/}}
{{- define "airbyte.workloads.resources.mainContainer.cpu.limit" }}
{{- .Values.global.workloads.resources.mainContainer.cpu.limit  }}
{{- end }}

{{/*
Renders the JOB_MAIN_CONTAINER_CPU_LIMIT environment variable
*/}}
{{- define "airbyte.workloads.resources.mainContainer.cpu.limit.env" }}
- name: JOB_MAIN_CONTAINER_CPU_LIMIT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: JOB_MAIN_CONTAINER_CPU_LIMIT
{{- end }}

{{/*
Renders the global.workloads.resources.mainContainer.cpu.request value
*/}}
{{- define "airbyte.workloads.resources.mainContainer.cpu.request" }}
{{- .Values.global.workloads.resources.mainContainer.cpu.request  }}
{{- end }}

{{/*
Renders the JOB_MAIN_CONTAINER_CPU_REQUEST environment variable
*/}}
{{- define "airbyte.workloads.resources.mainContainer.cpu.request.env" }}
- name: JOB_MAIN_CONTAINER_CPU_REQUEST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: JOB_MAIN_CONTAINER_CPU_REQUEST
{{- end }}

{{/*
Renders the global.workloads.resources.mainContainer.memory.limit value
*/}}
{{- define "airbyte.workloads.resources.mainContainer.memory.limit" }}
{{- .Values.global.workloads.resources.mainContainer.memory.limit  }}
{{- end }}

{{/*
Renders the JOB_MAIN_CONTAINER_MEMORY_LIMIT environment variable
*/}}
{{- define "airbyte.workloads.resources.mainContainer.memory.limit.env" }}
- name: JOB_MAIN_CONTAINER_MEMORY_LIMIT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: JOB_MAIN_CONTAINER_MEMORY_LIMIT
{{- end }}

{{/*
Renders the global.workloads.resources.mainContainer.memory.request value
*/}}
{{- define "airbyte.workloads.resources.mainContainer.memory.request" }}
{{- .Values.global.workloads.resources.mainContainer.memory.request  }}
{{- end }}

{{/*
Renders the JOB_MAIN_CONTAINER_MEMORY_REQUEST environment variable
*/}}
{{- define "airbyte.workloads.resources.mainContainer.memory.request.env" }}
- name: JOB_MAIN_CONTAINER_MEMORY_REQUEST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: JOB_MAIN_CONTAINER_MEMORY_REQUEST
{{- end }}

{{/*
Renders the global.workloads.resources.check.cpu.limit value
*/}}
{{- define "airbyte.workloads.resources.check.cpu.limit" }}
{{- .Values.global.workloads.resources.check.cpu.limit  }}
{{- end }}

{{/*
Renders the CHECK_JOB_MAIN_CONTAINER_CPU_LIMIT environment variable
*/}}
{{- define "airbyte.workloads.resources.check.cpu.limit.env" }}
- name: CHECK_JOB_MAIN_CONTAINER_CPU_LIMIT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CHECK_JOB_MAIN_CONTAINER_CPU_LIMIT
{{- end }}

{{/*
Renders the global.workloads.resources.check.cpu.request value
*/}}
{{- define "airbyte.workloads.resources.check.cpu.request" }}
{{- .Values.global.workloads.resources.check.cpu.request  }}
{{- end }}

{{/*
Renders the CHECK_JOB_MAIN_CONTAINER_CPU_REQUEST environment variable
*/}}
{{- define "airbyte.workloads.resources.check.cpu.request.env" }}
- name: CHECK_JOB_MAIN_CONTAINER_CPU_REQUEST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CHECK_JOB_MAIN_CONTAINER_CPU_REQUEST
{{- end }}

{{/*
Renders the global.workloads.resources.check.memory.limit value
*/}}
{{- define "airbyte.workloads.resources.check.memory.limit" }}
{{- .Values.global.workloads.resources.check.memory.limit  }}
{{- end }}

{{/*
Renders the CHECK_JOB_MAIN_CONTAINER_MEMORY_LIMIT environment variable
*/}}
{{- define "airbyte.workloads.resources.check.memory.limit.env" }}
- name: CHECK_JOB_MAIN_CONTAINER_MEMORY_LIMIT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CHECK_JOB_MAIN_CONTAINER_MEMORY_LIMIT
{{- end }}

{{/*
Renders the global.workloads.resources.check.memory.request value
*/}}
{{- define "airbyte.workloads.resources.check.memory.request" }}
{{- .Values.global.workloads.resources.check.memory.request  }}
{{- end }}

{{/*
Renders the CHECK_JOB_MAIN_CONTAINER_MEMORY_REQUEST environment variable
*/}}
{{- define "airbyte.workloads.resources.check.memory.request.env" }}
- name: CHECK_JOB_MAIN_CONTAINER_MEMORY_REQUEST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CHECK_JOB_MAIN_CONTAINER_MEMORY_REQUEST
{{- end }}

{{/*
Renders the global.workloads.resources.discover.cpu.limit value
*/}}
{{- define "airbyte.workloads.resources.discover.cpu.limit" }}
{{- .Values.global.workloads.resources.discover.cpu.limit  }}
{{- end }}

{{/*
Renders the DISCOVER_JOB_MAIN_CONTAINER_CPU_LIMIT environment variable
*/}}
{{- define "airbyte.workloads.resources.discover.cpu.limit.env" }}
- name: DISCOVER_JOB_MAIN_CONTAINER_CPU_LIMIT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DISCOVER_JOB_MAIN_CONTAINER_CPU_LIMIT
{{- end }}

{{/*
Renders the global.workloads.resources.discover.cpu.request value
*/}}
{{- define "airbyte.workloads.resources.discover.cpu.request" }}
{{- .Values.global.workloads.resources.discover.cpu.request  }}
{{- end }}

{{/*
Renders the DISCOVER_JOB_MAIN_CONTAINER_CPU_REQUEST environment variable
*/}}
{{- define "airbyte.workloads.resources.discover.cpu.request.env" }}
- name: DISCOVER_JOB_MAIN_CONTAINER_CPU_REQUEST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DISCOVER_JOB_MAIN_CONTAINER_CPU_REQUEST
{{- end }}

{{/*
Renders the global.workloads.resources.discover.memory.limit value
*/}}
{{- define "airbyte.workloads.resources.discover.memory.limit" }}
{{- .Values.global.workloads.resources.discover.memory.limit  }}
{{- end }}

{{/*
Renders the DISCOVER_JOB_MAIN_CONTAINER_MEMORY_LIMIT environment variable
*/}}
{{- define "airbyte.workloads.resources.discover.memory.limit.env" }}
- name: DISCOVER_JOB_MAIN_CONTAINER_MEMORY_LIMIT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DISCOVER_JOB_MAIN_CONTAINER_MEMORY_LIMIT
{{- end }}

{{/*
Renders the global.workloads.resources.discover.memory.request value
*/}}
{{- define "airbyte.workloads.resources.discover.memory.request" }}
{{- .Values.global.workloads.resources.discover.memory.request  }}
{{- end }}

{{/*
Renders the DISCOVER_JOB_MAIN_CONTAINER_MEMORY_REQUEST environment variable
*/}}
{{- define "airbyte.workloads.resources.discover.memory.request.env" }}
- name: DISCOVER_JOB_MAIN_CONTAINER_MEMORY_REQUEST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DISCOVER_JOB_MAIN_CONTAINER_MEMORY_REQUEST
{{- end }}

{{/*
Renders the global.workloads.resources.fileTransfer.storage.limit value
*/}}
{{- define "airbyte.workloads.resources.fileTransfer.storage.limit" }}
{{- .Values.global.workloads.resources.fileTransfer.storage.limit  }}
{{- end }}

{{/*
Renders the FILE_TRANSFER_EPHEMERAL_STORAGE_LIMIT environment variable
*/}}
{{- define "airbyte.workloads.resources.fileTransfer.storage.limit.env" }}
- name: FILE_TRANSFER_EPHEMERAL_STORAGE_LIMIT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: FILE_TRANSFER_EPHEMERAL_STORAGE_LIMIT
{{- end }}

{{/*
Renders the global.workloads.resources.fileTransfer.storage.request value
*/}}
{{- define "airbyte.workloads.resources.fileTransfer.storage.request" }}
{{- .Values.global.workloads.resources.fileTransfer.storage.request  }}
{{- end }}

{{/*
Renders the FILE_TRANSFER_EPHEMERAL_STORAGE_REQUEST environment variable
*/}}
{{- define "airbyte.workloads.resources.fileTransfer.storage.request.env" }}
- name: FILE_TRANSFER_EPHEMERAL_STORAGE_REQUEST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: FILE_TRANSFER_EPHEMERAL_STORAGE_REQUEST
{{- end }}

{{/*
Renders the global.workloads.resources.replication.cpu.limit value
*/}}
{{- define "airbyte.workloads.resources.replication.cpu.limit" }}
{{- .Values.global.workloads.resources.replication.cpu.limit  }}
{{- end }}

{{/*
Renders the REPLICATION_ORCHESTRATOR_CPU_LIMIT environment variable
*/}}
{{- define "airbyte.workloads.resources.replication.cpu.limit.env" }}
- name: REPLICATION_ORCHESTRATOR_CPU_LIMIT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: REPLICATION_ORCHESTRATOR_CPU_LIMIT
{{- end }}

{{/*
Renders the global.workloads.resources.replication.cpu.request value
*/}}
{{- define "airbyte.workloads.resources.replication.cpu.request" }}
{{- .Values.global.workloads.resources.replication.cpu.request  }}
{{- end }}

{{/*
Renders the REPLICATION_ORCHESTRATOR_CPU_REQUEST environment variable
*/}}
{{- define "airbyte.workloads.resources.replication.cpu.request.env" }}
- name: REPLICATION_ORCHESTRATOR_CPU_REQUEST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: REPLICATION_ORCHESTRATOR_CPU_REQUEST
{{- end }}

{{/*
Renders the global.workloads.resources.replication.memory.limit value
*/}}
{{- define "airbyte.workloads.resources.replication.memory.limit" }}
{{- .Values.global.workloads.resources.replication.memory.limit  }}
{{- end }}

{{/*
Renders the REPLICATION_ORCHESTRATOR_MEMORY_LIMIT environment variable
*/}}
{{- define "airbyte.workloads.resources.replication.memory.limit.env" }}
- name: REPLICATION_ORCHESTRATOR_MEMORY_LIMIT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: REPLICATION_ORCHESTRATOR_MEMORY_LIMIT
{{- end }}

{{/*
Renders the global.workloads.resources.replication.memory.request value
*/}}
{{- define "airbyte.workloads.resources.replication.memory.request" }}
{{- .Values.global.workloads.resources.replication.memory.request  }}
{{- end }}

{{/*
Renders the REPLICATION_ORCHESTRATOR_MEMORY_REQUEST environment variable
*/}}
{{- define "airbyte.workloads.resources.replication.memory.request.env" }}
- name: REPLICATION_ORCHESTRATOR_MEMORY_REQUEST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: REPLICATION_ORCHESTRATOR_MEMORY_REQUEST
{{- end }}

{{/*
Renders the global.workloads.resources.sidecar.cpu.limit value
*/}}
{{- define "airbyte.workloads.resources.sidecar.cpu.limit" }}
{{- .Values.global.workloads.resources.sidecar.cpu.limit  }}
{{- end }}

{{/*
Renders the SIDECAR_MAIN_CONTAINER_CPU_LIMIT environment variable
*/}}
{{- define "airbyte.workloads.resources.sidecar.cpu.limit.env" }}
- name: SIDECAR_MAIN_CONTAINER_CPU_LIMIT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: SIDECAR_MAIN_CONTAINER_CPU_LIMIT
{{- end }}

{{/*
Renders the global.workloads.resources.sidecar.cpu.request value
*/}}
{{- define "airbyte.workloads.resources.sidecar.cpu.request" }}
{{- .Values.global.workloads.resources.sidecar.cpu.request  }}
{{- end }}

{{/*
Renders the SIDECAR_MAIN_CONTAINER_CPU_REQUEST environment variable
*/}}
{{- define "airbyte.workloads.resources.sidecar.cpu.request.env" }}
- name: SIDECAR_MAIN_CONTAINER_CPU_REQUEST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: SIDECAR_MAIN_CONTAINER_CPU_REQUEST
{{- end }}

{{/*
Renders the global.workloads.resources.sidecar.memory.limit value
*/}}
{{- define "airbyte.workloads.resources.sidecar.memory.limit" }}
{{- .Values.global.workloads.resources.sidecar.memory.limit  }}
{{- end }}

{{/*
Renders the SIDECAR_MAIN_CONTAINER_MEMORY_LIMIT environment variable
*/}}
{{- define "airbyte.workloads.resources.sidecar.memory.limit.env" }}
- name: SIDECAR_MAIN_CONTAINER_MEMORY_LIMIT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: SIDECAR_MAIN_CONTAINER_MEMORY_LIMIT
{{- end }}

{{/*
Renders the global.workloads.resources.sidecar.memory.request value
*/}}
{{- define "airbyte.workloads.resources.sidecar.memory.request" }}
{{- .Values.global.workloads.resources.sidecar.memory.request  }}
{{- end }}

{{/*
Renders the SIDECAR_MAIN_CONTAINER_MEMORY_REQUEST environment variable
*/}}
{{- define "airbyte.workloads.resources.sidecar.memory.request.env" }}
- name: SIDECAR_MAIN_CONTAINER_MEMORY_REQUEST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: SIDECAR_MAIN_CONTAINER_MEMORY_REQUEST
{{- end }}

{{/*
Renders the set of all workloads.resources environment variables
*/}}
{{- define "airbyte.workloads.resources.envs" }}
{{- include "airbyte.workloads.resources.useConnectorResourceDefaults.env" . }}
{{- include "airbyte.workloads.resources.mainContainer.cpu.limit.env" . }}
{{- include "airbyte.workloads.resources.mainContainer.cpu.request.env" . }}
{{- include "airbyte.workloads.resources.mainContainer.memory.limit.env" . }}
{{- include "airbyte.workloads.resources.mainContainer.memory.request.env" . }}
{{- include "airbyte.workloads.resources.check.cpu.limit.env" . }}
{{- include "airbyte.workloads.resources.check.cpu.request.env" . }}
{{- include "airbyte.workloads.resources.check.memory.limit.env" . }}
{{- include "airbyte.workloads.resources.check.memory.request.env" . }}
{{- include "airbyte.workloads.resources.discover.cpu.limit.env" . }}
{{- include "airbyte.workloads.resources.discover.cpu.request.env" . }}
{{- include "airbyte.workloads.resources.discover.memory.limit.env" . }}
{{- include "airbyte.workloads.resources.discover.memory.request.env" . }}
{{- include "airbyte.workloads.resources.fileTransfer.storage.limit.env" . }}
{{- include "airbyte.workloads.resources.fileTransfer.storage.request.env" . }}
{{- include "airbyte.workloads.resources.replication.cpu.limit.env" . }}
{{- include "airbyte.workloads.resources.replication.cpu.request.env" . }}
{{- include "airbyte.workloads.resources.replication.memory.limit.env" . }}
{{- include "airbyte.workloads.resources.replication.memory.request.env" . }}
{{- include "airbyte.workloads.resources.sidecar.cpu.limit.env" . }}
{{- include "airbyte.workloads.resources.sidecar.cpu.request.env" . }}
{{- include "airbyte.workloads.resources.sidecar.memory.limit.env" . }}
{{- include "airbyte.workloads.resources.sidecar.memory.request.env" . }}
{{- end }}

{{/*
Renders the set of all workloads.resources config map variables
*/}}
{{- define "airbyte.workloads.resources.configVars" }}
CONNECTOR_SPECIFIC_RESOURCE_DEFAULTS_ENABLED: {{ include "airbyte.workloads.resources.useConnectorResourceDefaults" . | quote }}
JOB_MAIN_CONTAINER_CPU_LIMIT: {{ include "airbyte.workloads.resources.mainContainer.cpu.limit" . | quote }}
JOB_MAIN_CONTAINER_CPU_REQUEST: {{ include "airbyte.workloads.resources.mainContainer.cpu.request" . | quote }}
JOB_MAIN_CONTAINER_MEMORY_LIMIT: {{ include "airbyte.workloads.resources.mainContainer.memory.limit" . | quote }}
JOB_MAIN_CONTAINER_MEMORY_REQUEST: {{ include "airbyte.workloads.resources.mainContainer.memory.request" . | quote }}
CHECK_JOB_MAIN_CONTAINER_CPU_LIMIT: {{ include "airbyte.workloads.resources.check.cpu.limit" . | quote }}
CHECK_JOB_MAIN_CONTAINER_CPU_REQUEST: {{ include "airbyte.workloads.resources.check.cpu.request" . | quote }}
CHECK_JOB_MAIN_CONTAINER_MEMORY_LIMIT: {{ include "airbyte.workloads.resources.check.memory.limit" . | quote }}
CHECK_JOB_MAIN_CONTAINER_MEMORY_REQUEST: {{ include "airbyte.workloads.resources.check.memory.request" . | quote }}
DISCOVER_JOB_MAIN_CONTAINER_CPU_LIMIT: {{ include "airbyte.workloads.resources.discover.cpu.limit" . | quote }}
DISCOVER_JOB_MAIN_CONTAINER_CPU_REQUEST: {{ include "airbyte.workloads.resources.discover.cpu.request" . | quote }}
DISCOVER_JOB_MAIN_CONTAINER_MEMORY_LIMIT: {{ include "airbyte.workloads.resources.discover.memory.limit" . | quote }}
DISCOVER_JOB_MAIN_CONTAINER_MEMORY_REQUEST: {{ include "airbyte.workloads.resources.discover.memory.request" . | quote }}
FILE_TRANSFER_EPHEMERAL_STORAGE_LIMIT: {{ include "airbyte.workloads.resources.fileTransfer.storage.limit" . | quote }}
FILE_TRANSFER_EPHEMERAL_STORAGE_REQUEST: {{ include "airbyte.workloads.resources.fileTransfer.storage.request" . | quote }}
REPLICATION_ORCHESTRATOR_CPU_LIMIT: {{ include "airbyte.workloads.resources.replication.cpu.limit" . | quote }}
REPLICATION_ORCHESTRATOR_CPU_REQUEST: {{ include "airbyte.workloads.resources.replication.cpu.request" . | quote }}
REPLICATION_ORCHESTRATOR_MEMORY_LIMIT: {{ include "airbyte.workloads.resources.replication.memory.limit" . | quote }}
REPLICATION_ORCHESTRATOR_MEMORY_REQUEST: {{ include "airbyte.workloads.resources.replication.memory.request" . | quote }}
SIDECAR_MAIN_CONTAINER_CPU_LIMIT: {{ include "airbyte.workloads.resources.sidecar.cpu.limit" . | quote }}
SIDECAR_MAIN_CONTAINER_CPU_REQUEST: {{ include "airbyte.workloads.resources.sidecar.cpu.request" . | quote }}
SIDECAR_MAIN_CONTAINER_MEMORY_LIMIT: {{ include "airbyte.workloads.resources.sidecar.memory.limit" . | quote }}
SIDECAR_MAIN_CONTAINER_MEMORY_REQUEST: {{ include "airbyte.workloads.resources.sidecar.memory.request" . | quote }}
{{- end }}

{{/*
Renders the set of all workloads.resources secrets
*/}}
{{- define "airbyte.workloads.resources.secrets" }}
{{- end }}

{{/*
Workloads Configuration
*/}}

{{/*
Renders the worker secret name
*/}}
{{- define "airbyte.worker.secretName" }}
{{- if .Values.worker.secretName }}
  {{- .Values.worker.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the worker.activityMaxAttempt value
*/}}
{{- define "airbyte.worker.activityMaxAttempt" }}
{{- .Values.worker.activityMaxAttempt | default "" }}
{{- end }}

{{/*
Renders the ACTIVITY_MAX_ATTEMPT environment variable
*/}}
{{- define "airbyte.worker.activityMaxAttempt.env" }}
- name: ACTIVITY_MAX_ATTEMPT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: ACTIVITY_MAX_ATTEMPT
{{- end }}

{{/*
Renders the worker.activityInitialDelayBetweenAttemptsSeconds value
*/}}
{{- define "airbyte.worker.activityInitialDelayBetweenAttemptsSeconds" }}
{{- .Values.worker.activityInitialDelayBetweenAttemptsSeconds  }}
{{- end }}

{{/*
Renders the ACTIVITY_INITIAL_DELAY_BETWEEN_ATTEMPTS_SECONDS environment variable
*/}}
{{- define "airbyte.worker.activityInitialDelayBetweenAttemptsSeconds.env" }}
- name: ACTIVITY_INITIAL_DELAY_BETWEEN_ATTEMPTS_SECONDS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: ACTIVITY_INITIAL_DELAY_BETWEEN_ATTEMPTS_SECONDS
{{- end }}

{{/*
Renders the worker.activityMaxDelayBetweenAttemptsSeconds value
*/}}
{{- define "airbyte.worker.activityMaxDelayBetweenAttemptsSeconds" }}
{{- .Values.worker.activityMaxDelayBetweenAttemptsSeconds  }}
{{- end }}

{{/*
Renders the ACTIVITY_MAX_DELAY_BETWEEN_ATTEMPTS_SECONDS environment variable
*/}}
{{- define "airbyte.worker.activityMaxDelayBetweenAttemptsSeconds.env" }}
- name: ACTIVITY_MAX_DELAY_BETWEEN_ATTEMPTS_SECONDS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: ACTIVITY_MAX_DELAY_BETWEEN_ATTEMPTS_SECONDS
{{- end }}

{{/*
Renders the worker.configRoot value
*/}}
{{- define "airbyte.worker.configRoot" }}
{{- "/configs" }}
{{- end }}

{{/*
Renders the CONFIG_ROOT environment variable
*/}}
{{- define "airbyte.worker.configRoot.env" }}
- name: CONFIG_ROOT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CONFIG_ROOT
{{- end }}

{{/*
Renders the worker.maxNotifyWorkers value
*/}}
{{- define "airbyte.worker.maxNotifyWorkers" }}
{{- .Values.worker.maxNotifyWorkers | default "30" }}
{{- end }}

{{/*
Renders the MAX_NOTIFY_WORKERS environment variable
*/}}
{{- define "airbyte.worker.maxNotifyWorkers.env" }}
- name: MAX_NOTIFY_WORKERS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: MAX_NOTIFY_WORKERS
{{- end }}

{{/*
Renders the worker.maxSyncWorkers value
*/}}
{{- define "airbyte.worker.maxSyncWorkers" }}
{{- .Values.worker.maxSyncWorkers | default "5" }}
{{- end }}

{{/*
Renders the MAX_SYNC_WORKER environment variable
*/}}
{{- define "airbyte.worker.maxSyncWorkers.env" }}
- name: MAX_SYNC_WORKER
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: MAX_SYNC_WORKER
{{- end }}

{{/*
Renders the worker.shouldRunNotifyWorkflows value
*/}}
{{- define "airbyte.worker.shouldRunNotifyWorkflows" }}
{{- .Values.worker.shouldRunNotifyWorkflows | default true }}
{{- end }}

{{/*
Renders the SHOULD_RUN_NOTIFY_WORKFLOWS environment variable
*/}}
{{- define "airbyte.worker.shouldRunNotifyWorkflows.env" }}
- name: SHOULD_RUN_NOTIFY_WORKFLOWS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: SHOULD_RUN_NOTIFY_WORKFLOWS
{{- end }}

{{/*
Renders the worker.syncJobMaxAttempts value
*/}}
{{- define "airbyte.worker.syncJobMaxAttempts" }}
{{- .Values.worker.syncJobMaxAttempts | default "3" }}
{{- end }}

{{/*
Renders the SYNC_JOB_MAX_ATTEMPTS environment variable
*/}}
{{- define "airbyte.worker.syncJobMaxAttempts.env" }}
- name: SYNC_JOB_MAX_ATTEMPTS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: SYNC_JOB_MAX_ATTEMPTS
{{- end }}

{{/*
Renders the worker.syncJobMaxTimeoutDays value
*/}}
{{- define "airbyte.worker.syncJobMaxTimeoutDays" }}
{{- .Values.worker.syncJobMaxTimeoutDays | default "3" }}
{{- end }}

{{/*
Renders the SYNC_JOB_MAX_TIMEOUT_DAYS environment variable
*/}}
{{- define "airbyte.worker.syncJobMaxTimeoutDays.env" }}
- name: SYNC_JOB_MAX_TIMEOUT_DAYS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: SYNC_JOB_MAX_TIMEOUT_DAYS
{{- end }}

{{/*
Renders the worker.syncJobInitRetryTimeoutMinutes value
*/}}
{{- define "airbyte.worker.syncJobInitRetryTimeoutMinutes" }}
{{- .Values.worker.syncJobInitRetryTimeoutMinutes | default "5" }}
{{- end }}

{{/*
Renders the SYNC_JOB_INIT_RETRY_TIMEOUT_MINUTES environment variable
*/}}
{{- define "airbyte.worker.syncJobInitRetryTimeoutMinutes.env" }}
- name: SYNC_JOB_INIT_RETRY_TIMEOUT_MINUTES
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: SYNC_JOB_INIT_RETRY_TIMEOUT_MINUTES
{{- end }}

{{/*
Renders the worker.useStreamCapableState value
*/}}
{{- define "airbyte.worker.useStreamCapableState" }}
{{- .Values.worker.useStreamCapableState | default true }}
{{- end }}

{{/*
Renders the USE_STREAM_CAPABLE_STATE environment variable
*/}}
{{- define "airbyte.worker.useStreamCapableState.env" }}
- name: USE_STREAM_CAPABLE_STATE
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: USE_STREAM_CAPABLE_STATE
{{- end }}

{{/*
Renders the worker.workflowFailureRestartDelaySeconds value
*/}}
{{- define "airbyte.worker.workflowFailureRestartDelaySeconds" }}
{{- .Values.worker.workflowFailureRestartDelaySeconds  }}
{{- end }}

{{/*
Renders the WORKFLOW_FAILURE_RESTART_DELAY_SECONDS environment variable
*/}}
{{- define "airbyte.worker.workflowFailureRestartDelaySeconds.env" }}
- name: WORKFLOW_FAILURE_RESTART_DELAY_SECONDS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WORKFLOW_FAILURE_RESTART_DELAY_SECONDS
{{- end }}

{{/*
Renders the worker.workspaceDockerMount value
*/}}
{{- define "airbyte.worker.workspaceDockerMount" }}
{{- .Values.worker.workspaceDockerMount | default "airbyte_workspace" }}
{{- end }}

{{/*
Renders the WORKSPACE_DOCKER_MOUNT environment variable
*/}}
{{- define "airbyte.worker.workspaceDockerMount.env" }}
- name: WORKSPACE_DOCKER_MOUNT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WORKSPACE_DOCKER_MOUNT
{{- end }}

{{/*
Renders the worker.workspaceRoot value
*/}}
{{- define "airbyte.worker.workspaceRoot" }}
{{- .Values.worker.workspaceRoot | default "/workspace" }}
{{- end }}

{{/*
Renders the WORKSPACE_ROOT environment variable
*/}}
{{- define "airbyte.worker.workspaceRoot.env" }}
- name: WORKSPACE_ROOT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WORKSPACE_ROOT
{{- end }}

{{/*
Renders the worker.environment value
*/}}
{{- define "airbyte.worker.environment" }}
{{- "kubernetes" }}
{{- end }}

{{/*
Renders the WORKER_ENVIRONMENT environment variable
*/}}
{{- define "airbyte.worker.environment.env" }}
- name: WORKER_ENVIRONMENT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: WORKER_ENVIRONMENT
{{- end }}

{{/*
Renders the set of all worker environment variables
*/}}
{{- define "airbyte.worker.envs" }}
{{- include "airbyte.worker.activityMaxAttempt.env" . }}
{{- include "airbyte.worker.activityInitialDelayBetweenAttemptsSeconds.env" . }}
{{- include "airbyte.worker.activityMaxDelayBetweenAttemptsSeconds.env" . }}
{{- include "airbyte.worker.configRoot.env" . }}
{{- include "airbyte.worker.maxNotifyWorkers.env" . }}
{{- include "airbyte.worker.maxSyncWorkers.env" . }}
{{- include "airbyte.worker.shouldRunNotifyWorkflows.env" . }}
{{- include "airbyte.worker.syncJobMaxAttempts.env" . }}
{{- include "airbyte.worker.syncJobMaxTimeoutDays.env" . }}
{{- include "airbyte.worker.syncJobInitRetryTimeoutMinutes.env" . }}
{{- include "airbyte.worker.useStreamCapableState.env" . }}
{{- include "airbyte.worker.workflowFailureRestartDelaySeconds.env" . }}
{{- include "airbyte.worker.workspaceDockerMount.env" . }}
{{- include "airbyte.worker.workspaceRoot.env" . }}
{{- include "airbyte.worker.environment.env" . }}
{{- end }}

{{/*
Renders the set of all worker config map variables
*/}}
{{- define "airbyte.worker.configVars" }}
ACTIVITY_MAX_ATTEMPT: {{ include "airbyte.worker.activityMaxAttempt" . | quote }}
ACTIVITY_INITIAL_DELAY_BETWEEN_ATTEMPTS_SECONDS: {{ include "airbyte.worker.activityInitialDelayBetweenAttemptsSeconds" . | quote }}
ACTIVITY_MAX_DELAY_BETWEEN_ATTEMPTS_SECONDS: {{ include "airbyte.worker.activityMaxDelayBetweenAttemptsSeconds" . | quote }}
CONFIG_ROOT: {{ include "airbyte.worker.configRoot" . | quote }}
MAX_NOTIFY_WORKERS: {{ include "airbyte.worker.maxNotifyWorkers" . | quote }}
MAX_SYNC_WORKER: {{ include "airbyte.worker.maxSyncWorkers" . | quote }}
SHOULD_RUN_NOTIFY_WORKFLOWS: {{ include "airbyte.worker.shouldRunNotifyWorkflows" . | quote }}
SYNC_JOB_MAX_ATTEMPTS: {{ include "airbyte.worker.syncJobMaxAttempts" . | quote }}
SYNC_JOB_MAX_TIMEOUT_DAYS: {{ include "airbyte.worker.syncJobMaxTimeoutDays" . | quote }}
SYNC_JOB_INIT_RETRY_TIMEOUT_MINUTES: {{ include "airbyte.worker.syncJobInitRetryTimeoutMinutes" . | quote }}
USE_STREAM_CAPABLE_STATE: {{ include "airbyte.worker.useStreamCapableState" . | quote }}
WORKFLOW_FAILURE_RESTART_DELAY_SECONDS: {{ include "airbyte.worker.workflowFailureRestartDelaySeconds" . | quote }}
WORKSPACE_DOCKER_MOUNT: {{ include "airbyte.worker.workspaceDockerMount" . | quote }}
WORKSPACE_ROOT: {{ include "airbyte.worker.workspaceRoot" . | quote }}
WORKER_ENVIRONMENT: {{ include "airbyte.worker.environment" . | quote }}
{{- end }}

{{/*
Renders the set of all worker secrets
*/}}
{{- define "airbyte.worker.secrets" }}
{{- end }}

{{/*
Workloads Configuration
*/}}

{{/*
Renders the workloadLauncher secret name
*/}}
{{- define "airbyte.workloadLauncher.images.secretName" }}
{{- if .Values.workloadLauncher.secretName }}
  {{- .Values.workloadLauncher.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the workloadLauncher.connectorSidecar.image value
*/}}
{{- define "airbyte.workloadLauncher.images.connectorSidecar.image" }}
{{- include "imageUrl" (list .Values.workloadLauncher.connectorSidecar.image $) }}
{{- end }}

{{/*
Renders the CONNECTOR_SIDECAR_IMAGE environment variable
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
{{- .Values.workloadLauncher.containerOrchestrator.enabled  }}
{{- end }}

{{/*
Renders the CONTAINER_ORCHESTRATOR_ENABLED environment variable
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
Renders the CONTAINER_ORCHESTRATOR_IMAGE environment variable
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
Renders the WORKLOAD_INIT_IMAGE environment variable
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
{{- include "airbyte.workloadLauncher.images.connectorSidecar.image.env" . }}
{{- include "airbyte.workloadLauncher.images.containerOrchestrator.enabled.env" . }}
{{- include "airbyte.workloadLauncher.images.containerOrchestrator.image.env" . }}
{{- include "airbyte.workloadLauncher.images.workloadInit.image.env" . }}
{{- end }}

{{/*
Renders the set of all workloadLauncher.images config map variables
*/}}
{{- define "airbyte.workloadLauncher.images.configVars" }}
CONNECTOR_SIDECAR_IMAGE: {{ include "airbyte.workloadLauncher.images.connectorSidecar.image" . | quote }}
CONTAINER_ORCHESTRATOR_ENABLED: {{ include "airbyte.workloadLauncher.images.containerOrchestrator.enabled" . | quote }}
CONTAINER_ORCHESTRATOR_IMAGE: {{ include "airbyte.workloadLauncher.images.containerOrchestrator.image" . | quote }}
WORKLOAD_INIT_IMAGE: {{ include "airbyte.workloadLauncher.images.workloadInit.image" . | quote }}
{{- end }}

{{/*
Renders the set of all workloadLauncher.images secrets
*/}}
{{- define "airbyte.workloadLauncher.images.secrets" }}
{{- end }}

{{/*
Workloads Configuration
*/}}

{{/*
Renders the workloadLauncher secret name
*/}}
{{- define "airbyte.workloadLauncher.secretName" }}
{{- if .Values.workloadLauncher.secretName }}
  {{- .Values.workloadLauncher.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the workloadLauncher.enabled value
*/}}
{{- define "airbyte.workloadLauncher.enabled" }}
{{- .Values.workloadLauncher.enabled | default true }}
{{- end }}

{{/*
Renders the WORKLOAD_LAUNCHER_ENABLED environment variable
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
Renders the WORKLOAD_LAUNCHER_PARALLELISM environment variable
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
Renders the set of all workloadLauncher secrets
*/}}
{{- define "airbyte.workloadLauncher.secrets" }}
{{- end }}

{{/*
Workloads Configuration
*/}}

{{/*
Renders the global.workloads secret name
*/}}
{{- define "airbyte.workloads.secretName" }}
{{- if .Values.global.workloads.secretName }}
  {{- .Values.global.workloads.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.workloads.containerOrchestrator.secretName value
*/}}
{{- define "airbyte.workloads.containerOrchestrator.secretName" }}
{{- include "airbyte.storage.secretName" . }}
{{- end }}

{{/*
Renders the CONTAINER_ORCHESTRATOR_SECRET_NAME environment variable
*/}}
{{- define "airbyte.workloads.containerOrchestrator.secretName.env" }}
- name: CONTAINER_ORCHESTRATOR_SECRET_NAME
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CONTAINER_ORCHESTRATOR_SECRET_NAME
{{- end }}

{{/*
Renders the global.workloads.containerOrchestrator.dataplane.secretMountPath value
*/}}
{{- define "airbyte.workloads.containerOrchestrator.dataplane.secretMountPath" }}
{{- "/secrets/dataplane-creds" }}
{{- end }}

{{/*
Renders the CONTAINER_ORCHESTRATOR_DATA_PLANE_CREDS_SECRET_MOUNT_PATH environment variable
*/}}
{{- define "airbyte.workloads.containerOrchestrator.dataplane.secretMountPath.env" }}
- name: CONTAINER_ORCHESTRATOR_DATA_PLANE_CREDS_SECRET_MOUNT_PATH
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CONTAINER_ORCHESTRATOR_DATA_PLANE_CREDS_SECRET_MOUNT_PATH
{{- end }}

{{/*
Renders the global.workloads.containerOrchestrator.dataplane.secretName value
*/}}
{{- define "airbyte.workloads.containerOrchestrator.dataplane.secretName" }}
{{- include "airbyte.workloads.secretName" . }}
{{- end }}

{{/*
Renders the CONTAINER_ORCHESTRATOR_DATA_PLANE_CREDS_SECRET_NAME environment variable
*/}}
{{- define "airbyte.workloads.containerOrchestrator.dataplane.secretName.env" }}
- name: CONTAINER_ORCHESTRATOR_DATA_PLANE_CREDS_SECRET_NAME
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CONTAINER_ORCHESTRATOR_DATA_PLANE_CREDS_SECRET_NAME
{{- end }}

{{/*
Renders the global.workloads.containerOrchestrator.javaOpts value
*/}}
{{- define "airbyte.workloads.containerOrchestrator.javaOpts" }}
{{- "-XX:+ExitOnOutOfMemoryError -XX:MaxRAMPercentage=75.0 -javaagent:/app/dd-java-agent.jar -Ddd.profiling.enabled=true -XX:FlightRecorderOptions=stackdepth=256 -Ddd.trace.sample.rate=0.5 -Ddd.trace.request_header.tags=User-Agent:http.useragent" }}
{{- end }}

{{/*
Renders the CONTAINER_ORCHESTRATOR_JAVA_OPTS environment variable
*/}}
{{- define "airbyte.workloads.containerOrchestrator.javaOpts.env" }}
- name: CONTAINER_ORCHESTRATOR_JAVA_OPTS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CONTAINER_ORCHESTRATOR_JAVA_OPTS
{{- end }}

{{/*
Renders the global.workloads.containerOrchestrator.secretMountPath value
*/}}
{{- define "airbyte.workloads.containerOrchestrator.secretMountPath" }}
{{- "/secrets/gcp-creds" }}
{{- end }}

{{/*
Renders the CONTAINER_ORCHESTRATOR_SECRET_MOUNT_PATH environment variable
*/}}
{{- define "airbyte.workloads.containerOrchestrator.secretMountPath.env" }}
- name: CONTAINER_ORCHESTRATOR_SECRET_MOUNT_PATH
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: CONTAINER_ORCHESTRATOR_SECRET_MOUNT_PATH
{{- end }}

{{/*
Renders the global.workloads.kubernetesClientMaxIdleConnections value
*/}}
{{- define "airbyte.workloads.kubernetesClientMaxIdleConnections" }}
{{- .Values.global.workloads.kubernetesClientMaxIdleConnections | default 100 }}
{{- end }}

{{/*
Renders the KUBERNETES_CLIENT_MAX_IDLE_CONNECTIONS environment variable
*/}}
{{- define "airbyte.workloads.kubernetesClientMaxIdleConnections.env" }}
- name: KUBERNETES_CLIENT_MAX_IDLE_CONNECTIONS
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: KUBERNETES_CLIENT_MAX_IDLE_CONNECTIONS
{{- end }}

{{/*
Renders the global.workloads.kuberentesClientMaxRetries value
*/}}
{{- define "airbyte.workloads.kuberentesClientMaxRetries" }}
{{- .Values.global.workloads.kuberentesClientMaxRetries  }}
{{- end }}

{{/*
Renders the KUBERNETES_CLIENT_MAX_RETRIES environment variable
*/}}
{{- define "airbyte.workloads.kuberentesClientMaxRetries.env" }}
- name: KUBERNETES_CLIENT_MAX_RETRIES
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: KUBERNETES_CLIENT_MAX_RETRIES
{{- end }}

{{/*
Renders the global.workloads.pubSub.enabled value
*/}}
{{- define "airbyte.workloads.pubSub.enabled" }}
{{- .Values.global.workloads.pubSub.enabled  }}
{{- end }}

{{/*
Renders the PUB_SUB_ENABLED environment variable
*/}}
{{- define "airbyte.workloads.pubSub.enabled.env" }}
- name: PUB_SUB_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: PUB_SUB_ENABLED
{{- end }}

{{/*
Renders the global.workloads.pubSub.topicName value
*/}}
{{- define "airbyte.workloads.pubSub.topicName" }}
{{- .Values.global.workloads.pubSub.topicName | default "" }}
{{- end }}

{{/*
Renders the PUB_SUB_TOPIC_NAME environment variable
*/}}
{{- define "airbyte.workloads.pubSub.topicName.env" }}
- name: PUB_SUB_TOPIC_NAME
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: PUB_SUB_TOPIC_NAME
{{- end }}

{{/*
Renders the set of all workloads environment variables
*/}}
{{- define "airbyte.workloads.envs" }}
{{- include "airbyte.workloads.containerOrchestrator.secretName.env" . }}
{{- include "airbyte.workloads.containerOrchestrator.dataplane.secretMountPath.env" . }}
{{- include "airbyte.workloads.containerOrchestrator.dataplane.secretName.env" . }}
{{- include "airbyte.workloads.containerOrchestrator.javaOpts.env" . }}
{{- include "airbyte.workloads.containerOrchestrator.secretMountPath.env" . }}
{{- include "airbyte.workloads.kubernetesClientMaxIdleConnections.env" . }}
{{- include "airbyte.workloads.kuberentesClientMaxRetries.env" . }}
{{- include "airbyte.workloads.pubSub.enabled.env" . }}
{{- include "airbyte.workloads.pubSub.topicName.env" . }}
{{- end }}

{{/*
Renders the set of all workloads config map variables
*/}}
{{- define "airbyte.workloads.configVars" }}
CONTAINER_ORCHESTRATOR_SECRET_NAME: {{ include "airbyte.workloads.containerOrchestrator.secretName" . | quote }}
CONTAINER_ORCHESTRATOR_DATA_PLANE_CREDS_SECRET_MOUNT_PATH: {{ include "airbyte.workloads.containerOrchestrator.dataplane.secretMountPath" . | quote }}
CONTAINER_ORCHESTRATOR_DATA_PLANE_CREDS_SECRET_NAME: {{ include "airbyte.workloads.containerOrchestrator.dataplane.secretName" . | quote }}
CONTAINER_ORCHESTRATOR_JAVA_OPTS: {{ include "airbyte.workloads.containerOrchestrator.javaOpts" . | quote }}
CONTAINER_ORCHESTRATOR_SECRET_MOUNT_PATH: {{ include "airbyte.workloads.containerOrchestrator.secretMountPath" . | quote }}
KUBERNETES_CLIENT_MAX_IDLE_CONNECTIONS: {{ include "airbyte.workloads.kubernetesClientMaxIdleConnections" . | quote }}
KUBERNETES_CLIENT_MAX_RETRIES: {{ include "airbyte.workloads.kuberentesClientMaxRetries" . | quote }}
PUB_SUB_ENABLED: {{ include "airbyte.workloads.pubSub.enabled" . | quote }}
PUB_SUB_TOPIC_NAME: {{ include "airbyte.workloads.pubSub.topicName" . | quote }}
{{- end }}

{{/*
Renders the set of all workloads secrets
*/}}
{{- define "airbyte.workloads.secrets" }}
{{- end }}

{{/*
Workloads Configuration
*/}}

{{/*
Renders the global.workloads.queues secret name
*/}}
{{- define "airbyte.workloads.queues.secretName" }}
{{- if .Values.global.workloads.queues.secretName }}
  {{- .Values.global.workloads.queues.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.workloads.queues.check value
*/}}
{{- define "airbyte.workloads.queues.check" }}
{{- join " " .Values.global.workloads.queues.check }}
{{- end }}

{{/*
Renders the DATA_CHECK_TASK_QUEUES environment variable
*/}}
{{- define "airbyte.workloads.queues.check.env" }}
- name: DATA_CHECK_TASK_QUEUES
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DATA_CHECK_TASK_QUEUES
{{- end }}

{{/*
Renders the global.workloads.queues.discover value
*/}}
{{- define "airbyte.workloads.queues.discover" }}
{{- join " " .Values.global.workloads.queues.discover }}
{{- end }}

{{/*
Renders the DATA_DISCOVER_TASK_QUEUES environment variable
*/}}
{{- define "airbyte.workloads.queues.discover.env" }}
- name: DATA_DISCOVER_TASK_QUEUES
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DATA_DISCOVER_TASK_QUEUES
{{- end }}

{{/*
Renders the global.workloads.queues.sync value
*/}}
{{- define "airbyte.workloads.queues.sync" }}
{{- join " " .Values.global.workloads.queues.sync }}
{{- end }}

{{/*
Renders the DATA_SYNC_TASK_QUEUES environment variable
*/}}
{{- define "airbyte.workloads.queues.sync.env" }}
- name: DATA_SYNC_TASK_QUEUES
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: DATA_SYNC_TASK_QUEUES
{{- end }}

{{/*
Renders the set of all workloads.queues environment variables
*/}}
{{- define "airbyte.workloads.queues.envs" }}
{{- include "airbyte.workloads.queues.check.env" . }}
{{- include "airbyte.workloads.queues.discover.env" . }}
{{- include "airbyte.workloads.queues.sync.env" . }}
{{- end }}

{{/*
Renders the set of all workloads.queues config map variables
*/}}
{{- define "airbyte.workloads.queues.configVars" }}
DATA_CHECK_TASK_QUEUES: {{ include "airbyte.workloads.queues.check" . | quote }}
DATA_DISCOVER_TASK_QUEUES: {{ include "airbyte.workloads.queues.discover" . | quote }}
DATA_SYNC_TASK_QUEUES: {{ include "airbyte.workloads.queues.sync" . | quote }}
{{- end }}

{{/*
Renders the set of all workloads.queues secrets
*/}}
{{- define "airbyte.workloads.queues.secrets" }}
{{- end }}

