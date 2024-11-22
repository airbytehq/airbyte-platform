{{/*
Telemetry Configuration
*/}}

{{/*
Renders the global.metrics secret name
*/}}
{{- define "airbyte.otel.secretName" }}
{{- if .Values.global.metrics.secretName }}
  {{- .Values.global.metrics.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.metrics.otel.exporter value
*/}}
{{- define "airbyte.otel.otel.exporter" }}
{{- "http://$(DD_AGENT_HOST):4317" }}
{{- end }}

{{/*
Renders the OTEL_EXPORTER_OTLP_ENDPOINT environment variable
*/}}
{{- define "airbyte.otel.otel.exporter.env" }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: OTEL_EXPORTER_OTLP_ENDPOINT
{{- end }}

{{/*
Renders the global.metrics.otel.exporter.protocol value
*/}}
{{- define "airbyte.otel.otel.exporter.protocol" }}
{{- "grpc" }}
{{- end }}

{{/*
Renders the OTEL_EXPORTER_OTLP_PROTOCOL environment variable
*/}}
{{- define "airbyte.otel.otel.exporter.protocol.env" }}
- name: OTEL_EXPORTER_OTLP_PROTOCOL
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: OTEL_EXPORTER_OTLP_PROTOCOL
{{- end }}

{{/*
Renders the global.metrics.otel.exporter.timeout value
*/}}
{{- define "airbyte.otel.otel.exporter.timeout" }}
{{- "30000" }}
{{- end }}

{{/*
Renders the OTEL_EXPORTER_OTLP_TIMEOUT environment variable
*/}}
{{- define "airbyte.otel.otel.exporter.timeout.env" }}
- name: OTEL_EXPORTER_OTLP_TIMEOUT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: OTEL_EXPORTER_OTLP_TIMEOUT
{{- end }}

{{/*
Renders the global.metrics.otel.exporter.metricExportInterval value
*/}}
{{- define "airbyte.otel.otel.exporter.metricExportInterval" }}
{{- "10000" }}
{{- end }}

{{/*
Renders the OTEL_METRIC_EXPORT_INTERVAL environment variable
*/}}
{{- define "airbyte.otel.otel.exporter.metricExportInterval.env" }}
- name: OTEL_METRIC_EXPORT_INTERVAL
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: OTEL_METRIC_EXPORT_INTERVAL
{{- end }}

{{/*
Renders the global.metrics.otel.exporter.name value
*/}}
{{- define "airbyte.otel.otel.exporter.name" }}
{{- "otlp" }}
{{- end }}

{{/*
Renders the OTEL_METRICS_EXPORTER environment variable
*/}}
{{- define "airbyte.otel.otel.exporter.name.env" }}
- name: OTEL_METRICS_EXPORTER
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: OTEL_METRICS_EXPORTER
{{- end }}

{{/*
Renders the global.metrics.otel.resourceAttributes value
*/}}
{{- define "airbyte.otel.otel.resourceAttributes" }}
{{- "service.name=$(SERVICE_NAME),deployment.environment={{ .Values.global.env }},service.version={{ include \"common.images.version\" }}" }}
{{- end }}

{{/*
Renders the OTEL_RESOURCE_ATTRIBUTES environment variable
*/}}
{{- define "airbyte.otel.otel.resourceAttributes.env" }}
- name: OTEL_RESOURCE_ATTRIBUTES
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: OTEL_RESOURCE_ATTRIBUTES
{{- end }}

{{/*
Renders the set of all otel environment variables
*/}}
{{- define "airbyte.otel.envs" }}
{{- include "airbyte.otel.otel.exporter.env" . }}
{{- include "airbyte.otel.otel.exporter.protocol.env" . }}
{{- include "airbyte.otel.otel.exporter.timeout.env" . }}
{{- include "airbyte.otel.otel.exporter.metricExportInterval.env" . }}
{{- include "airbyte.otel.otel.exporter.name.env" . }}
{{- include "airbyte.otel.otel.resourceAttributes.env" . }}
{{- end }}

{{/*
Renders the set of all otel config map variables
*/}}
{{- define "airbyte.otel.configVars" }}
OTEL_EXPORTER_OTLP_ENDPOINT: {{ include "airbyte.otel.otel.exporter" . | quote }}
OTEL_EXPORTER_OTLP_PROTOCOL: {{ include "airbyte.otel.otel.exporter.protocol" . | quote }}
OTEL_EXPORTER_OTLP_TIMEOUT: {{ include "airbyte.otel.otel.exporter.timeout" . | quote }}
OTEL_METRIC_EXPORT_INTERVAL: {{ include "airbyte.otel.otel.exporter.metricExportInterval" . | quote }}
OTEL_METRICS_EXPORTER: {{ include "airbyte.otel.otel.exporter.name" . | quote }}
OTEL_RESOURCE_ATTRIBUTES: {{ include "airbyte.otel.otel.resourceAttributes" . | quote }}
{{- end }}

{{/*
Renders the set of all otel secrets
*/}}
{{- define "airbyte.otel.secrets" }}
{{- end }}

{{/*
Telemetry Configuration
*/}}

{{/*
Renders the global.tracking secret name
*/}}
{{- define "airbyte.tracking.secretName" }}
{{- if .Values.global.tracking.secretName }}
  {{- .Values.global.tracking.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.tracking.enabled value
*/}}
{{- define "airbyte.tracking.enabled" }}
{{- .Values.global.tracking.enabled | default true }}
{{- end }}

{{/*
Renders the TRACKING_ENABLED environment variable
*/}}
{{- define "airbyte.tracking.enabled.env" }}
- name: TRACKING_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: TRACKING_ENABLED
{{- end }}

{{/*
Renders the global.tracking.strategy value
*/}}
{{- define "airbyte.tracking.strategy" }}
{{- .Values.global.tracking.strategy | default "segment" }}
{{- end }}

{{/*
Renders the TRACKING_STRATEGY environment variable
*/}}
{{- define "airbyte.tracking.strategy.env" }}
- name: TRACKING_STRATEGY
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: TRACKING_STRATEGY
{{- end }}

{{/*
Renders the global.tracking.segment.writeKeySecretKey value
*/}}
{{- define "airbyte.tracking.segment.writeKeySecretKey" }}
{{- .Values.global.tracking.segment.writeKeySecretKey | default "7UDdp5K55CyiGgsauOr2pNNujGvmhaeu" }}
{{- end }}

{{/*
Renders the SEGMENT_WRITE_KEY environment variable
*/}}
{{- define "airbyte.tracking.segment.writeKeySecretKey.env" }}
- name: SEGMENT_WRITE_KEY
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: SEGMENT_WRITE_KEY
{{- end }}

{{/*
Renders the set of all tracking environment variables
*/}}
{{- define "airbyte.tracking.envs" }}
{{- include "airbyte.tracking.enabled.env" . }}
{{- include "airbyte.tracking.strategy.env" . }}
{{- include "airbyte.tracking.segment.writeKeySecretKey.env" . }}
{{- end }}

{{/*
Renders the set of all tracking config map variables
*/}}
{{- define "airbyte.tracking.configVars" }}
TRACKING_ENABLED: {{ include "airbyte.tracking.enabled" . | quote }}
TRACKING_STRATEGY: {{ include "airbyte.tracking.strategy" . | quote }}
SEGMENT_WRITE_KEY: {{ include "airbyte.tracking.segment.writeKeySecretKey" . | quote }}
{{- end }}

{{/*
Renders the set of all tracking secrets
*/}}
{{- define "airbyte.tracking.secrets" }}
{{- end }}

{{/*
Telemetry Configuration
*/}}

{{/*
Renders the global.metrics secret name
*/}}
{{- define "airbyte.metrics.secretName" }}
{{- if .Values.global.metrics.secretName }}
  {{- .Values.global.metrics.secretName | quote }}
{{- else }}
  {{- .Release.Name }}-airbyte-secrets
{{- end }}
{{- end }}

{{/*
Renders the global.metrics.client value
*/}}
{{- define "airbyte.metrics.client" }}
{{- .Values.global.metrics.client  }}
{{- end }}

{{/*
Renders the METRIC_CLIENT environment variable
*/}}
{{- define "airbyte.metrics.client.env" }}
- name: METRIC_CLIENT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: METRIC_CLIENT
{{- end }}

{{/*
Renders the global.metrics.micrometer.enabled value
*/}}
{{- define "airbyte.metrics.micrometer.enabled" }}
{{- .Values.global.metrics.micrometer.enabled  }}
{{- end }}

{{/*
Renders the MICROMETER_METRICS_ENABLED environment variable
*/}}
{{- define "airbyte.metrics.micrometer.enabled.env" }}
- name: MICROMETER_METRICS_ENABLED
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: MICROMETER_METRICS_ENABLED
{{- end }}

{{/*
Renders the global.metrics.micrometer.statsdFlavor value
*/}}
{{- define "airbyte.metrics.micrometer.statsdFlavor" }}
{{- .Values.global.metrics.micrometer.statsdFlavor | default "datadog" }}
{{- end }}

{{/*
Renders the MICROMETER_METRICS_STATSD_FLAVOR environment variable
*/}}
{{- define "airbyte.metrics.micrometer.statsdFlavor.env" }}
- name: MICROMETER_METRICS_STATSD_FLAVOR
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: MICROMETER_METRICS_STATSD_FLAVOR
{{- end }}

{{/*
Renders the global.metrics.otel.collector.endpoint value
*/}}
{{- define "airbyte.metrics.otel.collector.endpoint" }}
{{- .Values.global.metrics.otel.collector.endpoint  }}
{{- end }}

{{/*
Renders the OTEL_COLLECTOR_ENDPOINT environment variable
*/}}
{{- define "airbyte.metrics.otel.collector.endpoint.env" }}
- name: OTEL_COLLECTOR_ENDPOINT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: OTEL_COLLECTOR_ENDPOINT
{{- end }}

{{/*
Renders the global.metrics.statsd.host value
*/}}
{{- define "airbyte.metrics.statsd.host" }}
{{- .Values.global.metrics.statsd.host | default "localhost" }}
{{- end }}

{{/*
Renders the STATSD_HOST environment variable
*/}}
{{- define "airbyte.metrics.statsd.host.env" }}
- name: STATSD_HOST
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: STATSD_HOST
{{- end }}

{{/*
Renders the global.metrics.statsd.port value
*/}}
{{- define "airbyte.metrics.statsd.port" }}
{{- .Values.global.metrics.statsd.port | default "8125" }}
{{- end }}

{{/*
Renders the STATSD_PORT environment variable
*/}}
{{- define "airbyte.metrics.statsd.port.env" }}
- name: STATSD_PORT
  valueFrom:
    configMapKeyRef:
      name: {{ .Release.Name }}-airbyte-env
      key: STATSD_PORT
{{- end }}

{{/*
Renders the set of all metrics environment variables
*/}}
{{- define "airbyte.metrics.envs" }}
{{- include "airbyte.metrics.client.env" . }}
{{- include "airbyte.metrics.micrometer.enabled.env" . }}
{{- include "airbyte.metrics.micrometer.statsdFlavor.env" . }}
{{- include "airbyte.metrics.otel.collector.endpoint.env" . }}
{{- include "airbyte.metrics.statsd.host.env" . }}
{{- include "airbyte.metrics.statsd.port.env" . }}
{{- end }}

{{/*
Renders the set of all metrics config map variables
*/}}
{{- define "airbyte.metrics.configVars" }}
METRIC_CLIENT: {{ include "airbyte.metrics.client" . | quote }}
MICROMETER_METRICS_ENABLED: {{ include "airbyte.metrics.micrometer.enabled" . | quote }}
MICROMETER_METRICS_STATSD_FLAVOR: {{ include "airbyte.metrics.micrometer.statsdFlavor" . | quote }}
OTEL_COLLECTOR_ENDPOINT: {{ include "airbyte.metrics.otel.collector.endpoint" . | quote }}
STATSD_HOST: {{ include "airbyte.metrics.statsd.host" . | quote }}
STATSD_PORT: {{ include "airbyte.metrics.statsd.port" . | quote }}
{{- end }}

{{/*
Renders the set of all metrics secrets
*/}}
{{- define "airbyte.metrics.secrets" }}
{{- end }}

