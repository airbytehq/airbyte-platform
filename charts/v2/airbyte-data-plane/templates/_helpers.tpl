{{/*
Expand the name of the chart.
*/}}
{{- define "airbyte-data-plane.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "airbyte-data-plane.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "airbyte-data-plane.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "airbyte-data-plane.labels" -}}
helm.sh/chart: {{ include "airbyte-data-plane.chart" . }}
{{ include "airbyte-data-plane.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "airbyte-data-plane.selectorLabels" -}}
app.kubernetes.io/name: {{ include "airbyte-data-plane.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "airbyte-data-plane.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "airbyte-data-plane.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Construct comma separated list of key/value pairs from object (useful for ENV var values)
*/}}
{{- define "airbyte-data-plane.flattenMap" -}}
{{- $kvList := list -}}
{{- range $key, $value := . -}}
{{- $kvList = printf "%s=%s" $key $value | mustAppend $kvList -}}
{{- end -}}
{{ join "," $kvList }}
{{- end -}}

{{/*
Construct semi-colon delimited list of comma separated key/value pairs from array of objects (useful for ENV var values)
*/}}
{{- define "airbyte-data-plane.flattenArrayMap" -}}
{{- $mapList := list -}}
{{- range $element := . -}}
{{- $mapList = include "airbyte-data-plane.flattenMap" $element | mustAppend $mapList -}}
{{- end -}}
{{ join ";" $mapList }}
{{- end -}}

{{/*
Convert tags to a comma-separated list of key=value pairs.
*/}}
{{- define "airbyte-data-plane.tagsToString" -}}
{{- $result := list -}}
{{- range . -}}
  {{- $key := .key -}}
  {{- $value := .value -}}
  {{- if eq (typeOf $value) "bool" -}}
    {{- $value = ternary "true" "false" $value -}}
  {{- end -}}
  {{- $result = append $result (printf "%s=%s" $key $value) -}}
{{- end -}}
{{- join "," $result -}}
{{- end -}}
