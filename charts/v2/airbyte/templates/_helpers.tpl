{{/*
Expand the name of the chart.
*/}}
{{- define "airbyte.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Returns the name of a given component
*/}}
{{- define "airbyte.componentName" -}}
{{- $tplPathParts := split "/" $.Template.Name }}
{{- $indexLast := printf "_%d" (sub (len $tplPathParts) 2) }}
{{- $componentName := trimPrefix "airbyte-" (index $tplPathParts $indexLast) }}
{{- printf "%s" $componentName }}
{{- end }}

{{/*
Returns the name of a given component with the `airbyte-` prefix
*/}}
{{- define "airbyte.componentNameWithAirbytePrefix" -}}
{{- printf "airbyte-%s" (include "airbyte.componentName" .) }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "airbyte.fullname" -}}
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
{{- define "airbyte.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "airbyte.labels" -}}
helm.sh/chart: {{ include "airbyte.chart" . }}
{{ include "airbyte.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "airbyte.selectorLabels" -}}
{{ $tplPathParts := split "/" $.Template.Name }}
{{ $indexLast := printf "_%d" (sub (len $tplPathParts) 2) }}
{{ $componentName := trimPrefix "airbyte-" (index $tplPathParts $indexLast) }}
airbyte: {{ $componentName }}
app.kubernetes.io/name: {{ $componentName }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}


{{/*
Common DB labels
*/}}
{{- define "airbyte.databaseLabels" -}}
helm.sh/chart: {{ include "airbyte.chart" . }}
{{ include "airbyte.databaseSelectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector DB labels
*/}}
{{- define "airbyte.databaseSelectorLabels" -}}
app.kubernetes.io/name: {{ printf "%s-db" .Release.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Common DB labels
*/}}
{{- define "airbyte.minioLabels" -}}
helm.sh/chart: {{ include "airbyte.chart" . }}
{{ include "airbyte.minioSelectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector DB labels
*/}}
{{- define "airbyte.minioSelectorLabels" -}}
app.kubernetes.io/name: {{ printf "%s-minio" .Release.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "airbyte.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "airbyte.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Construct comma separated list of key/value pairs from object (useful for ENV var values)
*/}}
{{- define "airbyte.flattenMap" -}}
{{- $kvList := list -}}
{{- range $key, $value := . -}}
{{- $kvList = printf "%s=%s" $key $value | mustAppend $kvList -}}
{{- end -}}
{{ join "," $kvList }}
{{- end -}}

{{/*
Construct semi-colon delimited list of comma separated key/value pairs from array of objects (useful for ENV var values)
*/}}
{{- define "airbyte.flattenArrayMap" -}}
{{- $mapList := list -}}
{{- range $element := . -}}
{{- $mapList = include "airbyte.flattenMap" $element | mustAppend $mapList -}}
{{- end -}}
{{ join ";" $mapList }}
{{- end -}}

{{/*
Convert tags to a comma-separated list of key=value pairs.
*/}}
{{- define "airbyte.tagsToString" -}}
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

{{/*
Hook for passing in extra config map vars
*/}}
{{- define "airbyte.extra.configVars" }}
{{- end }}

{{/*
Hook for passing in extra secrets
*/}}
{{- define "airbyte.extra.secrets" }}
{{- end }}


{{/*
Returns a comma-delimited string of imagePullSecret names.
Usage:
  {{ include "airbyte.imagePullSecretNames" (dict "secrets" .Values.global.imagePullSecrets "extra" (list "foo" "bar")) }}
*/}}
{{- define "airbyte.imagePullSecretNames" -}}
  {{- $secrets := default list .secrets }}
  {{- $extra := default list .extra }}
  {{- $names := list }}
  {{- range $secrets }}
    {{- $names = append $names .name }}
  {{- end }}
  {{- $all := concat $names $extra }}
  {{- join "," $all }}
{{- end }}

{{/*
Renders a value that contains template perhaps with scope if the scope is present.
Usage:
{{ include "airbyte.tplvalues.render" ( dict "value" .Values.path.to.the.Value "context" $ ) }}
{{ include "airbyte.tplvalues.render" ( dict "value" .Values.path.to.the.Value "context" $ "scope" $app ) }}
*/}}
{{- define "airbyte.tplvalues.render" -}}
{{- $value := typeIs "string" .value | ternary .value (.value | toYaml) }}
{{- if contains "{{" (toJson .value) }}
  {{- if .scope }}
      {{- tpl (cat "{{- with $.RelativeScope -}}" $value "{{- end }}") (merge (dict "RelativeScope" .scope) .context) }}
  {{- else }}
    {{- tpl $value .context }}
  {{- end }}
{{- else }}
    {{- $value }}
{{- end }}
{{- end -}}
