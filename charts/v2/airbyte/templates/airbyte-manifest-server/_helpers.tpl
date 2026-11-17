{{/*
The manifest server runs in the release namespace, or with manifestServer.runInJobsNamespace next to the direct job
runner, where the jobs namespace's network policies cover the calls it makes from customer config.
*/}}
{{- define "airbyte.manifestServer.namespace" }}
{{- if .Values.manifestServer.runInJobsNamespace }}
{{- include "airbyte.directJobRunner.namespace" . }}
{{- else }}
{{- include "airbyte.namespace" . }}
{{- end }}
{{- end }}

{{/*
Env for a manifest server in the jobs namespace. The env ConfigMaps only exist in the airbyte namespace, so ConfigMap
references become literal values and other secret references are dropped, like the runner's. The JWT signature secret
comes from the runner's copy in the jobs namespace, so both read the same value.
*/}}
{{- define "airbyte.manifestServer.jobsNamespaceEnvs" }}
{{- $secretName := .Values.directJobRunner.manifestServerSecretName }}
{{- if not $secretName }}
{{- fail "manifestServer.runInJobsNamespace needs directJobRunner.manifestServerSecretName, the jobs-namespace secret with the JWT signature secret" }}
{{- end }}
{{- $vars := dict }}
{{- range $tpl := list "airbyte.common.configVars" "airbyte.logging.configVars" "airbyte.metrics.configVars" }}
{{- $vars = merge $vars (include $tpl $ | fromYaml) }}
{{- end }}
{{- range $tpl := list "airbyte.common.envs" "airbyte.logging.envs" "airbyte.metrics.envs" }}
{{- range $env := (include $tpl $ | fromYamlArray) }}
{{- if and $env.valueFrom $env.valueFrom.configMapKeyRef }}
{{- $key := $env.valueFrom.configMapKeyRef.key }}
{{- if not (hasKey $vars $key) }}
{{- fail (printf "airbyte-manifest-server: no config value for %s" $key) }}
{{- end }}
- name: {{ $env.name }}
  value: {{ index $vars $key | toString | quote }}
{{- else if not (and $env.valueFrom $env.valueFrom.secretKeyRef) }}
{{ list $env | toYaml }}
{{- end }}
{{- end }}
{{- end }}
{{- range $env := (include "airbyte.auth.jwt.envs" . | fromYamlArray) }}
{{- $_ := set $env.valueFrom.secretKeyRef "name" $secretName }}
{{ list $env | toYaml }}
{{- end }}
{{- end }}
