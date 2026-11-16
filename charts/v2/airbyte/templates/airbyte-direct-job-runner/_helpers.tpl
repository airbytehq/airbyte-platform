{{/*
The runner runs next to connector pods, so connect probes leave through the same egress policy. Cloud sets
global.jobs.kube.namespace; OSS runs jobs in the release namespace.
*/}}
{{- define "airbyte.directJobRunner.namespace" }}
{{- .Values.global.jobs.kube.namespace | default (include "airbyte.namespace" .) }}
{{- end }}

{{/*
Config env for the runner. The shared env ConfigMap only exists in the airbyte namespace, so a pod in the jobs
namespace cannot reference it: ConfigMap references become literal values, fieldRefs are kept, and secret references
are dropped because those secrets may not exist in the jobs namespace.
*/}}
{{- define "airbyte.directJobRunner.configEnvs" }}
{{- $vars := dict }}
{{- range $tpl := list "airbyte.common.configVars" "airbyte.cluster.configVars" "airbyte.workloadApiServer.configVars" "airbyte.workloadLauncher.configVars" "airbyte.micronaut.configVars" "airbyte.metrics.configVars" "airbyte.storage.configVars" "airbyte.secretsManager.configVars" "airbyte.featureFlags.configVars" "airbyte.directJobRunner.configVars" }}
{{- $vars = merge $vars (include $tpl $ | fromYaml) }}
{{- end }}
{{- range $tpl := list "airbyte.common.envs" "airbyte.cluster.envs" "airbyte.workloadApiServer.envs" "airbyte.workloadLauncher.controlPlane.tokenEndpoint.env" "airbyte.micronaut.envs" "airbyte.metrics.envs" "airbyte.storage.envs" "airbyte.secretsManager.envs" "airbyte.featureFlags.envs" "airbyte.directJobRunner.envs" }}
{{- range $env := (include $tpl $ | fromYamlArray) }}
{{- if and $env.valueFrom $env.valueFrom.configMapKeyRef }}
{{- $key := $env.valueFrom.configMapKeyRef.key }}
{{- if not (hasKey $vars $key) }}
{{- fail (printf "airbyte-direct-job-runner: no config value for %s" $key) }}
{{- end }}
- name: {{ $env.name }}
  value: {{ index $vars $key | toString | quote }}
{{- else if not (and $env.valueFrom $env.valueFrom.secretKeyRef) }}
{{ list $env | toYaml }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Secret-backed storage credentials. Secrets are namespaced, so storageSecretName points at a copy in the jobs
namespace when the global storage secret only exists in the airbyte namespace.
*/}}
{{- define "airbyte.directJobRunner.storageSecretEnvs" }}
{{- $secretName := .Values.directJobRunner.storageSecretName }}
{{- range $env := (include "airbyte.storage.envs" . | fromYamlArray) }}
{{- if and $env.valueFrom $env.valueFrom.secretKeyRef }}
{{- if $secretName }}
{{- $_ := set $env.valueFrom.secretKeyRef "name" $secretName }}
{{- end }}
{{ list $env | toYaml }}
{{- end }}
{{- end }}
{{- end }}

{{/*
What the runner needs to run checks for Airbyte's manifest-only connectors on the release's manifest server (whose
address comes with the config envs): the CDK version the server runs (its image tag), and the JWT signature secret it
checks calls against. Secrets are namespaced, so manifestServerSecretName names a copy in the jobs namespace that has
the secret's key; the runner gets neither setting without it, so enabling the manifest server alone can't leave the
runner unable to start, and the runner sends the server nothing.
*/}}
{{- define "airbyte.directJobRunner.manifestServerEnvs" }}
{{- $secretName := .Values.directJobRunner.manifestServerSecretName }}
{{- if and .Values.manifestServer.enabled $secretName }}
- name: MANIFEST_SERVER_CDK_VERSION
  value: {{ .Values.manifestServer.image.tag | quote }}
{{- range $env := (include "airbyte.auth.jwt.envs" . | fromYamlArray) }}
{{- $_ := set $env.valueFrom.secretKeyRef "name" $secretName }}
{{ list $env | toYaml }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Secret-backed feature flag credentials (the LaunchDarkly key), for reading platform.direct-check-images. Secrets are
namespaced, so featureFlagsSecretName points at a copy in the jobs namespace when the global feature flags secret only
exists in the airbyte namespace. The reference is optional: without the key the runner still starts, reads every flag
as its default, and takes no checks, so they run in pods.
*/}}
{{- define "airbyte.directJobRunner.featureFlagsSecretEnvs" }}
{{- $secretName := .Values.directJobRunner.featureFlagsSecretName }}
{{- range $env := (include "airbyte.featureFlags.envs" . | fromYamlArray) }}
{{- if and $env.valueFrom $env.valueFrom.secretKeyRef }}
{{- if $secretName }}
{{- $_ := set $env.valueFrom.secretKeyRef "name" $secretName }}
{{- end }}
{{- $_ := set $env.valueFrom.secretKeyRef "optional" true }}
{{ list $env | toYaml }}
{{- end }}
{{- end }}
{{- end }}

{{/*
The runner never calls the Kubernetes API, so by default it gets its own token-less ServiceAccount instead of depending
on the shared one, which not every jobs namespace has. Set directJobRunner.serviceAccountName to use an existing one.
*/}}
{{- define "airbyte.directJobRunner.serviceAccountName" }}
{{- .Values.directJobRunner.serviceAccountName | default (printf "%s-direct-job-runner" .Release.Name) }}
{{- end }}

{{/*
Secret-backed secret store credentials, for hydrating check configs. They point at the secrets check pods read in the
jobs namespace: the Vault token comes from authTokenRefName, like the launcher passes to check pods.
*/}}
{{- define "airbyte.directJobRunner.secretsManagerSecretEnvs" }}
{{- range $env := (include "airbyte.secretsManager.envs" . | fromYamlArray) }}
{{- if and $env.valueFrom $env.valueFrom.secretKeyRef }}
{{- if eq $env.name "VAULT_AUTH_TOKEN" }}
{{- $_ := set $env.valueFrom.secretKeyRef "name" (include "airbyte.secretsManager.vault.authTokenRefName" $) }}
{{- $_ := set $env.valueFrom.secretKeyRef "key" (include "airbyte.secretsManager.vault.authTokenSecretKey" $) }}
{{- end }}
{{ list $env | toYaml }}
{{- end }}
{{- end }}
{{- end }}
