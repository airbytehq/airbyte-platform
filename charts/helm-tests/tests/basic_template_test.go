package tests

import (
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHelmTemplateWithDefaultValues(t *testing.T) {

	chartYaml := renderChart(t, BaseHelmOptions())
	envMap := getConfigMap(chartYaml, "airbyte-airbyte-env")

	t.Run("storage configs", func(t *testing.T) {
		assert.Equal(t, envMap.Data["STORAGE_TYPE"], "minio")
		assert.Equal(t, envMap.Data["STORAGE_BUCKET_LOG"], "airbyte-storage")
		assert.Equal(t, envMap.Data["STORAGE_BUCKET_STATE"], "airbyte-storage")
		assert.Equal(t, envMap.Data["MINIO_ENDPOINT"], "http://airbyte-minio-svc:9000")
		assert.Equal(t, envMap.Data["S3_PATH_STYLE_ACCESS"], "true")
	})

	t.Run("metrics client", func(t *testing.T) {
		assert.Empty(t, envMap.Data["METRIC_CLIENT"])
		assert.Empty(t, envMap.Data["OTEL_COLLECTOR_ENDPOINT"])
	})

	t.Run("airbyte-env configmap", func(t *testing.T) {
		// Make sure the env config map has all (and only) the expected keys.
		configMap := getConfigMap(chartYaml, "airbyte-airbyte-env")
		keys := slices.Collect(maps.Keys(configMap.Data))
		assert.ElementsMatch(t, keys, commonConfigMapKeys)
	})

	t.Run("airbyte-secrets secret", func(t *testing.T) {
		// Make sure the secret has all (and only) the expected keys.
		secret := getSecret(chartYaml, "airbyte-airbyte-secrets")
		keys := slices.Collect(maps.Keys(secret.StringData))
		assert.ElementsMatch(t, keys, commonSecretkeys)
	})

	t.Run("the airbyte-airbyte-yml secret is not created by default", func(t *testing.T) {
		// The airbyte-airbyte-yml secret is not created by default.
		// The global.airbyteYml value must be set in order to render this resource.
		assertNoResource(t, chartYaml, "Secret", "airbyte-airbyte-yml")
	})

	t.Run("service account is created by default", func(t *testing.T) {
		assert.NotNil(t, getServiceAccount(chartYaml, "airbyte-admin"))
		assert.NotNil(t, getRole(chartYaml, "airbyte-admin-role"))
		getRoleBinding(chartYaml, "airbyte-admin-binding")
	})
}

func TestAirbyteYmlSecret(t *testing.T) {
	// The airbyte-airbyte-yml secret is created when the global.airbyteYml value is set.
	opts := BaseHelmOptions()
	opts.SetFiles = map[string]string{
		"global.airbyteYml": "fixtures/airbyte.yaml",
	}
	chartYml := renderChart(t, opts)
	secret := getSecret(chartYml, "airbyte-airbyte-yml")
	assert.Equal(t, secret.Name, "airbyte-airbyte-yml")
	assert.NotEmpty(t, secret.Data["fileContents"])
}

func TestEnterpriseConfigKeys(t *testing.T) {
	opts := BaseHelmOptionsForEnterpriseWithValues()
	chartYaml := renderChart(t, opts)

	configMap := getConfigMap(chartYaml, "airbyte-airbyte-env")
	keys := slices.Collect(maps.Keys(configMap.Data))
	assert.ElementsMatch(t, keys, enterpriseEditionConfigMapKeys)

	secret := getSecret(chartYaml, "airbyte-airbyte-secrets")
	keys = slices.Collect(maps.Keys(secret.StringData))
	assert.ElementsMatch(t, keys, enterpriseEditionSecretKeys)
}

func TestProConfigKeys(t *testing.T) {
	opts := BaseHelmOptions()
	opts.SetValues["global.edition"] = "pro"
	opts.SetValues["global.auth.instanceAdmin.firstName"] = "Octavia"
	opts.SetValues["global.auth.instanceAdmin.lastName"] = "Squidington"
	chartYaml := renderChart(t, opts)

	configMap := getConfigMap(chartYaml, "airbyte-airbyte-env")
	keys := slices.Collect(maps.Keys(configMap.Data))
	assert.ElementsMatch(t, keys, enterpriseEditionConfigMapKeys)

	secret := getSecret(chartYaml, "airbyte-airbyte-secrets")
	keys = slices.Collect(maps.Keys(secret.StringData))
	assert.ElementsMatch(t, keys, enterpriseEditionSecretKeys)
}

// These are all of the common keys that we expect to see populated in the config map,
// regardless of the value of `global.edition`.
var commonConfigMapKeys = []string{
	"ACTIVITY_INITIAL_DELAY_BETWEEN_ATTEMPTS_SECONDS",
	"ACTIVITY_MAX_ATTEMPT",
	"ACTIVITY_MAX_DELAY_BETWEEN_ATTEMPTS_SECONDS",
	"AIRBYTE_API_HOST",
	"AIRBYTE_SERVER_HOST",
	"AIRBYTE_EDITION",
	"AIRBYTE_URL",
	"AIRBYTE_VERSION",
	"API_URL",
	"CONFIGS_DATABASE_MINIMUM_FLYWAY_MIGRATION_VERSION",
	"CONFIG_API_HOST",
	"CONFIG_ROOT",
	"CONNECTOR_BUILDER_API_HOST",
	"CONNECTOR_BUILDER_API_URL",
	"CONNECTOR_BUILDER_SERVER_API_HOST",
	"CONTAINER_ORCHESTRATOR_IMAGE",
	"CONNECTOR_SIDECAR_IMAGE",
	"CRON_MICRONAUT_ENVIRONMENTS",
	"DATABASE_DB",
	"DATABASE_HOST",
	"DATABASE_PORT",
	"DATABASE_URL",
	"DATA_DOCKER_MOUNT",
	"DB_DOCKER_MOUNT",
	"FILE_TRANSFER_EPHEMERAL_STORAGE_LIMIT",
	"FILE_TRANSFER_EPHEMERAL_STORAGE_REQUEST",
	"GOOGLE_APPLICATION_CREDENTIALS",
	"INTERNAL_API_HOST",
	"JOBS_DATABASE_MINIMUM_FLYWAY_MIGRATION_VERSION",
	"JOB_KUBE_CONNECTOR_IMAGE_REGISTRY",
	"JOB_KUBE_LOCAL_VOLUME_ENABLED",
	"JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_SECRET",
	"JOB_MAIN_CONTAINER_CPU_LIMIT",
	"JOB_MAIN_CONTAINER_CPU_REQUEST",
	"JOB_MAIN_CONTAINER_MEMORY_LIMIT",
	"JOB_MAIN_CONTAINER_MEMORY_REQUEST",
	"KEYCLOAK_DATABASE_URL",
	"KEYCLOAK_INTERNAL_HOST",
	"KUBERNETES_CLIENT_MAX_IDLE_CONNECTIONS",
	"LAUNCHER_MICRONAUT_ENVIRONMENTS",
	"LOG_LEVEL",
	"MAX_NOTIFY_WORKERS",
	"METRIC_CLIENT",
	"MICROMETER_METRICS_ENABLED",
	"MICROMETER_METRICS_STATSD_FLAVOR",
	"MINIO_ENDPOINT",
	"OTEL_COLLECTOR_ENDPOINT",
	"RUN_DATABASE_MIGRATION_ON_STARTUP",
	"S3_PATH_STYLE_ACCESS",
	"SEGMENT_WRITE_KEY",
	"SERVER_MICRONAUT_ENVIRONMENTS",
	"SHOULD_RUN_NOTIFY_WORKFLOWS",
	"STATSD_HOST",
	"STATSD_PORT",
	"STORAGE_BUCKET_ACTIVITY_PAYLOAD",
	"STORAGE_BUCKET_LOG",
	"STORAGE_BUCKET_STATE",
	"STORAGE_BUCKET_WORKLOAD_OUTPUT",
	"STORAGE_TYPE",
	"TEMPORAL_HOST",
	"TEMPORAL_WORKER_PORTS",
	"TRACKING_STRATEGY",
	"WEBAPP_URL",
	"WORKERS_MICRONAUT_ENVIRONMENTS",
	"WORKER_ENVIRONMENT",
	"WORKFLOW_FAILURE_RESTART_DELAY_SECONDS",
	"WORKLOAD_API_HOST",
	"WORKLOAD_INIT_IMAGE",
	"WORKLOAD_LAUNCHER_PARALLELISM",
	"WORKSPACE_DOCKER_MOUNT",
	"WORKSPACE_ROOT",
	"PUB_SUB_ENABLED",
	"PUB_SUB_TOPIC_NAME",
	"ENTERPRISE_SOURCE_STUBS_URL",
}

var proEditionConfigMapKeys = append([]string{
	"INITIAL_USER_FIRST_NAME",
	"INITIAL_USER_LAST_NAME",
	"KEYCLOAK_PORT",
	"KEYCLOAK_HOSTNAME_URL",
	"KEYCLOAK_JAVA_OPTS_APPEND",
}, commonConfigMapKeys...)

// update these if they ever diverge from "pro"
var enterpriseEditionConfigMapKeys = proEditionConfigMapKeys

var commonSecretkeys = []string{
	"DATABASE_USER",
	"DATABASE_PASSWORD",
	"MINIO_ACCESS_KEY_ID",
	"MINIO_SECRET_ACCESS_KEY",
	"WORKLOAD_API_BEARER_TOKEN",
}

var proEditionSecretKeys = append([]string{
	"KEYCLOAK_ADMIN_USER",
	"KEYCLOAK_ADMIN_PASSWORD",
}, commonSecretkeys...)

// update these if they ever diverge from "pro"
var enterpriseEditionSecretKeys = proEditionSecretKeys
