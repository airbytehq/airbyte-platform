//go:build template

package test

import (
	"testing"

	"github.com/gruntwork-io/terratest/modules/helm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TODO: move this to a common package or file
// These are all of the common keys that we expect to see populated in the config map, regardless of the value of
// `global.edition`.
var commonConfigMapKeys = toStringSet(
	"ACTIVITY_INITIAL_DELAY_BETWEEN_ATTEMPTS_SECONDS",
	"ACTIVITY_MAX_ATTEMPT",
	"ACTIVITY_MAX_DELAY_BETWEEN_ATTEMPTS_SECONDS",
	"AIRBYTE_API_HOST",
	"AIRBYTE_EDITION",
	"AIRBYTE_VERSION",
	"API_URL",
	"AUTO_DETECT_SCHEMA",
	"CONFIGS_DATABASE_MINIMUM_FLYWAY_MIGRATION_VERSION",
	"CONFIG_API_HOST",
	"CONFIG_ROOT",
	"CONNECTOR_BUILDER_API_HOST",
	"CONNECTOR_BUILDER_API_URL",
	"CONNECTOR_BUILDER_SERVER_API_HOST",
	"CONTAINER_ORCHESTRATOR_IMAGE",
	"CRON_MICRONAUT_ENVIRONMENTS",
	"DATABASE_DB",
	"DATABASE_HOST",
	"DATABASE_PORT",
	"DATABASE_URL",
	"DATA_DOCKER_MOUNT",
	"DB_DOCKER_MOUNT",
	"GOOGLE_APPLICATION_CREDENTIALS",
	"INTERNAL_API_HOST",
	"JOBS_DATABASE_MINIMUM_FLYWAY_MIGRATION_VERSION",
	"JOB_MAIN_CONTAINER_CPU_LIMIT",
	"JOB_MAIN_CONTAINER_CPU_REQUEST",
	"JOB_MAIN_CONTAINER_MEMORY_LIMIT",
	"JOB_MAIN_CONTAINER_MEMORY_REQUEST",
	"KEYCLOAK_DATABASE_URL",
	"KEYCLOAK_INTERNAL_HOST",
	"KUBERNETES_CLIENT_MAX_IDLE_CONNECTIONS",
	"LAUNCHER_MICRONAUT_ENVIRONMENTS",
	"LOCAL_ROOT",
	"LOG4J_CONFIGURATION_FILE",
	"MAX_NOTIFY_WORKERS",
	"METRIC_CLIENT",
	"MICROMETER_METRICS_ENABLED",
	"MICROMETER_METRICS_STATSD_FLAVOR",
	"MINIO_ENDPOINT",
	"OTEL_COLLECTOR_ENDPOINT",
	"RUN_DATABASE_MIGRATION_ON_STARTUP",
	"S3_PATH_STYLE_ACCESS",
	"SEGMENT_WRITE_KEY",
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
	"WORKLOAD_LAUNCHER_PARALLELISM",
	"WORKSPACE_DOCKER_MOUNT",
	"WORKSPACE_ROOT",
)

var proEditionConfigMapKeys = toStringSet(
	"KEYCLOAK_INTERNAL_HOST",
	"KEYCLOAK_PORT",
	"KEYCLOAK_HOSTNAME_URL",
	"KEYCLOAK_JAVA_OPTS_APPEND",
)

// update these if they ever diverge from "pro"
var enterpriseEditionConfigMapKeys = proEditionConfigMapKeys

var commonSecretkeys = toStringSet(
	"DATABASE_USER",
	"DEFAULT_MINIO_ACCESS_KEY",
	"DEFAULT_MINIO_SECRET_KEY",
	"WORKLOAD_API_BEARER_TOKEN",
)

var proEditionSecretKeys = toStringSet(
	"KEYCLOAK_ADMIN_USER",
	"KEYCLOAK_ADMIN_PASSWORD",
)

// update these if they ever diverge from "pro"
var enterpriseEditionSecretKeys = proEditionSecretKeys

func TestHelmTemplateWithDefaultValues(t *testing.T) {

	t.Run("basic template render", func(t *testing.T) {
		helmOpts := baseHelmOptions()
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		require.NoError(t, err, "failure rendering template")
	})

	t.Run("verify airbyte-env configmap for edition", func(t *testing.T) {
		cases := []struct {
			edition      string
			expectedKeys set[string]
		}{
			{
				edition:      "community",
				expectedKeys: commonConfigMapKeys,
			},
			{
				edition:      "enterprise",
				expectedKeys: enterpriseEditionConfigMapKeys.union(commonConfigMapKeys),
			},
			{
				edition:      "pro",
				expectedKeys: proEditionConfigMapKeys.union(commonConfigMapKeys),
			},
		}

		for _, c := range cases {
			t.Run("edition="+c.edition, func(t *testing.T) {
				helmOpts := baseHelmOptions()
				helmOpts.SetValues["global.edition"] = c.edition

				configMapYaml, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", []string{"templates/env-configmap.yaml"})
				require.NoError(t, err, "failure rendering template")

				configMap, err := getConfigMap(configMapYaml, "airbyte-airbyte-env")
				assert.NotNil(t, configMap)
				require.NoError(t, err)

				// verify keys that we find the expected keys
				for _, key := range c.expectedKeys.keys() {
					_, ok := configMap.Data[key]
					assert.True(t, ok, "expected key %s in ConfigMap for edition %s", key, c.edition)
				}

				// verify that we don't find any unexpected keys
				for key := range configMap.Data {
					assert.True(t, c.expectedKeys.contains(key), "%s is not an expected ConfigMap key for edition %s", key, c.edition)
				}
			})
		}
	})

	t.Run("verify airbyte-secrets secret for edition", func(t *testing.T) {
		cases := []struct {
			edition      string
			expectedKeys set[string]
		}{
			{
				edition:      "community",
				expectedKeys: commonSecretkeys,
			},
			{
				edition:      "enterprise",
				expectedKeys: commonSecretkeys.union(enterpriseEditionSecretKeys),
			},
			{
				edition:      "pro",
				expectedKeys: commonSecretkeys.union(proEditionSecretKeys),
			},
		}

		for _, c := range cases {
			t.Run("edition="+c.edition, func(t *testing.T) {
				helmOpts := baseHelmOptions()
				helmOpts.SetValues["global.edition"] = c.edition

				secretYaml, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", []string{"templates/secret.yaml"})
				require.NoError(t, err, "failure rendering template")

				secret, err := getSecret(secretYaml, "airbyte-airbyte-secrets")
				assert.NotNil(t, secret)
				require.NoError(t, err)

				for _, key := range c.expectedKeys.keys() {
					_, ok := secret.StringData[key]
					assert.True(t, ok, "expected key %s not found in secret", key)
				}
			})
		}
	})

	// TODO (angel): Re-enable this when we can conditionally create the airbyte-yaml secret
	//t.Run("verify airbyte-yml secret", func(t *testing.T) {
	//	cases := []struct {
	//		airbyteYamlFile string
	//		shouldRender    bool
	//	}{
	//		{
	//			airbyteYamlFile: "",
	//			shouldRender:    false,
	//		},
	//		{
	//			airbyteYamlFile: "fixtures/airbyte.yaml",
	//			shouldRender:    true,
	//		},
	//	}

	//	for _, c := range cases {
	//		t.Run("airbyteYaml="+c.airbyteYamlFile, func(t *testing.T) {
	//			helmOpts := baseHelmOptions()

	//			if c.airbyteYamlFile == "" {
	//				_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", []string{"templates/airbyte-yml-secret.yaml"})
	//				require.Error(t, err, "template should not render if empty")
	//				return
	//			}

	//			if c.airbyteYamlFile != "" {
	//				helmOpts.SetFiles = map[string]string{
	//					"airbyteYml": c.airbyteYamlFile,
	//				}
	//			}

	//			secretYaml, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", []string{"templates/airbyte-yml-secret.yaml"})
	//			require.NoError(t, err, "failure rendering template")

	//			secret, err := getSecret(secretYaml, "airbyte-airbyte-yml")
	//			assert.NotNil(t, secret)
	//			require.NoError(t, err)

	//			if c.shouldRender {
	//				assert.Equal(t, secret.Name, "airbyte-airbyte-yml")
	//				assert.NotEmpty(t, secret.Data["fileContents"])
	//			} else {
	//				assert.Empty(t, secret.Name)
	//			}
	//		})
	//	}
	//})

	t.Run("default storage configs", func(t *testing.T) {
		helmOpts := baseHelmOptions()

		configMapYaml, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", []string{"templates/env-configmap.yaml"})
		require.NoError(t, err, "failure rendering template")

		configMap, err := getConfigMap(configMapYaml, "airbyte-airbyte-env")
		assert.NotNil(t, configMap)
		require.NoError(t, err)

		// default should be in-cluster minio
		assert.Equal(t, configMap.Data["STORAGE_TYPE"], "minio")
		assert.Equal(t, configMap.Data["STORAGE_BUCKET_LOG"], "airbyte-storage")
		assert.Equal(t, configMap.Data["STORAGE_BUCKET_STATE"], "airbyte-storage")
		assert.Equal(t, configMap.Data["MINIO_ENDPOINT"], "http://airbyte-minio-svc:9000")
		assert.Equal(t, configMap.Data["S3_PATH_STYLE_ACCESS"], "true")
	})

	t.Run("default metrics client", func(t *testing.T) {
		helmOpts := baseHelmOptions()

		configMapYaml, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", []string{"templates/env-configmap.yaml"})
		require.NoError(t, err, "failure rendering template")

		configMap, err := getConfigMap(configMapYaml, "airbyte-airbyte-env")
		assert.NotNil(t, configMap)
		require.NoError(t, err)

		assert.Empty(t, configMap.Data["METRIC_CLIENT"])
		assert.Empty(t, configMap.Data["OTEL_COLLECTOR_ENDPOINT"])
	})

	t.Run("service account is created by default", func(t *testing.T) {
		helmOpts := baseHelmOptions()

		tmplYaml, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", []string{"templates/serviceaccount.yaml"})
		require.NoError(t, err, "failure rendering template")

		serviceAccount, err := getServiceAccount(tmplYaml, "airbyte-admin")
		assert.NotNil(t, serviceAccount)
		require.NoError(t, err)

		role, err := getRole(tmplYaml, "airbyte-admin-role")
		assert.NotNil(t, role)
		require.NoError(t, err)

		roleBinding, err := getRoleBinding(tmplYaml, "airbyte-admin-binding")
		assert.NotNil(t, roleBinding)
		require.NoError(t, err)
	})
}
