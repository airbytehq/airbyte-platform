package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestDefaultTemporalConfig(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	temporalDeploy := chartYaml.GetResourceByKindAndName("Deployment", "airbyte-temporal")
	assert.NotNil(t, temporalDeploy)

	expectedEnvVars := []helmtests.ExpectedEnvVar{
		helmtests.ExpectedConfigMapVar().Name("AUTO_SETUP").RefName("airbyte-airbyte-env").Value("true"),
		helmtests.ExpectedConfigMapVar().Name("DYNAMIC_CONFIG_FILE_PATH").RefName("airbyte-airbyte-env").Value("config/dynamicconfig/development.yaml"),
		helmtests.ExpectedConfigMapVar().Name("TEMPORAL_HOST").RefName("airbyte-airbyte-env").Value("airbyte-temporal:7233"),
	}

	releaseApps := appsForRelease("airbyte")
	for _, name := range []string{"server", "worker", "workload-launcher"} {
		app := releaseApps[name]
		chartYaml.VerifyEnvVarsForApp(t, app.Kind, app.FQN(), expectedEnvVars)
	}
}

func TestTemporalCloudConfig(t *testing.T) {
	t.Run("disable the deployment if using temporal cloud", func(tt *testing.T) {
		opts := helmtests.BaseHelmOptions()
		opts.SetValues["global.temporal.cloud.enabled"] = "true"
		chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
		assert.NoError(tt, err)

		temporalDeploy := chartYaml.GetResourceByKindAndName("Deployment", "airbyte-temporal")
		assert.Nil(tt, temporalDeploy, "Temporal deployment should be disabled when using temporal cloud")
	})
}

func TestDefaultTemporalDatabaseConfiguration(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	// By default, we should be using the same database credentials as the primary database
	expectedEnvVars := []helmtests.ExpectedEnvVar{
		helmtests.ExpectedConfigMapVar().Name("POSTGRES_SEEDS").RefName("airbyte-airbyte-env").RefKey("DATABASE_HOST").Value("airbyte-db-svc.ab.svc.cluster.local"),
		helmtests.ExpectedConfigMapVar().Name("DB_PORT").RefName("airbyte-airbyte-env").RefKey("DATABASE_PORT").Value("5432"),
		helmtests.ExpectedSecretVar().Name("POSTGRES_USER").RefName("airbyte-airbyte-secrets").RefKey("DATABASE_USER").Value("airbyte"),
		helmtests.ExpectedSecretVar().Name("POSTGRES_PWD").RefName("airbyte-airbyte-secrets").RefKey("DATABASE_PASSWORD").Value("airbyte"),
	}

	releaseApps := appsForRelease("airbyte")
	for _, name := range []string{"temporal"} {
		app := releaseApps[name]
		chartYaml.VerifyEnvVarsForApp(t, app.Kind, app.FQN(), expectedEnvVars)
	}
}

func TestTemporalDatabaseConfiguration(t *testing.T) {
	t.Run("temporal inherits custom database configuration", func(tt *testing.T) {
		opts := helmtests.BaseHelmOptions()
		opts.SetValues["global.database.secretName"] = "custom-db-secret"
		opts.SetValues["global.database.userSecretKey"] = "custom-user-secret-key"
		opts.SetValues["global.database.passwordSecretKey"] = "custom-password-secret-key"
		chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
		assert.NoError(t, err)

		// We should inherit the same datase credential keys from the primary database config
		expectedEnvVars := []helmtests.ExpectedEnvVar{
			helmtests.ExpectedConfigMapVar().Name("POSTGRES_SEEDS").RefName("airbyte-airbyte-env").RefKey("DATABASE_HOST").Value("airbyte-db-svc.ab.svc.cluster.local"),
			helmtests.ExpectedConfigMapVar().Name("DB_PORT").RefName("airbyte-airbyte-env").RefKey("DATABASE_PORT").Value("5432"),
			helmtests.ExpectedSecretVar().Name("POSTGRES_USER").RefName("custom-db-secret").RefKey("custom-user-secret-key"),
			helmtests.ExpectedSecretVar().Name("POSTGRES_PWD").RefName("custom-db-secret").RefKey("custom-password-secret-key"),
		}

		releaseApps := appsForRelease("airbyte")
		for _, name := range []string{"temporal"} {
			app := releaseApps[name]
			chartYaml.VerifyEnvVarsForApp(t, app.Kind, app.FQN(), expectedEnvVars)
		}
	})

	t.Run("temporal-specific database configuration overrides global configuration", func(tt *testing.T) {
		opts := helmtests.BaseHelmOptions()
		opts.SetValues["global.database.secretName"] = "global-db-secret"
		opts.SetValues["global.database.userSecretKey"] = "global-user-secret-key"
		opts.SetValues["global.database.passwordSecretKey"] = "global-password-secret-key"
		opts.SetValues["temporal.database.userSecretKey"] = "temporal-user-secret-key"
		opts.SetValues["temporal.database.passwordSecretKey"] = "temporal-password-secret-key"
		chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
		assert.NoError(t, err)

		// Temporal-specific overrides should take precedence over global configuration
		expectedEnvVars := []helmtests.ExpectedEnvVar{
			helmtests.ExpectedConfigMapVar().Name("POSTGRES_SEEDS").RefName("airbyte-airbyte-env").RefKey("DATABASE_HOST").Value("airbyte-db-svc.ab.svc.cluster.local"),
			helmtests.ExpectedConfigMapVar().Name("DB_PORT").RefName("airbyte-airbyte-env").RefKey("DATABASE_PORT").Value("5432"),
			helmtests.ExpectedSecretVar().Name("POSTGRES_USER").RefName("global-db-secret").RefKey("temporal-user-secret-key"),
			helmtests.ExpectedSecretVar().Name("POSTGRES_PWD").RefName("global-db-secret").RefKey("temporal-password-secret-key"),
		}

		releaseApps := appsForRelease("airbyte")
		for _, name := range []string{"temporal"} {
			app := releaseApps[name]
			chartYaml.VerifyEnvVarsForApp(t, app.Kind, app.FQN(), expectedEnvVars)
		}
	})
}
