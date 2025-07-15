package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestDefaultDatabaseConfiguration(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	expectedEnvVars := []helmtests.ExpectedEnvVar{
		helmtests.ExpectedConfigMapVar().Name("DATABASE_HOST").RefName("airbyte-airbyte-env").Value("airbyte-db-svc.ab.svc.cluster.local"),
		helmtests.ExpectedConfigMapVar().Name("DATABASE_PORT").RefName("airbyte-airbyte-env").Value("5432"),
		helmtests.ExpectedSecretVar().Name("DATABASE_USER").RefName("airbyte-airbyte-secrets").Value("airbyte"),
		helmtests.ExpectedSecretVar().Name("DATABASE_PASSWORD").RefName("airbyte-airbyte-secrets").Value("airbyte"),
	}

	releaseApps := appsForRelease("airbyte")
	for _, name := range []string{"server", "worker", "workload-api-server"} {
		app := releaseApps[name]
		chartYaml.VerifyEnvVarsForApp(t, app.Kind, app.FQN(), expectedEnvVars)
	}
}

func TestDatabaseConfiguration(t *testing.T) {
	t.Run("override default credentials", func(tt *testing.T) {
		opts := helmtests.BaseHelmOptions()
		opts.SetValues["global.database.user"] = "custom-user"
		opts.SetValues["global.database.password"] = "custom-password"
		chartYaml, err := helmtests.RenderHelmChart(tt, opts, chartPath, "airbyte", nil)
		assert.NoError(t, err)

		expectedEnvVars := []helmtests.ExpectedEnvVar{
			helmtests.ExpectedConfigMapVar().Name("DATABASE_HOST").RefName("airbyte-airbyte-env").Value("airbyte-db-svc.ab.svc.cluster.local"),
			helmtests.ExpectedConfigMapVar().Name("DATABASE_PORT").RefName("airbyte-airbyte-env").Value("5432"),
			helmtests.ExpectedSecretVar().Name("DATABASE_USER").RefName("airbyte-airbyte-secrets").Value("custom-user"),
			helmtests.ExpectedSecretVar().Name("DATABASE_PASSWORD").RefName("airbyte-airbyte-secrets").Value("custom-password"),
		}

		releaseApps := appsForRelease("airbyte")
		for _, name := range []string{"server", "worker", "workload-api-server"} {
			app := releaseApps[name]
			chartYaml.VerifyEnvVarsForApp(tt, app.Kind, app.FQN(), expectedEnvVars)
		}

	})

	t.Run("set user-specified secretName with default coordinates", func(tt *testing.T) {
		opts := helmtests.BaseHelmOptions()
		opts.SetValues["global.database.secretName"] = "custom-secret"
		chartYaml, err := helmtests.RenderHelmChart(tt, opts, chartPath, "airbyte", nil)
		assert.NoError(t, err)

		expectedEnvVars := []helmtests.ExpectedEnvVar{
			helmtests.ExpectedConfigMapVar().Name("DATABASE_HOST").RefName("airbyte-airbyte-env").Value("airbyte-db-svc.ab.svc.cluster.local"),
			helmtests.ExpectedConfigMapVar().Name("DATABASE_PORT").RefName("airbyte-airbyte-env").Value("5432"),
			helmtests.ExpectedSecretVar().Name("DATABASE_USER").RefName("custom-secret"),
			helmtests.ExpectedSecretVar().Name("DATABASE_PASSWORD").RefName("custom-secret"),
		}

		releaseApps := appsForRelease("airbyte")
		for _, name := range []string{"server", "worker", "workload-api-server"} {
			app := releaseApps[name]
			chartYaml.VerifyEnvVarsForApp(tt, app.Kind, app.FQN(), expectedEnvVars)
		}

	})

	t.Run("set user-specified secretName with custom secret coordinates", func(tt *testing.T) {
		opts := helmtests.BaseHelmOptions()
		opts.SetValues["global.database.secretName"] = "custom-secret"
		opts.SetValues["global.database.userSecretKey"] = "CUSTOM_USER_KEY"
		opts.SetValues["global.database.passwordSecretKey"] = "CUSTOM_PASSWORD_KEY"
		chartYaml, err := helmtests.RenderHelmChart(tt, opts, chartPath, "airbyte", nil)
		assert.NoError(t, err)

		expectedEnvVars := []helmtests.ExpectedEnvVar{
			helmtests.ExpectedConfigMapVar().Name("DATABASE_HOST").RefName("airbyte-airbyte-env").Value("airbyte-db-svc.ab.svc.cluster.local"),
			helmtests.ExpectedConfigMapVar().Name("DATABASE_PORT").RefName("airbyte-airbyte-env").Value("5432"),
			helmtests.ExpectedSecretVar().Name("DATABASE_USER").RefName("custom-secret").RefKey("CUSTOM_USER_KEY"),
			helmtests.ExpectedSecretVar().Name("DATABASE_PASSWORD").RefName("custom-secret").RefKey("CUSTOM_PASSWORD_KEY"),
		}

		releaseApps := appsForRelease("airbyte")
		for _, name := range []string{"server", "worker", "workload-api-server"} {
			app := releaseApps[name]
			chartYaml.VerifyEnvVarsForApp(tt, app.Kind, app.FQN(), expectedEnvVars)
		}
	})
}
