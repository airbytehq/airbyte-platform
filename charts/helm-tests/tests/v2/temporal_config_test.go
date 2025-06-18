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
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("AUTO_SETUP").Value("true"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DYNAMIC_CONFIG_FILE_PATH").Value("config/dynamicconfig/development.yaml"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("TEMPORAL_HOST").Value("airbyte-temporal:7233"),
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
