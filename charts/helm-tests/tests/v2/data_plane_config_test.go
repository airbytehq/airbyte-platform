package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestDefaultDataPlaneConfig(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	expectedEnvVars := []helmtests.ExpectedEnvVar{
		helmtests.ExpectedSecretVar().RefName("airbyte-auth-secrets").RefKey("DATAPLANE_CLIENT_ID"),
		helmtests.ExpectedSecretVar().RefName("airbyte-auth-secrets").RefKey("DATAPLANE_CLIENT_SECRET"),
	}

	releaseApps := appsForRelease("airbyte")
	for _, name := range []string{"workload-launcher"} {
		app := releaseApps[name]
		chartYaml.VerifyEnvVarsForApp(t, app.Kind, app.FQN(), expectedEnvVars)
	}
}

func TestCustomSecretDataPlaneConfig(t *testing.T) {
	t.Run("should default to referencing the common secret when not a hybrid cluster", func(tt *testing.T) {
		opts := helmtests.BaseHelmOptions()
		// IFF the cluster type is not "hybrid" then read the values from the secret at `workloadLauncher.dataPlane.secretName`
		opts.SetValues["global.cluster.type"] = "data-plane"
		chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
		assert.NoError(tt, err)

		expectedEnvVars := []helmtests.ExpectedEnvVar{
			helmtests.ExpectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("DATAPLANE_CLIENT_ID"),
			helmtests.ExpectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("DATAPLANE_CLIENT_SECRET"),
		}

		releaseApps := appsForRelease("airbyte")
		for _, name := range []string{"workload-launcher"} {
			app := releaseApps[name]
			chartYaml.VerifyEnvVarsForApp(tt, app.Kind, app.FQN(), expectedEnvVars)
		}
	})

	t.Run("should reference the user-defined secret name", func(tt *testing.T) {
		opts := helmtests.BaseHelmOptions()
		// IFF the cluster type is not "hybrid" then read the values from the secret at `workloadLauncher.dataPlane.secretName`
		opts.SetValues["global.cluster.type"] = "data-plane"
		opts.SetValues["workloadLauncher.dataPlane.secretName"] = "data-plane-secrets"
		chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
		assert.NoError(tt, err)

		expectedEnvVars := []helmtests.ExpectedEnvVar{
			helmtests.ExpectedSecretVar().RefName("data-plane-secrets").RefKey("DATAPLANE_CLIENT_ID"),
			helmtests.ExpectedSecretVar().RefName("data-plane-secrets").RefKey("DATAPLANE_CLIENT_SECRET"),
		}

		releaseApps := appsForRelease("airbyte")
		for _, name := range []string{"workload-launcher"} {
			app := releaseApps[name]
			chartYaml.VerifyEnvVarsForApp(tt, app.Kind, app.FQN(), expectedEnvVars)
		}
	})

}
