package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestBootloaderManagedSecretMatchesWorkloadLauncher(t *testing.T) {
	// This test is meant to ensure that on a vanilla install (no values.yml), the coordinates where the workload-launcher reads the
	// managed data plane secrets created by the bootloader match.
	opts := helmtests.BaseHelmOptions()
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	// bootloader
	expectedEnvVars := []helmtests.ExpectedEnvVar{
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATAPLANE_CLIENT_ID_SECRET_KEY").Value("dataplane-client-id"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATAPLANE_CLIENT_SECRET_SECRET_KEY").Value("dataplane-client-secret"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("AB_KUBERNETES_SECRET_NAME").Value("airbyte-auth-secrets"),
	}

	releaseApps := appsForRelease("airbyte")
	for _, name := range []string{"bootloader"} {
		app := releaseApps[name]
		chartYaml.VerifyEnvVarsForApp(t, app.Kind, app.FQN(), expectedEnvVars)
	}

	// workloadLauncher
	expectedEnvVars = []helmtests.ExpectedEnvVar{
		helmtests.ExpectedSecretVar().Name("DATAPLANE_CLIENT_ID").RefName("airbyte-auth-secrets").RefKey("dataplane-client-id"),
		helmtests.ExpectedSecretVar().Name("DATAPLANE_CLIENT_SECRET").RefName("airbyte-auth-secrets").RefKey("dataplane-client-secret"),
	}

	for _, name := range []string{"workload-launcher"} {
		app := releaseApps[name]
		chartYaml.VerifyEnvVarsForApp(t, app.Kind, app.FQN(), expectedEnvVars)
	}
}

func TestDefaultDataPlaneConfig(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	expectedEnvVars := []helmtests.ExpectedEnvVar{
		helmtests.ExpectedSecretVar().Name("DATAPLANE_CLIENT_ID").RefName("airbyte-auth-secrets").RefKey("dataplane-client-id"),
		helmtests.ExpectedSecretVar().Name("DATAPLANE_CLIENT_SECRET").RefName("airbyte-auth-secrets").RefKey("dataplane-client-secret"),
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
			helmtests.ExpectedSecretVar().Name("DATAPLANE_CLIENT_ID").RefName("airbyte-airbyte-secrets").RefKey("dataplane-client-id"),
			helmtests.ExpectedSecretVar().Name("DATAPLANE_CLIENT_SECRET").RefName("airbyte-airbyte-secrets").RefKey("dataplane-client-secret"),
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
			helmtests.ExpectedSecretVar().Name("DATAPLANE_CLIENT_ID").RefName("data-plane-secrets").RefKey("dataplane-client-id"),
			helmtests.ExpectedSecretVar().Name("DATAPLANE_CLIENT_SECRET").RefName("data-plane-secrets").RefKey("dataplane-client-secret"),
		}

		releaseApps := appsForRelease("airbyte")
		for _, name := range []string{"workload-launcher"} {
			app := releaseApps[name]
			chartYaml.VerifyEnvVarsForApp(tt, app.Kind, app.FQN(), expectedEnvVars)
		}
	})

	t.Run("should reference the user-defined secret name and secret coordinates", func(tt *testing.T) {
		opts := helmtests.BaseHelmOptions()
		// IFF the cluster type is not "hybrid" then read the values from the secret at `workloadLauncher.dataPlane.secretName`
		opts.SetValues["global.cluster.type"] = "data-plane"
		opts.SetValues["workloadLauncher.dataPlane.secretName"] = "data-plane-secrets"
		opts.SetValues["workloadLauncher.dataPlane.clientIdSecretKey"] = "DATAPLANE_CLIENT_ID"
		opts.SetValues["workloadLauncher.dataPlane.clientSecretSecretKey"] = "DATAPLANE_CLIENT_SECRET"
		chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
		assert.NoError(tt, err)

		expectedEnvVars := []helmtests.ExpectedEnvVar{
			helmtests.ExpectedSecretVar().Name("DATAPLANE_CLIENT_ID").RefName("data-plane-secrets").RefKey("DATAPLANE_CLIENT_ID"),
			helmtests.ExpectedSecretVar().Name("DATAPLANE_CLIENT_SECRET").RefName("data-plane-secrets").RefKey("DATAPLANE_CLIENT_SECRET"),
		}

		releaseApps := appsForRelease("airbyte")
		for _, name := range []string{"workload-launcher"} {
			app := releaseApps[name]
			chartYaml.VerifyEnvVarsForApp(tt, app.Kind, app.FQN(), expectedEnvVars)
		}

	})

}

func TestLauncherSecretKeyOverridesMatchConfigMap(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["workloadLauncher.dataPlane.clientIdSecretKey"] = "MY_CUSTOM_KEY"
	opts.SetValues["workloadLauncher.dataPlane.clientSecretSecretKey"] = "MY_CUSTOM_SECRET_KEY"
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	releaseApps := appsForRelease("airbyte")
	bootloader := releaseApps["bootloader"]
	workloadLauncher := releaseApps["workload-launcher"]
	chartYaml.VerifyEnvVarsForApp(t, bootloader.Kind, bootloader.FQN(), []helmtests.ExpectedEnvVar{
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATAPLANE_CLIENT_ID_SECRET_KEY").Value("MY_CUSTOM_KEY"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATAPLANE_CLIENT_SECRET_SECRET_KEY").Value("MY_CUSTOM_SECRET_KEY"),
	})
	chartYaml.VerifyEnvVarsForApp(t, workloadLauncher.Kind, workloadLauncher.FQN(), []helmtests.ExpectedEnvVar{
		helmtests.ExpectedSecretVar().Name("DATAPLANE_CLIENT_ID").RefName("airbyte-auth-secrets").RefKey("MY_CUSTOM_KEY"),
		helmtests.ExpectedSecretVar().Name("DATAPLANE_CLIENT_SECRET").RefName("airbyte-auth-secrets").RefKey("MY_CUSTOM_SECRET_KEY"),
	})
}

func TestDataPlaneSecretKeyPrecedence(t *testing.T) {
	tests := []struct {
		name           string
		globalId       string
		globalSecret   string
		launcherId     string
		launcherSecret string
		expectedId     string
		expectedSecret string
	}{
		{
			name:           "global only",
			globalId:       "GLOBAL_KEY",
			globalSecret:   "GLOBAL_SECRET_KEY",
			expectedId:     "GLOBAL_KEY",
			expectedSecret: "GLOBAL_SECRET_KEY",
		},
		{
			name:           "launcher overrides global",
			globalId:       "GLOBAL_KEY",
			globalSecret:   "GLOBAL_SECRET_KEY",
			launcherId:     "LAUNCHER_KEY",
			launcherSecret: "LAUNCHER_SECRET_KEY",
			expectedId:     "LAUNCHER_KEY",
			expectedSecret: "LAUNCHER_SECRET_KEY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := helmtests.BaseHelmOptions()
			opts.SetValues["global.auth.dataPlane.clientIdSecretKey"] = tt.globalId
			opts.SetValues["global.auth.dataPlane.clientSecretSecretKey"] = tt.globalSecret
			if tt.launcherId != "" {
				opts.SetValues["workloadLauncher.dataPlane.clientIdSecretKey"] = tt.launcherId
				opts.SetValues["workloadLauncher.dataPlane.clientSecretSecretKey"] = tt.launcherSecret
			}
			chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
			assert.NoError(t, err)

			releaseApps := appsForRelease("airbyte")
			bootloader := releaseApps["bootloader"]
			workloadLauncher := releaseApps["workload-launcher"]
			chartYaml.VerifyEnvVarsForApp(t, bootloader.Kind, bootloader.FQN(), []helmtests.ExpectedEnvVar{
				helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATAPLANE_CLIENT_ID_SECRET_KEY").Value(tt.expectedId),
				helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATAPLANE_CLIENT_SECRET_SECRET_KEY").Value(tt.expectedSecret),
			})
			chartYaml.VerifyEnvVarsForApp(t, workloadLauncher.Kind, workloadLauncher.FQN(), []helmtests.ExpectedEnvVar{
				helmtests.ExpectedSecretVar().Name("DATAPLANE_CLIENT_ID").RefName("airbyte-auth-secrets").RefKey(tt.expectedId),
				helmtests.ExpectedSecretVar().Name("DATAPLANE_CLIENT_SECRET").RefName("airbyte-auth-secrets").RefKey(tt.expectedSecret),
			})
		})
	}
}
