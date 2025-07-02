package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestDefaultConnectorImageRegistry(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	expectedEnvVars := []helmtests.ExpectedEnvVar{
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("JOB_KUBE_CONNECTOR_IMAGE_REGISTRY").Value(""),
	}

	releaseApps := appsForRelease("airbyte")
	rapp := releaseApps["workload-launcher"]
	app := chartYaml.GetResourceByKindAndName(rapp.Kind, rapp.FQN())
	assert.NotNil(t, app, "unable to find %s named '%s'", rapp.Kind, rapp.FQN())

	pod := helmtests.GetPodSpec(app)
	assert.NotNil(t, pod, "unable to extract pod spec from %s: %s", rapp.Kind, rapp.FQN())
	chartYaml.VerifyEnvVarsForApp(t, rapp.Kind, rapp.FQN(), expectedEnvVars)
}

func TestOverrideConnectorImageRegistry(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["global.image.registry"] = "not-the-default"
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	expectedEnvVars := []helmtests.ExpectedEnvVar{
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("JOB_KUBE_CONNECTOR_IMAGE_REGISTRY").Value("not-the-default"),
	}

	releaseApps := appsForRelease("airbyte")
	rapp := releaseApps["workload-launcher"]
	app := chartYaml.GetResourceByKindAndName(rapp.Kind, rapp.FQN())
	assert.NotNil(t, app, "unable to find %s named '%s'", rapp.Kind, rapp.FQN())

	pod := helmtests.GetPodSpec(app)
	assert.NotNil(t, pod, "unable to extract pod spec from %s: %s", rapp.Kind, rapp.FQN())
	chartYaml.VerifyEnvVarsForApp(t, rapp.Kind, rapp.FQN(), expectedEnvVars)
}
