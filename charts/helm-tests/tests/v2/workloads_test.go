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

func TestOverrideConnectorImageRegistryWithWorkloadLauncherSpecific(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["global.image.registry"] = "global-registry"
	opts.SetValues["workloadLauncher.connector.image.registry"] = "connector-specific-registry"
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	expectedEnvVars := []helmtests.ExpectedEnvVar{
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("JOB_KUBE_CONNECTOR_IMAGE_REGISTRY").Value("connector-specific-registry"),
	}

	releaseApps := appsForRelease("airbyte")
	rapp := releaseApps["workload-launcher"]
	app := chartYaml.GetResourceByKindAndName(rapp.Kind, rapp.FQN())
	assert.NotNil(t, app, "unable to find %s named '%s'", rapp.Kind, rapp.FQN())

	pod := helmtests.GetPodSpec(app)
	assert.NotNil(t, pod, "unable to extract pod spec from %s: %s", rapp.Kind, rapp.FQN())
	chartYaml.VerifyEnvVarsForApp(t, rapp.Kind, rapp.FQN(), expectedEnvVars)
}

func TestLegacyJobResources(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["global.jobs.resources.limits.cpu"] = "500m"
	opts.SetValues["global.jobs.resources.limits.memory"] = "1Gi"
	opts.SetValues["global.jobs.resources.requests.cpu"] = "250m"
	opts.SetValues["global.jobs.resources.requests.memory"] = "512Mi"
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	configMap := helmtests.GetConfigMap(chartYaml.String(), "airbyte-airbyte-env")
	assert.NotNil(t, configMap)
	assert.Equal(t, "500m", configMap.Data["JOB_MAIN_CONTAINER_CPU_LIMIT"])
	assert.Equal(t, "250m", configMap.Data["JOB_MAIN_CONTAINER_CPU_REQUEST"])
	assert.Equal(t, "1Gi", configMap.Data["JOB_MAIN_CONTAINER_MEMORY_LIMIT"])
	assert.Equal(t, "512Mi", configMap.Data["JOB_MAIN_CONTAINER_MEMORY_REQUEST"])
	assert.Equal(t, "", configMap.Data["CHECK_JOB_MAIN_CONTAINER_CPU_LIMIT"])
	assert.Equal(t, "", configMap.Data["DISCOVER_JOB_MAIN_CONTAINER_CPU_LIMIT"])
	assert.Equal(t, "", configMap.Data["SIDECAR_MAIN_CONTAINER_CPU_LIMIT"])
}

func TestWorkloadResourcesTakePrecedenceOverLegacyJobResources(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["global.jobs.resources.limits.cpu"] = "500m"
	opts.SetValues["global.jobs.resources.limits.memory"] = "1Gi"
	opts.SetValues["global.jobs.resources.requests.cpu"] = "250m"
	opts.SetValues["global.jobs.resources.requests.memory"] = "512Mi"
	opts.SetValues["global.workloads.resources.mainContainer.cpu.limit"] = "1000m"
	opts.SetValues["global.workloads.resources.mainContainer.cpu.request"] = "750m"
	opts.SetValues["global.workloads.resources.mainContainer.memory.limit"] = "2Gi"
	opts.SetValues["global.workloads.resources.mainContainer.memory.request"] = "1Gi"
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	configMap := helmtests.GetConfigMap(chartYaml.String(), "airbyte-airbyte-env")
	assert.NotNil(t, configMap)
	assert.Equal(t, "1000m", configMap.Data["JOB_MAIN_CONTAINER_CPU_LIMIT"])
	assert.Equal(t, "750m", configMap.Data["JOB_MAIN_CONTAINER_CPU_REQUEST"])
	assert.Equal(t, "2Gi", configMap.Data["JOB_MAIN_CONTAINER_MEMORY_LIMIT"])
	assert.Equal(t, "1Gi", configMap.Data["JOB_MAIN_CONTAINER_MEMORY_REQUEST"])
}

func TestWorkloadResourcesFallbackIsPerKey(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["global.jobs.resources.limits.cpu"] = "500m"
	opts.SetValues["global.jobs.resources.limits.memory"] = "1Gi"
	opts.SetValues["global.jobs.resources.requests.cpu"] = "250m"
	opts.SetValues["global.jobs.resources.requests.memory"] = "512Mi"
	opts.SetValues["global.workloads.resources.mainContainer.cpu.limit"] = "1000m"
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	configMap := helmtests.GetConfigMap(chartYaml.String(), "airbyte-airbyte-env")
	assert.NotNil(t, configMap)
	assert.Equal(t, "1000m", configMap.Data["JOB_MAIN_CONTAINER_CPU_LIMIT"])
	assert.Equal(t, "250m", configMap.Data["JOB_MAIN_CONTAINER_CPU_REQUEST"])
	assert.Equal(t, "1Gi", configMap.Data["JOB_MAIN_CONTAINER_MEMORY_LIMIT"])
	assert.Equal(t, "512Mi", configMap.Data["JOB_MAIN_CONTAINER_MEMORY_REQUEST"])
}
