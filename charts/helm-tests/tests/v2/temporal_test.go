package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestTemporalDisabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["temporal.enabled"] = "false"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify temporal resources are not created when disabled
	helmtests.AssertNoResource(t, chartYaml.String(), "Deployment", "airbyte-temporal")
	helmtests.AssertNoResource(t, chartYaml.String(), "Service", "airbyte-temporal")
	helmtests.AssertNoResource(t, chartYaml.String(), "ConfigMap", "airbyte-temporal-config")
}

func TestTemporalEnabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["temporal.enabled"] = "true"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify temporal resources are created when enabled
	assert.NotNil(t, helmtests.GetDeployment(chartYaml.String(), "airbyte-temporal"))
	assert.NotNil(t, helmtests.GetService(chartYaml.String(), "airbyte-temporal"))
}

func TestTemporalAutoSetup(t *testing.T) {
	t.Run("schema init container and namespace sidecar are rendered by default", func(tt *testing.T) {
		opts := helmtests.BaseHelmOptions()
		chartYaml, err := helmtests.RenderHelmChart(tt, opts, chartPath, "airbyte", nil)
		assert.NoError(tt, err)

		deploy := helmtests.GetDeployment(chartYaml.String(), "airbyte-temporal")
		assert.NotNil(tt, deploy)
		assert.Equal(tt, "temporalio/server:1.30.8", deploy.Spec.Template.Spec.Containers[0].Image)
		initContainers := deploy.Spec.Template.Spec.InitContainers
		assert.Len(tt, initContainers, 1)
		assert.Equal(tt, "temporal-schema-setup", initContainers[0].Name)
		assert.Equal(tt, "temporalio/admin-tools:1.30.8", initContainers[0].Image)
		initEnv := helmtests.EnvVarMap(initContainers[0].Env)
		for _, name := range []string{"DB", "POSTGRES_SEEDS", "DB_PORT", "POSTGRES_USER", "POSTGRES_PWD"} {
			assert.Contains(tt, initEnv, name)
		}

		containers := deploy.Spec.Template.Spec.Containers
		assert.Len(tt, containers, 2)
		assert.Equal(tt, "temporal-namespace-setup", containers[1].Name)
		assert.Equal(tt, "temporalio/admin-tools:1.30.8", containers[1].Image)
		assert.Equal(tt, "airbyte-temporal:7233", helmtests.EnvVarMap(containers[1].Env)["TEMPORAL_ADDRESS"].Value)
	})

	t.Run("autoSetup disabled skips schema and namespace setup", func(tt *testing.T) {
		opts := helmtests.BaseHelmOptions()
		opts.SetValues["temporal.autoSetup"] = "false"
		chartYaml, err := helmtests.RenderHelmChart(tt, opts, chartPath, "airbyte", nil)
		assert.NoError(tt, err)

		deploy := helmtests.GetDeployment(chartYaml.String(), "airbyte-temporal")
		assert.NotNil(tt, deploy)
		assert.Empty(tt, deploy.Spec.Template.Spec.InitContainers)
		assert.Len(tt, deploy.Spec.Template.Spec.Containers, 1)
	})

	t.Run("extraInitContainers are kept after the schema init container", func(tt *testing.T) {
		opts := helmtests.BaseHelmOptions()
		opts.SetJsonValues = map[string]string{"temporal.extraInitContainers": `[{"name":"extra","image":"busybox"}]`}
		chartYaml, err := helmtests.RenderHelmChart(tt, opts, chartPath, "airbyte", nil)
		assert.NoError(tt, err)

		initContainers := helmtests.GetDeployment(chartYaml.String(), "airbyte-temporal").Spec.Template.Spec.InitContainers
		assert.Len(tt, initContainers, 2)
		assert.Equal(tt, "temporal-schema-setup", initContainers[0].Name)
		assert.Equal(tt, "extra", initContainers[1].Name)
	})
}
