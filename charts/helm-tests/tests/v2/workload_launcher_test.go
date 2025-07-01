package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestWorkloadLauncherDisabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["workloadLauncher.enabled"] = "false"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify workload-launcher resources are not created when disabled
	helmtests.AssertNoResource(t, chartYaml.String(), "Deployment", "airbyte-workload-launcher")
}

func TestWorkloadLauncherEnabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["workloadLauncher.enabled"] = "true"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify workload-launcher resources are created when enabled
	assert.NotNil(t, helmtests.GetDeployment(chartYaml.String(), "airbyte-workload-launcher"))
}
