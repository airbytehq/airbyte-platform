package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestWorkloadApiServerDisabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["workloadApiServer.enabled"] = "false"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify workload-api-server resources are not created when disabled
	helmtests.AssertNoResource(t, chartYaml.String(), "Deployment", "airbyte-workload-api-server")
	helmtests.AssertNoResource(t, chartYaml.String(), "Service", "airbyte-workload-api-server-svc")
}

func TestWorkloadApiServerEnabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["workloadApiServer.enabled"] = "true"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify workload-api-server resources are created when enabled
	assert.NotNil(t, helmtests.GetDeployment(chartYaml.String(), "airbyte-workload-api-server"))
	assert.NotNil(t, helmtests.GetService(chartYaml.String(), "airbyte-workload-api-server-svc"))
}
