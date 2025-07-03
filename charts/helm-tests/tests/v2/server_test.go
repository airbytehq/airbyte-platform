package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestServerDisabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["server.enabled"] = "false"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify server resources are not created when disabled
	helmtests.AssertNoResource(t, chartYaml.String(), "Deployment", "airbyte-server")
	helmtests.AssertNoResource(t, chartYaml.String(), "Service", "airbyte-airbyte-server-svc")
}

func TestServerEnabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["server.enabled"] = "true"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify server resources are created when enabled
	assert.NotNil(t, helmtests.GetDeployment(chartYaml.String(), "airbyte-server"))
	assert.NotNil(t, helmtests.GetService(chartYaml.String(), "airbyte-airbyte-server-svc"))
}
