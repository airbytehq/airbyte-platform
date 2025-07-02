package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestWebappDisabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["webapp.enabled"] = "false"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify webapp resources are not created when disabled
	helmtests.AssertNoResource(t, chartYaml.String(), "Deployment", "airbyte-webapp")
	helmtests.AssertNoResource(t, chartYaml.String(), "Service", "airbyte-airbyte-webapp-svc")
}

func TestWebappEnabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["webapp.enabled"] = "true"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify webapp resources are created when enabled
	assert.NotNil(t, helmtests.GetDeployment(chartYaml.String(), "airbyte-webapp"))
	assert.NotNil(t, helmtests.GetService(chartYaml.String(), "airbyte-airbyte-webapp-svc"))
}
