package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestTemporalUiDisabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["temporalUi.enabled"] = "false"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify temporal-ui resources are not created when disabled
	helmtests.AssertNoResource(t, chartYaml.String(), "Deployment", "airbyte-temporal-ui")
	helmtests.AssertNoResource(t, chartYaml.String(), "Service", "airbyte-airbyte-temporal-ui-svc")
}

func TestTemporalUiEnabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["temporalUi.enabled"] = "true"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify temporal-ui resources are created when enabled
	assert.NotNil(t, helmtests.GetDeployment(chartYaml.String(), "airbyte-temporal-ui"))
	assert.NotNil(t, helmtests.GetService(chartYaml.String(), "airbyte-airbyte-temporal-ui-svc"))
}
