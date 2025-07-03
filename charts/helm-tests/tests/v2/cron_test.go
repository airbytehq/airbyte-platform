package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestCronDisabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["cron.enabled"] = "false"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify cron resources are not created when disabled
	helmtests.AssertNoResource(t, chartYaml.String(), "Deployment", "airbyte-cron")
}

func TestCronEnabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["cron.enabled"] = "true"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify cron resources are created when enabled
	assert.NotNil(t, helmtests.GetDeployment(chartYaml.String(), "airbyte-cron"))
}
