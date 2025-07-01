package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestConnectorRolloutWorkerDisabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["connectorRolloutWorker.enabled"] = "false"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify connector-rollout-worker resources are not created when disabled
	helmtests.AssertNoResource(t, chartYaml.String(), "Deployment", "airbyte-connector-rollout-worker")
}

func TestConnectorRolloutWorkerEnabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["connectorRolloutWorker.enabled"] = "true"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify connector-rollout-worker resources are created when enabled
	assert.NotNil(t, helmtests.GetDeployment(chartYaml.String(), "airbyte-connector-rollout-worker"))
}
