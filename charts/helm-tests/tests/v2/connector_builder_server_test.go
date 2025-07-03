package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestConnectorBuilderServerDisabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["connectorBuilderServer.enabled"] = "false"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify connector-builder-server resources are not created when disabled
	helmtests.AssertNoResource(t, chartYaml.String(), "Deployment", "airbyte-connector-builder-server")
	helmtests.AssertNoResource(t, chartYaml.String(), "Service", "airbyte-airbyte-connector-builder-server-svc")
}

func TestConnectorBuilderServerEnabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["connectorBuilderServer.enabled"] = "true"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify connector-builder-server resources are created when enabled
	assert.NotNil(t, helmtests.GetDeployment(chartYaml.String(), "airbyte-connector-builder-server"))
	assert.NotNil(t, helmtests.GetService(chartYaml.String(), "airbyte-airbyte-connector-builder-server-svc"))
}
