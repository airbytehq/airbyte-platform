package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestFeatureflagServerDisabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["featureflagServer.enabled"] = "false"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify featureflag-server resources are not created when disabled
	helmtests.AssertNoResource(t, chartYaml.String(), "Deployment", "airbyte-featureflag-server")
	helmtests.AssertNoResource(t, chartYaml.String(), "Service", "airbyte-featureflag-server-svc")
}

func TestFeatureflagServerEnabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["featureflagServer.enabled"] = "true"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify featureflag-server resources are created when enabled
	assert.NotNil(t, helmtests.GetDeployment(chartYaml.String(), "airbyte-featureflag-server"))
	assert.NotNil(t, helmtests.GetService(chartYaml.String(), "airbyte-featureflag-server-svc"))
}
