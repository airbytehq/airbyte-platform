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

func TestServerNodePortConfiguration(t *testing.T) {
	t.Run("nodePort is omitted for ClusterIP", func(t *testing.T) {
		opts := helmtests.BaseHelmOptions()
		opts.SetValues["server.service.nodePort"] = "30081"
		chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
		assert.NoError(t, err)

		service := helmtests.GetService(chartYaml.String(), "airbyte-airbyte-server-svc")
		assert.NotNil(t, service)
		assert.Equal(t, int32(0), service.Spec.Ports[0].NodePort)
	})

	t.Run("nodePort is rendered for NodePort", func(t *testing.T) {
		opts := helmtests.BaseHelmOptions()
		opts.SetValues["server.service.type"] = "NodePort"
		opts.SetValues["server.service.nodePort"] = "30081"
		chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
		assert.NoError(t, err)

		service := helmtests.GetService(chartYaml.String(), "airbyte-airbyte-server-svc")
		assert.NotNil(t, service)
		assert.Equal(t, int32(30081), service.Spec.Ports[0].NodePort)
	})
}
