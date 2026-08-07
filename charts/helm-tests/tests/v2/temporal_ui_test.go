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
	opts.SetValues["temporalUi.extraVolumes[0].name"] = "temporal-ui-extra"
	opts.SetValues["temporalUi.extraVolumes[0].emptyDir.medium"] = "Memory"
	opts.SetValues["temporalUi.resources.limits.cpu"] = "100m"
	opts.SetValues["temporalUi.resources.limits.memory"] = "128Mi"

	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	// Verify temporal-ui resources are created when enabled
	deployment := helmtests.GetDeployment(chartYaml.String(), "airbyte-temporal-ui")
	assert.NotNil(t, deployment)
	assert.NotNil(t, helmtests.GetService(chartYaml.String(), "airbyte-airbyte-temporal-ui-svc"))
	assert.Equal(t, "temporal-ui-extra", deployment.Spec.Template.Spec.Volumes[0].Name)
	assert.NotNil(t, deployment.Spec.Template.Spec.Volumes[0].EmptyDir)
	assert.NotNil(t, deployment.Spec.Template.Spec.SecurityContext)
	assert.Equal(t, int64(1000), *deployment.Spec.Template.Spec.SecurityContext.FSGroup)
	assert.Equal(t, "100m", deployment.Spec.Template.Spec.Containers[0].Resources.Limits.Cpu().String())
	assert.Equal(t, "128Mi", deployment.Spec.Template.Spec.Containers[0].Resources.Limits.Memory().String())
	assert.NotNil(t, deployment.Spec.Template.Spec.Containers[0].SecurityContext)
}
