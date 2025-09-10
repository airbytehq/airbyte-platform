package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/gruntwork-io/terratest/modules/helm"
	"github.com/stretchr/testify/assert"
)

func TestDefaultEnterpriseConfig(t *testing.T) {
	helmOpts := helmtests.BaseHelmOptionsForEnterprise()
	chartYaml, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	cm := helmtests.GetConfigMap(chartYaml, "airbyte-airbyte-env")
	assert.NotNil(t, cm)

	assert.Equal(t, "true", cm.Data["API_AUTHORIZATION_ENABLED"])
}
