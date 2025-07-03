package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestKeycloakDisabledCommunityEdition(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["global.edition"] = "community"
	opts.SetValues["keycloak.enabled"] = "false"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify keycloak resources are not created when disabled or community edition
	helmtests.AssertNoResource(t, chartYaml.String(), "StatefulSet", "airbyte-keycloak")
	helmtests.AssertNoResource(t, chartYaml.String(), "Service", "airbyte-airbyte-keycloak-svc")
	helmtests.AssertNoResource(t, chartYaml.String(), "Service", "airbyte-airbyte-keycloak-headless-svc")
}

func TestKeycloakDisabledEnterpriseEdition(t *testing.T) {
	opts := helmtests.BaseHelmOptionsForEnterprise()
	opts.SetValues["keycloak.enabled"] = "false"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify keycloak resources are not created when disabled even with enterprise edition
	helmtests.AssertNoResource(t, chartYaml.String(), "StatefulSet", "airbyte-keycloak")
	helmtests.AssertNoResource(t, chartYaml.String(), "Service", "airbyte-airbyte-keycloak-svc")
	helmtests.AssertNoResource(t, chartYaml.String(), "Service", "airbyte-airbyte-keycloak-headless-svc")
}

func TestKeycloakEnabledEnterpriseEdition(t *testing.T) {
	opts := helmtests.BaseHelmOptionsForEnterprise()
	opts.SetValues["keycloak.enabled"] = "true"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify keycloak resources are created when enabled and enterprise edition
	assert.NotNil(t, helmtests.GetStatefulSet(chartYaml.String(), "airbyte-keycloak"))
	assert.NotNil(t, helmtests.GetService(chartYaml.String(), "airbyte-airbyte-keycloak-svc"))
	assert.NotNil(t, helmtests.GetService(chartYaml.String(), "airbyte-airbyte-keycloak-headless-svc"))
}

func TestKeycloakEnabledCommunityEdition(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["global.edition"] = "community"
	opts.SetValues["keycloak.enabled"] = "true"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify keycloak resources are not created when community edition (even if enabled)
	helmtests.AssertNoResource(t, chartYaml.String(), "StatefulSet", "airbyte-keycloak")
	helmtests.AssertNoResource(t, chartYaml.String(), "Service", "airbyte-airbyte-keycloak-svc")
	helmtests.AssertNoResource(t, chartYaml.String(), "Service", "airbyte-airbyte-keycloak-headless-svc")
}
