package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNamespacedResourcesUseReleaseNamespace(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.KubectlOptions.Namespace = "foo"

	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	require.NoError(t, err)

	for _, resource := range []struct {
		kind string
		name string
	}{
		{kind: "Deployment", name: "airbyte-server"},
		{kind: "Service", name: "airbyte-airbyte-server-svc"},
		{kind: "ConfigMap", name: "airbyte-airbyte-env"},
	} {
		object := chartYaml.GetResourceByKindAndName(resource.kind, resource.name)
		require.NotNil(t, object, "%s %s was not rendered", resource.kind, resource.name)
		assert.Equal(t, "foo", object.(interface{ GetNamespace() string }).GetNamespace())
	}
}

func TestNamespacedResourcesUseNamespaceOverride(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.KubectlOptions.Namespace = "foo"
	opts.SetValues["namespaceOverride"] = "bar"

	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	require.NoError(t, err)

	object := chartYaml.GetResourceByKindAndName("Deployment", "airbyte-server")
	require.NotNil(t, object)
	assert.Equal(t, "bar", object.(interface{ GetNamespace() string }).GetNamespace())

	configMap := helmtests.GetConfigMap(chartYaml.String(), "airbyte-airbyte-env")
	require.NotNil(t, configMap)
	assert.Equal(t, "airbyte-db-svc.bar.svc.cluster.local", configMap.Data["DATABASE_HOST"])
	assert.Contains(t, configMap.Data["INTERNAL_API_HOST"], ".bar:")
}
