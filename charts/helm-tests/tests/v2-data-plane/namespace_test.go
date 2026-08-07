package dataplanetests

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
		{kind: "Deployment", name: "airbyte-airbyte-data-plane"},
		{kind: "ConfigMap", name: "airbyte-airbyte-data-plane-env"},
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

	object := chartYaml.GetResourceByKindAndName("Deployment", "airbyte-airbyte-data-plane")
	require.NotNil(t, object)
	assert.Equal(t, "bar", object.(interface{ GetNamespace() string }).GetNamespace())

	configMap := helmtests.GetConfigMap(chartYaml.String(), "airbyte-airbyte-data-plane-env")
	require.NotNil(t, configMap)
	assert.Equal(t, "http://airbyte-minio-svc.bar:9000", configMap.Data["MINIO_ENDPOINT"])
}
