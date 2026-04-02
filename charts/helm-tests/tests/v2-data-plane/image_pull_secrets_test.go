package dataplanetests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestImagePullSecretsConfigMap(t *testing.T) {
	t.Run("should extract secret name from imagePullSecrets objects", func(t *testing.T) {
		opts := helmtests.BaseHelmOptions()
		opts.SetJsonValues["imagePullSecrets"] = `[{"name": "artifactory"}]`

		chartYaml, _ := helmtests.RenderHelmChart(t, opts, chartPath, "test", nil)
		cm := helmtests.GetConfigMap(string(chartYaml), "test-airbyte-data-plane-env")
		assert.NotNil(t, cm)
		assert.Equal(t, "artifactory", cm.Data["JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_SECRET"])
	})

	t.Run("should combine imagePullSecrets with mainContainerImagePullSecret", func(t *testing.T) {
		opts := helmtests.BaseHelmOptions()
		opts.SetJsonValues["imagePullSecrets"] = `[{"name": "artifactory"}]`
		opts.SetValues["jobs.kube.mainContainerImagePullSecret"] = "docker-hub"

		chartYaml, _ := helmtests.RenderHelmChart(t, opts, chartPath, "test", nil)
		cm := helmtests.GetConfigMap(string(chartYaml), "test-airbyte-data-plane-env")
		assert.NotNil(t, cm)
		assert.Equal(t, "artifactory,docker-hub", cm.Data["JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_SECRET"])
	})

	t.Run("should handle multiple imagePullSecrets", func(t *testing.T) {
		opts := helmtests.BaseHelmOptions()
		opts.SetJsonValues["imagePullSecrets"] = `[{"name": "artifactory"}, {"name": "gcr-secret"}]`

		chartYaml, _ := helmtests.RenderHelmChart(t, opts, chartPath, "test", nil)
		cm := helmtests.GetConfigMap(string(chartYaml), "test-airbyte-data-plane-env")
		assert.NotNil(t, cm)
		assert.Equal(t, "artifactory,gcr-secret", cm.Data["JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_SECRET"])
	})
}
