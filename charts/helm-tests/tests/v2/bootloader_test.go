package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestBootloaderAnnotations(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["airbyteBootloader.annotations.owner"] = "platform"
	opts.SetValues[`airbyteBootloader.annotations.helm\.sh/hook`] = "invalid"
	opts.SetValues["airbyteBootloader.podAnnotations.team"] = "data"
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	bootloader := helmtests.GetPod(chartYaml.String(), "airbyte-bootloader")
	assert.NotNil(t, bootloader)
	assert.Equal(t, "platform", bootloader.Annotations["owner"])
	assert.Equal(t, "data", bootloader.Annotations["team"])
	assert.Equal(t, "pre-install,pre-upgrade", bootloader.Annotations["helm.sh/hook"])
	assert.Equal(t, "0", bootloader.Annotations["helm.sh/hook-weight"])
}

func TestBootloaderNullAnnotations(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["airbyteBootloader.annotations"] = "null"
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	bootloader := helmtests.GetPod(chartYaml.String(), "airbyte-bootloader")
	assert.NotNil(t, bootloader)
	assert.Equal(t, "pre-install,pre-upgrade", bootloader.Annotations["helm.sh/hook"])
	assert.Equal(t, "0", bootloader.Annotations["helm.sh/hook-weight"])
}
