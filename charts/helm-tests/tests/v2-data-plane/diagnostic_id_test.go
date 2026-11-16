package dataplanetests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/require"
)

func TestDataPlaneDiagnosticId(t *testing.T) {
	cases := map[string]string{
		"empty by default":      "",
		"set from dataPlane.id": "0f3c1f0e-6f0b-4d53-9a55-3e0d2f6c8a11",
	}
	for name, id := range cases {
		t.Run(name, func(t *testing.T) {
			opts := helmtests.BaseHelmOptions()
			if id != "" {
				opts.SetValues["dataPlane.id"] = id
			}

			chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
			require.NoError(t, err)

			chartYaml.VerifyEnvVarsForApp(t, "Deployment", "airbyte-airbyte-data-plane", []helmtests.ExpectedEnvVar{
				helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-data-plane-env").RefKey("DATAPLANE_DIAGNOSTIC_ID").Value(id),
			})
		})
	}
}
