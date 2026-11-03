package dataplanetests

import (
	"encoding/json"
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func TestAzureSecretReferenceOverrides(t *testing.T) {
	names := []string{
		"AB_AZURE_KEY_VAULT_CLIENT_ID_REF_NAME",
		"AB_AZURE_KEY_VAULT_CLIENT_ID_REF_REF_KEY",
		"AB_AZURE_KEY_VAULT_CLIENT_SECRET_REF_REF_NAME",
		"AB_AZURE_KEY_VAULT_CLIENT_SECRET_REF_REF_KEY",
	}
	defaults := []string{"airbyte-airbyte-data-plane-secrets", "AB_AZURE_KEY_VAULT_CLIENT_ID", "airbyte-airbyte-data-plane-secrets", "AB_AZURE_KEY_VAULT_CLIENT_SECRET"}
	custom := []string{"custom-azure-secret", "custom-id-key", "custom-azure-secret", "custom-secret-key"}
	cases := map[string][]int{"no workaround": {}, "complete workaround": {0, 1, 2, 3}, "name only": {0}, "key only": {1}, "secret name only": {2}, "secret key only": {3}, "mixed partial": {0, 3}}
	for name, indices := range cases {
		t.Run(name, func(t *testing.T) {
			opts := helmtests.BaseHelmOptions()
			opts.SetValues["secretsManager.type"] = "AZURE_KEY_VAULT"
			opts.SetValues["secretsManager.azureKeyVault.clientId"] = "test-client-id"
			opts.SetValues["secretsManager.azureKeyVault.clientSecret"] = "test-client-secret"
			overrides := []corev1.EnvVar{}
			for _, i := range indices {
				overrides = append(overrides, corev1.EnvVar{Name: names[i], Value: custom[i]})
			}
			data, err := json.Marshal(overrides)
			require.NoError(t, err)
			opts.SetJsonValues["workloadLauncher.extraEnv"] = string(data)
			rendered, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
			require.NoError(t, err)
			deployment := helmtests.GetDeployment(rendered.String(), "airbyte-airbyte-data-plane")
			require.NotNil(t, deployment)
			config := helmtests.GetConfigMap(rendered.String(), "airbyte-airbyte-data-plane-env")
			require.NotNil(t, config)
			envs := deployment.Spec.Template.Spec.Containers[0].Env
			for i, name := range names {
				matches := []corev1.EnvVar{}
				for _, env := range envs {
					if env.Name == name {
						matches = append(matches, env)
					}
				}
				require.Len(t, matches, 1, name)
				overridden := false
				for _, index := range indices {
					if index == i {
						overridden = true
					}
				}
				if overridden {
					require.Equal(t, corev1.EnvVar{Name: name, Value: custom[i]}, matches[0])
				} else {
					require.NotNil(t, matches[0].ValueFrom, name)
					ref := matches[0].ValueFrom.ConfigMapKeyRef
					require.NotNil(t, ref, name)
					require.Equal(t, "airbyte-airbyte-data-plane-env", ref.Name)
					require.Equal(t, name, ref.Key)
					require.Equal(t, defaults[i], config.Data[ref.Key])
				}
			}
			for _, env := range envs {
				require.NotContains(t, []string{"AB_AZURE_KEY_CLIENT_ID_REF_NAME", "AB_AZURE_KEY_CLIENT_ID_REF_KEY", "AB_AZURE_KEY_CLIENT_SECRET_REF_NAME", "AB_AZURE_KEY_CLIENT_SECRET_REF_KEY"}, env.Name)
			}
		})
	}
}
