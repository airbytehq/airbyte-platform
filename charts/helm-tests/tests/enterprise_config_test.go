//go:build template || enterprise

package test

import (
	"fmt"
	"testing"

	"github.com/gruntwork-io/terratest/modules/helm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicEnterpriseConfigWithHelmValues(t *testing.T) {
	t.Run("require global.auth.instanceAdmin.firstName", func(t *testing.T) {
		helmOpts := baseHelmOptions()
		helmOpts.SetValues["global.edition"] = "enterprise"
		helmOpts.SetValues["global.enterprise.licenseKeySecretName"] = "airbyte-license"
		helmOpts.SetValues["global.auth.instanceAdmin.firstName"] = ""
		helmOpts.SetValues["global.auth.instanceAdmin.lastName"] = "Squidington"
		helmOpts.SetValues["global.auth.instanceAdmin.passwordSecretName"] = "airbyte-admin"
		helmOpts.SetValues["global.auth.instanceAdmin.emailSecretName"] = "airbyte-admin"
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		require.ErrorContains(t, err, "You must set `global.auth.instanceAdmin.firstName` when `global.edition` is 'enterprise'")
	})

	t.Run("require global.auth.instanceAdmin.lastName", func(t *testing.T) {
		helmOpts := baseHelmOptions()
		helmOpts.SetValues["global.edition"] = "enterprise"
		helmOpts.SetValues["global.enterprise.licenseKeySecretName"] = "airbyte-license"
		helmOpts.SetValues["global.auth.instanceAdmin.firstName"] = "Octavia"
		helmOpts.SetValues["global.auth.instanceAdmin.lastName"] = ""
		helmOpts.SetValues["global.auth.instanceAdmin.passwordSecretName"] = "airbyte-admin"
		helmOpts.SetValues["global.auth.instanceAdmin.emailSecretName"] = "airbyte-admin"
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		require.ErrorContains(t, err, "You must set `global.auth.instanceAdmin.lastName` when `global.edition` is 'enterprise'")
	})

	t.Run("require global.auth.instanceAdmin.emailSecretName", func(t *testing.T) {
		helmOpts := baseHelmOptions()
		helmOpts.SetValues["global.edition"] = "enterprise"
		helmOpts.SetValues["global.enterprise.licenseKeyExistingSecret"] = "airbyte-license"
		helmOpts.SetValues["global.auth.instanceAdmin.firstName"] = "Octavia"
		helmOpts.SetValues["global.auth.instanceAdmin.lastName"] = "Squidington"
		helmOpts.SetValues["global.auth.instanceAdmin.emailSecretName"] = ""
		helmOpts.SetValues["global.auth.instanceAdmin.passwordSecretName"] = "airbyte-admin"
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		require.ErrorContains(t, err, "You must set `global.auth.instanceAdmin.emailSecretName` when `global.edition` is 'enterprise'")
	})

	t.Run("require global.auth.instanceAdmin.passwordSecretName", func(t *testing.T) {
		helmOpts := baseHelmOptions()
		helmOpts.SetValues["global.edition"] = "enterprise"
		helmOpts.SetValues["global.enterprise.licenseKeyExistingSecret"] = "airbyte-license"
		helmOpts.SetValues["global.auth.instanceAdmin.firstName"] = "Octavia"
		helmOpts.SetValues["global.auth.instanceAdmin.lastName"] = "Squidington"
		helmOpts.SetValues["global.auth.instanceAdmin.emailSecretName"] = "airbyte-admin"
		helmOpts.SetValues["global.auth.instanceAdmin.passwordSecretName"] = ""
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		require.ErrorContains(t, err, "You must set `global.auth.instanceAdmin.passwordSecretName` when `global.edition` is 'enterprise'")
	})

	t.Run("should set enterprise config env vars", func(t *testing.T) {
		helmOpts := baseHelmOptionsForEnterpriseWithValues()
		helmOpts.SetValues["global.enterprise.licenseKeySecretName"] = "airbyte-license"
		helmOpts.SetValues["global.auth.instanceAdmin.emailSecretName"] = "sso-secrets"
		helmOpts.SetValues["global.auth.instanceAdmin.passwordSecretName"] = "sso-secrets"
		helmOpts.SetValues["global.auth.identityProvider.type"] = "okta"
		helmOpts.SetValues["global.auth.identityProvider.oidc.domain"] = "sso.example.org"
		helmOpts.SetValues["global.auth.identityProvider.oidc.appName"] = "sso-app"
		helmOpts.SetValues["global.auth.identityProvider.oidc.clientIdSecretName"] = "sso-secrets"
		helmOpts.SetValues["global.auth.identityProvider.oidc.clientIdSecretKey"] = "client-id"
		helmOpts.SetValues["global.auth.identityProvider.oidc.clientSecretSecretName"] = "sso-secrets"
		helmOpts.SetValues["global.auth.identityProvider.oidc.clientSecretSecretKey"] = "client-secret"

		chartYaml, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		require.NotNil(t, chartYaml)
		require.NoError(t, err)

		t.Run("should set required env vars for keycloak setup job", func(t *testing.T) {
			expectedEnvVarKeys := map[string]expectedEnvVar{
				"AIRBYTE_URL":             expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("AIRBYTE_URL"),
				"INITIAL_USER_FIRST_NAME": expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("INITIAL_USER_FIRST_NAME"),
				"INITIAL_USER_LAST_NAME":  expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("INITIAL_USER_LAST_NAME"),
				"INITIAL_USER_EMAIL":      expectedSecretVar().RefName("sso-secrets").RefKey("initial-user-email"),
				"INITIAL_USER_PASSWORD":   expectedSecretVar().RefName("sso-secrets").RefKey("initial-user-password"),
				"IDENTITY_PROVIDER_TYPE":  expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("IDENTITY_PROVIDER_TYPE"),
				"OIDC_DOMAIN":             expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("OIDC_DOMAIN"),
				"OIDC_APP_NAME":           expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("OIDC_APP_NAME"),
				"OIDC_CLIENT_ID":          expectedSecretVar().RefName("sso-secrets").RefKey("client-id"),
				"OIDC_CLIENT_SECRET":      expectedSecretVar().RefName("sso-secrets").RefKey("client-secret"),
			}

			// Verify that the keycloak setup job has the correct vars
			keycloakSetupJob, err := getJob(chartYaml, "airbyte-keycloak-setup")
			assert.NotNil(t, keycloakSetupJob)
			assert.NoError(t, err)

			keycloakEnvVars := envVarMap(keycloakSetupJob.Spec.Template.Spec.Containers[0].Env)
			for k, expected := range expectedEnvVarKeys {
				actual, ok := keycloakEnvVars[k]
				assert.True(t, ok, fmt.Sprintf("`%s` should be declared as an environment variable", k))
				verifyEnvVar(t, expected, actual)
			}
		})

		t.Run("should set required env vars for the airbyte server", func(t *testing.T) {
			expectedEnvVarKeys := map[string]expectedEnvVar{
				"AIRBYTE_LICENSE_KEY":     expectedSecretVar().RefName("airbyte-license").RefKey("license-key"),
				"AIRBYTE_URL":             expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("AIRBYTE_URL"),
				"INITIAL_USER_FIRST_NAME": expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("INITIAL_USER_FIRST_NAME"),
				"INITIAL_USER_LAST_NAME":  expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("INITIAL_USER_LAST_NAME"),
				"INITIAL_USER_EMAIL":      expectedSecretVar().RefName("sso-secrets").RefKey("initial-user-email"),
				"INITIAL_USER_PASSWORD":   expectedSecretVar().RefName("sso-secrets").RefKey("initial-user-password"),
				"IDENTITY_PROVIDER_TYPE":  expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("IDENTITY_PROVIDER_TYPE"),
				"OIDC_DOMAIN":             expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("OIDC_DOMAIN"),
				"OIDC_APP_NAME":           expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("OIDC_APP_NAME"),
				"OIDC_CLIENT_ID":          expectedSecretVar().RefName("sso-secrets").RefKey("client-id"),
				"OIDC_CLIENT_SECRET":      expectedSecretVar().RefName("sso-secrets").RefKey("client-secret"),
			}

			// Verify that the airbyte server deployment has the correct vars
			airbyteServerDep, err := getDeployment(chartYaml, "airbyte-server")
			assert.NotNil(t, airbyteServerDep)
			assert.NoError(t, err)

			airbyteServerEnvVars := envVarMap(airbyteServerDep.Spec.Template.Spec.Containers[0].Env)
			for k, expected := range expectedEnvVarKeys {
				actual, ok := airbyteServerEnvVars[k]
				assert.True(t, ok, fmt.Sprintf("`%s` should be declared as an environment variable", k))
				verifyEnvVar(t, expected, actual)
			}
		})

	})

	t.Run("should require a licenseKeySecretName when edition is enterprise", func(t *testing.T) {
		helmOpts := baseHelmOptionsForEnterpriseWithValues()
		helmOpts.SetValues["global.enterprise.licenseKeySecretName"] = "airbyte-license"

		chartYaml, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		require.NotNil(t, chartYaml)
		require.NoError(t, err)

		// Verify that the airbyte server deployment has the correct vars
		airbyteServerDep, err := getDeployment(chartYaml, "airbyte-server")
		expectedEnvVarKeys := map[string]expectedEnvVar{
			"AIRBYTE_LICENSE_KEY": expectedSecretVar().RefName("airbyte-license").RefKey("license-key"),
		}

		airbyteServerEnvVars := envVarMap(airbyteServerDep.Spec.Template.Spec.Containers[0].Env)
		for k, expected := range expectedEnvVarKeys {
			actual, ok := airbyteServerEnvVars[k]
			assert.True(t, ok, fmt.Sprintf("`%s` should be declared as an environment variable", k))
			verifyEnvVar(t, expected, actual)
		}
	})
}
