package tests

import (
	"fmt"
	"testing"

	"github.com/gruntwork-io/terratest/modules/helm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicEnterpriseConfigWithHelmValues(t *testing.T) {
	t.Run("require global.enterprise.secretName", func(t *testing.T) {
		helmOpts := BaseHelmOptions()
		helmOpts.SetValues["global.edition"] = "enterprise"
		// set to empty string since it has a default value
		helmOpts.SetValues["global.enterprise.secretName"] = ""
		helmOpts.SetValues["global.auth.instanceAdmin.firstName"] = "Ocvatia"
		helmOpts.SetValues["global.auth.instanceAdmin.lastName"] = "Squidington"
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		require.ErrorContains(t, err, "You must set `global.enterprise.secretName` when `global.edition` is 'enterprise'")
	})

	t.Run("require global.enterprise.licenseKeySecretKey", func(t *testing.T) {
		helmOpts := BaseHelmOptions()
		helmOpts.SetValues["global.edition"] = "enterprise"
		// set to empty string since it has a default value
		helmOpts.SetValues["global.enterprise.licenseKeySecretKey"] = ""
		helmOpts.SetValues["global.auth.instanceAdmin.firstName"] = "Ocvatia"
		helmOpts.SetValues["global.auth.instanceAdmin.lastName"] = "Squidington"
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		require.ErrorContains(t, err, "You must set `global.enterprise.licenseKeySecretKey` when `global.edition` is 'enterprise'")
	})

	t.Run("require global.auth.instanceAdmin.secretName", func(t *testing.T) {
		helmOpts := BaseHelmOptions()
		helmOpts.SetValues["global.edition"] = "enterprise"
		// set to empty string since it has a default value
		helmOpts.SetValues["global.auth.instanceAdmin.secretName"] = ""
		helmOpts.SetValues["global.auth.instanceAdmin.firstName"] = "Ocvatia"
		helmOpts.SetValues["global.auth.instanceAdmin.lastName"] = "Squidington"
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		require.ErrorContains(t, err, "You must set `global.auth.instanceAdmin.secretName` when `global.edition` is 'enterprise'")
	})

	t.Run("require global.auth.instanceAdmin.firstName", func(t *testing.T) {
		helmOpts := BaseHelmOptions()
		helmOpts.SetValues["global.edition"] = "enterprise"
		helmOpts.SetValues["global.auth.instanceAdmin.firstName"] = ""
		helmOpts.SetValues["global.auth.instanceAdmin.lastName"] = "Squidington"
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		require.ErrorContains(t, err, "You must set `global.auth.instanceAdmin.firstName` when `global.edition` is 'enterprise'")
	})

	t.Run("require global.auth.instanceAdmin.lastName", func(t *testing.T) {
		helmOpts := BaseHelmOptions()
		helmOpts.SetValues["global.edition"] = "enterprise"
		helmOpts.SetValues["global.auth.instanceAdmin.firstName"] = "Octavia"
		helmOpts.SetValues["global.auth.instanceAdmin.lastName"] = ""
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		require.ErrorContains(t, err, "You must set `global.auth.instanceAdmin.lastName` when `global.edition` is 'enterprise'")
	})

	t.Run("require global.auth.instanceAdmin.emailSecretKey", func(t *testing.T) {
		helmOpts := BaseHelmOptions()
		helmOpts.SetValues["global.edition"] = "enterprise"
		helmOpts.SetValues["global.auth.instanceAdmin.firstName"] = "Octavia"
		helmOpts.SetValues["global.auth.instanceAdmin.lastName"] = "Squidington"
		// set to empty string since it has a default value
		helmOpts.SetValues["global.auth.instanceAdmin.emailSecretKey"] = ""
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		require.ErrorContains(t, err, "You must set `global.auth.instanceAdmin.emailSecretKey` when `global.edition` is 'enterprise'")
	})

	t.Run("require global.auth.instanceAdmin.passwordSecretKey", func(t *testing.T) {
		helmOpts := BaseHelmOptions()
		helmOpts.SetValues["global.edition"] = "enterprise"
		helmOpts.SetValues["global.auth.instanceAdmin.firstName"] = "Octavia"
		helmOpts.SetValues["global.auth.instanceAdmin.lastName"] = "Squidington"
		// set to empty string since it has a default value
		helmOpts.SetValues["global.auth.instanceAdmin.passwordSecretKey"] = ""
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		require.ErrorContains(t, err, "You must set `global.auth.instanceAdmin.passwordSecretKey` when `global.edition` is 'enterprise'")
	})

	t.Run("require global.auth.identityProvider.secretName when enabling SSO", func(t *testing.T) {
		helmOpts := BaseHelmOptions()
		helmOpts.SetValues["global.edition"] = "enterprise"
		helmOpts.SetValues["global.auth.instanceAdmin.firstName"] = "Octavia"
		helmOpts.SetValues["global.auth.instanceAdmin.lastName"] = "Squidington"
		helmOpts.SetValues["global.auth.identityProvider.secretName"] = ""
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		require.ErrorContains(t, err, "You must set `global.auth.identityProvider.secretName` when enabling SSO")
	})

	t.Run("require global.auth.identityProvider.type when enabling SSO", func(t *testing.T) {
		helmOpts := BaseHelmOptions()
		helmOpts.SetValues["global.edition"] = "enterprise"
		helmOpts.SetValues["global.auth.instanceAdmin.firstName"] = "Octavia"
		helmOpts.SetValues["global.auth.instanceAdmin.lastName"] = "Squidington"
		helmOpts.SetValues["global.auth.identityProvider.secretName"] = "sso-secrets"
		helmOpts.SetValues["global.auth.identityProvider.type"] = ""
		helmOpts.SetValues["global.auth.identityProvider.oidc.domain"] = "sso.example.com"
		helmOpts.SetValues["global.auth.identityProvider.oidc.appName"] = "example-app"
		helmOpts.SetValues["global.auth.identityProvider.oidc.clientIdSecretKey"] = "client-id"
		helmOpts.SetValues["global.auth.identityProvider.oidc.clientSecretSecretKey"] = "client-secret"
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		require.ErrorContains(t, err, "You must set `global.auth.identityProvider.type` when enabling SSO")
	})

	t.Run("require global.auth.identityProvider.oidc.domain when enabling SSO", func(t *testing.T) {
		helmOpts := BaseHelmOptions()
		helmOpts.SetValues["global.edition"] = "enterprise"
		helmOpts.SetValues["global.auth.instanceAdmin.firstName"] = "Octavia"
		helmOpts.SetValues["global.auth.instanceAdmin.lastName"] = "Squidington"
		helmOpts.SetValues["global.auth.identityProvider.secretName"] = "sso-secrets"
		helmOpts.SetValues["global.auth.identityProvider.type"] = "oidc"
		helmOpts.SetValues["global.auth.identityProvider.oidc.domain"] = ""
		helmOpts.SetValues["global.auth.identityProvider.oidc.appName"] = "example-app"
		helmOpts.SetValues["global.auth.identityProvider.oidc.clientIdSecretKey"] = "client-id"
		helmOpts.SetValues["global.auth.identityProvider.oidc.clientSecretSecretKey"] = "client-secret"
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		require.ErrorContains(t, err, "You must set `global.auth.identityProvider.oidc.domain` when enabling SSO")
	})

	t.Run("require global.auth.identityProvider.oidc.appName when enabling SSO", func(t *testing.T) {
		helmOpts := BaseHelmOptions()
		helmOpts.SetValues["global.edition"] = "enterprise"
		helmOpts.SetValues["global.auth.instanceAdmin.firstName"] = "Octavia"
		helmOpts.SetValues["global.auth.instanceAdmin.lastName"] = "Squidington"
		helmOpts.SetValues["global.auth.identityProvider.secretName"] = "sso-secrets"
		helmOpts.SetValues["global.auth.identityProvider.type"] = "oidc"
		helmOpts.SetValues["global.auth.identityProvider.oidc.domain"] = "sso.example.com"
		helmOpts.SetValues["global.auth.identityProvider.oidc.appName"] = ""
		helmOpts.SetValues["global.auth.identityProvider.oidc.clientIdSecretKey"] = "client-id"
		helmOpts.SetValues["global.auth.identityProvider.oidc.clientSecretSecretKey"] = "client-secret"
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		require.ErrorContains(t, err, "You must set `global.auth.identityProvider.oidc.appName` when enabling SSO")
	})

	t.Run("require global.auth.identityProvider.oidc.clientIdSecretKey when enabling SSO", func(t *testing.T) {
		helmOpts := BaseHelmOptions()
		helmOpts.SetValues["global.edition"] = "enterprise"
		helmOpts.SetValues["global.auth.instanceAdmin.firstName"] = "Octavia"
		helmOpts.SetValues["global.auth.instanceAdmin.lastName"] = "Squidington"
		helmOpts.SetValues["global.auth.identityProvider.secretName"] = "sso-secrets"
		helmOpts.SetValues["global.auth.identityProvider.type"] = "oidc"
		helmOpts.SetValues["global.auth.identityProvider.oidc.domain"] = "sso.example.com"
		helmOpts.SetValues["global.auth.identityProvider.oidc.appName"] = "example-app"
		helmOpts.SetValues["global.auth.identityProvider.oidc.clientIdSecretKey"] = ""
		helmOpts.SetValues["global.auth.identityProvider.oidc.clientSecretSecretKey"] = "client-secret"
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		require.ErrorContains(t, err, "You must set `global.auth.identityProvider.oidc.clientIdSecretKey` when enabling SSO")
	})

	t.Run("require global.auth.identityProvider.oidc.clientSecretSecretKey when enabling SSO", func(t *testing.T) {
		helmOpts := BaseHelmOptions()
		helmOpts.SetValues["global.edition"] = "enterprise"
		helmOpts.SetValues["global.auth.instanceAdmin.firstName"] = "Octavia"
		helmOpts.SetValues["global.auth.instanceAdmin.lastName"] = "Squidington"
		helmOpts.SetValues["global.auth.identityProvider.secretName"] = "sso-secrets"
		helmOpts.SetValues["global.auth.identityProvider.type"] = "oidc"
		helmOpts.SetValues["global.auth.identityProvider.oidc.domain"] = "sso.example.com"
		helmOpts.SetValues["global.auth.identityProvider.oidc.appName"] = "example-app"
		helmOpts.SetValues["global.auth.identityProvider.oidc.clientIdSecretKey"] = "client-id"
		helmOpts.SetValues["global.auth.identityProvider.oidc.clientSecretSecretKey"] = ""
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		require.ErrorContains(t, err, "You must set `global.auth.identityProvider.oidc.clientSecretSecretKey` when enabling SSO")
	})

	t.Run("should set enterprise config env vars", func(t *testing.T) {
		helmOpts := BaseHelmOptionsForEnterpriseWithValues()
		helmOpts.SetValues["global.enterprise.secretName"] = "airbyte-license"
		helmOpts.SetValues["global.auth.instanceAdmin.secretName"] = "sso-secrets"
		helmOpts.SetValues["global.auth.identityProvider.secretName"] = "sso-secrets"
		helmOpts.SetValues["global.auth.identityProvider.type"] = "oidc"
		helmOpts.SetValues["global.auth.identityProvider.oidc.domain"] = "sso.example.org"
		helmOpts.SetValues["global.auth.identityProvider.oidc.appName"] = "sso-app"
		helmOpts.SetValues["global.auth.identityProvider.oidc.clientIdSecretKey"] = "client-id"
		helmOpts.SetValues["global.auth.identityProvider.oidc.clientSecretSecretKey"] = "client-secret"

		chartYaml := renderChart(t, helmOpts)

		t.Run("should set required env vars for keycloak setup job", func(t *testing.T) {
			expectedEnvVarKeys := map[string]expectedEnvVar{
				"AIRBYTE_URL":             expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("AIRBYTE_URL"),
				"INITIAL_USER_FIRST_NAME": expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("INITIAL_USER_FIRST_NAME"),
				"INITIAL_USER_LAST_NAME":  expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("INITIAL_USER_LAST_NAME"),
				"INITIAL_USER_EMAIL":      expectedSecretVar().RefName("sso-secrets").RefKey("instance-admin-email"),
				"INITIAL_USER_PASSWORD":   expectedSecretVar().RefName("sso-secrets").RefKey("instance-admin-password"),
				"IDENTITY_PROVIDER_TYPE":  expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("IDENTITY_PROVIDER_TYPE"),
				"OIDC_DOMAIN":             expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("OIDC_DOMAIN"),
				"OIDC_APP_NAME":           expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("OIDC_APP_NAME"),
				"OIDC_CLIENT_ID":          expectedSecretVar().RefName("sso-secrets").RefKey("client-id"),
				"OIDC_CLIENT_SECRET":      expectedSecretVar().RefName("sso-secrets").RefKey("client-secret"),
			}

			// Verify that the keycloak setup job has the correct vars
			keycloakSetupJob := getJob(chartYaml, "airbyte-keycloak-setup")

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
				"INITIAL_USER_EMAIL":      expectedSecretVar().RefName("sso-secrets").RefKey("instance-admin-email"),
				"INITIAL_USER_PASSWORD":   expectedSecretVar().RefName("sso-secrets").RefKey("instance-admin-password"),
				"IDENTITY_PROVIDER_TYPE":  expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("IDENTITY_PROVIDER_TYPE"),
				"OIDC_DOMAIN":             expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("OIDC_DOMAIN"),
				"OIDC_APP_NAME":           expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("OIDC_APP_NAME"),
				"OIDC_CLIENT_ID":          expectedSecretVar().RefName("sso-secrets").RefKey("client-id"),
				"OIDC_CLIENT_SECRET":      expectedSecretVar().RefName("sso-secrets").RefKey("client-secret"),
			}

			// Verify that the airbyte server deployment has the correct vars
			airbyteServerDep := getDeployment(chartYaml, "airbyte-server")

			airbyteServerEnvVars := envVarMap(airbyteServerDep.Spec.Template.Spec.Containers[0].Env)
			for k, expected := range expectedEnvVarKeys {
				actual, ok := airbyteServerEnvVars[k]
				assert.True(t, ok, fmt.Sprintf("`%s` should be declared as an environment variable", k))
				verifyEnvVar(t, expected, actual)
			}
		})

		t.Run("should configure keycloak to use the 'KEYCLOAK_DATABASE_URL'", func(t *testing.T) {
			expectedEnvVarKeys := map[string]expectedEnvVar{
				"KEYCLOAK_DATABASE_URL": expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("KEYCLOAK_DATABASE_URL"),
			}

			keycloakSS := getStatefulSet(chartYaml, "airbyte-keycloak")

			keycloakEnvVars := envVarMap(keycloakSS.Spec.Template.Spec.Containers[0].Env)
			for k, expected := range expectedEnvVarKeys {
				actual, ok := keycloakEnvVars[k]
				assert.True(t, ok, fmt.Sprintf("`%s` should be declared as an environment variable", k))
				verifyEnvVar(t, expected, actual)
			}
		})
	})
}

func TestKeycloakInitContainerOverride(t *testing.T) {
	t.Run("default keycloak readiness image is airbyte/utils", func(t *testing.T) {
		helmOpts := BaseHelmOptionsForEnterpriseWithValues()
		helmOpts.SetValues["global.auth.instanceAdmin.firstName"] = "Octavia"
		helmOpts.SetValues["global.auth.instanceAdmin.lastName"] = "Squidington"
		helmOpts.SetValues["global.auth.identityProvider.secretName"] = "sso-secrets"
		helmOpts.SetValues["global.auth.identityProvider.type"] = "oidc"
		helmOpts.SetValues["global.auth.identityProvider.oidc.domain"] = "example.com"
		helmOpts.SetValues["global.auth.identityProvider.oidc.appName"] = "example-app"
		helmOpts.SetValues["global.auth.identityProvider.oidc.clientIdSecretKey"] = "client-id"
		helmOpts.SetValues["global.auth.identityProvider.oidc.clientSecretSecretKey"] = "client-secret"

		chartYaml := renderChart(t, helmOpts)
		keycloakStatefulSet := getStatefulSet(chartYaml, "airbyte-keycloak")

		keycloakInitContainers := keycloakStatefulSet.Spec.Template.Spec.InitContainers
		assert.Equal(t, "postgres:13-alpine", keycloakInitContainers[0].Image)
	})

	t.Run("override init container image", func(t *testing.T) {
		helmOpts := BaseHelmOptionsForEnterpriseWithValues()
		helmOpts.SetValues["global.auth.instanceAdmin.firstName"] = "Octavia"
		helmOpts.SetValues["global.auth.instanceAdmin.lastName"] = "Squidington"
		helmOpts.SetValues["global.auth.identityProvider.secretName"] = "sso-secrets"
		helmOpts.SetValues["global.auth.identityProvider.type"] = "oidc"
		helmOpts.SetValues["global.auth.identityProvider.oidc.domain"] = "example.com"
		helmOpts.SetValues["global.auth.identityProvider.oidc.appName"] = "example-app"
		helmOpts.SetValues["global.auth.identityProvider.oidc.clientIdSecretKey"] = "client-id"
		helmOpts.SetValues["global.auth.identityProvider.oidc.clientSecretSecretKey"] = "client-secret"
		helmOpts.SetValues["keycloak.initContainers.initDb.image"] = "airbyte/custom-postgres-image"

		chartYaml := renderChart(t, helmOpts)
		keycloakStatefulSet := getStatefulSet(chartYaml, "airbyte-keycloak")

		keycloakInitContainers := keycloakStatefulSet.Spec.Template.Spec.InitContainers
		assert.Equal(t, "airbyte/custom-postgres-image", keycloakInitContainers[0].Image)
	})
}
