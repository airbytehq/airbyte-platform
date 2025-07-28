package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestDefaultSecretManagerConfig(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	configMap := helmtests.GetConfigMap(chartYaml.String(), "airbyte-airbyte-env")
	assert.NoError(t, err)
	assert.NotNil(t, configMap)

	// default is empty value
	v, ok := configMap.Data["SECRET_PERSISTENCE"]
	assert.True(t, ok)
	assert.Empty(t, v, "value should be empty")

	releaseApps := appsForRelease("airbyte")
	for _, name := range []string{"bootloader", "server", "worker", "workload-launcher"} {
		rapp := releaseApps[name]
		app := chartYaml.GetResourceByKindAndName(rapp.Kind, rapp.FQN())
		assert.NotNil(t, app, "unable to find %s named '%s'", rapp.Kind, rapp.FQN())

		pod := helmtests.GetPodSpec(app)
		assert.NotNil(t, pod, "unable to extract pod spec from %s: %s", rapp.Kind, rapp.FQN())
	}
}

func TestVaultSecretManagerConfig(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["global.secretsManager.type"] = "VAULT"
	opts.SetJsonValues["global.secretsManager.vault"] = "{}"
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	expectedEnvVars := []helmtests.ExpectedEnvVar{
		helmtests.ExpectedConfigMapVar().Name("SECRET_PERSISTENCE").RefName("airbyte-airbyte-env").Value("VAULT"),
		helmtests.ExpectedConfigMapVar().Name("VAULT_ADDRESS").RefName("airbyte-airbyte-env").Value("http://airbyte-vault-svc.ab:8200"),
		helmtests.ExpectedConfigMapVar().Name("VAULT_PREFIX").RefName("airbyte-airbyte-env").Value("secret/"),
		helmtests.ExpectedSecretVar().Name("VAULT_AUTH_TOKEN").RefName("airbyte-airbyte-secrets").Value(""),
		helmtests.ExpectedConfigMapVar().Name("VAULT_AUTH_TOKEN_REF_NAME").RefName("airbyte-airbyte-env").Value("airbyte-airbyte-secrets"),
		helmtests.ExpectedConfigMapVar().Name("VAULT_AUTH_TOKEN_REF_KEY").RefName("airbyte-airbyte-env").Value("VAULT_AUTH_TOKEN"),
	}

	releaseApps := appsForRelease("airbyte")
	for _, name := range []string{"server", "worker", "workload-launcher"} {
		app := releaseApps[name]
		chartYaml.VerifyEnvVarsForApp(t, app.Kind, app.FQN(), expectedEnvVars)
	}

	t.Run("should allow overriding the secret coordinates", func(tt *testing.T) {
		opts := helmtests.BaseHelmOptions()
		opts.SetValues["global.secretsManager.type"] = "VAULT"
		opts.SetValues["global.secretsManager.vault.authTokenSecretKey"] = "CUSTOM_AUTH_TOKEN_SECRET_KEY"
		chartYaml, err := helmtests.RenderHelmChart(tt, opts, chartPath, "airbyte", nil)
		assert.NoError(tt, err)

		expectedEnvVars := []helmtests.ExpectedEnvVar{
			helmtests.ExpectedConfigMapVar().Name("SECRET_PERSISTENCE").RefName("airbyte-airbyte-env").Value("VAULT"),
			helmtests.ExpectedConfigMapVar().Name("VAULT_ADDRESS").RefName("airbyte-airbyte-env").Value("http://airbyte-vault-svc.ab:8200"),
			helmtests.ExpectedConfigMapVar().Name("VAULT_PREFIX").RefName("airbyte-airbyte-env").Value("secret/"),
			helmtests.ExpectedSecretVar().Name("VAULT_AUTH_TOKEN").RefKey("CUSTOM_AUTH_TOKEN_SECRET_KEY").RefName("airbyte-airbyte-secrets").Value(""),
			helmtests.ExpectedConfigMapVar().Name("VAULT_AUTH_TOKEN_REF_NAME").RefName("airbyte-airbyte-env").Value("airbyte-airbyte-secrets"),
			helmtests.ExpectedConfigMapVar().Name("VAULT_AUTH_TOKEN_REF_KEY").RefName("airbyte-airbyte-env").Value("CUSTOM_AUTH_TOKEN_SECRET_KEY"),
		}

		releaseApps := appsForRelease("airbyte")
		for _, name := range []string{"server", "worker", "workload-launcher"} {
			app := releaseApps[name]
			chartYaml.VerifyEnvVarsForApp(tt, app.Kind, app.FQN(), expectedEnvVars)
		}
	})

	t.Run("the ref name should derive from the secretName", func(tt *testing.T) {
		opts := helmtests.BaseHelmOptions()
		opts.SetValues["global.secretsManager.type"] = "VAULT"
		opts.SetValues["global.secretsManager.secretName"] = "custom-secret"
		opts.SetValues["global.secretsManager.vault.authTokenSecretKey"] = "CUSTOM_AUTH_TOKEN_SECRET_KEY"
		chartYaml, err := helmtests.RenderHelmChart(tt, opts, chartPath, "airbyte", nil)
		assert.NoError(tt, err)

		expectedEnvVars := []helmtests.ExpectedEnvVar{
			helmtests.ExpectedConfigMapVar().Name("SECRET_PERSISTENCE").RefName("airbyte-airbyte-env").Value("VAULT"),
			helmtests.ExpectedConfigMapVar().Name("VAULT_ADDRESS").RefName("airbyte-airbyte-env").Value("http://airbyte-vault-svc.ab:8200"),
			helmtests.ExpectedConfigMapVar().Name("VAULT_PREFIX").RefName("airbyte-airbyte-env").Value("secret/"),
			helmtests.ExpectedSecretVar().Name("VAULT_AUTH_TOKEN").RefKey("CUSTOM_AUTH_TOKEN_SECRET_KEY").RefName("custom-secret").Value(""),
			helmtests.ExpectedConfigMapVar().Name("VAULT_AUTH_TOKEN_REF_NAME").RefName("airbyte-airbyte-env").Value("custom-secret"),
			helmtests.ExpectedConfigMapVar().Name("VAULT_AUTH_TOKEN_REF_KEY").RefName("airbyte-airbyte-env").Value("CUSTOM_AUTH_TOKEN_SECRET_KEY"),
		}

		releaseApps := appsForRelease("airbyte")
		for _, name := range []string{"server", "worker", "workload-launcher"} {
			app := releaseApps[name]
			chartYaml.VerifyEnvVarsForApp(tt, app.Kind, app.FQN(), expectedEnvVars)
		}
	})
}

func TestAwsSecretManagerSecretManagerConfig(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["global.secretsManager.type"] = "AWS_SECRET_MANAGER"
	opts.SetValues["global.secretsManager.awsSecretManager.authenticationType"] = "credentials"
	opts.SetValues["global.secretsManager.awsSecretManager.region"] = "us-east-1"
	opts.SetValues["global.secretsManager.awsSecretManager.accessKeyId"] = "fake-access-key-id"
	opts.SetValues["global.secretsManager.awsSecretManager.secretAccessKey"] = "fake-secret-access-key"
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	expectedEnvVars := []helmtests.ExpectedEnvVar{
		helmtests.ExpectedConfigMapVar().Name("SECRET_PERSISTENCE").RefName("airbyte-airbyte-env").Value("AWS_SECRET_MANAGER"),
		helmtests.ExpectedConfigMapVar().Name("AWS_SECRET_MANAGER_REGION").RefName("airbyte-airbyte-env").Value("us-east-1"),
		helmtests.ExpectedConfigMapVar().Name("AWS_SECRET_MANAGER_SECRET_TAGS").RefName("airbyte-airbyte-env").Value(""),
		helmtests.ExpectedConfigMapVar().Name("AWS_KMS_KEY_ARN").RefName("airbyte-airbyte-env").Value(""),
		helmtests.ExpectedSecretVar().Name("AWS_SECRET_MANAGER_ACCESS_KEY_ID").RefName("airbyte-airbyte-secrets").Value("fake-access-key-id"),
		helmtests.ExpectedSecretVar().Name("AWS_SECRET_MANAGER_SECRET_ACCESS_KEY").RefName("airbyte-airbyte-secrets").Value("fake-secret-access-key"),
	}

	releaseApps := appsForRelease("airbyte")
	for _, name := range []string{"server", "worker", "workload-launcher"} {
		app := releaseApps[name]
		chartYaml.VerifyEnvVarsForApp(t, app.Kind, app.FQN(), expectedEnvVars)
	}
}

func TestGoogleSecretManagerSecretManagerConfig(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["global.secretsManager.type"] = "GOOGLE_SECRET_MANAGER"
	opts.SetValues["global.secretsManager.googleSecretManager.projectId"] = "test-project"
	opts.SetValues["global.secretsManager.googleSecretManager.credentials"] = "fake-credentials"
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	expectedEnvVars := []helmtests.ExpectedEnvVar{
		helmtests.ExpectedConfigMapVar().Name("SECRET_PERSISTENCE").RefName("airbyte-airbyte-env").Value("GOOGLE_SECRET_MANAGER"),
		helmtests.ExpectedConfigMapVar().Name("SECRET_STORE_GCP_PROJECT_ID").RefName("airbyte-airbyte-env").Value("test-project"),
		helmtests.ExpectedSecretVar().Name("SECRET_STORE_GCP_CREDENTIALS").RefName("airbyte-airbyte-secrets").Value("fake-credentials"),
	}

	releaseApps := appsForRelease("airbyte")
	for _, name := range []string{"server", "worker", "workload-launcher"} {
		app := releaseApps[name]
		chartYaml.VerifyEnvVarsForApp(t, app.Kind, app.FQN(), expectedEnvVars)
	}
}

func TestAzureKeyVaultSecretManagerConfig(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["global.secretsManager.type"] = "AZURE_KEY_VAULT"
	opts.SetValues["global.secretsManager.azureKeyVault.tenantId"] = "test-tenant-id"
	opts.SetValues["global.secretsManager.azureKeyVault.vaultUrl"] = "http://my-test-vault-url"
	opts.SetValues["global.secretsManager.azureKeyVault.clientId"] = "test-client-id"
	opts.SetValues["global.secretsManager.azureKeyVault.clientSecret"] = "test-client-secret"
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	expectedEnvVars := []helmtests.ExpectedEnvVar{
		helmtests.ExpectedConfigMapVar().Name("SECRET_PERSISTENCE").RefName("airbyte-airbyte-env").Value("AZURE_KEY_VAULT"),
		helmtests.ExpectedConfigMapVar().Name("AB_AZURE_KEY_VAULT_TENANT_ID").RefName("airbyte-airbyte-env").Value("test-tenant-id"),
		helmtests.ExpectedConfigMapVar().Name("AB_AZURE_KEY_VAULT_VAULT_URL").RefName("airbyte-airbyte-env").Value("http://my-test-vault-url"),
		helmtests.ExpectedSecretVar().Name("AB_AZURE_KEY_VAULT_CLIENT_ID").RefName("airbyte-airbyte-secrets").Value("test-client-id"),
		helmtests.ExpectedSecretVar().Name("AB_AZURE_KEY_VAULT_CLIENT_SECRET").RefName("airbyte-airbyte-secrets").Value("test-client-secret"),
	}

	releaseApps := appsForRelease("airbyte")
	for _, name := range []string{"server", "worker", "workload-launcher"} {
		app := releaseApps[name]
		chartYaml.VerifyEnvVarsForApp(t, app.Kind, app.FQN(), expectedEnvVars)
	}
}
