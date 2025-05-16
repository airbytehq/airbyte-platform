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
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("SECRET_PERSISTENCE").Value("VAULT"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("VAULT_ADDRESS").Value("http://airbyte-vault-svc.ab:8200"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("VAULT_PREFIX").Value("secret/"),
		helmtests.ExpectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("VAULT_AUTH_TOKEN").Value(""),
	}

	releaseApps := appsForRelease("airbyte")
	for _, name := range []string{"server", "worker", "workload-launcher"} {
		app := releaseApps[name]
		chartYaml.VerifyEnvVarsForApp(t, app.Kind, app.FQN(), expectedEnvVars)
	}
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
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("SECRET_PERSISTENCE").Value("AWS_SECRET_MANAGER"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("AWS_SECRET_MANAGER_REGION").Value("us-east-1"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("AWS_SECRET_MANAGER_SECRET_TAGS").Value(""),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("AWS_KMS_KEY_ARN").Value(""),
		helmtests.ExpectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("AWS_SECRET_MANAGER_ACCESS_KEY_ID").Value("fake-access-key-id"),
		helmtests.ExpectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("AWS_SECRET_MANAGER_SECRET_ACCESS_KEY").Value("fake-secret-access-key"),
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
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("SECRET_PERSISTENCE").Value("GOOGLE_SECRET_MANAGER"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("SECRET_STORE_GCP_PROJECT_ID").Value("test-project"),
		helmtests.ExpectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("SECRET_STORE_GCP_CREDENTIALS").Value("fake-credentials"),
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
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("SECRET_PERSISTENCE").Value("AZURE_KEY_VAULT"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("AB_AZURE_KEY_VAULT_TENANT_ID").Value("test-tenant-id"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("AB_AZURE_KEY_VAULT_VAULT_URL").Value("http://my-test-vault-url"),
		helmtests.ExpectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("AB_AZURE_KEY_VAULT_CLIENT_ID").Value("test-client-id"),
		helmtests.ExpectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("AB_AZURE_KEY_VAULT_CLIENT_SECRET").Value("test-client-secret"),
	}

	releaseApps := appsForRelease("airbyte")
	for _, name := range []string{"server", "worker", "workload-launcher"} {
		app := releaseApps[name]
		chartYaml.VerifyEnvVarsForApp(t, app.Kind, app.FQN(), expectedEnvVars)
	}
}
