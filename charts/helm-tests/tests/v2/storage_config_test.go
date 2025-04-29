package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestDefaultStorageConfig(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	expectedEnvVars := []helmtests.ExpectedEnvVar{
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("MINIO_ENDPOINT").Value("http://airbyte-minio-svc.ab:9000"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("S3_PATH_STYLE_ACCESS").Value("true"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("STORAGE_BUCKET_ACTIVITY_PAYLOAD").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("STORAGE_BUCKET_LOG").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("STORAGE_BUCKET_STATE").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("STORAGE_BUCKET_WORKLOAD_OUTPUT").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("STORAGE_TYPE").Value("minio"),
		helmtests.ExpectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("AWS_ACCESS_KEY_ID").Value("minio"),
		helmtests.ExpectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("AWS_SECRET_ACCESS_KEY").Value("minio123"),
	}

	releaseApps := appsForRelease("airbyte")
	for _, name := range []string{"server", "worker", "workload-launcher"} {
		app := releaseApps[name]
		chartYaml.VerifyEnvVarsForApp(t, app.Kind, app.FQN(), expectedEnvVars)
	}
}

func TestGcsStorageConfig(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["global.storage.type"] = "gcs"
	// Base64 encoded `{"fake": "fake"}`
	opts.SetValues["global.storage.gcs.credentialsJson"] = "eyJmYWtlIjogImZha2UifQ=="

	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	expectedEnvVars := []helmtests.ExpectedEnvVar{
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("GOOGLE_APPLICATION_CREDENTIALS").Value("/secrets/gcp-creds/gcp.json"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("STORAGE_BUCKET_ACTIVITY_PAYLOAD").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("STORAGE_BUCKET_LOG").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("STORAGE_BUCKET_STATE").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("STORAGE_BUCKET_WORKLOAD_OUTPUT").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("STORAGE_TYPE").Value("gcs"),
		helmtests.ExpectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("GOOGLE_APPLICATION_CREDENTIALS_JSON").Value("eyJmYWtlIjogImZha2UifQ=="),
	}

	releaseApps := appsForRelease("airbyte")
	for _, name := range []string{"server", "worker", "workload-launcher"} {
		app := releaseApps[name]
		chartYaml.VerifyEnvVarsForApp(t, app.Kind, app.FQN(), expectedEnvVars)
	}
}

func TestS3StorageConfig(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["global.storage.type"] = "s3"
	opts.SetValues["global.storage.s3.authenticationType"] = "credentials"
	opts.SetValues["global.storage.s3.accessKeyId"] = "access-key-id"
	opts.SetValues["global.storage.s3.secretAccessKey"] = "secret-access-key"

	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	expectedEnvVars := []helmtests.ExpectedEnvVar{
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("AWS_DEFAULT_REGION").Value(""),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("AWS_AUTHENTICATION_TYPE").Value("credentials"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("STORAGE_BUCKET_ACTIVITY_PAYLOAD").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("STORAGE_BUCKET_LOG").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("STORAGE_BUCKET_STATE").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("STORAGE_BUCKET_WORKLOAD_OUTPUT").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("STORAGE_TYPE").Value("s3"),
		helmtests.ExpectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("AWS_ACCESS_KEY_ID").Value("access-key-id"),
		helmtests.ExpectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("AWS_SECRET_ACCESS_KEY").Value("secret-access-key"),
	}

	releaseApps := appsForRelease("airbyte")
	for _, name := range []string{"server", "worker", "workload-launcher"} {
		app := releaseApps[name]
		chartYaml.VerifyEnvVarsForApp(t, app.Kind, app.FQN(), expectedEnvVars)
	}
}

func TestAzureStorageConfig(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["global.storage.type"] = "azure"
	opts.SetValues["global.storage.azure.connectionString"] = "super-duper-secret-string"

	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	expectedEnvVars := []helmtests.ExpectedEnvVar{
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("STORAGE_BUCKET_ACTIVITY_PAYLOAD").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("STORAGE_BUCKET_LOG").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("STORAGE_BUCKET_STATE").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("STORAGE_BUCKET_WORKLOAD_OUTPUT").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("STORAGE_TYPE").Value("azure"),
		helmtests.ExpectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("AZURE_STORAGE_CONNECTION_STRING").Value("super-duper-secret-string"),
	}

	releaseApps := appsForRelease("airbyte")
	for _, name := range []string{"server", "worker", "workload-launcher"} {
		app := releaseApps[name]
		chartYaml.VerifyEnvVarsForApp(t, app.Kind, app.FQN(), expectedEnvVars)
	}
}
