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
		helmtests.ExpectedConfigMapVar().Name("MINIO_ENDPOINT").RefName("airbyte-airbyte-env").Value("http://airbyte-minio-svc.ab:9000"),
		helmtests.ExpectedConfigMapVar().Name("S3_PATH_STYLE_ACCESS").RefName("airbyte-airbyte-env").Value("true"),
		helmtests.ExpectedConfigMapVar().Name("STORAGE_BUCKET_ACTIVITY_PAYLOAD").RefName("airbyte-airbyte-env").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().Name("STORAGE_BUCKET_LOG").RefName("airbyte-airbyte-env").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().Name("STORAGE_BUCKET_STATE").RefName("airbyte-airbyte-env").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().Name("STORAGE_BUCKET_WORKLOAD_OUTPUT").RefName("airbyte-airbyte-env").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().Name("STORAGE_TYPE").RefName("airbyte-airbyte-env").Name("STORAGE_TYPE").Value("minio"),
		helmtests.ExpectedSecretVar().Name("AWS_ACCESS_KEY_ID").RefName("airbyte-airbyte-secrets").Name("AWS_ACCESS_KEY_ID").Value("minio"),
		helmtests.ExpectedSecretVar().Name("AWS_SECRET_ACCESS_KEY").RefName("airbyte-airbyte-secrets").Value("minio123"),
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
		helmtests.ExpectedConfigMapVar().Name("GOOGLE_APPLICATION_CREDENTIALS").RefName("airbyte-airbyte-env").Value("/secrets/gcp-creds/gcp.json"),
		helmtests.ExpectedConfigMapVar().Name("STORAGE_BUCKET_ACTIVITY_PAYLOAD").RefName("airbyte-airbyte-env").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().Name("STORAGE_BUCKET_LOG").RefName("airbyte-airbyte-env").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().Name("STORAGE_BUCKET_STATE").RefName("airbyte-airbyte-env").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().Name("STORAGE_BUCKET_WORKLOAD_OUTPUT").RefName("airbyte-airbyte-env").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().Name("STORAGE_TYPE").RefName("airbyte-airbyte-env").Value("gcs"),
		helmtests.ExpectedSecretVar().Name("GOOGLE_APPLICATION_CREDENTIALS_JSON").RefName("airbyte-airbyte-secrets").Value("eyJmYWtlIjogImZha2UifQ=="),
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
		helmtests.ExpectedConfigMapVar().Name("AWS_DEFAULT_REGION").RefName("airbyte-airbyte-env").Value(""),
		helmtests.ExpectedConfigMapVar().Name("AWS_AUTHENTICATION_TYPE").RefName("airbyte-airbyte-env").Value("credentials"),
		helmtests.ExpectedConfigMapVar().Name("STORAGE_BUCKET_ACTIVITY_PAYLOAD").RefName("airbyte-airbyte-env").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().Name("STORAGE_BUCKET_LOG").RefName("airbyte-airbyte-env").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().Name("STORAGE_BUCKET_STATE").RefName("airbyte-airbyte-env").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().Name("STORAGE_BUCKET_WORKLOAD_OUTPUT").RefName("airbyte-airbyte-env").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().Name("STORAGE_TYPE").RefName("airbyte-airbyte-env").Value("s3"),
		helmtests.ExpectedSecretVar().Name("AWS_ACCESS_KEY_ID").RefName("airbyte-airbyte-secrets").Value("access-key-id"),
		helmtests.ExpectedSecretVar().Name("AWS_SECRET_ACCESS_KEY").RefName("airbyte-airbyte-secrets").Value("secret-access-key"),
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
		helmtests.ExpectedConfigMapVar().Name("STORAGE_BUCKET_ACTIVITY_PAYLOAD").RefName("airbyte-airbyte-env").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().Name("STORAGE_BUCKET_LOG").RefName("airbyte-airbyte-env").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().Name("STORAGE_BUCKET_STATE").RefName("airbyte-airbyte-env").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().Name("STORAGE_BUCKET_WORKLOAD_OUTPUT").RefName("airbyte-airbyte-env").Value("airbyte-bucket"),
		helmtests.ExpectedConfigMapVar().Name("STORAGE_TYPE").RefName("airbyte-airbyte-env").Value("azure"),
		helmtests.ExpectedSecretVar().Name("AZURE_STORAGE_CONNECTION_STRING").RefName("airbyte-airbyte-secrets").Value("super-duper-secret-string"),
	}

	releaseApps := appsForRelease("airbyte")
	for _, name := range []string{"server", "worker", "workload-launcher"} {
		app := releaseApps[name]
		chartYaml.VerifyEnvVarsForApp(t, app.Kind, app.FQN(), expectedEnvVars)
	}
}
