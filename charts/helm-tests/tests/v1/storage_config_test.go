package tests

import (
	"fmt"
	"strings"
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/gruntwork-io/terratest/modules/helm"
	"github.com/stretchr/testify/assert"
)

const (
	defaultStorageSecretName = "airbyte-airbyte-secrets"
)

func TestDefaultStorage(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	chartYaml := helmtests.RenderChart(t, opts, chartPath)
	cm := helmtests.GetConfigMap(chartYaml, "airbyte-airbyte-env")

	expect := map[string]string{
		"MINIO_ENDPOINT":                  "http://airbyte-minio-svc:9000",
		"S3_PATH_STYLE_ACCESS":            "true",
		"STORAGE_BUCKET_ACTIVITY_PAYLOAD": "airbyte-storage",
		"STORAGE_BUCKET_LOG":              "airbyte-storage",
		"STORAGE_BUCKET_STATE":            "airbyte-storage",
		"STORAGE_BUCKET_WORKLOAD_OUTPUT":  "airbyte-storage",
		"STORAGE_TYPE":                    "minio",
	}

	for k, v := range expect {
		assert.Equal(t, v, cm.Data[k], "for key "+k)
	}
}

func TestGcsStorage(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	// Base64 encoded `{"fake": "fake"}`
	opts.SetValues["global.storage.type"] = "gcs"
	opts.SetValues["global.storage.gcs.credentialsJson"] = "eyJmYWtlIjogImZha2UifQ=="
	chartYaml := helmtests.RenderChart(t, opts, chartPath)

	cm := helmtests.GetConfigMap(chartYaml, "airbyte-airbyte-env")
	expect := map[string]string{
		"GOOGLE_APPLICATION_CREDENTIALS":  "/secrets/gcs-log-creds/gcp.json",
		"S3_PATH_STYLE_ACCESS":            "",
		"STORAGE_BUCKET_ACTIVITY_PAYLOAD": "airbyte-storage",
		"STORAGE_BUCKET_LOG":              "airbyte-storage",
		"STORAGE_BUCKET_STATE":            "airbyte-storage",
		"STORAGE_BUCKET_WORKLOAD_OUTPUT":  "airbyte-storage",
		"STORAGE_TYPE":                    "gcs",
	}
	for k, v := range expect {
		assert.Equal(t, v, cm.Data[k], "for key "+k)
	}

	secret := helmtests.GetSecret(chartYaml, "airbyte-gcs-log-creds")
	assert.Equal(t, []byte(`{"fake": "fake"}`), secret.Data["gcp.json"])
}

func TestS3Storage(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["global.storage.type"] = "s3"
	opts.SetValues["global.storage.s3.authenticationType"] = "credentials"
	chartYaml := helmtests.RenderChart(t, opts, chartPath)

	cm := helmtests.GetConfigMap(chartYaml, "airbyte-airbyte-env")
	expect := map[string]string{
		"AWS_DEFAULT_REGION":              "",
		"S3_PATH_STYLE_ACCESS":            "",
		"STORAGE_BUCKET_ACTIVITY_PAYLOAD": "airbyte-storage",
		"STORAGE_BUCKET_LOG":              "airbyte-storage",
		"STORAGE_BUCKET_STATE":            "airbyte-storage",
		"STORAGE_BUCKET_WORKLOAD_OUTPUT":  "airbyte-storage",
		"STORAGE_TYPE":                    "s3",
	}
	for k, v := range expect {
		assert.Equal(t, v, cm.Data[k], "for key "+k)
	}
}

func verifyCredentialsForDeployments(t *testing.T, helmOpts *helm.Options, expectedEnvVars map[string]helmtests.ExpectedEnvVar, deployments []string) {
	chartYaml, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	type testCase struct {
		DeploymentName  string
		ExpectedEnvVars map[string]helmtests.ExpectedEnvVar
	}

	var cases []testCase

	for _, d := range deployments {
		cases = append(cases, testCase{DeploymentName: d, ExpectedEnvVars: expectedEnvVars})
	}

	storageType := helmOpts.SetValues["global.storage.type"]
	for _, c := range cases {
		t.Run(fmt.Sprintf("deployment %s contains expected %s credentials env vars", c.DeploymentName, strings.ToUpper(storageType)), func(t *testing.T) {
			dep := helmtests.GetDeployment(chartYaml, c.DeploymentName)

			actualVars := helmtests.EnvVarMap(dep.Spec.Template.Spec.Containers[0].Env)
			for k, expected := range c.ExpectedEnvVars {
				actual, ok := actualVars[k]
				assert.True(t, ok, fmt.Sprintf("`%s` should be declared as an environment variable", k))
				helmtests.VerifyEnvVar(t, chartYaml, expected, actual)
			}
		})
	}
}

func verifyVolumeMountsForDeployments(t *testing.T, helmOpts *helm.Options, expectedVolumeMounts []helmtests.ExpectedVolumeMount, deployments []string) {
	chartYaml, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	type testCase struct {
		DeploymentName       string
		ExpectedVolumeMounts []helmtests.ExpectedVolumeMount
	}

	var cases []testCase

	for _, d := range deployments {
		cases = append(cases, testCase{DeploymentName: d, ExpectedVolumeMounts: expectedVolumeMounts})
	}

	storageType := helmOpts.SetValues["global.storage.type"]
	for _, c := range cases {
		t.Run(fmt.Sprintf("deployment %s contains expected %s volume mounts", c.DeploymentName, strings.ToUpper(storageType)), func(t *testing.T) {
			dep := helmtests.GetDeployment(chartYaml, c.DeploymentName)

			for _, expected := range c.ExpectedVolumeMounts {
				helmtests.VerifyVolumeMountForPod(t, expected, dep.Spec.Template.Spec)
			}
		})
	}
}

func TestS3StorageConfigurationSecrets(t *testing.T) {
	t.Run("authentication type: credentials", func(t *testing.T) {
		t.Run("default storageSecretName", func(t *testing.T) {
			helmOpts := helmtests.BaseHelmOptionsForStorageType("s3")
			helmOpts.SetValues["global.storage.s3.authenticationType"] = "credentials"

			expectedEnvVarKeys := map[string]helmtests.ExpectedEnvVar{
				"AWS_ACCESS_KEY_ID":     helmtests.ExpectedSecretVar().RefName(defaultStorageSecretName).RefKey("s3-access-key-id"),
				"AWS_SECRET_ACCESS_KEY": helmtests.ExpectedSecretVar().RefName(defaultStorageSecretName).RefKey("s3-secret-access-key"),
			}
			verifyCredentialsForDeployments(t, helmOpts, expectedEnvVarKeys, []string{"airbyte-server", "airbyte-worker", "airbyte-workload-launcher"})
		})

		t.Run("user-defined storageSecretName", func(t *testing.T) {
			helmOpts := helmtests.BaseHelmOptionsForStorageType("s3")
			helmOpts.SetValues["global.storage.s3.authenticationType"] = "credentials"
			helmOpts.SetValues["global.storage.secretName"] = "user-defined-secret"

			expectedEnvVarKeys := map[string]helmtests.ExpectedEnvVar{
				"AWS_ACCESS_KEY_ID":     helmtests.ExpectedSecretVar().RefName("user-defined-secret").RefKey("s3-access-key-id"),
				"AWS_SECRET_ACCESS_KEY": helmtests.ExpectedSecretVar().RefName("user-defined-secret").RefKey("s3-secret-access-key"),
			}
			verifyCredentialsForDeployments(t, helmOpts, expectedEnvVarKeys, []string{"airbyte-server", "airbyte-worker", "airbyte-workload-launcher"})
		})
	})

	t.Run("authentication type: instanceProfile", func(t *testing.T) {
		helmOpts := helmtests.BaseHelmOptionsForStorageType("s3")
		helmOpts.SetValues["global.storage.s3.authenticationType"] = "instanceProfile"

		// The AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must not be set
		expectedEnvVarKeys := map[string]helmtests.ExpectedEnvVar{
			"AWS_ACCESS_KEY_ID":     helmtests.ExpectedSecretVar().RefName("user-defined-secret").RefKey("s3-access-key-id"),
			"AWS_SECRET_ACCESS_KEY": helmtests.ExpectedSecretVar().RefName("user-defined-secret").RefKey("s3-secret-access-key"),
		}

		chartYaml, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		assert.NoError(t, err)

		cases := []struct {
			Name              string
			UnexpectedEnvVars map[string]helmtests.ExpectedEnvVar
		}{
			{
				Name:              "airbyte-server",
				UnexpectedEnvVars: expectedEnvVarKeys,
			},
			{
				Name:              "airbyte-worker",
				UnexpectedEnvVars: expectedEnvVarKeys,
			},
			{
				Name:              "airbyte-workload-launcher",
				UnexpectedEnvVars: expectedEnvVarKeys,
			},
		}

		for _, c := range cases {
			t.Run(fmt.Sprintf("deployment %s must not contain AWS credential env vars", c.Name), func(t *testing.T) {
				dep := helmtests.GetDeployment(chartYaml, c.Name)

				actualVars := helmtests.EnvVarMap(dep.Spec.Template.Spec.Containers[0].Env)
				for k := range c.UnexpectedEnvVars {
					_, ok := actualVars[k]
					assert.False(t, ok, fmt.Sprintf("`%s` should be declared as an environment variable", k))
				}
			})
		}
	})
}

func TestGCSStorageConfigurationSecrets(t *testing.T) {
	t.Run("should return an error if global.storage.gcs is not set", func(t *testing.T) {
		helmOpts := helmtests.BaseHelmOptionsForStorageType("gcs")
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		assert.ErrorContains(t, err, "You must set 'global.storage.gcs'")
	})

	t.Run("should return an error if global.storage.gcs.credentialsJson is not set and default secret is used", func(t *testing.T) {
		helmOpts := helmtests.BaseHelmOptionsForStorageType("gcs")
		helmOpts.SetValues["global.storage.gcs.someKey"] = "dummy-value"
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		assert.ErrorContains(t, err, "You must set 'global.storage.gcs.credentialsJson'")
	})

	t.Run("should not create gcs-log-creds secret if `storageSecretName` is set", func(t *testing.T) {
		helmOpts := helmtests.BaseHelmOptionsForStorageType("gcs")
		helmOpts.SetValues["global.storage.gcs.projectId"] = "project-id"
		helmOpts.SetValues["global.storage.storageSecretName"] = "airbyte-config-secrets"

		expectedVolumeMounts := []helmtests.ExpectedVolumeMount{
			helmtests.ExpectedSecretVolumeMount().
				RefName("airbyte-config-secrets").
				Volume("gcs-log-creds-volume").
				MountPath("/secrets/gcs-log-creds"),
		}
		verifyVolumeMountsForDeployments(t, helmOpts, expectedVolumeMounts, []string{"airbyte-server", "airbyte-worker", "airbyte-workload-launcher"})
	})

	t.Run("should mount credentials from a secret", func(t *testing.T) {
		t.Run("using the default secret", func(t *testing.T) {
			helmOpts := helmtests.BaseHelmOptionsForStorageType("gcs")
			helmOpts.SetValues["global.storage.gcs.credentialsJson"] = "dummy-value"
			expectedVolumeMounts := []helmtests.ExpectedVolumeMount{
				helmtests.ExpectedSecretVolumeMount().
					RefName("airbyte-gcs-log-creds").
					Volume("gcs-log-creds-volume").
					MountPath("/secrets/gcs-log-creds"),
			}
			verifyVolumeMountsForDeployments(t, helmOpts, expectedVolumeMounts, []string{"airbyte-server", "airbyte-worker", "airbyte-workload-launcher"})
		})

		t.Run("using a user-defined secret", func(t *testing.T) {
			helmOpts := helmtests.BaseHelmOptionsForStorageType("gcs")
			helmOpts.SetValues["global.storage.gcs.credentialsJson"] = "dummy-value"
			helmOpts.SetValues["global.storage.storageSecretName"] = "customer-secret"
			expectedVolumeMounts := []helmtests.ExpectedVolumeMount{
				helmtests.ExpectedSecretVolumeMount().
					RefName("customer-secret").
					Volume("gcs-log-creds-volume").
					MountPath("/secrets/gcs-log-creds"),
			}
			verifyVolumeMountsForDeployments(t, helmOpts, expectedVolumeMounts, []string{"airbyte-server", "airbyte-worker", "airbyte-workload-launcher"})
		})
	})
}
