//go:build template || storage_config

package test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gruntwork-io/terratest/modules/helm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	defaultStorageSecretName = "airbyte-config-secrets"
)

func TestBasicStorageConfiguration(t *testing.T) {
	helmOpts := baseHelmOptions()

	t.Run("configmap and secret keys", func(t *testing.T) {
		type ExpectedSecret struct {
			Template             string
			Name                 string
			ExpectedSecretValues map[string][]byte
		}

		cases := []struct {
			Type                    string
			SetValues               map[string]string
			ExpectedConfigMapValues map[string]string
			ExpectedSecret          *ExpectedSecret
		}{

			{
				Type:      "minio",
				SetValues: map[string]string{},
				ExpectedConfigMapValues: map[string]string{
					"LOG4J_CONFIGURATION_FILE":        "log4j2-minio.xml",
					"MINIO_ENDPOINT":                  "http://airbyte-minio-svc:9000",
					"S3_PATH_STYLE_ACCESS":            "true",
					"STORAGE_BUCKET_ACTIVITY_PAYLOAD": "airbyte-storage",
					"STORAGE_BUCKET_LOG":              "airbyte-storage",
					"STORAGE_BUCKET_STATE":            "airbyte-storage",
					"STORAGE_BUCKET_WORKLOAD_OUTPUT":  "airbyte-storage",
					"STORAGE_TYPE":                    "minio",
				},
				ExpectedSecret: nil,
			},
			{
				Type: "gcs",
				SetValues: map[string]string{
					// Base64 encoded `{"fake": "fake"}`
					"global.storage.gcs.credentialsJson": "eyJmYWtlIjogImZha2UifQ==",
				},
				ExpectedConfigMapValues: map[string]string{
					"GOOGLE_APPLICATION_CREDENTIALS":  "/secrets/gcs-log-creds/gcp.json",
					"LOG4J_CONFIGURATION_FILE":        "log4j2-gcs.xml",
					"S3_PATH_STYLE_ACCESS":            "",
					"STORAGE_BUCKET_ACTIVITY_PAYLOAD": "airbyte-storage",
					"STORAGE_BUCKET_LOG":              "airbyte-storage",
					"STORAGE_BUCKET_STATE":            "airbyte-storage",
					"STORAGE_BUCKET_WORKLOAD_OUTPUT":  "airbyte-storage",
					"STORAGE_TYPE":                    "gcs",
				},
				ExpectedSecret: &ExpectedSecret{
					Template: "templates/gcs-log-creds-secret.yaml",
					Name:     "airbyte-gcs-log-creds",
					ExpectedSecretValues: map[string][]byte{
						"gcp.json": []byte(`{"fake": "fake"}`),
					},
				},
			},
			{
				Type:      "s3",
				SetValues: map[string]string{},
				ExpectedConfigMapValues: map[string]string{
					"AWS_DEFAULT_REGION":              "",
					"LOG4J_CONFIGURATION_FILE":        "log4j2-s3.xml",
					"S3_PATH_STYLE_ACCESS":            "",
					"STORAGE_BUCKET_ACTIVITY_PAYLOAD": "airbyte-storage",
					"STORAGE_BUCKET_LOG":              "airbyte-storage",
					"STORAGE_BUCKET_STATE":            "airbyte-storage",
					"STORAGE_BUCKET_WORKLOAD_OUTPUT":  "airbyte-storage",
					"STORAGE_TYPE":                    "s3",
				},
			},
		}

		for _, c := range cases {
			t.Run(fmt.Sprintf("storage type %s", c.Type), func(t *testing.T) {
				helmOpts.SetValues["global.storage.type"] = c.Type
				for k, v := range c.SetValues {
					helmOpts.SetValues[k] = v
				}

				t.Run("verify config map values", func(t *testing.T) {
					configMapYaml, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", []string{"templates/env-configmap.yaml"})
					require.NoError(t, err, "failure rendering template")

					configMap, err := getConfigMap(configMapYaml, "airbyte-airbyte-env")
					assert.NotNil(t, configMap)
					require.NoError(t, err)

					// verify expected keys in the config map
					for k, v := range c.ExpectedConfigMapValues {
						assert.Equal(t, v, configMap.Data[k])
					}
				})

				t.Run("verify secret values", func(t *testing.T) {
					if c.ExpectedSecret != nil {
						secretYaml, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", []string{c.ExpectedSecret.Template})
						require.NoError(t, err, "failure rendering template")

						secret, err := getSecret(secretYaml, c.ExpectedSecret.Name)
						assert.NotNil(t, secret)
						require.NoError(t, err)

						for k, v := range c.ExpectedSecret.ExpectedSecretValues {
							assert.Equal(t, v, secret.Data[k])
						}
					}
				})
			})
		}
	})
}

func verifyCredentialsForDeployments(t *testing.T, helmOpts *helm.Options, expectedEnvVars map[string]expectedEnvVar, deployments []string) {
	chartYaml, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	type testCase struct {
		DeploymentName  string
		ExpectedEnvVars map[string]expectedEnvVar
	}

	var cases []testCase

	for _, d := range deployments {
		cases = append(cases, testCase{DeploymentName: d, ExpectedEnvVars: expectedEnvVars})
	}

	storageType := helmOpts.SetValues["global.storage.type"]
	for _, c := range cases {
		t.Run(fmt.Sprintf("deployment %s contains expected %s credentials env vars", c.DeploymentName, strings.ToUpper(storageType)), func(t *testing.T) {
			dep, err := getDeployment(chartYaml, c.DeploymentName)
			assert.NotNil(t, dep)
			assert.NoError(t, err)

			actualVars := envVarMap(dep.Spec.Template.Spec.Containers[0].Env)
			for k, expected := range c.ExpectedEnvVars {
				actual, ok := actualVars[k]
				assert.True(t, ok, fmt.Sprintf("`%s` should be declared as an environment variable", k))
				verifyEnvVar(t, expected, actual)
			}
		})
	}
}

func verifyVolumeMountsForDeployments(t *testing.T, helmOpts *helm.Options, expectedVolumeMounts []expectedVolumeMount, deployments []string) {
	chartYaml, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
	assert.NoError(t, err)

	type testCase struct {
		DeploymentName       string
		ExpectedVolumeMounts []expectedVolumeMount
	}

	var cases []testCase

	for _, d := range deployments {
		cases = append(cases, testCase{DeploymentName: d, ExpectedVolumeMounts: expectedVolumeMounts})
	}

	storageType := helmOpts.SetValues["global.storage.type"]
	for _, c := range cases {
		t.Run(fmt.Sprintf("deployment %s contains expected %s volume mounts", c.DeploymentName, strings.ToUpper(storageType)), func(t *testing.T) {
			dep, err := getDeployment(chartYaml, c.DeploymentName)
			assert.NotNil(t, dep)
			assert.NoError(t, err)

			for _, expected := range c.ExpectedVolumeMounts {
				verifyVolumeMountForPod(t, expected, dep.Spec.Template.Spec)
			}
		})
	}
}

func TestS3StorageConfigurationSecrets(t *testing.T) {
	t.Run("authentication type: credentials", func(t *testing.T) {
		t.Run("default storageSecretName", func(t *testing.T) {
			helmOpts := baseHelmOptionsForStorageType("s3")
			helmOpts.SetValues["global.storage.s3.authenticationType"] = "credentials"

			expectedEnvVarKeys := map[string]expectedEnvVar{
				"AWS_ACCESS_KEY_ID":     expectedSecretVar().RefName(defaultStorageSecretName).RefKey("s3-access-key-id"),
				"AWS_SECRET_ACCESS_KEY": expectedSecretVar().RefName(defaultStorageSecretName).RefKey("s3-secret-access-key"),
			}
			verifyCredentialsForDeployments(t, helmOpts, expectedEnvVarKeys, []string{"airbyte-server", "airbyte-worker", "airbyte-workload-launcher"})
		})

		t.Run("user-defined storageSecretName", func(t *testing.T) {
			helmOpts := baseHelmOptionsForStorageType("s3")
			helmOpts.SetValues["global.storage.s3.authenticationType"] = "credentials"
			helmOpts.SetValues["global.storage.storageSecretName"] = "user-defined-secret"

			expectedEnvVarKeys := map[string]expectedEnvVar{
				"AWS_ACCESS_KEY_ID":     expectedSecretVar().RefName("user-defined-secret").RefKey("s3-access-key-id"),
				"AWS_SECRET_ACCESS_KEY": expectedSecretVar().RefName("user-defined-secret").RefKey("s3-secret-access-key"),
			}
			verifyCredentialsForDeployments(t, helmOpts, expectedEnvVarKeys, []string{"airbyte-server", "airbyte-worker", "airbyte-workload-launcher"})
		})
	})

	t.Run("authentication type: instanceProfile", func(t *testing.T) {
		helmOpts := baseHelmOptionsForStorageType("s3")
		helmOpts.SetValues["global.storage.s3.authenticationType"] = "instanceProfile"

		// The AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must not be set
		expectedEnvVarKeys := map[string]expectedEnvVar{
			"AWS_ACCESS_KEY_ID":     expectedSecretVar().RefName("user-defined-secret").RefKey("s3-access-key-id"),
			"AWS_SECRET_ACCESS_KEY": expectedSecretVar().RefName("user-defined-secret").RefKey("s3-secret-access-key"),
		}

		chartYaml, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		assert.NoError(t, err)

		cases := []struct {
			Name              string
			UnexpectedEnvVars map[string]expectedEnvVar
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
				dep, err := getDeployment(chartYaml, c.Name)
				assert.NotNil(t, dep)
				assert.NoError(t, err)

				actualVars := envVarMap(dep.Spec.Template.Spec.Containers[0].Env)
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
		helmOpts := baseHelmOptionsForStorageType("gcs")
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		assert.ErrorContains(t, err, "You must set 'global.storage.gcs'")
	})

	t.Run("should return an error if global.storage.gcs.credentialsJson is not set", func(t *testing.T) {
		helmOpts := baseHelmOptionsForStorageType("gcs")
		helmOpts.SetValues["global.storage.gcs.someKey"] = "dummy-value"
		_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
		assert.ErrorContains(t, err, "You must set 'global.storage.gcs.credentialsJson'")
	})

	t.Run("should mount credentials from a secret", func(t *testing.T) {
		t.Run("using the default secret", func(t *testing.T) {
			helmOpts := baseHelmOptionsForStorageType("gcs")
			helmOpts.SetValues["global.storage.gcs.credentialsJson"] = "dummy-value"
			expectedVolumeMounts := []expectedVolumeMount{
				expectedSecretVolumeMount().
					RefName("airbyte-gcs-log-creds").
					Volume("gcs-log-creds-volume").
					MountPath("/secrets/gcs-log-creds"),
			}
			verifyVolumeMountsForDeployments(t, helmOpts, expectedVolumeMounts, []string{"airbyte-server", "airbyte-worker", "airbyte-workload-launcher"})
		})

		t.Run("using a user-defined secret", func(t *testing.T) {
			helmOpts := baseHelmOptionsForStorageType("gcs")
			helmOpts.SetValues["global.storage.gcs.credentialsJson"] = "dummy-value"
			helmOpts.SetValues["global.storage.storageSecretName"] = "customer-secret"
			expectedVolumeMounts := []expectedVolumeMount{
				expectedSecretVolumeMount().
					RefName("customer-secret").
					Volume("gcs-log-creds-volume").
					MountPath("/secrets/gcs-log-creds"),
			}
			verifyVolumeMountsForDeployments(t, helmOpts, expectedVolumeMounts, []string{"airbyte-server", "airbyte-worker", "airbyte-workload-launcher"})
		})
	})
}
