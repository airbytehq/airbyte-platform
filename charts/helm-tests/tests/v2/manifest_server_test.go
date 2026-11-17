package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/gruntwork-io/terratest/modules/helm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManifestServerDisabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["manifestServer.enabled"] = "false"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify manifest-server resources are not created when disabled
	helmtests.AssertNoResource(t, chartYaml.String(), "Deployment", "airbyte-manifest-server")
	helmtests.AssertNoResource(t, chartYaml.String(), "Service", "airbyte-airbyte-manifest-server-svc")
}

func TestManifestServerEnabled(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["manifestServer.enabled"] = "true"
	
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	assert.NoError(t, err)
	
	// Verify manifest-server resources are created when enabled
	assert.NotNil(t, helmtests.GetDeployment(chartYaml.String(), "airbyte-manifest-server"))
	assert.NotNil(t, helmtests.GetService(chartYaml.String(), "airbyte-airbyte-manifest-server-svc"))
}

func renderManifestServer(t *testing.T, values map[string]string) (helmtests.ChartYaml, error) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["manifestServer.enabled"] = "true"
	for k, v := range values {
		opts.SetValues[k] = v
	}
	return helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
}

func TestManifestServerRunsInTheReleaseNamespaceByDefault(t *testing.T) {
	chartYaml, err := renderManifestServer(t, map[string]string{"global.jobs.kube.namespace": "jobs"})
	require.NoError(t, err)

	deployment := helmtests.GetDeployment(chartYaml.String(), "airbyte-manifest-server")
	require.NotNil(t, deployment)
	assert.Equal(t, "ab", deployment.Namespace)
	assert.Equal(t, "ab", helmtests.GetService(chartYaml.String(), "airbyte-airbyte-manifest-server-svc").Namespace)
	container := deployment.Spec.Template.Spec.Containers[0]
	assert.Equal(t, "airbyte-airbyte-telemetry-env", container.EnvFrom[0].ConfigMapRef.Name)

	configMap := helmtests.GetConfigMap(chartYaml.String(), "airbyte-airbyte-env")
	require.NotNil(t, configMap)
	assert.Equal(t, "http://airbyte-airbyte-manifest-server-svc.ab:8080", configMap.Data["MANIFEST_SERVER_API_HOST"])
}

func TestManifestServerRunsInTheJobsNamespace(t *testing.T) {
	chartYaml, err := renderManifestServer(t, map[string]string{
		"manifestServer.runInJobsNamespace":        "true",
		"global.jobs.kube.namespace":               "jobs",
		"directJobRunner.manifestServerSecretName": "workload-launcher-secrets",
	})
	require.NoError(t, err)

	deployment := helmtests.GetDeployment(chartYaml.String(), "airbyte-manifest-server")
	require.NotNil(t, deployment)
	assert.Equal(t, "jobs", deployment.Namespace)
	assert.Equal(t, "jobs", helmtests.GetService(chartYaml.String(), "airbyte-airbyte-manifest-server-svc").Namespace)

	// The env ConfigMaps only exist in the airbyte namespace.
	container := deployment.Spec.Template.Spec.Containers[0]
	assert.Empty(t, container.EnvFrom)
	env := helmtests.EnvVarMap(container.Env)
	for name, v := range env {
		if v.ValueFrom != nil {
			assert.Nil(t, v.ValueFrom.ConfigMapKeyRef, "%s reads an airbyte-namespace ConfigMap", name)
		}
	}
	assert.NotEmpty(t, env["AIRBYTE_EDITION"].Value)
	// The same jobs-namespace secret the runner signs its calls with.
	require.NotNil(t, env["AB_JWT_SIGNATURE_SECRET"].ValueFrom)
	assert.Equal(t, "workload-launcher-secrets", env["AB_JWT_SIGNATURE_SECRET"].ValueFrom.SecretKeyRef.Name)

	configMap := helmtests.GetConfigMap(chartYaml.String(), "airbyte-airbyte-env")
	require.NotNil(t, configMap)
	assert.Equal(t, "http://airbyte-airbyte-manifest-server-svc.jobs:8080", configMap.Data["MANIFEST_SERVER_API_HOST"])
}

func TestManifestServerInTheJobsNamespaceNeedsTheRunnersSecret(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["manifestServer.enabled"] = "true"
	opts.SetValues["manifestServer.runInJobsNamespace"] = "true"
	opts.SetValues["global.jobs.kube.namespace"] = "jobs"

	_, err := helm.RenderTemplateE(t, opts, chartPath, "airbyte", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "directJobRunner.manifestServerSecretName")
}
