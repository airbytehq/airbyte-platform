package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func directJobRunnerOpts() map[string]string {
	return map[string]string{"directJobRunner.enabled": "true"}
}

func renderDirectJobRunner(t *testing.T, values map[string]string) helmtests.ChartYaml {
	opts := helmtests.BaseHelmOptions()
	for k, v := range values {
		opts.SetValues[k] = v
	}
	chartYaml, err := helmtests.RenderHelmChart(t, opts, chartPath, "airbyte", nil)
	require.NoError(t, err)
	return chartYaml
}

func directJobRunnerEnv(t *testing.T, chartYaml helmtests.ChartYaml) map[string]corev1.EnvVar {
	deployment := helmtests.GetDeployment(chartYaml.String(), "airbyte-direct-job-runner")
	require.NotNil(t, deployment)
	return helmtests.EnvVarMap(deployment.Spec.Template.Spec.Containers[0].Env)
}

func TestDirectJobRunnerDisabledByDefault(t *testing.T) {
	chartYaml := renderDirectJobRunner(t, nil)

	for _, kind := range []string{"Deployment", "HorizontalPodAutoscaler", "PodDisruptionBudget"} {
		helmtests.AssertNoResource(t, chartYaml.String(), kind, "airbyte-direct-job-runner")
	}
	helmtests.AssertNoResource(t, chartYaml.String(), "Service", "airbyte-airbyte-direct-job-runner-svc")

	configMap := helmtests.GetConfigMap(chartYaml.String(), "airbyte-airbyte-env")
	require.NotNil(t, configMap)
	assert.Equal(t, "", configMap.Data["DIRECT_JOB_RUNNER_URL"])
}

func TestDirectJobRunnerEnabled(t *testing.T) {
	chartYaml := renderDirectJobRunner(t, directJobRunnerOpts())

	for _, kind := range []string{"Deployment", "HorizontalPodAutoscaler", "PodDisruptionBudget"} {
		assert.NotNil(t, chartYaml.GetResourceByKindAndName(kind, "airbyte-direct-job-runner"), kind)
	}
	assert.NotNil(t, helmtests.GetService(chartYaml.String(), "airbyte-airbyte-direct-job-runner-svc"))

	configMap := helmtests.GetConfigMap(chartYaml.String(), "airbyte-airbyte-env")
	require.NotNil(t, configMap)
	assert.Equal(t, "http://airbyte-airbyte-direct-job-runner-svc.ab.svc:8080", configMap.Data["DIRECT_JOB_RUNNER_URL"])
}

func TestDirectJobRunnerRunsInJobsNamespace(t *testing.T) {
	values := directJobRunnerOpts()
	values["global.jobs.kube.namespace"] = "jobs"
	chartYaml := renderDirectJobRunner(t, values)

	for _, resource := range []struct{ kind, name string }{
		{"Deployment", "airbyte-direct-job-runner"},
		{"Service", "airbyte-airbyte-direct-job-runner-svc"},
		{"HorizontalPodAutoscaler", "airbyte-direct-job-runner"},
		{"PodDisruptionBudget", "airbyte-direct-job-runner"},
	} {
		object := chartYaml.GetResourceByKindAndName(resource.kind, resource.name)
		require.NotNil(t, object, "%s %s was not rendered", resource.kind, resource.name)
		assert.Equal(t, "jobs", object.(interface{ GetNamespace() string }).GetNamespace())
	}

	configMap := helmtests.GetConfigMap(chartYaml.String(), "airbyte-airbyte-env")
	require.NotNil(t, configMap)
	assert.Equal(t, "http://airbyte-airbyte-direct-job-runner-svc.jobs.svc:8080", configMap.Data["DIRECT_JOB_RUNNER_URL"])
}

func TestDirectJobRunnerDoesNotReferenceTheEnvConfigMap(t *testing.T) {
	values := directJobRunnerOpts()
	values["global.jobs.kube.namespace"] = "jobs"
	chartYaml := renderDirectJobRunner(t, values)

	env := directJobRunnerEnv(t, chartYaml)
	for name, v := range env {
		if v.ValueFrom != nil {
			assert.Nil(t, v.ValueFrom.ConfigMapKeyRef, "%s reads the airbyte-namespace ConfigMap", name)
		}
	}
	assert.Equal(t, "minio", env["STORAGE_TYPE"].Value)
	assert.Equal(t, "http://airbyte-airbyte-server-svc.ab:8001", env["INTERNAL_API_HOST"].Value)
	require.NotNil(t, env["STATSD_HOST"].ValueFrom)
	assert.Equal(t, "status.hostIP", env["STATSD_HOST"].ValueFrom.FieldRef.FieldPath)
	_, hasDummySecret := env["AIRBYTE_DUMMY_SECRET"]
	assert.False(t, hasDummySecret)
}

func TestDirectJobRunnerConnectSettings(t *testing.T) {
	t.Run("defaults to no blocking", func(t *testing.T) {
		env := directJobRunnerEnv(t, renderDirectJobRunner(t, directJobRunnerOpts()))

		assert.Equal(t, "false", env["AIRBYTE_DIRECT_JOB_RUNNER_CONNECT_BLOCK_INTERNAL_ADDRESSES"].Value)
		assert.Equal(t, "", env["AIRBYTE_DIRECT_JOB_RUNNER_CONNECT_BLOCKED_CIDRS"].Value)
		assert.Equal(t, "50", env["AIRBYTE_DIRECT_JOB_RUNNER_MAX_CONCURRENT"].Value)
	})

	t.Run("passes blocking settings through", func(t *testing.T) {
		values := directJobRunnerOpts()
		values["directJobRunner.connect.blockInternalAddresses"] = "true"
		values["directJobRunner.connect.blockedCidrs[0]"] = "172.32.0.0/16"
		values["directJobRunner.connect.blockedCidrs[1]"] = "172.33.0.0/16"
		values["directJobRunner.maxConcurrent"] = "20"
		env := directJobRunnerEnv(t, renderDirectJobRunner(t, values))

		assert.Equal(t, "true", env["AIRBYTE_DIRECT_JOB_RUNNER_CONNECT_BLOCK_INTERNAL_ADDRESSES"].Value)
		assert.Equal(t, "172.32.0.0/16,172.33.0.0/16", env["AIRBYTE_DIRECT_JOB_RUNNER_CONNECT_BLOCKED_CIDRS"].Value)
		assert.Equal(t, "20", env["AIRBYTE_DIRECT_JOB_RUNNER_MAX_CONCURRENT"].Value)
	})
}

func TestDirectJobRunnerStorageSecretName(t *testing.T) {
	t.Run("uses the global storage secret by default", func(t *testing.T) {
		env := directJobRunnerEnv(t, renderDirectJobRunner(t, directJobRunnerOpts()))

		require.NotNil(t, env["AWS_ACCESS_KEY_ID"].ValueFrom)
		assert.Equal(t, "airbyte-airbyte-secrets", env["AWS_ACCESS_KEY_ID"].ValueFrom.SecretKeyRef.Name)
	})

	t.Run("overrides the storage secret", func(t *testing.T) {
		values := directJobRunnerOpts()
		values["directJobRunner.storageSecretName"] = "workload-launcher-secrets"
		env := directJobRunnerEnv(t, renderDirectJobRunner(t, values))

		for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"} {
			require.NotNil(t, env[name].ValueFrom, name)
			assert.Equal(t, "workload-launcher-secrets", env[name].ValueFrom.SecretKeyRef.Name, name)
		}
	})
}

func TestDirectJobRunnerScheduling(t *testing.T) {
	values := directJobRunnerOpts()
	values["directJobRunner.nodeSelector.airbyte/node-pool"] = "direct-jobs"
	values["directJobRunner.tolerations[0].key"] = "airbyte/node-pool"
	values["directJobRunner.tolerations[0].operator"] = "Equal"
	values["directJobRunner.tolerations[0].value"] = "direct-jobs"
	values["directJobRunner.tolerations[0].effect"] = "NoSchedule"
	chartYaml := renderDirectJobRunner(t, values)

	deployment := helmtests.GetDeployment(chartYaml.String(), "airbyte-direct-job-runner")
	require.NotNil(t, deployment)
	spec := deployment.Spec.Template.Spec
	assert.Equal(t, map[string]string{"airbyte/node-pool": "direct-jobs"}, spec.NodeSelector)
	require.Len(t, spec.Tolerations, 1)
	assert.Equal(t, corev1.Toleration{Key: "airbyte/node-pool", Operator: "Equal", Value: "direct-jobs", Effect: "NoSchedule"}, spec.Tolerations[0])
	// Replicas are owned by the HPA when autoscaling is on.
	assert.Nil(t, deployment.Spec.Replicas)
}

func TestDirectJobRunnerWithoutAutoscaling(t *testing.T) {
	values := directJobRunnerOpts()
	values["directJobRunner.autoscaling.enabled"] = "false"
	chartYaml := renderDirectJobRunner(t, values)

	helmtests.AssertNoResource(t, chartYaml.String(), "HorizontalPodAutoscaler", "airbyte-direct-job-runner")
	deployment := helmtests.GetDeployment(chartYaml.String(), "airbyte-direct-job-runner")
	require.NotNil(t, deployment)
	require.NotNil(t, deployment.Spec.Replicas)
	assert.Equal(t, int32(2), *deployment.Spec.Replicas)
}

func TestDirectJobRunnerServiceAccount(t *testing.T) {
	t.Run("creates a token-less service account by default", func(t *testing.T) {
		values := directJobRunnerOpts()
		values["global.jobs.kube.namespace"] = "jobs"
		chartYaml := renderDirectJobRunner(t, values)

		sa := helmtests.GetServiceAccount(chartYaml.String(), "airbyte-direct-job-runner")
		require.NotNil(t, sa)
		assert.Equal(t, "jobs", sa.Namespace)
		require.NotNil(t, sa.AutomountServiceAccountToken)
		assert.False(t, *sa.AutomountServiceAccountToken)

		deployment := helmtests.GetDeployment(chartYaml.String(), "airbyte-direct-job-runner")
		require.NotNil(t, deployment)
		assert.Equal(t, "airbyte-direct-job-runner", deployment.Spec.Template.Spec.ServiceAccountName)
		require.NotNil(t, deployment.Spec.Template.Spec.AutomountServiceAccountToken)
		assert.False(t, *deployment.Spec.Template.Spec.AutomountServiceAccountToken)
	})

	t.Run("uses an existing service account without creating one", func(t *testing.T) {
		values := directJobRunnerOpts()
		values["directJobRunner.serviceAccountName"] = "existing-sa"
		chartYaml := renderDirectJobRunner(t, values)

		helmtests.AssertNoResource(t, chartYaml.String(), "ServiceAccount", "airbyte-direct-job-runner")
		deployment := helmtests.GetDeployment(chartYaml.String(), "airbyte-direct-job-runner")
		require.NotNil(t, deployment)
		assert.Equal(t, "existing-sa", deployment.Spec.Template.Spec.ServiceAccountName)
	})

	t.Run("creates nothing when disabled", func(t *testing.T) {
		chartYaml := renderDirectJobRunner(t, nil)

		helmtests.AssertNoResource(t, chartYaml.String(), "ServiceAccount", "airbyte-direct-job-runner")
	})
}

// Every Cloud data plane renders with cluster.type data-plane and GCS storage.
func TestDirectJobRunnerDataPlaneGcsVolumes(t *testing.T) {
	dataPlaneGcs := func() map[string]string {
		values := directJobRunnerOpts()
		values["global.cluster.type"] = "data-plane"
		values["global.storage.type"] = "gcs"
		values["global.storage.secretName"] = "gsm-creds"
		values["global.storage.gcs.credentialsJsonSecretKey"] = "gcp.json"
		values["global.workloads.containerOrchestrator.dataPlane.secretName"] = "dataplane-creds"
		return values
	}
	volumeSecrets := func(t *testing.T, values map[string]string) (map[string]string, map[string]string) {
		deployment := helmtests.GetDeployment(renderDirectJobRunner(t, values).String(), "airbyte-direct-job-runner")
		require.NotNil(t, deployment)
		volumes := map[string]string{}
		for _, v := range deployment.Spec.Template.Spec.Volumes {
			require.NotNil(t, v.Secret, v.Name)
			volumes[v.Name] = v.Secret.SecretName
		}
		mounts := map[string]string{}
		for _, m := range deployment.Spec.Template.Spec.Containers[0].VolumeMounts {
			mounts[m.Name] = m.MountPath
		}
		return volumes, mounts
	}

	t.Run("mounts the GCS and data plane credentials", func(t *testing.T) {
		volumes, mounts := volumeSecrets(t, dataPlaneGcs())

		assert.Equal(t, map[string]string{"gcs-log-creds-volume": "gsm-creds", "dataplane-creds-volume": "dataplane-creds"}, volumes)
		assert.Contains(t, mounts, "gcs-log-creds-volume")
		assert.Contains(t, mounts, "dataplane-creds-volume")
	})

	t.Run("reads the GCS credentials from storageSecretName", func(t *testing.T) {
		values := dataPlaneGcs()
		values["directJobRunner.storageSecretName"] = "jobs-gcs-creds"
		volumes, _ := volumeSecrets(t, values)

		assert.Equal(t, "jobs-gcs-creds", volumes["gcs-log-creds-volume"])
		assert.Equal(t, "dataplane-creds", volumes["dataplane-creds-volume"])
	})
}
