package test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/gruntwork-io/terratest/modules/helm"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBasicTopologyConfiguration(t *testing.T) {
	cases := []struct {
		kind       string
		name       string
		valuesPath string
	}{
		{
			kind:       "Deployment",
			name:       "airbyte-server",
			valuesPath: "server",
		},
		{
			kind:       "Deployment",
			name:       "airbyte-webapp",
			valuesPath: "webapp",
		},
		{
			kind:       "Deployment",
			name:       "airbyte-connector-builder-server",
			valuesPath: "connector-builder-server",
		},
		{
			kind:       "Deployment",
			name:       "airbyte-worker",
			valuesPath: "worker",
		},
		{
			kind:       "Deployment",
			name:       "airbyte-workload-api-server",
			valuesPath: "workload-api-server",
		},
		{
			kind:       "Deployment",
			name:       "airbyte-workload-launcher",
			valuesPath: "workload-launcher",
		},
		{
			kind:       "Deployment",
			name:       "airbyte-airbyte-api-server",
			valuesPath: "airbyte-api-server",
		},
		{
			kind:       "Deployment",
			name:       "airbyte-cron",
			valuesPath: "cron",
		},
		{
			kind:       "StatefulSet",
			name:       "airbyte-keycloak",
			valuesPath: "keycloak",
		},
		{
			kind:       "Job",
			name:       "airbyte-keycloak-setup",
			valuesPath: "keycloak-setup",
		},
		{
			kind:       "Pod",
			name:       "airbyte-airbyte-bootloader",
			valuesPath: "airbyte-bootloader",
		},
		{
			kind:       "Deployment",
			name:       "airbyte-metrics",
			valuesPath: "metrics",
		},
		{
			kind:       "Deployment",
			name:       "airbyte-temporal",
			valuesPath: "temporal",
		},
		{
			kind:       "Deployment",
			name:       "airbyte-pod-sweeper-pod-sweeper",
			valuesPath: "pod-sweeper",
		},
	}
	t.Run("configure nodeSelector for workloads", func(t *testing.T) {
		nodeSelector := map[string]string{
			"machineSize": "xlarge",
			"region":      "us-west-2",
		}

		for _, c := range cases {
			t.Run(fmt.Sprintf("verify nodeSelectors are set for %s: %s", c.kind, c.name), func(t *testing.T) {
				helmOpts := baseHelmOptions()
				helmOpts.SetValues["global.edition"] = "enterprise" // enables keycloak, etc
				helmOpts.SetValues["metrics.enabled"] = "true"
				helmOpts.SetValues["workload-api-server.enabled"] = "true"
				helmOpts.SetValues["workload-api-server.enabled"] = "true"
				helmOpts.SetValues["workload-launcher.enabled"] = "true"
				for k, v := range nodeSelector {
					helmOpts.SetValues[c.valuesPath+".nodeSelector."+k] = v
				}

				chartYaml, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
				assert.NotEmpty(t, chartYaml)
				assert.NoError(t, err)

				switch c.kind {
				case "Pod":
					pod, err := getPod(chartYaml, c.name)
					assert.NotNil(t, pod)
					assert.NoError(t, err)

					assert.Equal(t, pod.Name, c.name)
					assert.Equal(t, pod.Spec.NodeSelector, nodeSelector)
				case "Job":
					job, err := getJob(chartYaml, c.name)
					assert.NotNil(t, job)
					assert.NoError(t, err)

					assert.Equal(t, job.Name, c.name)
					assert.Equal(t, job.Spec.Template.Spec.NodeSelector, nodeSelector)
				case "Deployment":
					dep, err := getDeployment(chartYaml, c.name)
					assert.NotNil(t, dep)
					assert.NoError(t, err)

					assert.Equal(t, dep.Name, c.name)
					assert.Equal(t, dep.Spec.Template.Spec.NodeSelector, nodeSelector)
				case "StatefulSet":
					ss, err := getStatefulSet(chartYaml, c.name)
					assert.NotNil(t, ss)
					assert.NoError(t, err)

					assert.Equal(t, ss.Name, c.name)
					assert.Equal(t, ss.Spec.Template.Spec.NodeSelector, nodeSelector)

				default:
					t.Fatalf("unsupported resource kind: %s", c.kind)
				}
			})
		}
	})

	t.Run("configure tolerations for workloads", func(t *testing.T) {
		tolerations := []corev1.Toleration{
			{
				Key:      "key1",
				Operator: "Equal",
				Value:    "value1",
				Effect:   "NoSchedule",
			},
			{
				Key:      "key2",
				Operator: "Equal",
				Value:    "value2",
				Effect:   "NoSchedule",
			},
		}

		for _, c := range cases {
			t.Run(fmt.Sprintf("verify tolerations are set for %s: %s", c.kind, c.name), func(t *testing.T) {
				helmOpts := baseHelmOptions()
				helmOpts.SetValues["global.edition"] = "enterprise" // enables keycloak, etc
				helmOpts.SetValues["metrics.enabled"] = "true"
				helmOpts.SetValues["workload-api-server.enabled"] = "true"
				helmOpts.SetValues["workload-api-server.enabled"] = "true"
				helmOpts.SetValues["workload-launcher.enabled"] = "true"
				for i, tol := range tolerations {
					helmOpts.SetValues[c.valuesPath+fmt.Sprintf(".tolerations[%d]", i)+".Key"] = tol.Key
					helmOpts.SetValues[c.valuesPath+fmt.Sprintf(".tolerations[%d]", i)+".Operator"] = string(tol.Operator)
					helmOpts.SetValues[c.valuesPath+fmt.Sprintf(".tolerations[%d]", i)+".Value"] = tol.Value
					helmOpts.SetValues[c.valuesPath+fmt.Sprintf(".tolerations[%d]", i)+".Effect"] = string(tol.Effect)
				}

				chartYaml, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
				assert.NotEmpty(t, chartYaml)
				assert.NoError(t, err)

				switch c.kind {
				case "Pod":
					pod, err := getPod(chartYaml, c.name)
					assert.NotNil(t, pod)
					assert.NoError(t, err)

					assert.Equal(t, pod.Name, c.name)
					assert.Equal(t, pod.Spec.Tolerations, tolerations)
				case "Job":
					job, err := getJob(chartYaml, c.name)
					assert.NotNil(t, job)
					assert.NoError(t, err)

					assert.Equal(t, job.Name, c.name)
					assert.Equal(t, job.Spec.Template.Spec.Tolerations, tolerations)
				case "Deployment":
					dep, err := getDeployment(chartYaml, c.name)
					assert.NotNil(t, dep)
					assert.NoError(t, err)

					assert.Equal(t, dep.Name, c.name)
					assert.Equal(t, dep.Spec.Template.Spec.Tolerations, tolerations)
				case "StatefulSet":
					ss, err := getStatefulSet(chartYaml, c.name)
					assert.NotNil(t, ss)
					assert.NoError(t, err)

					assert.Equal(t, ss.Name, c.name)
					assert.Equal(t, ss.Spec.Template.Spec.Tolerations, tolerations)

				default:
					t.Fatalf("unsupported resource kind: %s", c.kind)
				}
			})
		}
	})

	t.Run("configure affinity for workloads", func(t *testing.T) {
		affinity := &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchFields: []corev1.NodeSelectorRequirement{
								{
									Key:      "node-size",
									Operator: "Equal",
									Values:   []string{"xlarge"},
								},
							},
						},
					},
				},
			},
			PodAffinity: &corev1.PodAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
					{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"color": "purple",
							},
						},
					},
				},
			},
		}

		for _, c := range cases {
			t.Run(fmt.Sprintf("verify affinities are set for %s: %s", c.kind, c.name), func(t *testing.T) {
				helmOpts := baseHelmOptions()
				helmOpts.SetValues["global.edition"] = "enterprise" // enables keycloak, etc
				helmOpts.SetValues["metrics.enabled"] = "true"
				helmOpts.SetValues["workload-api-server.enabled"] = "true"
				helmOpts.SetValues["workload-api-server.enabled"] = "true"
				helmOpts.SetValues["workload-launcher.enabled"] = "true"

				data, err := json.Marshal(affinity)
				if err != nil {
					t.Error(err)
				}
				helmOpts.SetJsonValues[c.valuesPath+".affinity"] = string(data)

				chartYaml, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
				assert.NotEmpty(t, chartYaml)
				assert.NoError(t, err)

				switch c.kind {
				case "Pod":
					pod, err := getPod(chartYaml, c.name)
					assert.NotNil(t, pod)
					assert.NoError(t, err)

					assert.Equal(t, pod.Name, c.name)
					assert.Equal(t, pod.Spec.Affinity, affinity)
				case "Job":
					job, err := getJob(chartYaml, c.name)
					assert.NotNil(t, job)
					assert.NoError(t, err)

					assert.Equal(t, job.Name, c.name)
					assert.Equal(t, job.Spec.Template.Spec.Affinity, affinity)
				case "Deployment":
					dep, err := getDeployment(chartYaml, c.name)
					assert.NotNil(t, dep)
					assert.NoError(t, err)

					assert.Equal(t, dep.Name, c.name)
					assert.Equal(t, dep.Spec.Template.Spec.Affinity, affinity)
				case "StatefulSet":
					ss, err := getStatefulSet(chartYaml, c.name)
					assert.NotNil(t, ss)
					assert.NoError(t, err)

					assert.Equal(t, ss.Name, c.name)
					assert.Equal(t, ss.Spec.Template.Spec.Affinity, affinity)

				default:
					t.Fatalf("unsupported resource kind: %s", c.kind)
				}
			})
		}
	})
}
