package tests

import (
	"encoding/json"
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBasicTopologyConfiguration(t *testing.T) {
	// use enterprise as a base because it enables more things by default.
	opts := helmtests.BaseHelmOptionsForEnterpriseWithValues()
	opts.SetValues["metrics.enabled"] = "true"

	expectSelector := map[string]string{
		"machineSize": "xlarge",
		"region":      "us-west-2",
	}

	expectTolerations := []corev1.Toleration{
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

	expectAffinity := &corev1.Affinity{
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

	affinityData, err := json.Marshal(expectAffinity)
	if err != nil {
		t.Fatal(err)
	}

	for _, app := range helmtests.AllApps {
		t.Run(app, func(t *testing.T) {
			helmtests.SetAppOpt(opts, app, "nodeSelector.machineSize", "xlarge")
			helmtests.SetAppOpt(opts, app, "nodeSelector.region", "us-west-2")

			helmtests.SetAppOpt(opts, app, "tolerations[0].key", "key1")
			helmtests.SetAppOpt(opts, app, "tolerations[0].operator", "Equal")
			helmtests.SetAppOpt(opts, app, "tolerations[0].value", "value1")
			helmtests.SetAppOpt(opts, app, "tolerations[0].effect", "NoSchedule")

			helmtests.SetAppOpt(opts, app, "tolerations[1].key", "key2")
			helmtests.SetAppOpt(opts, app, "tolerations[1].operator", "Equal")
			helmtests.SetAppOpt(opts, app, "tolerations[1].value", "value2")
			helmtests.SetAppOpt(opts, app, "tolerations[1].effect", "NoSchedule")

			opts.SetJsonValues[app+".affinity"] = string(affinityData)

			chartYaml := helmtests.RenderChart(t, opts, chartPath)
			spec := appPodSpec(chartYaml, app)
			assert.Equal(t, expectSelector, spec.NodeSelector)
			assert.Equal(t, expectTolerations, spec.Tolerations)
			assert.Equal(t, expectAffinity, spec.Affinity)
		})
	}
}

func appPodSpec(chartYaml, appName string) corev1.PodSpec {

	// most resources follow this naming pattern
	resourceName := "airbyte-" + appName

	switch appName {
	case "airbyte-bootloader":
		return helmtests.GetPod(chartYaml, resourceName).Spec
	case "keycloak-setup":
		return helmtests.GetJob(chartYaml, resourceName).Spec.Template.Spec
	case "keycloak", "db":
		return helmtests.GetStatefulSet(chartYaml, resourceName).Spec.Template.Spec
	default:
		return helmtests.GetDeployment(chartYaml, resourceName).Spec.Template.Spec
	}
}
