package helmtests

import (
	"testing"

	"github.com/gruntwork-io/terratest/modules/helm"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
)

type ChartYaml string

func RenderHelmChart(t *testing.T, helmOpts *helm.Options, chartPathOrUrl string, releaseName string, templateFiles []string, extraHelmArgs ...string) (ChartYaml, error) {
	chartYml, err := helm.RenderTemplateE(t, helmOpts, chartPathOrUrl, releaseName, templateFiles, extraHelmArgs...)
	assert.NoError(t, err)

	return ChartYaml(chartYml), nil
}

func (y ChartYaml) String() string {
	return string(y)
}

func (y *ChartYaml) GetResourceByKindAndName(kind string, name string) runtime.Object {
	for _, obj := range DecodeK8sResources(y.String()) {
		if obj.GetObjectKind().GroupVersionKind().Kind == kind && GetK8sObjName(obj) == name {
			return obj
		}
	}

	return nil
}

func (y *ChartYaml) VerifyEnvVarsForApp(t *testing.T, kind string, name string, expected []ExpectedEnvVar) {
	app := y.GetResourceByKindAndName(kind, name)
	assert.NotNil(t, app, "unable to find %s named '%s'", kind, name)

	pod := GetPodSpec(app)
	assert.NotNil(t, pod, "unable to extract pod spec from %s: %s", kind, name)

	envMap := EnvVarMap(pod.Containers[0].Env)

	for _, env := range expected {
		actual, ok := envMap[env.GetName()]
		assert.True(t, ok, "no such env var %s", env.GetName())
		VerifyEnvVar(t, y.String(), env, actual)
	}
}
