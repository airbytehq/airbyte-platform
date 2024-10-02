package tests

import (
	"strings"
	"testing"

	"github.com/gruntwork-io/terratest/modules/helm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbac "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
)

func renderChart(t *testing.T, opts *helm.Options) string {
	out, err := helm.RenderTemplateE(t, opts, chartPath, "airbyte", nil)
	require.NoError(t, err, "failure rendering template")
	return out
}

func decodeK8sResources(renderedYaml string) []runtime.Object {
	out := []runtime.Object{}
	chunks := strings.Split(renderedYaml, "---")
	for _, chunk := range chunks {
		if len(chunk) == 0 {
			continue
		}
		obj, _, err := scheme.Codecs.UniversalDeserializer().Decode([]byte(chunk), nil, nil)
		if err != nil {
			continue
		}
		out = append(out, obj)
	}
	return out
}

func getK8sObjName(obj runtime.Object) string {
	i, ok := obj.(interface{ GetName() string })
	if !ok {
		return ""
	}
	return i.GetName()
}

func getK8sResourceByKindAndName(renderedYaml, kind, name string) runtime.Object {
	objs := decodeK8sResources(renderedYaml)

	for _, obj := range objs {
		if obj.GetObjectKind().GroupVersionKind().Kind == kind && getK8sObjName(obj) == name {
			return obj
		}
	}

	return nil
}

func getPodSpec(obj runtime.Object) *corev1.PodSpec {
	switch z := obj.(type) {
	case *corev1.Pod:
		return &z.Spec
	case *batchv1.Job:
		return &z.Spec.Template.Spec
	case *appsv1.Deployment:
		return &z.Spec.Template.Spec
	case *appsv1.StatefulSet:
		return &z.Spec.Template.Spec
	default:
		return nil
	}
}

func assertNoResource(t *testing.T, renderedYaml, kind, name string) {
	m := getK8sResourceByKindAndName(renderedYaml, kind, name)
	assert.Nil(t, m)
}

func getConfigMap(renderedYaml, name string) *corev1.ConfigMap {
	return getK8sResourceByKindAndName(renderedYaml, "ConfigMap", name).(*corev1.ConfigMap)
}

func getSecret(renderedYaml, name string) *corev1.Secret {
	return getK8sResourceByKindAndName(renderedYaml, "Secret", name).(*corev1.Secret)
}

func getDeployment(renderedYaml, name string) *appsv1.Deployment {
	return getK8sResourceByKindAndName(renderedYaml, "Deployment", name).(*appsv1.Deployment)
}

func getStatefulSet(renderedYaml, name string) *appsv1.StatefulSet {
	return getK8sResourceByKindAndName(renderedYaml, "StatefulSet", name).(*appsv1.StatefulSet)
}

func getPod(renderedYaml, name string) *corev1.Pod {
	return getK8sResourceByKindAndName(renderedYaml, "Pod", name).(*corev1.Pod)
}

func getJob(renderedYaml, name string) *batchv1.Job {
	return getK8sResourceByKindAndName(renderedYaml, "Job", name).(*batchv1.Job)
}

func getService(renderedYaml, name string) *corev1.Service {
	return getK8sResourceByKindAndName(renderedYaml, "Service", name).(*corev1.Service)
}

func getServiceAccount(renderedYaml, name string) *corev1.ServiceAccount {
	return getK8sResourceByKindAndName(renderedYaml, "ServiceAccount", name).(*corev1.ServiceAccount)
}

func getRole(renderedYaml, name string) *rbac.Role {
	return getK8sResourceByKindAndName(renderedYaml, "Role", name).(*rbac.Role)
}

func getRoleBinding(renderedYaml, name string) *rbac.RoleBinding {
	return getK8sResourceByKindAndName(renderedYaml, "RoleBinding", name).(*rbac.RoleBinding)
}

func envVarMap(vars []corev1.EnvVar) map[string]corev1.EnvVar {
	m := make(map[string]corev1.EnvVar)
	for _, k := range vars {
		m[k.Name] = k
	}
	return m
}

type expectedEnvVar interface {
	RefName(name string) expectedEnvVar
	RefKey(key string) expectedEnvVar
}

type expectedVarFromConfigMap struct {
	// value to expect for `valueFrom.configMapKeyRef.name`
	refName string
	// value to expect for `valueFrom.configMapKeyRef.key`
	refKey string
}

func (e expectedVarFromConfigMap) RefName(n string) expectedEnvVar {
	e.refName = n
	return e
}

func (e expectedVarFromConfigMap) RefKey(k string) expectedEnvVar {
	e.refKey = k
	return e
}

func expectedConfigMapVar() *expectedVarFromConfigMap {
	return &expectedVarFromConfigMap{}
}

type expectedVarFromSecret struct {
	// value to expect for `valueFrom.secretKeyRef.name`
	refName string
	// value to expect for `valueFrom.secretKeyRef.key`
	refKey string
}

func (e expectedVarFromSecret) RefName(n string) expectedEnvVar {
	e.refName = n
	return e
}

func (e expectedVarFromSecret) RefKey(k string) expectedEnvVar {
	e.refKey = k
	return e
}

func expectedSecretVar() *expectedVarFromSecret {
	return &expectedVarFromSecret{}
}

func verifyEnvVar(t *testing.T, expected expectedEnvVar, actual corev1.EnvVar) {
	switch expected := expected.(type) {
	case expectedVarFromConfigMap:
		assert.NotNil(t, actual.ValueFrom.ConfigMapKeyRef)
		assert.Equal(t, expected.refName, actual.ValueFrom.ConfigMapKeyRef.Name)
		assert.Equal(t, expected.refKey, actual.ValueFrom.ConfigMapKeyRef.Key)
	case expectedVarFromSecret:
		assert.NotNil(t, actual.ValueFrom.SecretKeyRef)
		assert.Equal(t, expected.refName, actual.ValueFrom.SecretKeyRef.Name)
		assert.Equal(t, expected.refKey, actual.ValueFrom.SecretKeyRef.Key)
	}
}

type expectedVolumeMount interface {
	Volume(string) expectedVolumeMount
	MountPath(string) expectedVolumeMount
	SubPath(string) expectedVolumeMount
	RefName(string) expectedVolumeMount
}

type expectedVolumeMountFromConfigMap struct {
	volume    string
	mountPath string
	subPath   string
	refName   string
}

func expectedConfigMapVolumeMount() *expectedVolumeMountFromConfigMap {
	return &expectedVolumeMountFromConfigMap{}
}

func (v expectedVolumeMountFromConfigMap) Volume(name string) expectedVolumeMount {
	v.volume = name
	return v
}

func (v expectedVolumeMountFromConfigMap) MountPath(path string) expectedVolumeMount {
	v.mountPath = path
	return v
}

func (v expectedVolumeMountFromConfigMap) SubPath(subpath string) expectedVolumeMount {
	v.subPath = subpath
	return v
}

func (v expectedVolumeMountFromConfigMap) RefName(ref string) expectedVolumeMount {
	v.refName = ref
	return v
}

type expectedVolumeMountFromSecret struct {
	volume    string
	mountPath string
	subPath   string
	refName   string
}

func expectedSecretVolumeMount() *expectedVolumeMountFromSecret {
	return &expectedVolumeMountFromSecret{}
}

func (v expectedVolumeMountFromSecret) Volume(name string) expectedVolumeMount {
	v.volume = name
	return v
}

func (v expectedVolumeMountFromSecret) MountPath(path string) expectedVolumeMount {
	v.mountPath = path
	return v
}

func (v expectedVolumeMountFromSecret) SubPath(subpath string) expectedVolumeMount {
	v.subPath = subpath
	return v
}

func (v expectedVolumeMountFromSecret) RefName(ref string) expectedVolumeMount {
	v.refName = ref
	return v
}

func verifyVolumeMountForPod(t *testing.T, expected expectedVolumeMount, pod corev1.PodSpec) {
	var vol *corev1.Volume
	var volMnt *corev1.VolumeMount

	switch expected := expected.(type) {
	case expectedVolumeMountFromConfigMap:
		for _, v := range pod.Volumes {
			if v.Name == expected.volume {
				vol = &v
				break
			}
		}
		assert.NotNil(t, vol)

		for _, vm := range pod.Containers[0].VolumeMounts {
			if vm.Name == expected.volume {
				volMnt = &vm
				break
			}
		}
		assert.NotNil(t, volMnt)

		assert.NotNil(t, vol.ConfigMap)
		assert.Equal(t, expected.refName, vol.ConfigMap.Name, "volume configMap.name does not match")
		assert.Equal(t, expected.mountPath, vol, "volumeMount mountPath does not match")
		assert.Equal(t, expected.subPath, volMnt.SubPath, "volumeMount subPath does not match")
	case expectedVolumeMountFromSecret:
		for _, v := range pod.Volumes {
			if v.Name == expected.volume {
				vol = &v
				break
			}
		}
		assert.NotNil(t, vol)

		for _, vm := range pod.Containers[0].VolumeMounts {
			if vm.Name == expected.volume {
				volMnt = &vm
				break
			}
		}
		assert.NotNil(t, volMnt)

		assert.Equal(t, "t", "t")
		assert.NotNil(t, vol.Secret)
		assert.Equal(t, expected.refName, vol.Secret.SecretName, "volume secret.secretName does not match")
		assert.Equal(t, expected.mountPath, volMnt.MountPath, "volumeMount mountPath does not match")
		assert.Equal(t, expected.subPath, volMnt.SubPath, "volumeMount subPath does not match")
	}
}
