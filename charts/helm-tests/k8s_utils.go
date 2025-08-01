package helmtests

import (
	"log"
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

func RenderChart(t *testing.T, opts *helm.Options, chartPath string) string {
	out, err := helm.RenderTemplateE(t, opts, chartPath, "airbyte", nil)
	require.NoError(t, err, "failure rendering template")
	return out
}

func DecodeK8sResources(renderedYaml string) []runtime.Object {
	out := []runtime.Object{}
	chunks := strings.Split(renderedYaml, "---")
	for _, chunk := range chunks {
		chunk = DropYamlComments(chunk)
		if len(chunk) == 0 {
			continue
		}

		obj, _, err := scheme.Codecs.UniversalDeserializer().Decode([]byte(chunk), nil, nil)
		if err != nil {
			log.Printf("error decoding k8s object: %s\n", err)
			continue
		}
		out = append(out, obj)
	}
	return out
}

func DropYamlComments(src string) string {
	lines := strings.Split(src, "\n")
	filtered := []string{}
	for _, l := range lines {
		if strings.HasPrefix(l, "# ") {
			continue
		}
		filtered = append(filtered, l)
	}
	return strings.TrimSpace(strings.Join(filtered, "\n"))
}

func GetK8sObjName(obj runtime.Object) string {
	i, ok := obj.(interface{ GetName() string })
	if !ok {
		return ""
	}
	return i.GetName()
}

func GetK8sResourceByKindAndName(renderedYaml, kind, name string) runtime.Object {
	objs := DecodeK8sResources(renderedYaml)

	for _, obj := range objs {
		if obj.GetObjectKind().GroupVersionKind().Kind == kind && GetK8sObjName(obj) == name {
			return obj
		}
	}

	return nil
}

func GetPodSpec(obj runtime.Object) *corev1.PodSpec {
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

func AssertNoResource(t *testing.T, renderedYaml, kind, name string) {
	m := GetK8sResourceByKindAndName(renderedYaml, kind, name)
	assert.Nil(t, m)
}

func GetConfigMap(renderedYaml, name string) *corev1.ConfigMap {
	return GetK8sResourceByKindAndName(renderedYaml, "ConfigMap", name).(*corev1.ConfigMap)
}

func GetSecret(renderedYaml, name string) *corev1.Secret {
	obj := GetK8sResourceByKindAndName(renderedYaml, "Secret", name)
	if obj != nil {
		return obj.(*corev1.Secret)
	}

	return nil
}

func GetDeployment(renderedYaml, name string) *appsv1.Deployment {
	obj := GetK8sResourceByKindAndName(renderedYaml, "Deployment", name)
	if obj != nil {
		return obj.(*appsv1.Deployment)
	}

	return nil
}

func GetStatefulSet(renderedYaml, name string) *appsv1.StatefulSet {
	obj := GetK8sResourceByKindAndName(renderedYaml, "StatefulSet", name)
	if obj != nil {
		return obj.(*appsv1.StatefulSet)
	}

	return nil
}

func GetPod(renderedYaml, name string) *corev1.Pod {
	obj := GetK8sResourceByKindAndName(renderedYaml, "Pod", name)
	if obj != nil {
		return obj.(*corev1.Pod)
	}

	return nil
}

func GetJob(renderedYaml, name string) *batchv1.Job {
	obj := GetK8sResourceByKindAndName(renderedYaml, "Job", name)
	if obj != nil {
		return obj.(*batchv1.Job)
	}

	return nil
}

func GetService(renderedYaml, name string) *corev1.Service {
	obj := GetK8sResourceByKindAndName(renderedYaml, "Service", name)
	if obj != nil {
		return obj.(*corev1.Service)
	}

	return nil
}

func GetServiceAccount(renderedYaml, name string) *corev1.ServiceAccount {
	obj := GetK8sResourceByKindAndName(renderedYaml, "ServiceAccount", name)
	if obj != nil {
		return obj.(*corev1.ServiceAccount)
	}

	return nil
}

func GetRole(renderedYaml, name string) *rbac.Role {
	obj := GetK8sResourceByKindAndName(renderedYaml, "Role", name)
	if obj != nil {
		return obj.(*rbac.Role)
	}

	return nil
}

func GetRoleBinding(renderedYaml, name string) *rbac.RoleBinding {
	obj := GetK8sResourceByKindAndName(renderedYaml, "RoleBinding", name)
	if obj != nil {
		return obj.(*rbac.RoleBinding)
	}

	return nil
}

func EnvVarMap(vars []corev1.EnvVar) map[string]corev1.EnvVar {
	m := make(map[string]corev1.EnvVar)
	for _, k := range vars {
		m[k.Name] = k
	}
	return m
}

type ExpectedEnvVar interface {
	Name(name string) ExpectedEnvVar
	GetName() string
	RefName(name string) ExpectedEnvVar
	GetRefName() string
	RefKey(key string) ExpectedEnvVar
	GetRefKey() string
	Value(value string) ExpectedEnvVar
}

type ExpectedVarFromConfigMap struct {
	// environment variable name
	name string
	// value to expect in the configMap
	value string
	// value to expect for `valueFrom.configMapKeyRef.name`
	refName string
	// value to expect for `valueFrom.configMapKeyRef.key`
	refKey string
}

func (e ExpectedVarFromConfigMap) Name(n string) ExpectedEnvVar {
	e.name = n
	return e
}

func (e ExpectedVarFromConfigMap) GetName() string {
	if e.name != "" {
		return e.name
	}

	return e.refKey
}

func (e ExpectedVarFromConfigMap) RefName(n string) ExpectedEnvVar {
	e.refName = n
	return e
}

func (e ExpectedVarFromConfigMap) GetRefName() string {
	return e.refName
}

func (e ExpectedVarFromConfigMap) RefKey(k string) ExpectedEnvVar {
	e.refKey = k
	return e
}

func (e ExpectedVarFromConfigMap) GetRefKey() string {
	if e.refKey != "" {
		return e.refKey
	}

	return e.name
}

func (e ExpectedVarFromConfigMap) Value(v string) ExpectedEnvVar {
	e.value = v
	return e
}

func ExpectedConfigMapVar() *ExpectedVarFromConfigMap {
	return &ExpectedVarFromConfigMap{}
}

type ExpectedVarFromSecret struct {
	// environment variable name
	name string
	// value to expect in the secret
	value string
	// value to expect for `valueFrom.secretKeyRef.name`
	refName string
	// value to expect for `valueFrom.secretKeyRef.key`
	refKey string
}

func (e ExpectedVarFromSecret) Name(n string) ExpectedEnvVar {
	e.name = n
	return e
}

func (e ExpectedVarFromSecret) GetName() string {
	if e.name != "" {
		return e.name
	}

	return e.refKey
}

func (e ExpectedVarFromSecret) RefName(n string) ExpectedEnvVar {
	e.refName = n
	return e
}

func (e ExpectedVarFromSecret) GetRefName() string {
	return e.refName
}

func (e ExpectedVarFromSecret) RefKey(k string) ExpectedEnvVar {
	e.refKey = k
	return e
}

func (e ExpectedVarFromSecret) GetRefKey() string {
	if e.refKey != "" {
		return e.refKey
	}
	return e.name
}

func (e ExpectedVarFromSecret) Value(v string) ExpectedEnvVar {
	e.value = v
	return e
}

func ExpectedSecretVar() *ExpectedVarFromSecret {
	return &ExpectedVarFromSecret{}
}

func VerifyEnvVar(t *testing.T, chartYaml string, expected ExpectedEnvVar, actual corev1.EnvVar) {
	switch expected := expected.(type) {
	case ExpectedVarFromConfigMap:
		assert.NotNil(t, actual.ValueFrom, "env var '%s' has no 'valueFrom' set", expected.GetRefKey())
		assert.NotNil(t, actual.ValueFrom.ConfigMapKeyRef, "env var '%s' has no 'configMapRefKey' set", expected.GetRefKey())
		assert.Equal(t, expected.GetRefName(), actual.ValueFrom.ConfigMapKeyRef.Name, "env var '%s' configMapRef name should '%s'", expected.GetName(), expected.GetRefName())
		assert.Equal(t, expected.GetRefKey(), actual.ValueFrom.ConfigMapKeyRef.Key)

		configMap := GetConfigMap(chartYaml, expected.GetRefName())
		if configMap != nil {
			assert.Equal(t, expected.value, configMap.Data[expected.GetRefKey()], "expected configMap value for '%s'", expected.GetRefKey())
		}

	case ExpectedVarFromSecret:
		assert.NotNil(t, actual.ValueFrom, "env var '%s' has no 'valueFrom' set", expected.GetRefKey())
		assert.NotNil(t, actual.ValueFrom.SecretKeyRef, "env var '%s' has no 'secretRefKey' set", expected.GetRefKey())
		assert.Equal(t, expected.GetRefName(), actual.ValueFrom.SecretKeyRef.Name, "env var '%s' secretRef name should be '%s'", expected.GetName(), expected.GetRefName())
		assert.Equal(t, expected.GetRefKey(), actual.ValueFrom.SecretKeyRef.Key)

		secret := GetSecret(chartYaml, expected.GetRefName())
		if secret != nil {
			assert.Equal(t, expected.value, secret.StringData[expected.GetRefKey()], "expected secret value for '%s'", expected.GetRefKey())
		}
	}
}

type ExpectedVolumeMount interface {
	Volume(string) ExpectedVolumeMount
	MountPath(string) ExpectedVolumeMount
	SubPath(string) ExpectedVolumeMount
	RefName(string) ExpectedVolumeMount
}

type ExpectedVolumeMountFromConfigMap struct {
	volume    string
	mountPath string
	subPath   string
	refName   string
}

func ExpectedConfigMapVolumeMount() *ExpectedVolumeMountFromConfigMap {
	return &ExpectedVolumeMountFromConfigMap{}
}

func (v ExpectedVolumeMountFromConfigMap) Volume(name string) ExpectedVolumeMount {
	v.volume = name
	return v
}

func (v ExpectedVolumeMountFromConfigMap) MountPath(path string) ExpectedVolumeMount {
	v.mountPath = path
	return v
}

func (v ExpectedVolumeMountFromConfigMap) SubPath(subpath string) ExpectedVolumeMount {
	v.subPath = subpath
	return v
}

func (v ExpectedVolumeMountFromConfigMap) RefName(ref string) ExpectedVolumeMount {
	v.refName = ref
	return v
}

type ExpectedVolumeMountFromSecret struct {
	volume    string
	mountPath string
	subPath   string
	refName   string
}

func ExpectedSecretVolumeMount() *ExpectedVolumeMountFromSecret {
	return &ExpectedVolumeMountFromSecret{}
}

func (v ExpectedVolumeMountFromSecret) Volume(name string) ExpectedVolumeMount {
	v.volume = name
	return v
}

func (v ExpectedVolumeMountFromSecret) MountPath(path string) ExpectedVolumeMount {
	v.mountPath = path
	return v
}

func (v ExpectedVolumeMountFromSecret) SubPath(subpath string) ExpectedVolumeMount {
	v.subPath = subpath
	return v
}

func (v ExpectedVolumeMountFromSecret) RefName(ref string) ExpectedVolumeMount {
	v.refName = ref
	return v
}

func VerifyVolumeMountForPod(t *testing.T, expected ExpectedVolumeMount, pod corev1.PodSpec) {
	var vol *corev1.Volume
	var volMnt *corev1.VolumeMount

	switch expected := expected.(type) {
	case ExpectedVolumeMountFromConfigMap:
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
	case ExpectedVolumeMountFromSecret:
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

		assert.NotNil(t, vol.Secret)
		assert.Equal(t, expected.refName, vol.Secret.SecretName, "volume secret.secretName does not match")
		assert.Equal(t, expected.mountPath, volMnt.MountPath, "volumeMount mountPath does not match")
		assert.Equal(t, expected.subPath, volMnt.SubPath, "volumeMount subPath does not match")
	}
}
