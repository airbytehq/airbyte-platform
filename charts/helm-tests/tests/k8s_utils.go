package test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/gruntwork-io/terratest/modules/helm"
	"github.com/gruntwork-io/terratest/modules/logger"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbac "k8s.io/api/rbac/v1"
)

func baseHelmOptions() *helm.Options {
	return &helm.Options{
		Logger:        logger.Discard,
		SetValues:     make(map[string]string),
		SetJsonValues: make(map[string]string),
	}
}

func baseHelmOptionsForStorageType(t string) *helm.Options {
	opts := baseHelmOptions()
	opts.SetValues = map[string]string{
		"global.storage.type":       t,
		"workload-launcher.enabled": "true",
	}

	return opts
}

func getK8sResourceByKindAndName(renderedYaml, kind, name string) (map[string]any, error) {
	decoder := yaml.NewDecoder(strings.NewReader(renderedYaml))

	var err error
	for {
		var resource map[string]any
		err = decoder.Decode(&resource)
		if errors.Is(err, io.EOF) {
			break
		}

		if resource["kind"] == nil {
			continue
		}
		k := resource["kind"].(string)
		n := resource["metadata"].(map[string]any)["name"].(string)

		if k == kind && n == name {
			return resource, nil
		}
	}

	return nil, fmt.Errorf("could not find resource of Kind: %s Name: %s", kind, name)
}

func getConfigMap(renderedYaml, name string) (*corev1.ConfigMap, error) {
	m, err := getK8sResourceByKindAndName(renderedYaml, "ConfigMap", name)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	var cm corev1.ConfigMap
	err = json.Unmarshal(b, &cm)
	if err != nil {
		return nil, err
	}

	return &cm, nil
}

func getSecret(renderedYaml, name string) (*corev1.Secret, error) {
	m, err := getK8sResourceByKindAndName(renderedYaml, "Secret", name)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	var secret corev1.Secret
	err = json.Unmarshal(b, &secret)
	if err != nil {
		return nil, err
	}

	return &secret, nil
}

func getDeployment(renderedYaml, name string) (*appsv1.Deployment, error) {
	m, err := getK8sResourceByKindAndName(renderedYaml, "Deployment", name)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	var dep appsv1.Deployment
	err = json.Unmarshal(b, &dep)
	if err != nil {
		return nil, err
	}

	return &dep, nil
}

func getStatefulSet(renderedYaml, name string) (*appsv1.StatefulSet, error) {
	m, err := getK8sResourceByKindAndName(renderedYaml, "StatefulSet", name)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	var ss appsv1.StatefulSet
	err = json.Unmarshal(b, &ss)
	if err != nil {
		return nil, err
	}

	return &ss, nil
}

func getPod(renderedYaml, name string) (*corev1.Pod, error) {
	m, err := getK8sResourceByKindAndName(renderedYaml, "Pod", name)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	var p corev1.Pod
	err = json.Unmarshal(b, &p)
	if err != nil {
		return nil, err
	}

	return &p, nil
}

func getJob(renderedYaml, name string) (*batchv1.Job, error) {
	m, err := getK8sResourceByKindAndName(renderedYaml, "Job", name)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	var job batchv1.Job
	err = json.Unmarshal(b, &job)
	if err != nil {
		return nil, err
	}

	return &job, nil
}

func getService(renderedYaml, name string) (*corev1.Service, error) {
	m, err := getK8sResourceByKindAndName(renderedYaml, "Service", name)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	var svc corev1.Service
	err = json.Unmarshal(b, &svc)
	if err != nil {
		return nil, err
	}

	return &svc, nil
}

func getServiceAccount(renderedYaml, name string) (*corev1.ServiceAccount, error) {
	m, err := getK8sResourceByKindAndName(renderedYaml, "ServiceAccount", name)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	var sa corev1.ServiceAccount
	err = json.Unmarshal(b, &sa)
	if err != nil {
		return nil, err
	}

	return &sa, nil
}

func getRole(renderedYaml, name string) (*rbac.Role, error) {
	m, err := getK8sResourceByKindAndName(renderedYaml, "Role", name)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	var role rbac.Role
	err = json.Unmarshal(b, &role)
	if err != nil {
		return nil, err
	}

	return &role, nil
}

func getRoleBinding(renderedYaml, name string) (*rbac.RoleBinding, error) {
	m, err := getK8sResourceByKindAndName(renderedYaml, "RoleBinding", name)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	var binding rbac.RoleBinding
	err = json.Unmarshal(b, &binding)
	if err != nil {
		return nil, err
	}

	return &binding, nil
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
