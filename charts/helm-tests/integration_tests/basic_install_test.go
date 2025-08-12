package integration

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests/tests/v1"
	"github.com/gruntwork-io/terratest/modules/helm"
	"github.com/gruntwork-io/terratest/modules/k8s"
	"github.com/gruntwork-io/terratest/modules/logger"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBasicInstallWithDefaultValues(t *testing.T) {
	cls := NewKindCluster()
	err := cls.Provision()
	require.NoError(t, err, "failure provisioning KIND cluster")
	defer cls.Deprovision()

	releaseName := "airbyte-dev"
	releaseNamespace := "ab"

	kubectlOpts := &k8s.KubectlOptions{
		ConfigPath: cls.Kubeconfig(),
		Namespace:  releaseNamespace,
	}

	helm.AddRepo(t, &helm.Options{}, "bitnami", "https://charts.bitnami.com/bitnami")
	chartPath, err := filepath.Abs(chartPath)
	require.NoError(t, err)

	helmOpts := &helm.Options{
		Logger: logger.Discard,
		KubectlOptions: &k8s.KubectlOptions{
			Namespace: releaseNamespace,
		},
	}

	err = k8s.CreateNamespaceE(t, kubectlOpts, releaseNamespace)
	require.NoError(t, err)

	err = helm.InstallE(t, helmOpts, chartPath, releaseName)
	require.NoError(t, err)
}

func TestBasicEnterpriseInstallWithDefaultValues(t *testing.T) {
	cls := NewKindCluster()
	err := cls.Provision()
	require.NoError(t, err, "failure provisioning KIND cluster")
	defer cls.Deprovision()

	releaseName := "airbyte-dev"
	releaseNamespace := "ab"

	kubectlOpts := &k8s.KubectlOptions{
		ConfigPath: cls.Kubeconfig(),
		Namespace:  releaseNamespace,
	}

	helm.AddRepo(t, &helm.Options{}, "bitnami", "https://charts.bitnami.com/bitnami")
	chartPath, err := filepath.Abs(chartPath)
	require.NoError(t, err)

	err = k8s.CreateNamespaceE(t, kubectlOpts, releaseNamespace)
	require.NoError(t, err)

	t.Run("should fail to install if required values are missing", func(t *testing.T) {
		helmOpts := tests.BaseHelmOptionsForEnterprise()
		helmOpts.KubectlOptions = &k8s.KubectlOptions{
			Namespace: releaseNamespace,
		}
		err = helm.InstallE(t, helmOpts, chartPath, releaseName)
		defer helm.DeleteE(t, helmOpts, releaseName, true)
		require.Error(t, err)
	})

	t.Run("should install successfully with airbyte.yml values in values.yml", func(t *testing.T) {
		// create the airbyte-config-secrets secret
		k8sClient, err := k8s.GetKubernetesClientE(t)
		require.NoError(t, err)

		secret := corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "airbyte-config-secrets",
				Namespace: releaseNamespace,
			},
			StringData: map[string]string{
				"license-key":           "fake",
				"initial-user-email":    "octavia@example.com",
				"initial-user-password": "squidwardxoxo",
			},
		}

		_, err = k8sClient.CoreV1().Secrets(releaseNamespace).Create(context.Background(), &secret, metav1.CreateOptions{})
		require.NoError(t, err)
		defer k8sClient.CoreV1().Secrets(releaseName).Delete(context.Background(), "airbyte-config-secrets", metav1.DeleteOptions{})

		helmOpts := tests.BaseHelmOptionsForEnterpriseWithValues()
		helmOpts.KubectlOptions = &k8s.KubectlOptions{
			Namespace: releaseNamespace,
		}
		err = helm.InstallE(t, helmOpts, chartPath, releaseName)
		defer helm.DeleteE(t, helmOpts, releaseName, true)
		require.NoError(t, err)
	})

	t.Run("should install successfully with airbyte.yml as a file", func(t *testing.T) {
		opts := tests.BaseHelmOptions()
		opts.SetValues["global.edition"] = "enterprise"
		opts.KubectlOptions = &k8s.KubectlOptions{
			Namespace: releaseNamespace,
		}
		err = helm.InstallE(t, opts, chartPath, releaseName)
		defer helm.DeleteE(t, opts, releaseName, true)
		require.NoError(t, err)
	})
}
