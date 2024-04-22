//go:build install

package test

import (
	"path/filepath"
	"testing"

	"github.com/airbytehq/airbyte-platform-internal/helm-tests/pkg/cluster"
	"github.com/gruntwork-io/terratest/modules/helm"
	"github.com/gruntwork-io/terratest/modules/k8s"
	"github.com/gruntwork-io/terratest/modules/logger"
	"github.com/stretchr/testify/require"
)

func TestBasicInstallWithDefaultValues(t *testing.T) {
	cluster := cluster.NewKindCluster()
	err := cluster.Provision()
	require.NoError(t, err, "failure provisioning KIND cluster")
	defer cluster.Deprovision()

	releaseName := "airbyte-dev"
	releaseNamespace := "ab"

	kubectlOpts := &k8s.KubectlOptions{
		ConfigPath: cluster.Kubeconfig(),
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
