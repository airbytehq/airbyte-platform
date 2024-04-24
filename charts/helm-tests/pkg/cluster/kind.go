package cluster

import (
	"fmt"
	"os"
	"path"
	"time"

	"sigs.k8s.io/kind/pkg/cluster"
)

const (
	defaultK8sVersion = "1.29.0"
)

type KindClusterOption func(c *KindCluster) *KindCluster

// Name is an option to set the cluster name.
func Name(n string) KindClusterOption {
	return func(c *KindCluster) *KindCluster {
		c.name = n
		return c
	}
}

// Version is an option to set the cluster version.
func Version(v string) KindClusterOption {
	return func(c *KindCluster) *KindCluster {
		c.version = v
		return c
	}
}

// KubeConfigPath is an option to set the kubeconfig path.
func KubeConfigPath(p string) KindClusterOption {
	return func(c *KindCluster) *KindCluster {
		c.kubeconfigPath = p
		return c
	}
}

// KindCluster is a cluster provider that can create local KIND clusters.
type KindCluster struct {
	name           string
	version        string
	kubeconfigPath string

	provider *cluster.Provider
}

var _ ClusterProvider = (*KindCluster)(nil)

func NewKindCluster(opts ...KindClusterOption) *KindCluster {
	prov := cluster.NewProvider()
	kubeDir, _ := os.MkdirTemp("", ".kube")
	c := &KindCluster{
		// Auto-generated cluster name
		name:     fmt.Sprintf("airbyte-cluster-%d", time.Now().Unix()),
		provider: prov,
		version:  defaultK8sVersion,
	}

	for _, opt := range opts {
		c = opt(c)
	}

	c.kubeconfigPath = path.Join(kubeDir, c.name+".kubeconfig")

	return c
}

func (c *KindCluster) Provision() error {
	err := c.provider.Create(
		c.name,
		cluster.CreateWithWaitForReady(30*time.Second),
		cluster.CreateWithNodeImage("kindest/node:v"+c.version),
	)
	if err != nil {
		return err
	}

	err = c.provider.ExportKubeConfig(c.name, c.kubeconfigPath, false)
	if err != nil {
		return err
	}

	return nil
}

func (c *KindCluster) Deprovision() error {
	c.provider.Delete(c.name, c.kubeconfigPath)
	os.Remove(c.kubeconfigPath)
	return nil
}

func (c *KindCluster) Kubeconfig() string {
	return c.kubeconfigPath
}
