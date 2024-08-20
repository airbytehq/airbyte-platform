package cluster

// ClusterProvider is an interface for implementing a cluster provider.
type ClusterProvider interface {
	Provision() error
	Deprovision() error
	Kubeconfig() string
}
