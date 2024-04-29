package test

import (
	"os"
)

const chartRepo = "https://airbytehq.github.io/helm-charts"

var (
	chartPath  string
	k8sVersion string
)

func init() {
	if chartPath = os.Getenv("HELM_CHART_PATH"); chartPath == "" {
		os.Stderr.WriteString("HELM_CHART_PATH environment variable must be set")
	}

	k8sVersion = os.Getenv("K8S_VERSION")
}
