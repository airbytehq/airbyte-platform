package tests

import (
	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
)

var chartPath string = helmtests.DetermineChartPath("airbyte")
