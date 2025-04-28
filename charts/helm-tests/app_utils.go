package helmtests

import (
	"fmt"

	"github.com/gruntwork-io/terratest/modules/helm"
)

var AllApps = []string{
	"server",
	"webapp",
	"connector-builder-server",
	"worker",
	"cron",
	"keycloak",
	"keycloak-setup",
	"airbyte-bootloader",
	"metrics",
	"temporal",
	"workload-api-server",
	"workload-launcher",
}

func SetAppOpt(opts *helm.Options, appName, name, value string) {
	key := fmt.Sprintf("%s.%s", appName, name)
	opts.SetValues[key] = value
}
