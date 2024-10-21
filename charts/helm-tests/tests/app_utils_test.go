package tests

import (
	"fmt"

	"github.com/gruntwork-io/terratest/modules/helm"
)

var allApps = []string{
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
	"pod-sweeper",
	"workload-api-server",
	"workload-launcher",
}

func setAppOpt(opts *helm.Options, appName, name, value string) {
	key := fmt.Sprintf("%s.%s", appName, name)
	opts.SetValues[key] = value
}
