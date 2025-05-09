package helmtests

import (
	"github.com/gruntwork-io/terratest/modules/helm"
	"github.com/gruntwork-io/terratest/modules/k8s"
	"github.com/gruntwork-io/terratest/modules/logger"
)

func BaseHelmOptions() *helm.Options {
	return &helm.Options{
		Logger:         logger.Discard,
		SetValues:      make(map[string]string),
		SetJsonValues:  make(map[string]string),
		ExtraArgs:      make(map[string][]string),
		KubectlOptions: k8s.NewKubectlOptions("", "", "ab"),
		// BuildDependencies: true,
	}
}

func BaseHelmOptionsForEnterprise() *helm.Options {
	opts := BaseHelmOptions()
	opts.SetValues["global.edition"] = "enterprise"

	return opts
}

func BaseHelmOptionsForEnterpriseWithValues() *helm.Options {
	opts := BaseHelmOptions()
	opts.SetValues["global.edition"] = "enterprise"
	opts.SetValues["global.auth.instanceAdmin.firstName"] = "Octavia"
	opts.SetValues["global.auth.instanceAdmin.lastName"] = "Squidington"

	return opts
}

func BaseHelmOptionsForStorageType(t string) *helm.Options {
	opts := BaseHelmOptions()
	opts.SetValues = map[string]string{
		"global.storage.type": t,
	}

	return opts
}
