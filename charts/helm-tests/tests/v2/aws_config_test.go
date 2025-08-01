package tests

import (
	"testing"

	helmtests "github.com/airbytehq/airbyte-platform-internal/oss/charts/helm-tests"
	"github.com/stretchr/testify/assert"
)

func TestAwsAssumeRoleConfig(t *testing.T) {
	opts := helmtests.BaseHelmOptions()
	opts.SetValues["global.edition"] = "cloud"
	opts.SetValues["global.aws.secretName"] = "test-airbyte-secrets"
	opts.SetValues["global.aws.assumeRole.accessKeyIdSecretKey"] = "AWS_ACCESS_KEY_ID"
	opts.SetValues["global.aws.assumeRole.secretAccessKeySecretKey"] = "AWS_SECRET_ACCESS_KEY"
	chartYaml, err := helmtests.RenderHelmChart(t, opts, "../../../../../cloud/charts/airbyte-cloud", "airbyte", nil)
	assert.NoError(t, err)

	expectedEnvVars := []helmtests.ExpectedEnvVar{
		// NOTE: AWS_ASSUME_ROLE_SECRET_NAME should default to the global.aws.secretName if global.aws.assumeRole.secretName is not explicitly set
		helmtests.ExpectedConfigMapVar().Name("AWS_ASSUME_ROLE_SECRET_NAME").RefName("airbyte-airbyte-env").Value("test-airbyte-secrets"),
		helmtests.ExpectedSecretVar().Name("AWS_ASSUME_ROLE_ACCESS_KEY_ID").RefName("test-airbyte-secrets").RefKey("AWS_ACCESS_KEY_ID"),
		helmtests.ExpectedSecretVar().Name("AWS_ASSUME_ROLE_SECRET_ACCESS_KEY").RefName("test-airbyte-secrets").RefKey("AWS_SECRET_ACCESS_KEY"),
	}

	releaseApps := appsForRelease("airbyte")
	for _, name := range []string{"server", "worker"} {
		app := releaseApps[name]
		chartYaml.VerifyEnvVarsForApp(t, app.Kind, app.FQN(), expectedEnvVars)
	}
}
