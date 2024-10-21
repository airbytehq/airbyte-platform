package tests

import (
	"fmt"
	"testing"

	"github.com/gruntwork-io/terratest/modules/helm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

// all of the airbyte services that require database config keys
var databaseClients = []struct {
	kind            string
	name            string
	expectedEnvVars map[string]expectedEnvVar
}{
	{
		kind: "Deployment",
		name: "airbyte-server",
		expectedEnvVars: map[string]expectedEnvVar{
			"DATABASE_HOST":     expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_HOST"),
			"DATABASE_PORT":     expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_PORT"),
			"DATABASE_DB":       expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_DB"),
			"DATABASE_USER":     expectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("DATABASE_USER"),
			"DATABASE_PASSWORD": expectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("DATABASE_PASSWORD"),
			"DATABASE_URL":      expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_URL"),
		},
	},
	{
		kind: "Deployment",
		name: "airbyte-worker",
		expectedEnvVars: map[string]expectedEnvVar{
			"DATABASE_HOST":     expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_HOST"),
			"DATABASE_PORT":     expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_PORT"),
			"DATABASE_DB":       expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_DB"),
			"DATABASE_USER":     expectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("DATABASE_USER"),
			"DATABASE_PASSWORD": expectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("DATABASE_PASSWORD"),
			"DATABASE_URL":      expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_URL"),
		},
	},
	{
		kind: "Deployment",
		name: "airbyte-cron",
		expectedEnvVars: map[string]expectedEnvVar{
			"DATABASE_HOST":     expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_HOST"),
			"DATABASE_PORT":     expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_PORT"),
			"DATABASE_DB":       expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_DB"),
			"DATABASE_USER":     expectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("DATABASE_USER"),
			"DATABASE_PASSWORD": expectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("DATABASE_PASSWORD"),
			"DATABASE_URL":      expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_URL"),
		},
	},
	{
		kind: "StatefulSet",
		name: "airbyte-keycloak",
		expectedEnvVars: map[string]expectedEnvVar{
			"KEYCLOAK_DATABASE_USERNAME": expectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("DATABASE_USER"),
			"KEYCLOAK_DATABASE_PASSWORD": expectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("DATABASE_PASSWORD"),
			"KEYCLOAK_DATABASE_URL":      expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("KEYCLOAK_DATABASE_URL"),
		},
	},
	{
		kind: "Job",
		name: "airbyte-keycloak-setup",
		expectedEnvVars: map[string]expectedEnvVar{
			"DATABASE_HOST":         expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_HOST"),
			"DATABASE_PORT":         expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_PORT"),
			"DATABASE_DB":           expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_DB"),
			"DATABASE_USER":         expectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("DATABASE_USER"),
			"DATABASE_PASSWORD":     expectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("DATABASE_PASSWORD"),
			"KEYCLOAK_DATABASE_URL": expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("KEYCLOAK_DATABASE_URL"),
		},
	},
	{
		kind: "Pod",
		name: "airbyte-airbyte-bootloader",
		expectedEnvVars: map[string]expectedEnvVar{
			"DATABASE_HOST":     expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_HOST"),
			"DATABASE_PORT":     expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_PORT"),
			"DATABASE_DB":       expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_DB"),
			"DATABASE_USER":     expectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("DATABASE_USER"),
			"DATABASE_PASSWORD": expectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("DATABASE_PASSWORD"),
			"DATABASE_URL":      expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_URL"),
		},
	},
	{
		kind: "Deployment",
		name: "airbyte-metrics",
		expectedEnvVars: map[string]expectedEnvVar{
			"DATABASE_HOST":     expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_HOST"),
			"DATABASE_PORT":     expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_PORT"),
			"DATABASE_DB":       expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_DB"),
			"DATABASE_USER":     expectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("DATABASE_USER"),
			"DATABASE_PASSWORD": expectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("DATABASE_PASSWORD"),
			"DATABASE_URL":      expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_URL"),
		},
	},
	{
		kind: "Deployment",
		name: "airbyte-temporal",
		expectedEnvVars: map[string]expectedEnvVar{
			"POSTGRES_SEEDS": expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_HOST"),
			"DB_PORT":        expectedConfigMapVar().RefName("airbyte-airbyte-env").RefKey("DATABASE_PORT"),
			"POSTGRES_USER":  expectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("DATABASE_USER"),
			"POSTGRES_PWD":   expectedSecretVar().RefName("airbyte-airbyte-secrets").RefKey("DATABASE_PASSWORD"),
		},
	},
}

func TestDefaultDatabaseConfiguration(t *testing.T) {
	t.Run("should default to using an in-cluster database", func(t *testing.T) {
		helmOpts := BaseHelmOptionsForEnterpriseWithValues() // enable all the things
		helmOpts.SetValues["metrics.enabled"] = "true"
		chartYaml := renderChart(t, helmOpts)
		getStatefulSet(chartYaml, "airbyte-db")

		t.Run("default database credentials", func(t *testing.T) {
			secret := getSecret(chartYaml, "airbyte-airbyte-secrets")
			configMap := getConfigMap(chartYaml, "airbyte-airbyte-env")

			assert.Equal(t, "airbyte", secret.StringData["DATABASE_USER"])
			assert.Equal(t, "airbyte", secret.StringData["DATABASE_PASSWORD"])
			assert.Equal(t, "db-airbyte", configMap.Data["DATABASE_DB"])
		})

		t.Run("override default database credentials", func(t *testing.T) {
			helmOpts := BaseHelmOptionsForEnterpriseWithValues() // enable all the things
			helmOpts.SetValues["metrics.enabled"] = "true"
			helmOpts.SetValues["postgresql.postgresqlUsername"] = "override-user"
			helmOpts.SetValues["postgresql.postgresqlPassword"] = "override-pass"
			helmOpts.SetValues["postgresql.postgresqlDatabase"] = "override-db"

			chartYaml := renderChart(t, helmOpts)
			secret := getSecret(chartYaml, "airbyte-airbyte-secrets")
			configMap := getConfigMap(chartYaml, "airbyte-airbyte-env")

			assert.Equal(t, "override-user", secret.StringData["DATABASE_USER"])
			assert.Equal(t, "override-pass", secret.StringData["DATABASE_PASSWORD"])
			assert.Equal(t, "override-db", configMap.Data["DATABASE_DB"])
		})

		t.Run("database clients should include database config vars", func(t *testing.T) {
			for _, c := range databaseClients {
				t.Run(fmt.Sprintf("%s should include database config env vars", c.name), func(t *testing.T) {
					envVars := make(map[string]corev1.EnvVar)
					switch c.kind {
					case "Pod":
						pod := getPod(chartYaml, c.name)
						envVars = envVarMap(pod.Spec.Containers[0].Env)
					case "Job":
						job := getJob(chartYaml, c.name)
						envVars = envVarMap(job.Spec.Template.Spec.Containers[0].Env)
					case "Deployment":
						dep := getDeployment(chartYaml, c.name)
						envVars = envVarMap(dep.Spec.Template.Spec.Containers[0].Env)
					case "StatefulSet":
						ss := getStatefulSet(chartYaml, c.name)
						envVars = envVarMap(ss.Spec.Template.Spec.Containers[0].Env)
					}

					for k, expected := range c.expectedEnvVars {
						actual, ok := envVars[k]
						assert.True(t, ok, fmt.Sprintf("`%s` should be declared as an environment variable for %s %s", k, c.kind, c.name))
						verifyEnvVar(t, expected, actual)
					}
				})
			}
		})

	})
}

func TestExternalDatabaseConfiguration(t *testing.T) {
	fields := []string{
		"global.database.host",
		"global.database.port",
		"global.database.user",
		"global.database.database",
		"global.database.password",
	}

	secretFields := []string{
		"global.database.user",
		"global.database.password",
	}

	t.Run("should require `global.database.secretName` if any secret key ref is set", func(t *testing.T) {
		for _, f := range secretFields {
			t.Run(f, func(t *testing.T) {
				helmOpts := BaseHelmOptionsForEnterpriseWithValues() // enable all the things
				helmOpts.SetValues["metrics.enabled"] = "true"
				helmOpts.SetValues["postgresql.enabled"] = "false"
				helmOpts.SetValues["global.database.secretName"] = ""

				// Loop through all the fields and set the plaintext form for all but the current key.
				// For the current key we set the `xxxSecretKey` reference, to ensure they any of the secret key
				// references trigger the requirement for `global.database.secretName`
				for _, ff := range secretFields {
					if ff == f {
						helmOpts.SetValues[ff+"SecretKey"] = ff + "-key"
						continue
					}
					helmOpts.SetValues[ff] = ff + "-value"
				}

				_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
				require.ErrorContains(t, err, "You must set `global.database.secretName` when using an external database", f)
			})
		}
	})

	t.Run("should require the plaintext value of a field if its secretKey ref is not set", func(t *testing.T) {
		for _, f := range fields {
			helmOpts := BaseHelmOptionsForEnterpriseWithValues() // enable all the things
			helmOpts.SetValues["metrics.enabled"] = "true"
			helmOpts.SetValues["postgresql.enabled"] = "false"
			helmOpts.SetValues["global.database.secretName"] = ""

			t.Run(f, func(t *testing.T) {
				for _, ff := range fields {
					if ff == f {
						continue
					}
					helmOpts.SetValues[ff] = ff + "-value"
				}
				_, err := helm.RenderTemplateE(t, helmOpts, chartPath, "airbyte", nil)
				require.ErrorContains(t, err, fmt.Sprintf("You must set `%s` when using an external database", f))
			})
		}
	})

	t.Run("should set the DATABASE_USER in the generated secret when plaintext value is provided", func(t *testing.T) {
		helmOpts := BaseHelmOptionsForEnterpriseWithValues()
		helmOpts.SetValues["postgresql.enabled"] = "false"
		helmOpts.SetValues["global.database.secretName"] = "database-secret"
		helmOpts.SetValues["global.database.host"] = "localhost"
		helmOpts.SetValues["global.database.port"] = "5432"
		helmOpts.SetValues["global.database.database"] = "airbyte"
		helmOpts.SetValues["global.database.user"] = "octavia"
		helmOpts.SetValues["global.database.passwordSecretKey"] = "DATABASE_PASSWORD"

		chartYaml := renderChart(t, helmOpts)
		configMap := getConfigMap(chartYaml, "airbyte-airbyte-env")

		assert.Equal(t, "octavia", configMap.Data["DATABASE_USER"])
		_, ok := configMap.Data["DATABASE_PASSWORD"]
		assert.False(t, ok)
	})

	t.Run("should set the DATABASE_PASSWORD in the config map when plaintext value is provided", func(t *testing.T) {
		helmOpts := BaseHelmOptionsForEnterpriseWithValues()
		helmOpts.SetValues["postgresql.enabled"] = "false"
		helmOpts.SetValues["global.database.secretName"] = "database-secret"
		helmOpts.SetValues["global.database.host"] = "localhost"
		helmOpts.SetValues["global.database.port"] = "5432"
		helmOpts.SetValues["global.database.database"] = "airbyte"
		helmOpts.SetValues["global.database.userSecretKey"] = "DATABASE_USER"
		helmOpts.SetValues["global.database.password"] = "squidward"

		chartYaml := renderChart(t, helmOpts)
		configMap := getConfigMap(chartYaml, "airbyte-airbyte-env")

		assert.Equal(t, "squidward", configMap.Data["DATABASE_PASSWORD"])
		_, ok := configMap.Data["DATABASE_USER"]
		assert.False(t, ok)
	})
}
