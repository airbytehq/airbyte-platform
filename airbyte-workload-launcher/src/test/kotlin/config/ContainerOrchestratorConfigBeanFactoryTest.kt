package config

import io.airbyte.workload.launcher.config.ContainerOrchestratorConfigBeanFactory
import io.fabric8.kubernetes.api.model.EnvVarSource
import io.fabric8.kubernetes.api.model.SecretKeySelector
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class ContainerOrchestratorConfigBeanFactoryTest {
  companion object {
    val SECRET_NAME = "secretName"
    val SECRET_KEY = "secretKey"

    val ENV_VAR_NAME1 = "envVarName1"
    val ENV_VAR_VALUE1 = "envVarValue1"
    val ENV_VAR_NAME2 = "envVarName2"
    val ENV_VAR_VALUE2 = "envVarValue2"
    val ENV_VAR_NAME3 = "envVarName3"
  }

  @Test
  fun `test secrets env var map creation`() {
    val factory = ContainerOrchestratorConfigBeanFactory()
    val orchestratorSecretsEnvMap = factory.orchestratorSecretsEnvMap(SECRET_NAME, SECRET_KEY)
    assertEquals(1, orchestratorSecretsEnvMap.size)
    val envVarSource = orchestratorSecretsEnvMap["WORKLOAD_API_BEARER_TOKEN"]
    val secretKeyRef = envVarSource!!.secretKeyRef
    assertEquals(SECRET_NAME, secretKeyRef.name)
    assertEquals(SECRET_KEY, secretKeyRef.key)
  }

  @Test
  fun `test final env vars contain secret env vars an non-secret env vars`() {
    val factory = ContainerOrchestratorConfigBeanFactory()
    val orchestratorEnvVars =
      factory.orchestratorEnvVars(
        mapOf(Pair(ENV_VAR_NAME1, ENV_VAR_VALUE1), Pair(ENV_VAR_NAME2, ENV_VAR_VALUE2)),
        mapOf(Pair(ENV_VAR_NAME3, EnvVarSource(null, null, null, SecretKeySelector(SECRET_KEY, SECRET_NAME, false)))),
      ).sortedBy { it.name }

    assertEquals(ENV_VAR_NAME1, orchestratorEnvVars[0].name)
    assertEquals(ENV_VAR_VALUE1, orchestratorEnvVars[0].value)

    assertEquals(ENV_VAR_NAME2, orchestratorEnvVars[1].name)
    assertEquals(ENV_VAR_VALUE2, orchestratorEnvVars[1].value)

    assertEquals(ENV_VAR_NAME3, orchestratorEnvVars[2].name)
    assertNull(orchestratorEnvVars[2].value)
    assertEquals(SECRET_NAME, orchestratorEnvVars[2].valueFrom.secretKeyRef.name)
    assertEquals(SECRET_KEY, orchestratorEnvVars[2].valueFrom.secretKeyRef.key)
  }
}
