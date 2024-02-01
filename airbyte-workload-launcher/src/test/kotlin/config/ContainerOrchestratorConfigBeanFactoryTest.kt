package config

import io.airbyte.workload.launcher.config.ContainerOrchestratorConfigBeanFactory
import io.airbyte.workload.launcher.config.ContainerOrchestratorConfigBeanFactory.Companion.AWS_ASSUME_ROLE_ACCESS_KEY_ID_ENV_VAR
import io.airbyte.workload.launcher.config.ContainerOrchestratorConfigBeanFactory.Companion.AWS_ASSUME_ROLE_SECRET_ACCESS_KEY_ENV_VAR
import io.airbyte.workload.launcher.config.ContainerOrchestratorConfigBeanFactory.Companion.WORKLOAD_API_BEARER_TOKEN_ENV_VAR
import io.fabric8.kubernetes.api.model.EnvVarSource
import io.fabric8.kubernetes.api.model.SecretKeySelector
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class ContainerOrchestratorConfigBeanFactoryTest {
  companion object {
    const val AWS_ASSUMED_ROLE_ACCESS_KEY = "accessKey"
    const val AWS_ASSUMED_ROLE_SECRET_KEY = "secretKey"
    const val AWS_ASSUMED_ROLE_SECRET_NAME = "secretName"
    const val BEARER_TOKEN_SECRET_NAME = "secretName"
    const val BEARER_TOKEN_SECRET_KEY = "secretKey"

    const val ENV_VAR_NAME1 = "envVarName1"
    const val ENV_VAR_VALUE1 = "envVarValue1"
    const val ENV_VAR_NAME2 = "envVarName2"
    const val ENV_VAR_VALUE2 = "envVarValue2"
    const val ENV_VAR_NAME3 = "envVarName3"
  }

  @Test
  fun `test secrets env var map creation`() {
    val factory = ContainerOrchestratorConfigBeanFactory()
    val orchestratorSecretsEnvMap =
      factory.orchestratorSecretsEnvMap(
        BEARER_TOKEN_SECRET_NAME,
        BEARER_TOKEN_SECRET_KEY,
        AWS_ASSUMED_ROLE_ACCESS_KEY,
        AWS_ASSUMED_ROLE_SECRET_KEY,
        AWS_ASSUMED_ROLE_SECRET_NAME,
      )
    assertEquals(3, orchestratorSecretsEnvMap.size)
    val envVarSource = orchestratorSecretsEnvMap[WORKLOAD_API_BEARER_TOKEN_ENV_VAR]
    val secretKeyRef = envVarSource!!.secretKeyRef
    assertEquals(BEARER_TOKEN_SECRET_NAME, secretKeyRef.name)
    assertEquals(BEARER_TOKEN_SECRET_KEY, secretKeyRef.key)

    val awsAccessKey = orchestratorSecretsEnvMap[AWS_ASSUME_ROLE_ACCESS_KEY_ID_ENV_VAR]
    val awsAccessKeySecretRef = awsAccessKey!!.secretKeyRef
    assertEquals(AWS_ASSUMED_ROLE_SECRET_NAME, awsAccessKeySecretRef.name)
    assertEquals(AWS_ASSUMED_ROLE_ACCESS_KEY, awsAccessKeySecretRef.key)

    val awsSecretKey = orchestratorSecretsEnvMap[AWS_ASSUME_ROLE_SECRET_ACCESS_KEY_ENV_VAR]
    val awsSecretKeyRef = awsSecretKey!!.secretKeyRef
    assertEquals(AWS_ASSUMED_ROLE_SECRET_NAME, awsSecretKeyRef.name)
    assertEquals(AWS_ASSUMED_ROLE_SECRET_KEY, awsSecretKeyRef.key)
  }

  @Test
  fun `test secrets env var map creation with blank names`() {
    val factory = ContainerOrchestratorConfigBeanFactory()
    val orchestratorSecretsEnvMap =
      factory.orchestratorSecretsEnvMap(
        "",
        BEARER_TOKEN_SECRET_KEY,
        AWS_ASSUMED_ROLE_ACCESS_KEY,
        AWS_ASSUMED_ROLE_SECRET_KEY,
        "",
      )
    assertEquals(0, orchestratorSecretsEnvMap.size)
  }

  @Test
  fun `test final env vars contain secret env vars an non-secret env vars`() {
    val factory = ContainerOrchestratorConfigBeanFactory()
    val orchestratorEnvVars =
      factory.orchestratorEnvVars(
        mapOf(Pair(ENV_VAR_NAME1, ENV_VAR_VALUE1), Pair(ENV_VAR_NAME2, ENV_VAR_VALUE2)),
        mapOf(Pair(ENV_VAR_NAME3, EnvVarSource(null, null, null, SecretKeySelector(BEARER_TOKEN_SECRET_KEY, BEARER_TOKEN_SECRET_NAME, false)))),
      ).sortedBy { it.name }

    assertEquals(ENV_VAR_NAME1, orchestratorEnvVars[0].name)
    assertEquals(ENV_VAR_VALUE1, orchestratorEnvVars[0].value)

    assertEquals(ENV_VAR_NAME2, orchestratorEnvVars[1].name)
    assertEquals(ENV_VAR_VALUE2, orchestratorEnvVars[1].value)

    assertEquals(ENV_VAR_NAME3, orchestratorEnvVars[2].name)
    assertNull(orchestratorEnvVars[2].value)
    assertEquals(BEARER_TOKEN_SECRET_NAME, orchestratorEnvVars[2].valueFrom.secretKeyRef.name)
    assertEquals(BEARER_TOKEN_SECRET_KEY, orchestratorEnvVars[2].valueFrom.secretKeyRef.key)
  }
}
