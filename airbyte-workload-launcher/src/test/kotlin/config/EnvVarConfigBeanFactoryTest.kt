/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package config

import io.airbyte.workers.process.Metadata.AWS_ACCESS_KEY_ID
import io.airbyte.workers.process.Metadata.AWS_SECRET_ACCESS_KEY
import io.airbyte.workload.launcher.config.EnvVarConfigBeanFactory
import io.airbyte.workload.launcher.config.EnvVarConfigBeanFactory.Companion.AWS_ASSUME_ROLE_ACCESS_KEY_ID_ENV_VAR
import io.airbyte.workload.launcher.config.EnvVarConfigBeanFactory.Companion.AWS_ASSUME_ROLE_SECRET_ACCESS_KEY_ENV_VAR
import io.airbyte.workload.launcher.config.EnvVarConfigBeanFactory.Companion.WORKLOAD_API_BEARER_TOKEN_ENV_VAR
import io.airbyte.workload.launcher.config.OrchestratorEnvSingleton
import io.fabric8.kubernetes.api.model.EnvVarSource
import io.fabric8.kubernetes.api.model.SecretKeySelector
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.util.UUID

class EnvVarConfigBeanFactoryTest {
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
  fun `workload api secret env creation`() {
    val factory = EnvVarConfigBeanFactory()
    val envMap =
      factory.workloadApiSecretEnv(
        BEARER_TOKEN_SECRET_NAME,
        BEARER_TOKEN_SECRET_KEY,
      )
    assertEquals(1, envMap.size)
    val envVarSource = envMap[WORKLOAD_API_BEARER_TOKEN_ENV_VAR]
    val secretKeyRef = envVarSource!!.secretKeyRef
    assertEquals(BEARER_TOKEN_SECRET_NAME, secretKeyRef.name)
    assertEquals(BEARER_TOKEN_SECRET_KEY, secretKeyRef.key)
  }

  @Test
  fun `orchestrator aws assumed role secret creation`() {
    val factory = EnvVarConfigBeanFactory()
    val envMap =
      factory.orchestratorAwsAssumedRoleSecretEnv(
        AWS_ASSUMED_ROLE_ACCESS_KEY,
        AWS_ASSUMED_ROLE_SECRET_KEY,
        AWS_ASSUMED_ROLE_SECRET_NAME,
      )

    assertEquals(2, envMap.size)
    val awsAccessKey = envMap[AWS_ASSUME_ROLE_ACCESS_KEY_ID_ENV_VAR]
    val awsAccessKeySecretRef = awsAccessKey!!.secretKeyRef
    assertEquals(AWS_ASSUMED_ROLE_SECRET_NAME, awsAccessKeySecretRef.name)
    assertEquals(AWS_ASSUMED_ROLE_ACCESS_KEY, awsAccessKeySecretRef.key)

    val awsSecretKey = envMap[AWS_ASSUME_ROLE_SECRET_ACCESS_KEY_ENV_VAR]
    val awsSecretKeyRef = awsSecretKey!!.secretKeyRef
    assertEquals(AWS_ASSUMED_ROLE_SECRET_NAME, awsSecretKeyRef.name)
    assertEquals(AWS_ASSUMED_ROLE_SECRET_KEY, awsSecretKeyRef.key)
  }

  @Test
  fun `workload api secret env creation with blank names`() {
    val factory = EnvVarConfigBeanFactory()
    val envMap =
      factory.workloadApiSecretEnv(
        "",
        BEARER_TOKEN_SECRET_KEY,
      )
    assertEquals(0, envMap.size)
  }

  @Test
  fun `orchestrator aws assumed role secret creation with blank names`() {
    val factory = EnvVarConfigBeanFactory()
    val envMap =
      factory.orchestratorAwsAssumedRoleSecretEnv(
        AWS_ASSUMED_ROLE_ACCESS_KEY,
        AWS_ASSUMED_ROLE_SECRET_KEY,
        "",
      )
    assertEquals(0, envMap.size)
  }

  @Test
  fun `connector aws assumed role secret creation`() {
    val factory = EnvVarConfigBeanFactory()
    val envList =
      factory.connectorAwsAssumedRoleSecretEnv(
        AWS_ASSUMED_ROLE_ACCESS_KEY,
        AWS_ASSUMED_ROLE_SECRET_KEY,
        AWS_ASSUMED_ROLE_SECRET_NAME,
      )

    val awsAccessKey = envList.find { it.name == AWS_ACCESS_KEY_ID }
    val awsAccessKeySecretRef = awsAccessKey!!.valueFrom.secretKeyRef
    assertEquals(AWS_ASSUMED_ROLE_SECRET_NAME, awsAccessKeySecretRef.name)
    assertEquals(AWS_ASSUMED_ROLE_ACCESS_KEY, awsAccessKeySecretRef.key)

    val awsSecretKey = envList.find { it.name == AWS_SECRET_ACCESS_KEY }
    val awsSecretKeyRef = awsSecretKey!!.valueFrom.secretKeyRef
    assertEquals(AWS_ASSUMED_ROLE_SECRET_NAME, awsSecretKeyRef.name)
    assertEquals(AWS_ASSUMED_ROLE_SECRET_KEY, awsSecretKeyRef.key)
  }

  @Test
  fun `test final env vars contain secret env vars an non-secret env vars`() {
    val factory: OrchestratorEnvSingleton = mockk()
    every { factory.orchestratorEnvMap(any()) } returns (mapOf(Pair(ENV_VAR_NAME1, ENV_VAR_VALUE1), Pair(ENV_VAR_NAME2, ENV_VAR_VALUE2)))
    every {
      factory.secretEnvMap()
    } returns (
      mapOf(
        Pair(ENV_VAR_NAME3, EnvVarSource(null, null, null, SecretKeySelector(BEARER_TOKEN_SECRET_KEY, BEARER_TOKEN_SECRET_NAME, false))),
      )
    )
    every { factory.orchestratorEnvVars(any()) } answers { callOriginal() }
    val orchestratorEnvVars =
      factory.orchestratorEnvVars(
        UUID.randomUUID(),
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
