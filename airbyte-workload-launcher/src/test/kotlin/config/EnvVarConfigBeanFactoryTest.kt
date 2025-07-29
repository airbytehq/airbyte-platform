/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package config

import io.airbyte.workers.pod.Metadata.AWS_ACCESS_KEY_ID
import io.airbyte.workers.pod.Metadata.AWS_SECRET_ACCESS_KEY
import io.airbyte.workload.launcher.config.EnvVarConfigBeanFactory
import io.airbyte.workload.launcher.constants.EnvVarConstants.AWS_ASSUME_ROLE_ACCESS_KEY_ID_ENV_VAR
import io.airbyte.workload.launcher.constants.EnvVarConstants.AWS_ASSUME_ROLE_SECRET_ACCESS_KEY_ENV_VAR
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class EnvVarConfigBeanFactoryTest {
  companion object {
    const val AWS_ASSUMED_ROLE_ACCESS_KEY = "accessKey"
    const val AWS_ASSUMED_ROLE_SECRET_KEY = "secretKey"
    const val AWS_ASSUMED_ROLE_SECRET_NAME = "secretName"
  }

  @Test
  fun `orchestrator aws assumed role secret creation`() {
    val factory = EnvVarConfigBeanFactory()
    val envMap =
      factory.awsAssumedRoleSecretEnv(
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
  fun `orchestrator aws assumed role secret creation with blank names`() {
    val factory = EnvVarConfigBeanFactory()
    val envMap =
      factory.awsAssumedRoleSecretEnv(
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
}
