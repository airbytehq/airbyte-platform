/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.helper

import io.airbyte.commons.constants.WorkerConstants
import io.fabric8.kubernetes.api.model.EnvVar
import io.mockk.every
import io.mockk.spyk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow

private const val CONNECTOR_NAME = "postgres"
private const val CONNECTOR_VERSION = "2.0.5"
private const val IMAGE = "postgres:2.0.5"
private const val REGISTRY_NAME = "registry.internal:1234"
private const val REPOSITORY_ORG = "airbyte"
private const val IMAGE_WITH_PORT = "/$REGISTRY_NAME$REPOSITORY_ORG$IMAGE"

internal class ConnectorApmSupportHelperTest {
  private lateinit var supportHelper: ConnectorApmSupportHelper

  @BeforeEach
  fun setup() {
    supportHelper = ConnectorApmSupportHelper()
  }

  @Test
  fun testExtractAirbyteVersionFromImageName() {
    val imageName = supportHelper.getImageName(IMAGE)
    val imageVersion = supportHelper.getImageVersion(IMAGE)

    assertEquals(CONNECTOR_NAME, imageName)
    assertEquals(CONNECTOR_VERSION, imageVersion)
  }

  @Test
  fun testExtractAirbyteVersionFromImageNameWithRegistryPort() {
    val imageName = supportHelper.getImageName(IMAGE_WITH_PORT)
    val imageVersion = supportHelper.getImageVersion(IMAGE_WITH_PORT)

    assertEquals("/$REGISTRY_NAME$REPOSITORY_ORG$CONNECTOR_NAME", imageName)
    assertEquals(CONNECTOR_VERSION, imageVersion)
  }

  @Test
  fun testExtractAirbyteVersionFromBlankImageName() {
    val blankString = ""
    val nullString = null

    assertEquals(blankString, supportHelper.getImageName(blankString))
    assertEquals(blankString, supportHelper.getImageVersion(blankString))
    assertEquals(nullString, supportHelper.getImageName(nullString))
    assertEquals(nullString, supportHelper.getImageVersion(nullString))
  }

  @Test
  fun testAddServerNameAndVersionToEnvVars() {
    val envVars: MutableList<EnvVar> = mutableListOf()

    supportHelper.addServerNameAndVersionToEnvVars(IMAGE, envVars)

    assertEquals(2, envVars.size)
    assertTrue(envVars.contains(EnvVar(io.airbyte.commons.envvar.EnvVar.DD_SERVICE.name, CONNECTOR_NAME, null)))
    assertTrue(envVars.contains(EnvVar(io.airbyte.commons.envvar.EnvVar.DD_VERSION.name, CONNECTOR_VERSION, null)))
  }

  @Test
  fun testAddServerNameAndVersionToEnvVarsNullImage() {
    val envVars: MutableList<EnvVar> = mutableListOf()
    assertDoesNotThrow { supportHelper.addServerNameAndVersionToEnvVars(null, envVars) }
  }

  @Test
  fun testAddServerNameAndVersionToEnvVarsEmptyEnvVars() {
    assertDoesNotThrow { supportHelper.addServerNameAndVersionToEnvVars(IMAGE, null) }
  }

  @Test
  fun testAddApmEnvVars() {
    val ddAgentHost = "fake-agent-host"
    val ddDogstatsdPort = "12345"
    val envVars: MutableList<EnvVar> = mutableListOf()
    val supportHelper =
      spyk(ConnectorApmSupportHelper()) {
        every { getEnv() } returns
          mutableMapOf(
            io.airbyte.commons.envvar.EnvVar.DD_AGENT_HOST.name to ddAgentHost,
            io.airbyte.commons.envvar.EnvVar.DD_DOGSTATSD_PORT.name to ddDogstatsdPort,
          )
      }

    supportHelper.addApmEnvVars(envVars)

    assertEquals(3, envVars.size)
    assertTrue(envVars.contains(EnvVar(io.airbyte.commons.envvar.EnvVar.JAVA_OPTS.name, WorkerConstants.DD_ENV_VAR, null)))
    assertTrue(envVars.contains(EnvVar(io.airbyte.commons.envvar.EnvVar.DD_AGENT_HOST.name, ddAgentHost, null)))
    assertTrue(envVars.contains(EnvVar(io.airbyte.commons.envvar.EnvVar.DD_DOGSTATSD_PORT.name, ddDogstatsdPort, null)))
  }

  @Test
  fun testAddApmEnvVarsMissingEnvVars() {
    val envVars: MutableList<EnvVar> = mutableListOf()
    val supportHelper =
      spyk(ConnectorApmSupportHelper()) {
        every { getEnv() } returns mutableMapOf()
      }
    supportHelper.addApmEnvVars(envVars)

    assertEquals(1, envVars.size)
    assertTrue(envVars.contains(EnvVar(io.airbyte.commons.envvar.EnvVar.JAVA_OPTS.name, WorkerConstants.DD_ENV_VAR, null)))
  }
}
