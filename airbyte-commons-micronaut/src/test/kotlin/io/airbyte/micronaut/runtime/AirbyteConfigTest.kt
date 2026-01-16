/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.airbyte.config.Configs
import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteConfigDefaultTest {
  @Inject
  private lateinit var airbyteConfig: AirbyteConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(false, airbyteConfig.acceptanceTestEnabled)
    assertEquals("", airbyteConfig.airbyteUrl)
    assertEquals(DEFAULT_AIRBYTE_DEPLOYMENT_ENVIRONMENT, airbyteConfig.deploymentEnvironment)
    assertEquals("", airbyteConfig.licenseKey)
    assertEquals(Configs.AirbyteEdition.COMMUNITY, airbyteConfig.edition)
    assertEquals(DEFAULT_AIRBYTE_VERSION, airbyteConfig.version)
    assertEquals(DEFAULT_AIRBYTE_WORKSPACE_ROOT, airbyteConfig.workspaceRoot)
    assertEquals(DEFAULT_AIRBYTE_PROTOCOL_MINIMUM_VERSION, airbyteConfig.protocol.minVersion)
    assertEquals(DEFAULT_AIRBYTE_PROTOCOL_MAXIMUM_VERSION, airbyteConfig.protocol.maxVersion)
  }
}

@MicronautTest(propertySources = ["classpath:application-airbyte.yml"])
internal class AirbyteConfigTest {
  @Inject
  private lateinit var airbyteConfig: AirbyteConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(true, airbyteConfig.acceptanceTestEnabled)
    assertEquals("http://some-url", airbyteConfig.airbyteUrl)
    assertEquals("test", airbyteConfig.deploymentEnvironment)
    assertEquals("test-license-key", airbyteConfig.licenseKey)
    assertEquals(Configs.AirbyteEdition.ENTERPRISE, airbyteConfig.edition)
    assertEquals("dev-test", airbyteConfig.version)
    assertEquals("/workspace-test", airbyteConfig.workspaceRoot)
    assertEquals("0.0.1", airbyteConfig.protocol.minVersion)
    assertEquals("0.0.2", airbyteConfig.protocol.maxVersion)
  }
}
