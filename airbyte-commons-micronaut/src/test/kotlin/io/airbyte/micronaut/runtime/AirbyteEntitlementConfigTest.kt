/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteStiggClientConfigDefaultTest {
  @Inject
  private lateinit var airbyteStiggClientConfig: AirbyteStiggClientConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", airbyteStiggClientConfig.apiKey)
    assertEquals(false, airbyteStiggClientConfig.enabled)
    assertEquals("", airbyteStiggClientConfig.sidecarHost)
    assertEquals(8800, airbyteStiggClientConfig.sidecarPort)
  }
}

@MicronautTest(propertySources = ["classpath:application-stigg.yml"])
internal class AirbyteEntitlementConfigStiggTest {
  @Inject
  private lateinit var airbyteStiggClientConfig: AirbyteStiggClientConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("test-api-key", airbyteStiggClientConfig.apiKey)
    assertEquals(true, airbyteStiggClientConfig.enabled)
    assertEquals("test-stigg-sidecar-host", airbyteStiggClientConfig.sidecarHost)
    assertEquals(8080, airbyteStiggClientConfig.sidecarPort)
  }
}
