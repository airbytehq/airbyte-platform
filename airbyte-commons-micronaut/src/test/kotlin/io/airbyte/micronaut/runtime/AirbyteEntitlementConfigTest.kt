/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteEntitlementConfigDefaultTest {
  @Inject
  private lateinit var airbyteEntitlementConfig: AirbyteEntitlementConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(EntitlementClientType.DEFAULT, airbyteEntitlementConfig.client)
    assertEquals("", airbyteEntitlementConfig.stigg.apiKey)
    assertEquals(false, airbyteEntitlementConfig.stigg.enabled)
    assertEquals("", airbyteEntitlementConfig.stigg.sidecarHost)
    assertEquals(0, airbyteEntitlementConfig.stigg.sidecarPort)
  }
}

@MicronautTest(propertySources = ["classpath:application-entitlements-stigg.yml"])
internal class AirbyteEntitlementConfigStiggTest {
  @Inject
  private lateinit var airbyteEntitlementConfig: AirbyteEntitlementConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(EntitlementClientType.STIGG, airbyteEntitlementConfig.client)
    assertEquals("test-api-key", airbyteEntitlementConfig.stigg.apiKey)
    assertEquals(true, airbyteEntitlementConfig.stigg.enabled)
    assertEquals("test-stigg-sidecar-host", airbyteEntitlementConfig.stigg.sidecarHost)
    assertEquals(8080, airbyteEntitlementConfig.stigg.sidecarPort)
  }
}
