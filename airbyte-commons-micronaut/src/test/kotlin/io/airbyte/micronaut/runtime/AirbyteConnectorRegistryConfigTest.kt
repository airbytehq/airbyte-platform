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
internal class AirbyteConnectorRegistryConfigDefaultTest {
  @Inject
  private lateinit var airbyteConnectorRegistryConfig: AirbyteConnectorRegistryConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", airbyteConnectorRegistryConfig.enterprise.enterpriseStubsUrl)
    assertEquals("", airbyteConnectorRegistryConfig.remote.baseUrl)
    assertEquals(DEFAULT_CONNECTOR_REGISTRY_REMOTE_TIMEOUT_MS, airbyteConnectorRegistryConfig.remote.timeoutMs)
    assertEquals(DEFAULT_CONNECTOR_REGISTRY_SEED_PROVIDER, airbyteConnectorRegistryConfig.seedProvider)
  }
}

@MicronautTest(propertySources = ["classpath:application-connector-registry.yml"])
internal class AirbyteConnectorRegistryConfigOverridesTest {
  @Inject
  private lateinit var airbyteConnectorRegistryConfig: AirbyteConnectorRegistryConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("http://enterprise-stubs-test-url", airbyteConnectorRegistryConfig.enterprise.enterpriseStubsUrl)
    assertEquals("http://remote-test", airbyteConnectorRegistryConfig.remote.baseUrl)
    assertEquals(60L, airbyteConnectorRegistryConfig.remote.timeoutMs)
    assertEquals("test", airbyteConnectorRegistryConfig.seedProvider)
  }
}
