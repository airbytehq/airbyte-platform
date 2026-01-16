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
internal class AirbyteApiConfigDefaultTest {
  @Inject
  private lateinit var airbyteApiConfig: AirbyteApiConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", airbyteApiConfig.host)
  }
}

@MicronautTest(propertySources = ["classpath:application-api.yml"])
internal class AirbyteApiConfigOverridesTest {
  @Inject
  private lateinit var airbyteApiConfig: AirbyteApiConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("test-host", airbyteApiConfig.host)
  }
}
