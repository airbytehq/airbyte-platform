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
internal class AirbyteStripeConfigDefaultTest {
  @Inject
  private lateinit var airbyteStripeConfig: AirbyteStripeConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", airbyteStripeConfig.apiKey)
    assertEquals("", airbyteStripeConfig.endpointSecret)
  }
}

@MicronautTest(propertySources = ["classpath:application-stripe.yml"])
internal class AirbyteStripeConfigOverridesTest {
  @Inject
  private lateinit var airbyteStripeConfig: AirbyteStripeConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("test-api-key", airbyteStripeConfig.apiKey)
    assertEquals("test-endpoint-secret", airbyteStripeConfig.endpointSecret)
  }
}
