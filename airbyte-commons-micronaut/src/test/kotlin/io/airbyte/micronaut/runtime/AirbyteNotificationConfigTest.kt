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
internal class AirbyteNotificationConfigDefaultTest {
  @Inject
  private lateinit var arbyteNotificationConfig: AirbyteNotificationConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", arbyteNotificationConfig.customerIo.apiKey)
  }
}

@MicronautTest(propertySources = ["classpath:application-notification.yml"])
internal class AirbyteNotificationrConfigOverridesTest {
  @Inject
  private lateinit var arbyteNotificationConfig: AirbyteNotificationConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("test-api-key", arbyteNotificationConfig.customerIo.apiKey)
  }
}
