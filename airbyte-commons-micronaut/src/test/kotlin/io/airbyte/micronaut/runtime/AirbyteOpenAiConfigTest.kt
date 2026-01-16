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
internal class AirbyteOpenAiConfigDefaultTest {
  @Inject
  private lateinit var airbyteOpenAiConfig: AirbyteOpenAiConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", airbyteOpenAiConfig.apiKeys.failedSyncAssistant)
  }
}

@MicronautTest(propertySources = ["classpath:application-openai.yml"])
internal class AirbyteOpenAiConfigOverridesTest {
  @Inject
  private lateinit var airbyteOpenAiConfig: AirbyteOpenAiConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("test-failed-sync-assistant", airbyteOpenAiConfig.apiKeys.failedSyncAssistant)
  }
}
