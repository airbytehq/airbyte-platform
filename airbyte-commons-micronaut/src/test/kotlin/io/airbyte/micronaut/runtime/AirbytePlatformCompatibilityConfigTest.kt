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
internal class AirbytePlatformCompatibilityConfigDefaultTest {
  @Inject
  private lateinit var airbytePlatformCompatibilityConfig: AirbytePlatformCompatibilityConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(DEFAULT_PLATFORM_COMPATIBILITY_REMOTE_TIMEOUT_MS, airbytePlatformCompatibilityConfig.remote.timeoutMs)
  }
}

@MicronautTest(propertySources = ["classpath:application-platform-compatibility.yml"])
internal class AirbytePlatformCompatibilityConfigOverridesTest {
  @Inject
  private lateinit var airbytePlatformCompatibilityConfig: AirbytePlatformCompatibilityConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(100L, airbytePlatformCompatibilityConfig.remote.timeoutMs)
  }
}
