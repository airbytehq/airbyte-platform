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
internal class AirbyteSidecarConfigDefaultTest {
  @Inject
  private lateinit var airbyteSidecarConfig: AirbyteSidecarConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(DEFAULT_SIDECAR_FILE_TIMEOUT_MINUTES, airbyteSidecarConfig.fileTimeoutMinutes)
    assertEquals(DEFAULT_SIDECAR_FILE_TIMEOUT_MINUTES_WITHIN_SYNC, airbyteSidecarConfig.fileTimeoutMinutesWithinSync)
  }
}

@MicronautTest(propertySources = ["classpath:application-sidecar.yml"])
internal class AirbyteSidecarConfigOverridesTest {
  @Inject
  private lateinit var airbyteSidecarConfig: AirbyteSidecarConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(1, airbyteSidecarConfig.fileTimeoutMinutes)
    assertEquals(2, airbyteSidecarConfig.fileTimeoutMinutesWithinSync)
  }
}
