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
internal class AirbyteContainerConfigDefaultTest {
  @Inject
  private lateinit var airbyteContainerConfig: AirbyteContainerConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(false, airbyteContainerConfig.rootlessWorkload)
  }
}

@MicronautTest(propertySources = ["classpath:application-container.yml"])
internal class AirbyteContainerConfigOverridesTest {
  @Inject
  private lateinit var airbyteContainerConfig: AirbyteContainerConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(true, airbyteContainerConfig.rootlessWorkload)
  }
}
