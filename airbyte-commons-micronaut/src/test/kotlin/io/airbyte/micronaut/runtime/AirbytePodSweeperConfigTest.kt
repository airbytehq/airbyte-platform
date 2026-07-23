/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.runtime

import io.airbyte.micronaut.runtime.AirbytePodSweeperConfig
import io.airbyte.micronaut.runtime.DEFAULT_POD_SWEEPER_RATE_MINUTES
import io.airbyte.micronaut.runtime.DEFAULT_POD_SWEEPER_RUNNING_TTL_MINUTES
import io.airbyte.micronaut.runtime.DEFAULT_POD_SWEEPER_SUCCEEDED_TTL_MINUTES
import io.airbyte.micronaut.runtime.DEFAULT_POD_SWEEPER_UNSUCCESSFUL_TTL_MINUTES
import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration

@MicronautTest(environments = [Environment.TEST])
internal class AirbytePodSweeperConfigDefaultTest {
  @Inject
  private lateinit var airbytePodSweeperConfig: AirbytePodSweeperConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(DEFAULT_POD_SWEEPER_RUNNING_TTL_MINUTES, airbytePodSweeperConfig.runningTtl)
    assertEquals(DEFAULT_POD_SWEEPER_SUCCEEDED_TTL_MINUTES, airbytePodSweeperConfig.succeededTtl)
    assertEquals(DEFAULT_POD_SWEEPER_UNSUCCESSFUL_TTL_MINUTES, airbytePodSweeperConfig.unsuccessfulTtl)
    assertEquals(Duration.parse(DEFAULT_POD_SWEEPER_RATE_MINUTES), airbytePodSweeperConfig.rate)
  }
}

@MicronautTest(propertySources = ["classpath:application-pod-sweeper.yml"])
internal class AirbytePodSweeperConfigOverridesTest {
  @Inject
  private lateinit var airbytePodSweeperConfig: AirbytePodSweeperConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(1L, airbytePodSweeperConfig.runningTtl)
    assertEquals(2L, airbytePodSweeperConfig.succeededTtl)
    assertEquals(3L, airbytePodSweeperConfig.unsuccessfulTtl)
    assertEquals(Duration.parse("PT1M"), airbytePodSweeperConfig.rate)
  }
}
