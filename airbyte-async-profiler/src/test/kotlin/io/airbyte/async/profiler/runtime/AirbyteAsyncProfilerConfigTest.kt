/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.async.profiler.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteAsyncProfilerConfigDefaultTest {
  @Inject
  private lateinit var airbyteAsyncProfilerConfig: AirbyteAsyncProfilerConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(DEFAULT_PROFILER_DESTINATION_KEYWORD, airbyteAsyncProfilerConfig.destinationKeyword)
    assertEquals(DEFAULT_PROFILER_ORCHESTRATOR_KEYWORD, airbyteAsyncProfilerConfig.orchestratorKeyword)
    assertEquals(DEFAULT_PROFILER_MODE, airbyteAsyncProfilerConfig.profilingMode)
    assertEquals(DEFAULT_PROFILER_SOURCE_KEYWORD, airbyteAsyncProfilerConfig.sourceKeyword)
  }
}

@MicronautTest(propertySources = ["classpath:application-async-profiler.yml"])
internal class AirbyteAsyncProfilerConfigOverridesTest {
  @Inject
  private lateinit var airbyteAsyncProfilerConfig: AirbyteAsyncProfilerConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("test-destination-keyword", airbyteAsyncProfilerConfig.destinationKeyword)
    assertEquals("test-orchestrator-keyword", airbyteAsyncProfilerConfig.orchestratorKeyword)
    assertEquals("test-profiling-mode", airbyteAsyncProfilerConfig.profilingMode)
    assertEquals("test-source-keyword", airbyteAsyncProfilerConfig.sourceKeyword)
  }
}
