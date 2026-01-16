/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.async.profiler

import io.airbyte.async.profiler.runtime.AirbyteAsyncProfilerConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Context
import javax.annotation.PostConstruct

private val logger = KotlinLogging.logger {}

@Context
class ProfilerStarter(
  private val profilerThreadManager: ProfilerThreadManager,
  private val airbyteAsyncProfilerConfig: AirbyteAsyncProfilerConfig,
) {
  @PostConstruct
  fun start() {
    logger.info { "Starting Airbyte Connector Profiler" }
    val profilingJobs =
      listOf(
        ProfilerThreadManager.ProfilingJob(airbyteAsyncProfilerConfig.sourceKeyword, airbyteAsyncProfilerConfig.profilingMode),
        ProfilerThreadManager.ProfilingJob(airbyteAsyncProfilerConfig.destinationKeyword, airbyteAsyncProfilerConfig.profilingMode),
        ProfilerThreadManager.ProfilingJob(airbyteAsyncProfilerConfig.orchestratorKeyword, airbyteAsyncProfilerConfig.profilingMode),
      )
    profilerThreadManager.startProfilingJobs(profilingJobs)
    logger.info { "Finished Airbyte Connector Profiler" }
  }
}
