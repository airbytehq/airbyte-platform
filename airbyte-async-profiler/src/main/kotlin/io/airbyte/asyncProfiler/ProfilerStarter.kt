/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.asyncProfiler

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Context
import io.micronaut.context.annotation.Value
import javax.annotation.PostConstruct

private val logger = KotlinLogging.logger {}

@Context
class ProfilerStarter(
  private val profilerThreadManager: ProfilerThreadManager,
  @Value("\${airbyte.source-keyword}") private val sourceKeyword: String,
  @Value("\${airbyte.destination-keyword}") private val destinationKeyword: String,
  @Value("\${airbyte.orchestrator-keyword}") private val orchestratorKeyword: String,
) {
  @PostConstruct
  fun start() {
    logger.info { "Starting Airbyte Connector Profiler" }
    val profilingJobs =
      listOf(
        ProfilerThreadManager.ProfilingJob(sourceKeyword, "cpu"),
        ProfilerThreadManager.ProfilingJob(destinationKeyword, "cpu"),
        ProfilerThreadManager.ProfilingJob(orchestratorKeyword, "cpu"),
      )
    profilerThreadManager.startProfilingJobs(profilingJobs)
    logger.info { "Finished Airbyte Connector Profiler" }
  }
}
