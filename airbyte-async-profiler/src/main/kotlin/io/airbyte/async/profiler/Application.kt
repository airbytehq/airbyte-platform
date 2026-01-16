/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.async.profiler

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.runtime.Micronaut.build

private val logger = KotlinLogging.logger {}

fun main() {
  logger.info { "Profiler start" }

  /**
   * The application entrypoint, [ProfilerStarter], runs as part of ApplicationContext creation via a PostConstruct annotation.
   * This was done to improve startup time by preventing searching through all the beans.
   */

  build()
    .deduceCloudEnvironment(false)
    .deduceEnvironment(false)
    .start()
    // Explicitly call stop so that the application shuts down after [ProfilerStarter] runs
    .stop()

  logger.info { "Profiler end" }
}
