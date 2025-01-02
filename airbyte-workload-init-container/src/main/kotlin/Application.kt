/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.initContainer

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.ApplicationContext

private val logger = KotlinLogging.logger {}

fun main() {
  logger.info { "Init start" }

  /**
   * The application entrypoint, InputFetcher, runs as part of ApplicationContext creation via a PostConstruct annotation.
   * This was done to improve startup time by preventing searching through all the beans.
   */

  ApplicationContext.builder()
    .deduceEnvironment(false)
    .start()
    // Explicitly call stop so that the application shuts down after InputFetcher runs
    .stop()

  logger.info { "Init end" }
}
