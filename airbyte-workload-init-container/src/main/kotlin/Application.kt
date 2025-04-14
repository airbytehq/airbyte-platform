/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.initContainer

import io.airbyte.initContainer.system.ExitWithCode
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.runtime.Micronaut.build

private val logger = KotlinLogging.logger {}

fun main() {
  logger.info { "Init start" }

  /**
   * The application entrypoint, InputFetcher, runs as part of ApplicationContext creation via a PostConstruct annotation.
   * This was done to improve startup time by preventing searching through all the beans.
   */

  val applicationBuilder =
    build()
      .deduceCloudEnvironment(false)
      .deduceEnvironment(false)

  val applicationContext = applicationBuilder.build()
  var exitCode = 0
  with(applicationContext) {
    start()
    try {
      val inputFetcher = getBean(InputFetcher::class.java)
      inputFetcher.fetch()
    } catch (e: ExitWithCode) {
      exitCode = e.code
    }
    stop()
  }

  if (exitCode != 0) {
    logger.info { "Init end with error $exitCode" }
    kotlin.system.exitProcess(exitCode)
  } else {
    logger.info { "Init end" }
  }
}
