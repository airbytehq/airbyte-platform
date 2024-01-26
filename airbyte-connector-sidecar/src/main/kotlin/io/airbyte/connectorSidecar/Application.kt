/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.connectorSidecar

import io.airbyte.commons.timer.Stopwatch
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.ApplicationContext

private val logger = KotlinLogging.logger {}

fun main(args: Array<String>) {
  val stopwatch = Stopwatch()

  var applicationContext: ApplicationContext?
  stopwatch.start().use {
    applicationContext = ApplicationContext.run()
  }

  logger.info { "Context started" }
  logger.info { stopwatch }

  var sidecar: Sidecar?
  stopwatch.start().use {
    sidecar = applicationContext?.getBean(Sidecar::class.java)
  }

  logger.info { "Sidecar created" }
  logger.info { stopwatch }

  stopwatch.start().use {
    sidecar?.run()
  }
  logger.info { "Sidecar done" }
  logger.info { stopwatch }
}
