/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.connectorSidecar

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.runtime.Micronaut

private val logger = KotlinLogging.logger {}

fun main() {
  logger.info { "Sidecar start" }

  Micronaut.build()
    .deduceEnvironment(false)
    .start()

  logger.info { "Sidecar end" }
}
