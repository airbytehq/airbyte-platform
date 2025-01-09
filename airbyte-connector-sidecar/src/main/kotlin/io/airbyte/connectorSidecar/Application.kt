/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.connectorSidecar

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.runtime.Micronaut.build

private val logger = KotlinLogging.logger {}

fun main() {
  logger.info { "Sidecar start" }

  build()
    .deduceCloudEnvironment(false)
    .deduceEnvironment(false)
    .start()

  logger.info { "Sidecar end" }
}
