/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.initContainer

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.ApplicationContext

private val logger = KotlinLogging.logger {}

fun main() {
  logger.info { "Init start" }

  ApplicationContext.builder()
    .deduceEnvironment(false)
    .start()

  logger.info { "Init end" }
}
