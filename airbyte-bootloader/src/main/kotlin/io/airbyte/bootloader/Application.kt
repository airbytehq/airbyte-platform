/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.kotlin.context.getBean
import io.micronaut.runtime.Micronaut
import kotlin.system.exitProcess

private val log = KotlinLogging.logger {}

/**
 * Main application entry point responsible for starting the server and invoking the bootstrapping of the Airbyte environment.
 */
fun main(args: Array<String>) {
  try {
    val applicationContext =
      Micronaut
        .build(*args)
        .deduceCloudEnvironment(false)
        .deduceEnvironment(false)
        .start()
    applicationContext.getBean<Bootloader>().load()
    exitProcess(0)
  } catch (e: Exception) {
    log.error(e) { "Unable to bootstrap Airbyte environment." }
    exitProcess(-1)
  }
}
