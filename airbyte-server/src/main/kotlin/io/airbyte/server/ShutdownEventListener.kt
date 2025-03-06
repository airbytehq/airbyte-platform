/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.runtime.event.ApplicationShutdownEvent
import jakarta.inject.Singleton

private val log = KotlinLogging.logger {}

/**
 * Listens for shutdown signal and keeps server alive for 20 seconds to process requests on the fly.
 */
@Singleton
class ShutdownEventListener : ApplicationEventListener<ApplicationShutdownEvent?> {
  @Value("\${airbyte.shutdown.delay_ms}")
  private val shutdownDelayMillis = 0

  override fun onApplicationEvent(event: ApplicationShutdownEvent?) {
    log.info { "ShutdownEvent before wait" }
    try {
      // Sleep 20 seconds to make sure server is wrapping up last remaining requests before
      // closing the connections.
      Thread.sleep(shutdownDelayMillis.toLong())
    } catch (ex: Exception) {
      // silently fail at this stage because server is terminating.
      log.warn { "exception: $ex" }
    }
    log.info { "ShutdownEvent after wait" }
  }
}
