/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server

import io.airbyte.micronaut.runtime.AirbyteShutdownConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.runtime.event.ApplicationShutdownEvent
import jakarta.inject.Singleton

private val log = KotlinLogging.logger {}

/**
 * Listens for shutdown signal and keeps server alive for configured amount of time to process requests on the fly.
 */
@Singleton
class ShutdownEventListener(
  private val airbyteShutdownConfig: AirbyteShutdownConfig,
) : ApplicationEventListener<ApplicationShutdownEvent?> {
  override fun onApplicationEvent(event: ApplicationShutdownEvent?) {
    log.info { "ShutdownEvent before wait" }
    try {
      // Sleep to make sure server is wrapping up last remaining requests before
      // closing the connections.
      Thread.sleep(airbyteShutdownConfig.delayMs)
    } catch (ex: Exception) {
      // silently fail at this stage because server is terminating.
      log.warn { "exception: $ex" }
    }
    log.info { "ShutdownEvent after wait" }
  }
}
