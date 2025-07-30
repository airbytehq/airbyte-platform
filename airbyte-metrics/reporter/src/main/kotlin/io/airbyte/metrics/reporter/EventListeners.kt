/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.reporter

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.runtime.event.ApplicationShutdownEvent
import io.micronaut.runtime.event.ApplicationStartupEvent
import io.micronaut.runtime.event.annotation.EventListener
import jakarta.inject.Singleton
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

private val log = KotlinLogging.logger {}

/**
 * EventListeners registers event listeners for the startup and shutdown events from Micronaut.
 */
@Singleton
internal class EventListeners(
  private val emitters: List<Emitter>,
) {
  private val executor: ScheduledExecutorService = Executors.newScheduledThreadPool(emitters.size)

  /**
   * Manually registers all the emitters to run on startup.
   *
   * @param event unused but required in order to listen to the startup event.
   */
  @EventListener
  fun startEmitters(event: ApplicationStartupEvent?) {
    emitters.forEach(
      Consumer { emitter: Emitter ->
        executor.scheduleAtFixedRate(
          { emitter.emit() },
          0,
          emitter.getDuration().seconds,
          TimeUnit.SECONDS,
        )
      },
    )
    log.info { "registered ${emitters.size} emitters" }
  }

  /**
   * Attempts to cleanly shutdown the running emitters.
   *
   * @param event unused but required in order to listen to the shutdown event.
   */
  @EventListener
  fun stopEmitters(event: ApplicationShutdownEvent?) {
    log.info { "shutting down emitters" }
    executor.shutdown()
  }
}
