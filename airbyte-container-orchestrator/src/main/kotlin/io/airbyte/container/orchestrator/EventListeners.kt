/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator

import io.airbyte.commons.logging.LogClientManager
import io.airbyte.container.orchestrator.worker.RecordSchemaValidator
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.discovery.event.ServiceReadyEvent
import io.micronaut.runtime.event.annotation.EventListener
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.nio.file.Path

private val logger = KotlinLogging.logger {}

/**
 * Listen for changes in env variables.
 */
@Singleton
class EventListeners(
  @Named("jobRoot") private val jobRoot: Path,
  private val logClientManager: LogClientManager,
  private val recordSchemaValidator: RecordSchemaValidator?,
) {
  /**
   * Configures the logging for this app.
   *
   * @param unused required so Micronaut knows when to run this event-listener, but not used
   */
  @EventListener
  fun setLogging(unused: ServiceReadyEvent) {
    logger.debug { "started logging" }
    logClientManager.setJobMdc(jobRoot)
  }

  /**
   * Initializes the [RecordSchemaValidator], if present.
   *
   * @param unused required so Micronaut knows when to run this event-listener, but not used
   */
  @EventListener
  fun initializeValidator(unused: ServiceReadyEvent) {
    logger.debug { "Initializing record schema validator ${if (recordSchemaValidator == null) " (not present)" else "(present)"}..." }
    recordSchemaValidator?.initializeSchemaValidator()
  }
}
