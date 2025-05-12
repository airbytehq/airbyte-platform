/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator

import io.airbyte.commons.logging.LogClientManager
import io.airbyte.commons.temporal.TemporalUtils
import io.airbyte.persistence.job.models.JobRunConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.runtime.event.annotation.EventListener
import io.micronaut.runtime.server.event.ServerStartupEvent
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.nio.file.Path

private val logger = KotlinLogging.logger {}

/**
 * Listen for changes in env variables.
 */
@Singleton
class EventListeners(
  @Named("workspaceRoot") private val workspaceRoot: Path,
  private val jobRunConfig: JobRunConfig,
  private val logClientManager: LogClientManager,
) {
  /**
   * Configures the logging for this app.
   *
   * @param unused required so Micronaut knows when to run this event-listener, but not used
   */
  @EventListener
  fun setLogging(unused: ServerStartupEvent?) {
    logger.debug { "started logging" }
    logClientManager.setJobMdc(TemporalUtils.getJobRoot(workspaceRoot, jobRunConfig.getJobId(), jobRunConfig.getAttemptId()))
  }
}
