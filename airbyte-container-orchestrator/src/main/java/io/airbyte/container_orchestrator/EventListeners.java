/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container_orchestrator;

import io.airbyte.commons.logging.LogClientManager;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.micronaut.runtime.event.annotation.EventListener;
import io.micronaut.runtime.server.event.ServerStartupEvent;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listen for changes in env variables.
 */
@Singleton
public class EventListeners {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final JobRunConfig jobRunConfig;
  private final LogClientManager logClientManager;
  private final Path workspaceRoot;

  @Inject
  EventListeners(
                 @Named("workspaceRoot") final Path workspaceRoot,
                 final JobRunConfig jobRunConfig,
                 final LogClientManager logClientManager) {
    this.jobRunConfig = jobRunConfig;
    this.logClientManager = logClientManager;
    this.workspaceRoot = workspaceRoot;
  }

  /**
   * Configures the logging for this app.
   *
   * @param unused required so Micronaut knows when to run this event-listener, but not used
   */
  @EventListener
  void setLogging(final ServerStartupEvent unused) {
    log.debug("started logging");
    logClientManager.setJobMdc(TemporalUtils.getJobRoot(workspaceRoot, jobRunConfig.getJobId(), jobRunConfig.getAttemptId()));
  }

}
