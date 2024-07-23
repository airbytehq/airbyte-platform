/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container_orchestrator;

import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.config.EnvConfigs;
import io.airbyte.config.helpers.LogClientSingleton;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.micronaut.runtime.event.annotation.EventListener;
import io.micronaut.runtime.server.event.ServerStartupEvent;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.lang.invoke.MethodHandles;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listen for changes in env variables.
 */
@Singleton
public class EventListeners {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final EnvConfigs configs;
  private final JobRunConfig jobRunConfig;
  private final LogConfigs logConfigs;

  @Inject
  EventListeners(final EnvConfigs configs,
                 final JobRunConfig jobRunConfig,
                 final LogConfigs logConfigs) {
    this.configs = configs;
    this.jobRunConfig = jobRunConfig;
    this.logConfigs = logConfigs;
  }

  /**
   * Configures the logging for this app.
   *
   * @param unused required so Micronaut knows when to run this event-listener, but not used
   */
  @EventListener
  void setLogging(final ServerStartupEvent unused) {
    log.debug("started logging");

    // make sure the new configuration is picked up
    final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    ctx.reconfigure();

    LogClientSingleton.getInstance().setJobMdc(
        configs.getWorkerEnvironment(),
        logConfigs,
        TemporalUtils.getJobRoot(configs.getWorkspaceRoot(), jobRunConfig.getJobId(), jobRunConfig.getAttemptId()));
  }

}
