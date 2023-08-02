/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.helpers.LogClientSingleton;
import io.airbyte.config.helpers.LogConfigs;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import org.slf4j.Logger;

/**
 * Concrete implementation of AppendToAttemptLogActivity.
 */
@Singleton
public class AppendToAttemptLogActivityImpl implements AppendToAttemptLogActivity {

  private final Logger logger;
  private final LogClientSingleton logClientSingleton;
  private final LogConfigs logConfigs;
  private final Path workspaceRoot;
  private final WorkerEnvironment workerEnvironment;

  public AppendToAttemptLogActivityImpl(
                                        @Named("appendToAttemptLogger") final Logger logger,
                                        final LogClientSingleton logClientSingleton,
                                        final LogConfigs logConfigs,
                                        @Named("workspaceRoot") final Path workspaceRoot,
                                        final WorkerEnvironment workerEnvironment) {
    this.logger = logger;
    this.logClientSingleton = logClientSingleton;
    this.logConfigs = logConfigs;
    this.workspaceRoot = workspaceRoot;
    this.workerEnvironment = workerEnvironment;
  }

  @Override
  public LogOutput log(final LogInput input) {
    setMdc(input);

    final var msg = input.getMessage();

    switch (input.getLevel()) {
      case ERROR -> logger.error(msg);
      case WARN -> logger.warn(msg);
      default -> logger.info(msg);
    }

    return new LogOutput(true);
  }

  private void setMdc(final LogInput input) {
    final Path jobRoot = TemporalUtils.getJobRoot(
        workspaceRoot,
        String.valueOf(input.getJobId()),
        input.getAttemptNumber());

    logClientSingleton.setJobMdc(workerEnvironment, logConfigs, jobRoot);
  }

}
