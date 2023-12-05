/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.logging.LoggingHelper;
import io.airbyte.commons.logging.MdcScope;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.helpers.LogClientSingleton;
import io.airbyte.config.helpers.LogConfigs;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concrete implementation of AppendToAttemptLogActivity.
 */
@Singleton
public class AppendToAttemptLogActivityImpl implements AppendToAttemptLogActivity {

  @VisibleForTesting
  protected Logger logger = LoggerFactory.getLogger(AppendToAttemptLogActivityImpl.class);
  private final LogClientSingleton logClientSingleton;
  private final LogConfigs logConfigs;
  private final Path workspaceRoot;
  private final WorkerEnvironment workerEnvironment;

  public AppendToAttemptLogActivityImpl(
                                        final LogClientSingleton logClientSingleton,
                                        final LogConfigs logConfigs,
                                        @Named("workspaceRoot") final Path workspaceRoot,
                                        final WorkerEnvironment workerEnvironment) {
    this.logClientSingleton = logClientSingleton;
    this.logConfigs = logConfigs;
    this.workspaceRoot = workspaceRoot;
    this.workerEnvironment = workerEnvironment;
  }

  @Override
  public LogOutput log(final LogInput input) {
    if (input.getJobId() == null || input.getAttemptNumber() == null) {
      return new LogOutput(false);
    }

    setMdc(input);

    try {
      final var msg = input.getMessage();
      try (final var mdcScope = new MdcScope.Builder()
          .setLogPrefix(LoggingHelper.PLATFORM_LOGGER_PREFIX)
          .setPrefixColor(LoggingHelper.Color.CYAN_BACKGROUND)
          .build()) {

        switch (input.getLevel()) {
          case ERROR -> logger.error(msg);
          case WARN -> logger.warn(msg);
          default -> logger.info(msg);
        }
      }

      return new LogOutput(true);
    } finally {
      unsetMdc();
    }
  }

  private void setMdc(final LogInput input) {
    final Path jobRoot = TemporalUtils.getJobRoot(
        workspaceRoot,
        String.valueOf(input.getJobId()),
        input.getAttemptNumber());

    logClientSingleton.setJobMdc(workerEnvironment, logConfigs, jobRoot);
  }

  private void unsetMdc() {
    logClientSingleton.setJobMdc(workerEnvironment, logConfigs, null);
  }

}
