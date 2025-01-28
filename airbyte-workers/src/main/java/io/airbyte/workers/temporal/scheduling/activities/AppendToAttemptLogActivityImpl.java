/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.logging.LogClientManager;
import io.airbyte.commons.logging.LogSource;
import io.airbyte.commons.logging.MdcScope;
import io.airbyte.commons.temporal.TemporalUtils;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concrete implementation of AppendToAttemptLogActivity.
 */
@Singleton
@SuppressWarnings("PMD.AppendToAttemptLogActivityImpl")
public class AppendToAttemptLogActivityImpl implements AppendToAttemptLogActivity {

  @VisibleForTesting
  protected Logger logger = LoggerFactory.getLogger(AppendToAttemptLogActivityImpl.class);
  private final LogClientManager logClientManager;
  private final Path workspaceRoot;

  public AppendToAttemptLogActivityImpl(final LogClientManager logClientManager,
                                        @Named("workspaceRoot") final Path workspaceRoot) {
    this.logClientManager = logClientManager;
    this.workspaceRoot = workspaceRoot;
  }

  @SuppressWarnings("PMD.UnusedLocalVariable")
  @Override
  public LogOutput log(final LogInput input) {
    if (input.getJobId() == null || input.getAttemptNumber() == null) {
      return new LogOutput(false);
    }

    setMdc(input);

    try {
      final var msg = input.getMessage();
      try (final var mdcScope = new MdcScope.Builder().setExtraMdcEntries(LogSource.PLATFORM.toMdc()).build()) {

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
        (long) input.getAttemptNumber());

    logClientManager.setJobMdc(jobRoot);
  }

  private void unsetMdc() {
    logClientManager.setJobMdc(null);
  }

}
