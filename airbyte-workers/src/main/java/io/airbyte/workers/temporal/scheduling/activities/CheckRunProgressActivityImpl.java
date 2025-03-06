/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.workers.helpers.ProgressChecker;
import jakarta.inject.Singleton;
import java.io.IOException;

/**
 * Concrete CheckRunProgressActivity.
 */
@Singleton
public class CheckRunProgressActivityImpl implements CheckRunProgressActivity {

  private final ProgressChecker checker;

  public CheckRunProgressActivityImpl(final ProgressChecker checker) {
    this.checker = checker;
  }

  @Override
  public Output checkProgress(final Input input) {
    try {
      final boolean result = checker.check(input.getJobId(), input.getAttemptNo());

      return new Output(result);
    } catch (final IOException e) {
      throw new RetryableException(e);
    }
  }

}
