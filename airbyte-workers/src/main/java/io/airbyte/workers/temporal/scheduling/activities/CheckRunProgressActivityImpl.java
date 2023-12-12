/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.workers.helpers.ProgressChecker;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * Concrete CheckRunProgressActivity.
 */
@Slf4j
@Singleton
public class CheckRunProgressActivityImpl implements CheckRunProgressActivity {

  private final ProgressChecker checker;

  public CheckRunProgressActivityImpl(final ProgressChecker checker) {
    this.checker = checker;
  }

  @Override
  public Output checkProgress(final Input input) {
    final boolean result = checker.check(input.getJobId(), input.getAttemptNo());

    return new Output(result);
  }

}
