/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.featureflag.CheckReplicationProgress;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.FeatureFlagClient;
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

  private final FeatureFlagClient featureFlagClient;

  public CheckRunProgressActivityImpl(final ProgressChecker checker, final FeatureFlagClient featureFlagClient) {
    this.checker = checker;
    this.featureFlagClient = featureFlagClient;
  }

  @Override
  public Output checkProgress(final Input input) {
    final var enabled = featureFlagClient.boolVariation(CheckReplicationProgress.INSTANCE, new Connection(input.getConnectionId()));

    if (!enabled) {
      return new Output(false);
    }

    final boolean result = checker.check(input.getJobId(), input.getAttemptNo());

    return new Output(result);
  }

}
