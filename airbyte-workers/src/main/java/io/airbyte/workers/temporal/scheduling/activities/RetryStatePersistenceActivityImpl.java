/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.workers.helpers.RetryStateClient;
import jakarta.inject.Singleton;

/**
 * Concrete implementation of RetryStatePersistenceActivity. Delegates to non-temporal business
 * logic via RetryStatePersistence.
 */
@Singleton
public class RetryStatePersistenceActivityImpl implements RetryStatePersistenceActivity {

  final RetryStateClient client;

  public RetryStatePersistenceActivityImpl(final RetryStateClient client) {
    this.client = client;
  }

  @Override
  public HydrateOutput hydrateRetryState(final HydrateInput input) {
    final var manager = client.hydrateRetryState(input.getJobId());

    return new HydrateOutput(manager);
  }

  @Override
  public PersistOutput persistRetryState(final PersistInput input) {
    final var success = client.persistRetryState(input.getJobId(), input.getConnectionId(), input.getManager());

    return new PersistOutput(success);
  }

}
