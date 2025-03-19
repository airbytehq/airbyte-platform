/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionState;
import io.airbyte.api.model.generated.ConnectionStateCreateOrUpdate;
import io.airbyte.commons.converters.StateConverter;
import io.airbyte.commons.server.errors.SyncIsRunningException;
import io.airbyte.config.StateWrapper;
import io.airbyte.config.persistence.StatePersistence;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * StateHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
public class StateHandler {

  private final StatePersistence statePersistence;
  private final JobHistoryHandler jobHistoryHandler;

  public StateHandler(final StatePersistence statePersistence, final JobHistoryHandler jobHistoryHandler) {
    this.statePersistence = statePersistence;
    this.jobHistoryHandler = jobHistoryHandler;
  }

  public ConnectionState getState(final ConnectionIdRequestBody connectionIdRequestBody) throws IOException {
    final UUID connectionId = connectionIdRequestBody.getConnectionId();
    final Optional<StateWrapper> currentState = statePersistence.getCurrentState(connectionId);
    return StateConverter.toApi(connectionId, currentState.orElse(null));
  }

  public ConnectionState createOrUpdateState(final ConnectionStateCreateOrUpdate connectionStateCreateOrUpdate) throws IOException {
    final UUID connectionId = connectionStateCreateOrUpdate.getConnectionId();

    final StateWrapper convertedCreateOrUpdate = StateConverter.toInternal(connectionStateCreateOrUpdate.getConnectionState());
    statePersistence.updateOrCreateState(connectionId, convertedCreateOrUpdate);
    final Optional<StateWrapper> newInternalState = statePersistence.getCurrentState(connectionId);

    return StateConverter.toApi(connectionId, newInternalState.orElse(null));
  }

  public ConnectionState createOrUpdateStateSafe(final ConnectionStateCreateOrUpdate connectionStateCreateOrUpdate) throws IOException {
    if (jobHistoryHandler.getLatestRunningSyncJob(connectionStateCreateOrUpdate.getConnectionId()).isPresent()) {
      throw new SyncIsRunningException("State cannot be updated while a sync is running for this connection.");
    }

    return createOrUpdateState(connectionStateCreateOrUpdate);
  }

  public void wipeState(final UUID connectionId) throws IOException {
    statePersistence.eraseState(connectionId);
  }

}
