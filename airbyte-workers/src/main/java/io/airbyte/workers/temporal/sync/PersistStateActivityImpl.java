/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import static io.airbyte.commons.converters.StateConverter.convertClientStateTypeToInternal;
import static io.airbyte.config.helpers.StateMessageHelper.isMigration;
import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY;

import com.google.common.annotations.VisibleForTesting;
import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionState;
import io.airbyte.api.client.model.generated.ConnectionStateCreateOrUpdate;
import io.airbyte.commons.converters.StateConverter;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.State;
import io.airbyte.config.StateType;
import io.airbyte.config.StateWrapper;
import io.airbyte.config.helpers.StateMessageHelper;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.workers.internal.sync_persistence.SyncPersistenceImpl;
import jakarta.inject.Singleton;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * PersistStateActivityImpl.
 */
@Singleton
public class PersistStateActivityImpl implements PersistStateActivity {

  private final AirbyteApiClient airbyteApiClient;
  private final FeatureFlags featureFlags;

  public PersistStateActivityImpl(final AirbyteApiClient airbyteApiClient, final FeatureFlags featureFlags) {
    this.airbyteApiClient = airbyteApiClient;
    this.featureFlags = featureFlags;
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public boolean persist(final UUID connectionId, final StandardSyncOutput syncOutput, final ConfiguredAirbyteCatalog configuredCatalog) {
    MetricClientFactory.getMetricClient().count(OssMetricsRegistry.ACTIVITY_PERSIST_STATE, 1);

    ApmTraceUtils.addTagsToTrace(Map.of(CONNECTION_ID_KEY, connectionId.toString()));

    if (syncOutput.getCommitStateAsap() != null && syncOutput.getCommitStateAsap()) {
      // CommitStateAsap feature flag is true, states have been persisted during the replication activity.
      return false;
    }

    final State state = syncOutput.getState();
    if (state != null) {
      // todo: these validation logic should happen on server side.
      try {
        final Optional<StateWrapper> maybeStateWrapper = StateMessageHelper.getTypedState(state.getState(), featureFlags.useStreamCapableState());
        if (maybeStateWrapper.isPresent()) {
          MetricClientFactory.getMetricClient().count(OssMetricsRegistry.STATE_COMMIT_ATTEMPT_FROM_PERSIST_STATE, 1);

          final ConnectionState previousState =
              AirbyteApiClient.retryWithJitter(
                  () -> airbyteApiClient.getStateApi().getState(new ConnectionIdRequestBody().connectionId(connectionId)),
                  "get state");

          validate(configuredCatalog, maybeStateWrapper, previousState);

          AirbyteApiClient.retryWithJitter(
              () -> {
                airbyteApiClient.getStateApi().createOrUpdateState(
                    new ConnectionStateCreateOrUpdate()
                        .connectionId(connectionId)
                        .connectionState(StateConverter.toClient(connectionId, maybeStateWrapper.orElse(null))));
                return null;
              },
              "create or update state");
        }
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * Validates whether it is safe to persist the new state based on the previously saved state.
   *
   * @param configuredCatalog The configured catalog of streams for the connection.
   * @param newState The new state.
   * @param previousState The previous state.
   */
  private void validate(final ConfiguredAirbyteCatalog configuredCatalog,
                        final Optional<StateWrapper> newState,
                        final ConnectionState previousState) {
    /*
     * If state validation is enabled and the previous state exists and is not empty, make sure that
     * state will not be lost as part of the migration from legacy -> per stream.
     *
     * Otherwise, it is okay to update if the previous state is missing or empty.
     */
    if (featureFlags.needStateValidation() && !SyncPersistenceImpl.isStateEmpty(previousState)) {
      final StateType newStateType = newState.get().getStateType();
      final StateType prevStateType = convertClientStateTypeToInternal(previousState.getStateType());

      if (isMigration(newStateType, prevStateType) && newStateType == StateType.STREAM) {
        validateStreamStates(newState.get(), configuredCatalog);
      }
    }
  }

  @VisibleForTesting
  void validateStreamStates(final StateWrapper state, final ConfiguredAirbyteCatalog configuredCatalog) {
    SyncPersistenceImpl.validateStreamStates(state, configuredCatalog);
  }

}
