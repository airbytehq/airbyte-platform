/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY;

import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.ConnectionApi;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.InternalOperationResult;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.temporal.config.WorkerMode;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.util.Map;

/**
 * AutoDisableConnectionActivityImpl.
 */
@Singleton
@Requires(env = WorkerMode.CONTROL_PLANE)
public class AutoDisableConnectionActivityImpl implements AutoDisableConnectionActivity {

  private final FeatureFlags featureFlags;
  private final ConnectionApi connectionApi;

  @SuppressWarnings("LineLength")
  public AutoDisableConnectionActivityImpl(final FeatureFlags featureFlags, final ConnectionApi connectionApi) {
    this.featureFlags = featureFlags;
    this.connectionApi = connectionApi;
  }

  // Given a connection id, this activity will make an api call that will set a connection
  // to INACTIVE if auto-disable conditions defined by the API are met.
  // The api call will also send notifications if a connection is disabled or warned if it has reached
  // halfway to disable limits
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public AutoDisableConnectionOutput autoDisableFailingConnection(final AutoDisableConnectionActivityInput input) {
    ApmTraceUtils.addTagsToTrace(Map.of(CONNECTION_ID_KEY, input.getConnectionId()));
    if (featureFlags.autoDisablesFailingConnections()) {
      try {
        final InternalOperationResult autoDisableConnection = AirbyteApiClient.retryWithJitterThrows(
            () -> connectionApi.autoDisableConnection(new ConnectionIdRequestBody()
                .connectionId(input.getConnectionId())),
            "auto disable connection");
        return new AutoDisableConnectionOutput(autoDisableConnection.getSucceeded());
      } catch (final Exception e) {
        throw new RetryableException(e);
      }
    }

    return new AutoDisableConnectionOutput(false);
  }

}
