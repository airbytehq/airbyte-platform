/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY;

import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.InternalOperationResult;
import io.airbyte.commons.micronaut.EnvConstants;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpStatus;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Map;
import org.openapitools.client.infrastructure.ClientException;

/**
 * AutoDisableConnectionActivityImpl.
 */
@Singleton
@Requires(env = EnvConstants.CONTROL_PLANE)
public class AutoDisableConnectionActivityImpl implements AutoDisableConnectionActivity {

  private final AirbyteApiClient airbyteApiClient;

  @SuppressWarnings("LineLength")
  public AutoDisableConnectionActivityImpl(final AirbyteApiClient airbyteApiClient) {
    this.airbyteApiClient = airbyteApiClient;
  }

  // Given a connection id, this activity will make an api call that will set a connection
  // to INACTIVE if auto-disable conditions defined by the API are met.
  // The api call will also send notifications if a connection is disabled or warned if it has reached
  // halfway to disable limits
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public AutoDisableConnectionOutput autoDisableFailingConnection(final AutoDisableConnectionActivityInput input) {
    ApmTraceUtils.addTagsToTrace(Map.of(CONNECTION_ID_KEY, input.getConnectionId()));
    try {
      final InternalOperationResult autoDisableConnection =
          airbyteApiClient.getConnectionApi().autoDisableConnection(new ConnectionIdRequestBody(input.getConnectionId()));
      return new AutoDisableConnectionOutput(autoDisableConnection.getSucceeded());
    } catch (final ClientException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.getCode()) {
        throw e;
      }
      throw new RetryableException(e);
    } catch (final IOException e) {
      throw new RetryableException(e);
    }
  }

}
