/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY;

import com.google.common.annotations.VisibleForTesting;
import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.GetTaskQueueNameRequest;
import io.airbyte.commons.temporal.TemporalJobType;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.micronaut.http.HttpStatus;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.openapitools.client.infrastructure.ClientException;

/**
 * RouteToSyncTaskQueueActivityImpl.
 */
@Slf4j
@Singleton
public class RouteToSyncTaskQueueActivityImpl implements RouteToSyncTaskQueueActivity {

  private final AirbyteApiClient airbyteApiClient;

  public RouteToSyncTaskQueueActivityImpl(final AirbyteApiClient airbyteApiClient) {
    this.airbyteApiClient = airbyteApiClient;
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public RouteToSyncTaskQueueOutput route(final RouteToSyncTaskQueueInput input) {
    return routeToSync(input);
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public RouteToSyncTaskQueueOutput routeToSync(final RouteToSyncTaskQueueInput input) {
    return routeToTask(input, TemporalJobType.SYNC);
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public RouteToSyncTaskQueueOutput routeToCheckConnection(final RouteToSyncTaskQueueInput input) {
    return routeToTask(input, TemporalJobType.CHECK_CONNECTION);
  }

  @VisibleForTesting
  protected RouteToSyncTaskQueueOutput routeToTask(final RouteToSyncTaskQueueInput input, final TemporalJobType jobType) {
    ApmTraceUtils.addTagsToTrace(Map.of(CONNECTION_ID_KEY, input.getConnectionId()));

    final var req = new GetTaskQueueNameRequest(input.getConnectionId(), jobType.toString());

    try {
      final var resp = airbyteApiClient.getConnectionApi().getTaskQueueName(req);

      return new RouteToSyncTaskQueueOutput(resp.getTaskQueueName());
    } catch (final ClientException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.getCode()) {
        log.warn("Unable to find connectionId {}", input.getConnectionId(), e);
        throw new RuntimeException(e);
      }

      log.warn("Encountered an error while attempting to route connection {} to a task queue: \n{}", input.getConnectionId(), e);
      throw new RetryableException(e);
    } catch (final IOException e) {
      log.warn("Encountered an error while attempting to route connection {} to a task queue: \n{}", input.getConnectionId(), e);
      throw new RetryableException(e);
    }
  }

}
