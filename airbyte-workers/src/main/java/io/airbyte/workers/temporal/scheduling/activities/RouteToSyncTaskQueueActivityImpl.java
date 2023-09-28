/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY;

import com.google.common.annotations.VisibleForTesting;
import datadog.trace.api.Trace;
import io.airbyte.api.client.generated.ConnectionApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.GetTaskQueueNameRequest;
import io.airbyte.commons.temporal.TemporalJobType;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.micronaut.http.HttpStatus;
import jakarta.inject.Singleton;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * RouteToSyncTaskQueueActivityImpl.
 */
@Slf4j
@Singleton
public class RouteToSyncTaskQueueActivityImpl implements RouteToSyncTaskQueueActivity {

  private final ConnectionApi connectionApi;

  public RouteToSyncTaskQueueActivityImpl(final ConnectionApi connectionApi) {
    this.connectionApi = connectionApi;
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

    final var req = new GetTaskQueueNameRequest()
        .connectionId(input.getConnectionId())
        .temporalJobType(jobType.toString());

    try {
      final var resp = connectionApi.getTaskQueueName(req);

      return new RouteToSyncTaskQueueOutput(resp.getTaskQueueName());
    } catch (final ApiException e) {
      if (e.getCode() == HttpStatus.NOT_FOUND.getCode()) {
        log.warn("Unable to find connectionId {}", input.getConnectionId(), e);
        throw new RuntimeException(e);
      }

      log.warn("Encountered an error while attempting to route connection {} to a task queue: \n{}", input.getConnectionId(), e);
      throw new RetryableException(e);
    }
  }

}
