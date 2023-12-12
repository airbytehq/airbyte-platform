/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.WORKSPACE_ID_KEY;

import datadog.trace.api.Trace;
import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.workers.helpers.RetryStateClient;
import jakarta.inject.Singleton;
import java.util.Map;
import java.util.UUID;

/**
 * Concrete implementation of RetryStatePersistenceActivity. Delegates to non-temporal business
 * logic via RetryStatePersistence.
 */
@Singleton
public class RetryStatePersistenceActivityImpl implements RetryStatePersistenceActivity {

  final RetryStateClient client;
  final WorkspaceApi workspaceApi;

  public RetryStatePersistenceActivityImpl(final RetryStateClient client,
                                           final WorkspaceApi workspaceApi) {
    this.client = client;
    this.workspaceApi = workspaceApi;
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public HydrateOutput hydrateRetryState(final HydrateInput input) {
    ApmTraceUtils.addTagsToTrace(Map.of(CONNECTION_ID_KEY, input.getConnectionId()));
    final var workspaceId = getWorkspaceId(input.getConnectionId());
    ApmTraceUtils.addTagsToTrace(Map.of(WORKSPACE_ID_KEY, workspaceId));

    final var manager = client.hydrateRetryState(input.getJobId(), workspaceId);

    return new HydrateOutput(manager);
  }

  @Override
  public PersistOutput persistRetryState(final PersistInput input) {
    final var success = client.persistRetryState(input.getJobId(), input.getConnectionId(), input.getManager());

    return new PersistOutput(success);
  }

  private UUID getWorkspaceId(final UUID connectionId) {
    try {
      final WorkspaceRead workspace = workspaceApi.getWorkspaceByConnectionId(new ConnectionIdRequestBody().connectionId(connectionId));
      return workspace.getWorkspaceId();
    } catch (final ApiException e) {
      throw new RetryableException(e);
    }
  }

}
