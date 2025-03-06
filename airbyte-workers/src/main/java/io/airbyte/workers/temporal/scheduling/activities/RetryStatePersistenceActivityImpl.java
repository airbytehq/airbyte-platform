/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.WORKSPACE_ID_KEY;

import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.workers.helpers.RetryStateClient;
import io.micronaut.http.HttpStatus;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.openapitools.client.infrastructure.ClientException;

/**
 * Concrete implementation of RetryStatePersistenceActivity. Delegates to non-temporal business
 * logic via RetryStatePersistence.
 */
@Singleton
public class RetryStatePersistenceActivityImpl implements RetryStatePersistenceActivity {

  private final AirbyteApiClient airbyteApiClient;
  private final RetryStateClient client;

  public RetryStatePersistenceActivityImpl(final AirbyteApiClient airbyteApiClient, final RetryStateClient client) {
    this.airbyteApiClient = airbyteApiClient;
    this.client = client;
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
    try {
      final var success = client.persistRetryState(input.getJobId(), input.getConnectionId(), input.getManager());
      return new PersistOutput(success);
    } catch (final ClientException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.getCode()) {
        throw e;
      }
      throw new RetryableException(e);
    } catch (final IOException e) {
      throw new RetryableException(e);
    }
  }

  private UUID getWorkspaceId(final UUID connectionId) {
    try {
      final WorkspaceRead workspace =
          airbyteApiClient.getWorkspaceApi().getWorkspaceByConnectionId(new ConnectionIdRequestBody(connectionId));
      return workspace.getWorkspaceId();
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
