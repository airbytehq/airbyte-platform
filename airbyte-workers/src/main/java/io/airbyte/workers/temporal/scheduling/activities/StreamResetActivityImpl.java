/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;

import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.DeleteStreamResetRecordsForJobRequest;
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
 * StreamResetActivityImpl.
 */
@Singleton
@Requires(env = EnvConstants.CONTROL_PLANE)
public class StreamResetActivityImpl implements StreamResetActivity {

  private final AirbyteApiClient airbyteApiClient;

  public StreamResetActivityImpl(final AirbyteApiClient airbyteApiClient) {
    this.airbyteApiClient = airbyteApiClient;
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void deleteStreamResetRecordsForJob(final DeleteStreamResetRecordsForJobInput input) {
    ApmTraceUtils.addTagsToTrace(Map.of(CONNECTION_ID_KEY, input.getConnectionId(), JOB_ID_KEY, input.getJobId()));

    try {
      airbyteApiClient.getJobsApi()
          .deleteStreamResetRecordsForJob(new DeleteStreamResetRecordsForJobRequest(input.getConnectionId(), input.getJobId()));
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
