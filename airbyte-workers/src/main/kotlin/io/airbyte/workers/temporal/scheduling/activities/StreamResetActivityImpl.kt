/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import datadog.trace.api.Trace
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.DeleteStreamResetRecordsForJobRequest
import io.airbyte.commons.micronaut.EnvConstants
import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.workers.temporal.scheduling.activities.StreamResetActivity.DeleteStreamResetRecordsForJobInput
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpStatus
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.io.IOException

/**
 * StreamResetActivityImpl.
 */
@Singleton
@Requires(env = [EnvConstants.CONTROL_PLANE])
class StreamResetActivityImpl(
  private val airbyteApiClient: AirbyteApiClient,
) : StreamResetActivity {
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun deleteStreamResetRecordsForJob(input: DeleteStreamResetRecordsForJobInput) {
    ApmTraceUtils.addTagsToTrace(mapOf(CONNECTION_ID_KEY to input.connectionId, JOB_ID_KEY to input.jobId))

    try {
      airbyteApiClient.jobsApi
        .deleteStreamResetRecordsForJob(DeleteStreamResetRecordsForJobRequest(input.connectionId!!, input.jobId!!))
    } catch (e: ClientException) {
      if (e.statusCode == HttpStatus.NOT_FOUND.getCode()) {
        throw e
      }
      throw RetryableException(e)
    } catch (e: IOException) {
      throw RetryableException(e)
    }
  }
}
