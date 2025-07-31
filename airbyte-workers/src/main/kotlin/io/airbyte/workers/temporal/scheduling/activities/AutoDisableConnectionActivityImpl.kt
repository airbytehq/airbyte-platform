/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import datadog.trace.api.Trace
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.commons.micronaut.EnvConstants
import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity.AutoDisableConnectionActivityInput
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity.AutoDisableConnectionOutput
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpStatus
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.io.IOException
import java.util.Map
import java.util.UUID

/**
 * AutoDisableConnectionActivityImpl.
 */
@Singleton
@Requires(env = [EnvConstants.CONTROL_PLANE])
class AutoDisableConnectionActivityImpl(
  private val airbyteApiClient: AirbyteApiClient,
) : AutoDisableConnectionActivity {
  // Given a connection id, this activity will make an api call that will set a connection
  // to INACTIVE if auto-disable conditions defined by the API are met.
  // The api call will also send notifications if a connection is disabled or warned if it has reached
  // halfway to disable limits
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun autoDisableFailingConnection(input: AutoDisableConnectionActivityInput): AutoDisableConnectionOutput {
    ApmTraceUtils.addTagsToTrace(Map.of<String?, UUID?>(CONNECTION_ID_KEY, input.connectionId))
    try {
      val autoDisableConnection =
        airbyteApiClient.connectionApi.autoDisableConnection(ConnectionIdRequestBody(input.connectionId!!))
      return AutoDisableConnectionOutput(autoDisableConnection.succeeded)
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
