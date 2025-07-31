/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import datadog.trace.api.Trace
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.WORKSPACE_ID_KEY
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.workers.helpers.RetryStateClient
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.HydrateInput
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.HydrateOutput
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.PersistInput
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.PersistOutput
import io.micronaut.http.HttpStatus
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.io.IOException
import java.util.Map
import java.util.UUID

/**
 * Concrete implementation of RetryStatePersistenceActivity. Delegates to non-temporal business
 * logic via RetryStatePersistence.
 */
@Singleton
class RetryStatePersistenceActivityImpl(
  private val airbyteApiClient: AirbyteApiClient,
  private val client: RetryStateClient,
) : RetryStatePersistenceActivity {
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun hydrateRetryState(input: HydrateInput): HydrateOutput {
    ApmTraceUtils.addTagsToTrace(Map.of<String?, UUID?>(CONNECTION_ID_KEY, input.connectionId))
    val workspaceId = getWorkspaceId(input.connectionId!!)
    ApmTraceUtils.addTagsToTrace(Map.of<String?, UUID?>(WORKSPACE_ID_KEY, workspaceId))

    val manager = client.hydrateRetryState(input.jobId, workspaceId)

    return HydrateOutput(manager)
  }

  override fun persistRetryState(input: PersistInput): PersistOutput {
    try {
      val success = client.persistRetryState(input.jobId!!, input.connectionId!!, input.manager!!)
      return PersistOutput(success)
    } catch (e: ClientException) {
      if (e.statusCode == HttpStatus.NOT_FOUND.code) {
        throw e
      }
      throw RetryableException(e)
    } catch (e: IOException) {
      throw RetryableException(e)
    }
  }

  private fun getWorkspaceId(connectionId: UUID): UUID {
    try {
      val workspace =
        airbyteApiClient.workspaceApi.getWorkspaceByConnectionId(ConnectionIdRequestBody(connectionId))
      return workspace.workspaceId
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
