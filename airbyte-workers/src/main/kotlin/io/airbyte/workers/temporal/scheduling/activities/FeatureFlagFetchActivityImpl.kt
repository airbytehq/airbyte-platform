/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity.FeatureFlagFetchInput
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity.FeatureFlagFetchOutput
import io.micronaut.http.HttpStatus
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.io.IOException
import java.util.UUID

/**
 * Fetches feature flags to be used in temporal workflows.
 */
@Singleton
class FeatureFlagFetchActivityImpl(
  private val airbyteApiClient: AirbyteApiClient,
  private val featureFlagClient: FeatureFlagClient,
) : FeatureFlagFetchActivity {
  /**
   * Get workspace id for a connection id.
   *
   * @param connectionId connection id
   * @return workspace id
   */
  fun getWorkspaceId(connectionId: UUID): UUID {
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
      throw RuntimeException("Unable to get workspace ID for connection", e)
    }
  }

  override fun getFeatureFlags(input: FeatureFlagFetchInput): FeatureFlagFetchOutput {
    // Left as a placeholder for when we have feature flags to read for the ConnectionManagerWorkflow
    return FeatureFlagFetchOutput(
      mutableMapOf(),
    )
  }
}
