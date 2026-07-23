/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.featureflag.EnforceDataWorkerCapacity
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
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
   * Get organization id for a connection id.
   *
   * @param connectionId connection id
   * @return organization id
   */
  private fun getOrganizationId(connectionId: UUID): UUID? {
    try {
      val workspace =
        airbyteApiClient.workspaceApi.getWorkspaceByConnectionId(ConnectionIdRequestBody(connectionId))
      return workspace.organizationId
    } catch (e: ClientException) {
      if (e.statusCode == HttpStatus.NOT_FOUND.getCode()) {
        throw e
      }
      throw RetryableException(e)
    } catch (e: IOException) {
      throw RetryableException(e)
    }
  }

  override fun getFeatureFlags(input: FeatureFlagFetchInput): FeatureFlagFetchOutput {
    val organizationId =
      input.organizationId ?: input.connectionId?.let { connectionId ->
        getOrganizationId(connectionId)
      }

    if (organizationId == null) {
      return FeatureFlagFetchOutput(mutableMapOf())
    }

    val enforcementEnabled =
      try {
        featureFlagClient.boolVariation(EnforceDataWorkerCapacity, Organization(organizationId))
      } catch (_: Exception) {
        false
      }

    return FeatureFlagFetchOutput(
      mutableMapOf(EnforceDataWorkerCapacity.key to enforcementEnabled),
    )
  }
}
