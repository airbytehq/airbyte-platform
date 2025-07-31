/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.UseCommandCheck
import io.airbyte.featureflag.UseSyncV2
import io.airbyte.featureflag.Workspace
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity.FeatureFlagFetchInput
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity.FeatureFlagFetchOutput
import io.micronaut.http.HttpStatus
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.lang.invoke.MethodHandles
import java.util.List
import java.util.Map
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
    val workspaceId = getWorkspaceId(input.connectionId!!)

    val useCommandCheck =
      featureFlagClient.boolVariation(
        UseCommandCheck,
        Multi(List.of<Context?>(Connection(input.connectionId!!), Workspace(workspaceId))),
      )

    val useSyncV2 =
      featureFlagClient.boolVariation(
        UseSyncV2,
        Multi(List.of<Context?>(Connection(input.connectionId!!), Workspace(workspaceId))),
      )

    return FeatureFlagFetchOutput(
      Map.of<String?, Boolean?>(
        UseCommandCheck.key,
        useCommandCheck,
        UseSyncV2.key,
        useSyncV2,
      ),
    )
  }

  companion object {
    private val log: Logger? = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
  }
}
