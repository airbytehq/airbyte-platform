/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectorRolloutApi
import io.airbyte.api.client.model.generated.ConnectorRolloutResponse
import io.airbyte.api.client.model.generated.ConnectorRolloutState
import io.airbyte.api.client.model.generated.ConnectorRolloutUpdateStateRequestBody
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputCleanup
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.activity.Activity
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.io.IOException

private val logger = KotlinLogging.logger {}

@Singleton
class CleanupActivityImpl(private val airbyteApiClient: AirbyteApiClient) : CleanupActivity {
  init {
    logger.info { "Initialized CleanupActivityImpl" }
  }

  override fun cleanup(input: ConnectorRolloutActivityInputCleanup) {
    logger.info { "Cleaning up rollout for ${input.dockerRepository}:${input.dockerImageTag}" }

    val client: ConnectorRolloutApi = airbyteApiClient.connectorRolloutApi
    val body =
      ConnectorRolloutUpdateStateRequestBody(
        ConnectorRolloutState.valueOf(input.newState.toString().uppercase()),
        input.rolloutId,
        input.dockerRepository,
        input.dockerImageTag,
        input.actorDefinitionId,
      )

    return try {
      val response: ConnectorRolloutResponse = client.updateConnectorRolloutState(body)
      logger.info { "ConnectorRolloutResponse = ${response.data}" }
    } catch (e: IOException) {
      throw Activity.wrap(e)
    } catch (e: ClientException) {
      handleAirbyteApiClientException(e)
    }
  }
}
