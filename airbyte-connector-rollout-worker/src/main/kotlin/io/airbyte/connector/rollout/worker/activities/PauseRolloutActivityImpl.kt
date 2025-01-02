/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectorRolloutApi
import io.airbyte.api.client.model.generated.ConnectorRolloutResponse
import io.airbyte.api.client.model.generated.ConnectorRolloutState
import io.airbyte.api.client.model.generated.ConnectorRolloutUpdateStateRequestBody
import io.airbyte.connector.rollout.shared.ConnectorRolloutActivityHelpers
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputPause
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.activity.Activity
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.io.IOException

private val logger = KotlinLogging.logger {}

@Singleton
class PauseRolloutActivityImpl(private val airbyteApiClient: AirbyteApiClient) : PauseRolloutActivity {
  init {
    logger.info { "Initialized PauseRolloutActivityImpl" }
  }

  override fun pauseRollout(input: ConnectorRolloutActivityInputPause): ConnectorRolloutOutput {
    logger.info { "Pausing rollout for ${input.dockerRepository}:${input.dockerImageTag}" }

    val client: ConnectorRolloutApi = airbyteApiClient.connectorRolloutApi
    val body =
      ConnectorRolloutUpdateStateRequestBody(
        ConnectorRolloutState.PAUSED,
        input.rolloutId,
        input.dockerRepository,
        input.dockerImageTag,
        input.actorDefinitionId,
        null,
        null,
        input.pausedReason,
      )

    return try {
      val response: ConnectorRolloutResponse = client.updateConnectorRolloutState(body)
      logger.info { "ConnectorRolloutResponse = ${response.data}" }
      ConnectorRolloutActivityHelpers.mapToConnectorRollout(response.data)
    } catch (e: IOException) {
      throw Activity.wrap(e)
    } catch (e: ClientException) {
      handleAirbyteApiClientException(e)
    }
  }
}
