/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectorRolloutApi
import io.airbyte.api.client.model.generated.ConnectorRolloutStartRequestBody
import io.airbyte.api.client.model.generated.ConnectorRolloutStartResponse
import io.airbyte.connector.rollout.shared.ConnectorRolloutActivityHelpers
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputStart
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.activity.Activity
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.io.IOException

private val logger = KotlinLogging.logger {}

@Singleton
class StartRolloutActivityImpl(private val airbyteApiClient: AirbyteApiClient) : StartRolloutActivity {
  init {
    logger.info { "Initialized StartRolloutActivityImpl" }
  }

  override fun startRollout(
    workflowRunId: String,
    input: ConnectorRolloutActivityInputStart,
  ): ConnectorRolloutOutput {
    logger.info { "Activity startRollout Initializing rollout for ${input.dockerRepository}:${input.dockerImageTag}" }

    val client: ConnectorRolloutApi = airbyteApiClient.connectorRolloutApi
    val body =
      ConnectorRolloutStartRequestBody(
        input.rolloutId,
        workflowRunId,
        getRolloutStrategyFromInput(input.rolloutStrategy),
        input.updatedBy,
        true,
      )

    return try {
      logger.info { "Activity startRollout starting for ${input.dockerRepository}:${input.dockerImageTag}; baseUrl=${client.baseUrl}" }
      val response: ConnectorRolloutStartResponse = client.startConnectorRollout(body)

      logger.info { "Activity startRollout ConnectorRolloutStartResponse=${response.data}" }

      ConnectorRolloutActivityHelpers.mapToConnectorRollout(response.data)
    } catch (e: IOException) {
      throw Activity.wrap(e)
    } catch (e: ClientException) {
      handleAirbyteApiClientException(e)
    }
  }
}
