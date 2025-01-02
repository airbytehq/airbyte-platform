/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectorRolloutApi
import io.airbyte.api.client.model.generated.ConnectorRolloutRequestBody
import io.airbyte.api.client.model.generated.ConnectorRolloutResponse
import io.airbyte.connector.rollout.shared.ConnectorRolloutActivityHelpers
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputRollout
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.activity.Activity
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.io.IOException

private val logger = KotlinLogging.logger {}

@Singleton
class DoRolloutActivityImpl(private val airbyteApiClient: AirbyteApiClient) : DoRolloutActivity {
  init {
    logger.info { "Initialized DoRolloutActivityImpl" }
  }

  override fun doRollout(input: ConnectorRolloutActivityInputRollout): ConnectorRolloutOutput {
    logger.info { "Doing rollout for ${input.dockerRepository}:${input.dockerImageTag}" }

    val client: ConnectorRolloutApi = airbyteApiClient.connectorRolloutApi
    val body =
      ConnectorRolloutRequestBody(
        input.rolloutId,
        getRolloutStrategyFromInput(input.rolloutStrategy),
        input.actorIds,
        input.targetPercentage,
        input.updatedBy,
      )

    return try {
      val response: ConnectorRolloutResponse = client.doConnectorRollout(body)
      logger.info { "ConnectorRolloutResponse = ${response.data}" }
      ConnectorRolloutActivityHelpers.mapToConnectorRollout(response.data)
    } catch (e: IOException) {
      throw Activity.wrap(e)
    } catch (e: ClientException) {
      handleAirbyteApiClientException(e)
    }
  }
}
