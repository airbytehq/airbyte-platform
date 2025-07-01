/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectorRolloutApi
import io.airbyte.api.client.model.generated.ConnectorRolloutReadRequestBody
import io.airbyte.api.client.model.generated.ConnectorRolloutReadResponse
import io.airbyte.connector.rollout.shared.ConnectorRolloutActivityHelpers
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputGet
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.activity.Activity
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.io.IOException

private val logger = KotlinLogging.logger {}

@Singleton
class GetRolloutActivityImpl(
  private val airbyteApiClient: AirbyteApiClient,
) : GetRolloutActivity {
  init {
    logger.info { "Initialized GetRolloutActivityImpl" }
  }

  override fun getRollout(input: ConnectorRolloutActivityInputGet): ConnectorRolloutOutput {
    logger.info { "Getting rollout for ${input.dockerRepository}:${input.dockerImageTag}" }

    val client: ConnectorRolloutApi = airbyteApiClient.connectorRolloutApi
    val body = ConnectorRolloutReadRequestBody(input.rolloutId)

    return try {
      val response: ConnectorRolloutReadResponse = client.getConnectorRolloutById(body)
      logger.info { "ConnectorRolloutReadResponse = ${response.data}" }
      val mappedResponse = ConnectorRolloutActivityHelpers.mapToConnectorRollout(response.data)
      logger.info { "ConnectorRolloutReadResponse: actorSyncs.size=${mappedResponse.actorSyncs?.size}" }
      mappedResponse
    } catch (e: IOException) {
      throw Activity.wrap(e)
    } catch (e: ClientException) {
      handleAirbyteApiClientException(e)
    }
  }
}
