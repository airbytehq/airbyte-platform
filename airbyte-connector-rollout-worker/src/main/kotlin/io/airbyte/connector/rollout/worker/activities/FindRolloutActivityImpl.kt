/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectorRolloutApi
import io.airbyte.api.client.model.generated.ConnectorRolloutListRequestBody
import io.airbyte.api.client.model.generated.ConnectorRolloutListResponse
import io.airbyte.connector.rollout.shared.ConnectorRolloutActivityHelpers
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFind
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.activity.Activity
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.io.IOException

private val logger = KotlinLogging.logger {}

@Singleton
class FindRolloutActivityImpl(private val airbyteApiClient: AirbyteApiClient) : FindRolloutActivity {
  init {
    logger.info { "Initialized FindRolloutActivityImpl" }
  }

  override fun findRollout(input: ConnectorRolloutActivityInputFind): List<ConnectorRolloutOutput> {
    logger.info { "Finding rollout for ${input.dockerRepository}:${input.dockerImageTag}" }

    val client: ConnectorRolloutApi = airbyteApiClient.connectorRolloutApi
    val body =
      ConnectorRolloutListRequestBody(
        input.dockerImageTag,
        input.actorDefinitionId,
      )

    return try {
      val response: ConnectorRolloutListResponse = client.getConnectorRolloutsList(body)
      logger.info { "ConnectorRolloutListResponse = ${response.connectorRollouts}" }
      response.connectorRollouts?.map {
        ConnectorRolloutActivityHelpers.mapToConnectorRollout(it)
      } ?: emptyList()
    } catch (e: IOException) {
      throw Activity.wrap(e)
    } catch (e: ClientException) {
      handleAirbyteApiClientException(e)
    }
  }
}
