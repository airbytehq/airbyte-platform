/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectorRolloutApi
import io.airbyte.api.client.model.generated.ConnectorRolloutListRequestBody
import io.airbyte.api.client.model.generated.ConnectorRolloutListResponse
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputFind
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutOutput
import io.temporal.activity.Activity
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import org.slf4j.LoggerFactory
import java.io.IOException

@Singleton
class FindRolloutActivityImpl(private val airbyteApiClient: AirbyteApiClient) : FindRolloutActivity {
  private val log = LoggerFactory.getLogger(FindRolloutActivityImpl::class.java)

  init {
    log.info("Initialized FindRolloutActivityImpl")
  }

  override fun findRollout(input: ConnectorRolloutActivityInputFind): List<ConnectorRolloutOutput> {
    log.info("Finding rollout for ${input.dockerRepository}:${input.dockerImageTag}")

    val client: ConnectorRolloutApi = airbyteApiClient.connectorRolloutApi
    val body =
      ConnectorRolloutListRequestBody(
        input.dockerRepository,
        input.dockerImageTag,
        input.actorDefinitionId,
      )

    return try {
      val response: ConnectorRolloutListResponse = client.getConnectorRolloutsList(body)
      log.info("ConnectorRolloutListResponse = ${response.connectorRollouts}")
      response.connectorRollouts?.map {
        ConnectorRolloutActivityHelpers.mapToConnectorRollout(it)
      } ?: emptyList()
    } catch (e: IOException) {
      throw Activity.wrap(e)
    } catch (e: ClientException) {
      throw Activity.wrap(e)
    }
  }
}
