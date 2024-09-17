/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectorRolloutApi
import io.airbyte.api.client.model.generated.ConnectorRolloutRequestBody
import io.airbyte.api.client.model.generated.ConnectorRolloutResponse
import io.airbyte.api.client.model.generated.ConnectorRolloutStrategy
import io.airbyte.config.ConnectorRolloutConnection
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputUpdate
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutOutput
import io.temporal.activity.Activity
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.UUID

@Singleton
class UpdateRolloutActivityImpl(private val airbyteApiClient: AirbyteApiClient) : UpdateRolloutActivity {
  private val log = LoggerFactory.getLogger(UpdateRolloutActivityImpl::class.java)

  init {
    log.info("Initialized UpdateRolloutActivityImpl")
  }

  override fun updateRollout(input: ConnectorRolloutActivityInputUpdate): ConnectorRolloutOutput {
    log.info("Updating rollout for ${input.dockerRepository}:${input.dockerImageTag}")

    val client: ConnectorRolloutApi = airbyteApiClient.connectorRolloutApi
    val body =
      ConnectorRolloutRequestBody(
        input.rolloutId,
        input.actorIds,
        ConnectorRolloutStrategy.MANUAL,
      )

    return try {
      val response: ConnectorRolloutResponse = client.doConnectorRollout(body)
      log.info("ConnectorRolloutUpdateResponse = ${response.data}")
      ConnectorRolloutActivityHelpers.mapToConnectorRollout(response.data)
    } catch (e: IOException) {
      throw Activity.wrap(e)
    } catch (e: ClientException) {
      throw Activity.wrap(e)
    }
  }

  fun getConnectionById(connectionId: UUID): ConnectorRolloutConnection {
    return ConnectorRolloutConnection().withId(connectionId.toString())
  }
}
