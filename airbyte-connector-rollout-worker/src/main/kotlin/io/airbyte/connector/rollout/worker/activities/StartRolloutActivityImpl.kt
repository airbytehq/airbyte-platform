/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectorRolloutApi
import io.airbyte.api.client.model.generated.ConnectorRolloutCreateRequestBody
import io.airbyte.api.client.model.generated.ConnectorRolloutCreateResponse
import io.airbyte.api.client.model.generated.ConnectorRolloutStartRequestBody
import io.airbyte.api.client.model.generated.ConnectorRolloutStartResponse
import io.airbyte.api.client.model.generated.ConnectorRolloutStrategy
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputStart
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutOutput
import io.temporal.activity.Activity
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import org.slf4j.LoggerFactory
import java.io.IOException

@Singleton
class StartRolloutActivityImpl(private val airbyteApiClient: AirbyteApiClient) : StartRolloutActivity {
  private val log = LoggerFactory.getLogger(StartRolloutActivityImpl::class.java)

  init {
    log.info("Initialized StartRolloutActivityImpl")
  }

  private fun makeFakeRollout(inputStart: ConnectorRolloutActivityInputStart): ConnectorRolloutCreateResponse {
    val client: ConnectorRolloutApi = airbyteApiClient.connectorRolloutApi
    val body =
      ConnectorRolloutCreateRequestBody(
        inputStart.dockerRepository,
        inputStart.dockerImageTag,
        inputStart.actorDefinitionId,
        0,
        0,
        false,
        0,
        null,
      )
    return try {
      client.createConnectorRollout(body)
    } catch (e: IOException) {
      log.info(">>>>>>>>>>   Failed to create fake rollout")
      throw RuntimeException(e)
    }
  }

  override fun startRollout(
    workflowRunId: String,
    input: ConnectorRolloutActivityInputStart,
  ): ConnectorRolloutOutput {
    log.info("Initializing rollout for ${input.dockerRepository}:${input.dockerImageTag}")

    makeFakeRollout(input) // TODO: delete this

    val client: ConnectorRolloutApi = airbyteApiClient.connectorRolloutApi
    val body =
      ConnectorRolloutStartRequestBody(
        input.dockerRepository,
        input.dockerImageTag,
        input.actorDefinitionId,
        workflowRunId,
        ConnectorRolloutStrategy.MANUAL,
      )

    return try {
      val response: ConnectorRolloutStartResponse = client.startConnectorRollout(body)
      ConnectorRolloutActivityHelpers.mapToConnectorRollout(response.data)
    } catch (e: IOException) {
      throw Activity.wrap(e)
    } catch (e: ClientException) {
      throw Activity.wrap(e)
    }
  }
}
