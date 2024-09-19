/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectorRolloutApi
import io.airbyte.api.client.model.generated.ConnectorRolloutReadRequestBody
import io.airbyte.api.client.model.generated.ConnectorRolloutReadResponse
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputGet
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutOutput
import io.temporal.activity.Activity
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import org.slf4j.LoggerFactory
import java.io.IOException

@Singleton
class GetRolloutActivityImpl(private val airbyteApiClient: AirbyteApiClient) : GetRolloutActivity {
  private val log = LoggerFactory.getLogger(GetRolloutActivityImpl::class.java)

  init {
    log.info("Initialized GetRolloutActivityImpl")
  }

  override fun getRollout(input: ConnectorRolloutActivityInputGet): ConnectorRolloutOutput {
    log.info("Getting rollout for ${input.dockerRepository}:${input.dockerImageTag}")

    val client: ConnectorRolloutApi = airbyteApiClient.connectorRolloutApi
    val body = ConnectorRolloutReadRequestBody(input.rolloutId)

    return try {
      val response: ConnectorRolloutReadResponse = client.getConnectorRolloutById(body)
      log.info("ConnectorRolloutReadResponse = ${response.data}")
      ConnectorRolloutActivityHelpers.mapToConnectorRollout(response.data)
    } catch (e: IOException) {
      throw Activity.wrap(e)
    } catch (e: ClientException) {
      throw Activity.wrap(e)
    }
  }
}
