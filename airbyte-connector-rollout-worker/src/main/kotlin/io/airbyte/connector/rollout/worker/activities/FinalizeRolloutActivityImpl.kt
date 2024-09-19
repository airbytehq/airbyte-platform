/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectorRolloutApi
import io.airbyte.api.client.model.generated.ConnectorRolloutFinalizeRequestBody
import io.airbyte.api.client.model.generated.ConnectorRolloutFinalizeResponse
import io.airbyte.api.client.model.generated.ConnectorRolloutStateTerminal
import io.airbyte.api.client.model.generated.ConnectorRolloutStrategy
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputFinalize
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutOutput
import io.temporal.activity.Activity
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import org.slf4j.LoggerFactory
import java.io.IOException

@Singleton
class FinalizeRolloutActivityImpl(private val airbyteApiClient: AirbyteApiClient) : FinalizeRolloutActivity {
  private val log = LoggerFactory.getLogger(FinalizeRolloutActivityImpl::class.java)

  init {
    log.info("Initialized FinalizeRolloutActivityImpl")
  }

  override fun finalizeRollout(input: ConnectorRolloutActivityInputFinalize): ConnectorRolloutOutput {
    log.info("Finalizing rollout for ${input.dockerRepository}:${input.dockerImageTag}")

    val (state, errorMsg, failureReason) =
      when (input.result) {
        ConnectorRolloutFinalState.SUCCEEDED -> Triple(ConnectorRolloutStateTerminal.SUCCEEDED, null, null)
        ConnectorRolloutFinalState.FAILED_ROLLED_BACK -> Triple(ConnectorRolloutStateTerminal.FAILED_ROLLED_BACK, null, null)
        ConnectorRolloutFinalState.CANCELED_ROLLED_BACK -> Triple(ConnectorRolloutStateTerminal.CANCELED_ROLLED_BACK, null, null)
        else -> throw RuntimeException("Unexpected termination state: ${input.result}")
      }

    val client: ConnectorRolloutApi = airbyteApiClient.connectorRolloutApi
    val body =
      ConnectorRolloutFinalizeRequestBody(
        input.rolloutId,
        state,
        ConnectorRolloutStrategy.MANUAL,
        errorMsg,
        failureReason,
      )

    return try {
      val response: ConnectorRolloutFinalizeResponse = client.finalizeConnectorRollout(body)
      log.info("ConnectorRolloutFinalizeResponse = ${response.data}")
      ConnectorRolloutActivityHelpers.mapToConnectorRollout(response.data)
    } catch (e: IOException) {
      throw Activity.wrap(e)
    } catch (e: ClientException) {
      throw Activity.wrap(e)
    }
  }
}
