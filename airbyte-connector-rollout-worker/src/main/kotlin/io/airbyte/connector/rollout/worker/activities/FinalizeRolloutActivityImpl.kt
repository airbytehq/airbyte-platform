/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectorRolloutApi
import io.airbyte.api.client.model.generated.ConnectorRolloutFinalizeRequestBody
import io.airbyte.api.client.model.generated.ConnectorRolloutFinalizeResponse
import io.airbyte.api.client.model.generated.ConnectorRolloutStateTerminal
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.connector.rollout.shared.ConnectorRolloutActivityHelpers
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFinalize
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.activity.Activity
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.io.IOException

private val logger = KotlinLogging.logger {}

@Singleton
class FinalizeRolloutActivityImpl(private val airbyteApiClient: AirbyteApiClient) : FinalizeRolloutActivity {
  init {
    logger.info { "Initialized FinalizeRolloutActivityImpl" }
  }

  override fun finalizeRollout(input: ConnectorRolloutActivityInputFinalize): ConnectorRolloutOutput {
    logger.info { "Finalizing rollout for ${input.dockerRepository}:${input.dockerImageTag}" }

    val (state, errorMsg, failureReason) =
      when (input.result) {
        ConnectorRolloutFinalState.SUCCEEDED -> Triple(ConnectorRolloutStateTerminal.SUCCEEDED, null, null)
        ConnectorRolloutFinalState.FAILED_ROLLED_BACK -> Triple(ConnectorRolloutStateTerminal.FAILED_ROLLED_BACK, null, input.failedReason)
        ConnectorRolloutFinalState.CANCELED -> Triple(ConnectorRolloutStateTerminal.CANCELED, input.errorMsg, null)
        else -> throw RuntimeException("Unexpected termination state: ${input.result}")
      }

    val client: ConnectorRolloutApi = airbyteApiClient.connectorRolloutApi
    val body =
      ConnectorRolloutFinalizeRequestBody(
        input.rolloutId,
        state,
        getRolloutStrategyFromInput(input.rolloutStrategy),
        errorMsg,
        failureReason,
        input.updatedBy,
        input.retainPinsOnCancellation,
      )

    return try {
      val response: ConnectorRolloutFinalizeResponse = client.finalizeConnectorRollout(body)
      logger.info { "ConnectorRolloutFinalizeResponse = ${response.data}" }
      ConnectorRolloutActivityHelpers.mapToConnectorRollout(response.data)
    } catch (e: IOException) {
      throw Activity.wrap(e)
    } catch (e: ClientException) {
      handleAirbyteApiClientException(e)
    }
  }
}
