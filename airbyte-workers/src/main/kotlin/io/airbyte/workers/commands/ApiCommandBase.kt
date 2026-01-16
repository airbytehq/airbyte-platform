/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.commands

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.ApiException
import io.airbyte.api.client.model.generated.CancelCommandRequest
import io.airbyte.api.client.model.generated.CommandStatusRequest
import io.airbyte.api.client.model.generated.CommandStatusResponse
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.HttpStatus

private val logger = KotlinLogging.logger {}

abstract class ApiCommandBase<Input>(
  protected val airbyteApiClient: AirbyteApiClient,
) : ConnectorCommand<Input> {
  override fun isTerminal(id: String): Boolean {
    val status = airbyteApiClient.commandApi.getCommandStatus(CommandStatusRequest(id))

    return status.status == CommandStatusResponse.Status.COMPLETED ||
      status.status == CommandStatusResponse.Status.CANCELLED
  }

  override fun cancel(id: String) {
    try {
      airbyteApiClient.commandApi.cancelCommand(
        CancelCommandRequest(id),
      )
    } catch (e: ApiException) {
      when (e.statusCode) {
        HttpStatus.GONE.code -> logger.info { "Command $id already terminal. Cancellation is a no-op." }
        HttpStatus.NOT_FOUND.code -> logger.info { "Command $id not found. Cancellation is a no-op." }
        else -> {
          logger.error(e) { "Command $id failed to be cancelled due to API error. Status code: ${e.statusCode}" }
          throw e
        }
      }
    }
  }
}
